//! Клиент Schema Registry. Только чтение.
//!
//! Приложение не регистрирует схемы и не меняет subject: контракт топика —
//! общий на всех его потребителей, и завести в нём новую версию из инструмента
//! отладки значит показать свой черновик всему кластеру. Отправить можно тем,
//! что в реестре уже есть; чего там нет — ошибка с текстом, а не молчаливый
//! POST.
//!
//! Разбор ответов вынесен в свободные функции от `&str`: сеть в тестах не
//! поднять, а формат ответа — ровно то, на чём ломаются интеграции с
//! не-конфлюэнтными реализациями (Karapace, Apicurio), и проверять его надо.
//!
//! HTTP блокирующий. Из Tauri-команд он уходит в `spawn_blocking`, из
//! Kafka-воркера зовётся напрямую: тот и так живёт на выделенном потоке.

use std::collections::HashSet;
use std::time::Duration;

use serde::Deserialize;

/// Потолок ожидания ответа.
///
/// Короткий намеренно: по этому пути ходит ОТРИСОВКА ТАБЛИЦЫ — декодеру нужна
/// схема по id из сообщения. Недоступный реестр обязан обернуться строчкой
/// «не смогли», а не подвисшим на полминуты окном. Промах кэшируется, так что
/// платится это ожидание один раз на id, а не на строку.
const TIMEOUT: Duration = Duration::from_secs(5);

/// Сколько уровней ссылок разрешать.
///
/// Ссылки образуют граф, и цикл в нём — не наша забота, а ошибка того, кто его
/// зарегистрировал; но зациклиться на нём мы не имеем права. Десяти уровней
/// хватает любому реальному контракту с запасом.
const MAX_REFERENCE_DEPTH: usize = 10;

pub struct Registry {
    /// Без завершающего слэша: пути ниже дописываются со своим.
    url: String,
    /// Готовый заголовок `Authorization`. Собран один раз при создании: пароль
    /// достаётся из keychain, а macOS на каждое обращение к нему может показать
    /// диалог доступа — делать это на каждый запрос было бы издевательством.
    auth: Option<String>,
    agent: ureq::Agent,
}

/// Схема, как её отдал реестр, вместе со всем, на что она ссылается.
#[derive(Debug, Clone)]
pub struct RegisteredSchema {
    /// Тот самый id, что едет в заголовке сообщения.
    pub id: u32,
    pub schema: String,
    /// Тексты схем, на которые ссылается эта, уже разрешённые рекурсивно.
    pub references: Vec<String>,
}

impl Registry {
    pub fn new(
        url: &str,
        username: Option<&str>,
        password: Option<&str>,
        ca_bundle: Option<&str>,
    ) -> Result<Self, String> {
        let url = url.trim().trim_end_matches('/').to_string();
        if url.is_empty() {
            return Err("schema registry URL is empty".to_string());
        }

        let tls = match ca_bundle.filter(|p| !p.is_empty()) {
            Some(path) => ureq::tls::TlsConfig::builder().root_certs(read_roots(path)?),
            // Системное хранилище, а не подборка Mozilla: корпоративный
            // TLS-инспектор подписывает свой сертификат внутренним CA, которого
            // в webpki-roots нет и не будет, а в систему его ставят
            // централизованно.
            None => ureq::tls::TlsConfig::builder()
                .root_certs(ureq::tls::RootCerts::PlatformVerifier),
        };

        let config = ureq::Agent::config_builder()
            .timeout_global(Some(TIMEOUT))
            // Ответ 404 на «схемы с таким id нет» — это не транспортная ошибка,
            // а осмысленный ответ, и рассказать о нём надо своими словами. См.
            // `call`.
            .http_status_as_error(false)
            .tls_config(tls.build())
            .build();

        Ok(Self {
            url,
            auth: username
                .filter(|u| !u.is_empty())
                .map(|u| format!("Basic {}", base64(&format!("{u}:{}", password.unwrap_or(""))))),
            agent: config.into(),
        })
    }

    /// Список subject — содержимое выпадающего списка в настройках топика.
    pub fn subjects(&self) -> Result<Vec<String>, String> {
        let body = self.call("/subjects")?;
        serde_json::from_str(&body).map_err(|e| format!("unexpected /subjects response: {e}"))
    }

    /// Есть ли в реестре такой subject.
    ///
    /// Точечный вопрос, а не поиск по `/subjects`: спрашивают его ради
    /// автоопределения формата, то есть на каждое открытие топика, а список
    /// subject'ов корпоративного реестра — это тысячи имён в одном ответе.
    ///
    /// `Ok(false)` — реестр ответил «нет такого», и это ответ; `Err` — не
    /// ответил вовсе, и делать из молчания вывод «топик не avro-шный» нельзя.
    pub fn has_subject(&self, subject: &str) -> Result<bool, String> {
        let path = format!("/subjects/{}/versions", escape(subject));
        let (status, body) = self.get(&path)?;
        match status {
            200..=299 => Ok(true),
            404 => Ok(false),
            _ => Err(match registry_error(&body) {
                Some(message) => format!("schema registry says: {message} (HTTP {status})"),
                None => format!("schema registry answered HTTP {status} to {path}"),
            }),
        }
    }

    /// Версии одного subject, по возрастанию.
    pub fn versions(&self, subject: &str) -> Result<Vec<i32>, String> {
        let body = self.call(&format!("/subjects/{}/versions", escape(subject)))?;
        let mut versions: Vec<i32> = serde_json::from_str(&body)
            .map_err(|e| format!("unexpected versions response for {subject}: {e}"))?;
        versions.sort_unstable();
        Ok(versions)
    }

    /// Схема subject: конкретной версии или последней.
    pub fn by_subject(
        &self,
        subject: &str,
        version: Option<i32>,
    ) -> Result<RegisteredSchema, String> {
        let which = version.map_or_else(|| "latest".to_string(), |v| v.to_string());
        let body = self.call(&format!(
            "/subjects/{}/versions/{which}",
            escape(subject)
        ))?;
        let parsed = parse_schema(&body)
            .map_err(|e| format!("unexpected response for subject {subject}: {e}"))?;
        self.assemble(parsed, 0)
    }

    /// Схема по id из заголовка сообщения.
    pub fn by_id(&self, id: u32) -> Result<RegisteredSchema, String> {
        let body = self.call(&format!("/schemas/ids/{id}"))?;
        let mut parsed =
            parse_schema(&body).map_err(|e| format!("unexpected response for schema {id}: {e}"))?;
        // Ответ по id самого id не содержит — он и так был в запросе.
        parsed.id = Some(id);
        self.assemble(parsed, 0)
    }

    /// Догружает всё, на что ссылается схема, и складывает в один набор.
    ///
    /// Порядок не важен: `Schema::parse_list` разрешает перекрёстные ссылки
    /// сам, ему нужен весь набор, а не его топологическая сортировка.
    fn assemble(&self, parsed: ParsedSchema, depth: usize) -> Result<RegisteredSchema, String> {
        if let Some(kind) = parsed.schema_type.as_deref() {
            // Реестр общий на все форматы: в нём вполне может лежать protobuf
            // или JSON Schema. Разбирать такое как Avro — гарантированный
            // мусор, поэтому лучше сказать прямо.
            if !kind.eq_ignore_ascii_case("AVRO") {
                return Err(format!(
                    "the registry says this schema is {kind}, not AVRO"
                ));
            }
        }

        let mut references = Vec::new();
        let mut seen = HashSet::new();
        self.collect_references(&parsed.references, depth, &mut seen, &mut references)?;

        Ok(RegisteredSchema {
            id: parsed.id.unwrap_or_default(),
            schema: parsed.schema,
            references,
        })
    }

    fn collect_references(
        &self,
        refs: &[Reference],
        depth: usize,
        seen: &mut HashSet<String>,
        out: &mut Vec<String>,
    ) -> Result<(), String> {
        if refs.is_empty() {
            return Ok(());
        }
        if depth >= MAX_REFERENCE_DEPTH {
            return Err(format!(
                "schema references are nested deeper than {MAX_REFERENCE_DEPTH} levels; \
                 most likely they form a cycle"
            ));
        }

        for reference in refs {
            let key = format!("{}/{}", reference.subject, reference.version);
            if !seen.insert(key) {
                continue;
            }
            let body = self.call(&format!(
                "/subjects/{}/versions/{}",
                escape(&reference.subject),
                reference.version
            ))?;
            let parsed = parse_schema(&body).map_err(|e| {
                format!(
                    "unexpected response for referenced schema {}: {e}",
                    reference.name
                )
            })?;
            self.collect_references(&parsed.references, depth + 1, seen, out)?;
            out.push(parsed.schema);
        }
        Ok(())
    }

    /// GET по пути реестра.
    ///
    /// Ошибку статуса разбираем сами: у реестра тело ответа несёт `message` с
    /// человеческим объяснением («Subject not found»), и заменять его на «http
    /// status: 404» значило бы выбросить единственное полезное, что там было.
    fn call(&self, path: &str) -> Result<String, String> {
        let (status, body) = self.get(path)?;
        if !(200..300).contains(&status) {
            return Err(match registry_error(&body) {
                Some(message) => format!("schema registry says: {message} (HTTP {status})"),
                None => format!("schema registry answered HTTP {status} to {path}"),
            });
        }
        Ok(body)
    }

    /// Тот же GET, но со статусом наружу.
    ///
    /// Нужен там, где статус — часть ответа, а не сбой: 404 на вопрос «есть ли
    /// такой subject» это законное «нет», и превращать его в ошибку значило бы
    /// заставлять вызывающего разбирать её текст обратно.
    fn get(&self, path: &str) -> Result<(u16, String), String> {
        let url = format!("{}{path}", self.url);
        let mut request = self
            .agent
            .get(&url)
            // Вежливость к реализациям, которые по умолчанию отдают старый
            // формат. Confluent понимает и без него, Karapace с Apicurio — тоже.
            .header("Accept", "application/vnd.schemaregistry.v1+json, application/json");
        if let Some(auth) = &self.auth {
            request = request.header("Authorization", auth);
        }

        let mut response = request
            .call()
            .map_err(|e| format!("can't reach the schema registry at {url}: {e}"))?;
        let status = response.status().as_u16();
        let body = response
            .body_mut()
            .read_to_string()
            .map_err(|e| format!("can't read the schema registry response from {url}: {e}"))?;
        Ok((status, body))
    }
}

/// Ответ реестра про одну схему.
///
/// `schema` приезжает СТРОКОЙ с JSON внутри, а не вложенным объектом — так
/// устроен протокол, и это удобно: ровно строку и ждёт `Schema::parse_str`.
#[derive(Debug, Deserialize)]
struct ParsedSchema {
    #[serde(default)]
    id: Option<u32>,
    schema: String,
    /// Отсутствует у Confluent, когда схема — Avro: это значение по умолчанию.
    #[serde(rename = "schemaType", default)]
    schema_type: Option<String>,
    #[serde(default)]
    references: Vec<Reference>,
}

/// Ссылка на другую схему. `name` — то имя, под которым на неё ссылаются
/// внутри текста схемы; `subject` и `version` — где её взять.
#[derive(Debug, Deserialize)]
struct Reference {
    #[serde(default)]
    name: String,
    subject: String,
    version: i32,
}

fn parse_schema(body: &str) -> Result<ParsedSchema, String> {
    serde_json::from_str(body).map_err(|e| e.to_string())
}

/// Человеческое объяснение из тела ошибки, если оно там есть.
fn registry_error(body: &str) -> Option<String> {
    #[derive(Deserialize)]
    struct ErrorBody {
        message: Option<String>,
    }
    serde_json::from_str::<ErrorBody>(body)
        .ok()
        .and_then(|e| e.message)
        .filter(|m| !m.is_empty())
}

/// Экранирует то, что уходит в путь URL.
///
/// Имя subject задаёт не приложение, а кластер, и в нём встречается что угодно
/// — от слэша до кириллицы. Тащить ради этого крейт незачем: набор безопасных
/// символов известен, всё остальное едет процентами.
fn escape(segment: &str) -> String {
    let mut out = String::with_capacity(segment.len());
    for byte in segment.as_bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(*byte as char)
            }
            other => out.push_str(&format!("%{other:02X}")),
        }
    }
    out
}

/// Base64 для заголовка `Authorization`.
///
/// Своя реализация вместо крейта: единственное место применения, двадцать
/// строк, и никакого выбора алфавита — basic auth знает ровно один.
fn base64(input: &str) -> String {
    const ALPHABET: &[u8; 64] =
        b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let bytes = input.as_bytes();
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);

    for chunk in bytes.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let triple = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        for i in 0..4 {
            // Символы, под которые в исходных данных не было байт, — это
            // padding: их место занимает '='.
            if i <= chunk.len() {
                out.push(ALPHABET[((triple >> (18 - 6 * i)) & 0x3f) as usize] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

/// Читает PEM-бандл и оставляет из него сертификаты.
fn read_roots(path: &str) -> Result<ureq::tls::RootCerts, String> {
    let pem = std::fs::read(path).map_err(|e| format!("can't read CA bundle {path}: {e}"))?;
    let mut certs = Vec::new();
    for item in ureq::tls::parse_pem(&pem) {
        match item {
            Ok(ureq::tls::PemItem::Certificate(cert)) => certs.push(cert),
            // Приватный ключ в бандле корней — не наше дело, но и не ошибка:
            // такие файлы собирают конкатенацией и кладут в них лишнее.
            Ok(_) => {}
            Err(e) => return Err(format!("can't parse CA bundle {path}: {e}")),
        }
    }
    if certs.is_empty() {
        return Err(format!("{path} contains no certificates"));
    }
    Ok(ureq::tls::RootCerts::Specific(std::sync::Arc::new(certs)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_auth_matches_the_rfc_examples() {
        assert_eq!(base64("Aladdin:open sesame"), "QWxhZGRpbjpvcGVuIHNlc2FtZQ==");
        assert_eq!(base64(""), "");
        assert_eq!(base64("a"), "YQ==");
        assert_eq!(base64("ab"), "YWI=");
        assert_eq!(base64("abc"), "YWJj");
        assert_eq!(base64("abcd"), "YWJjZA==");
    }

    #[test]
    fn subject_names_are_escaped_into_the_path() {
        assert_eq!(escape("orders-value"), "orders-value");
        assert_eq!(escape("a/b"), "a%2Fb");
        assert_eq!(escape("тема"), "%D1%82%D0%B5%D0%BC%D0%B0");
    }

    /// Обычный ответ Confluent на `/subjects/x/versions/latest`: схема строкой,
    /// `schemaType` отсутствует, потому что Avro — значение по умолчанию.
    #[test]
    fn a_plain_avro_response_parses() {
        let body = r#"{
            "subject": "orders-value", "version": 3, "id": 41,
            "schema": "{\"type\":\"record\",\"name\":\"Order\",\"fields\":[]}"
        }"#;
        let parsed = parse_schema(body).unwrap();
        assert_eq!(parsed.id, Some(41));
        assert_eq!(parsed.schema_type, None);
        assert!(parsed.references.is_empty());
        assert!(parsed.schema.contains("\"name\":\"Order\""));
    }

    #[test]
    fn references_are_read_with_their_subject_and_version() {
        let body = r#"{
            "id": 7,
            "schema": "{\"type\":\"record\",\"name\":\"Order\",\"fields\":[{\"name\":\"total\",\"type\":\"common.Money\"}]}",
            "references": [{"name": "common.Money", "subject": "money-value", "version": 2}]
        }"#;
        let parsed = parse_schema(body).unwrap();
        assert_eq!(parsed.references.len(), 1);
        assert_eq!(parsed.references[0].subject, "money-value");
        assert_eq!(parsed.references[0].version, 2);
        assert_eq!(parsed.references[0].name, "common.Money");
    }

    /// Реестр общий на все форматы, и protobuf-схему надо отвергнуть словами,
    /// а не попыткой разобрать её как Avro.
    #[test]
    fn a_non_avro_schema_type_is_visible_in_the_parsed_response() {
        let body = r#"{"id": 1, "schemaType": "PROTOBUF", "schema": "syntax = \"proto3\";"}"#;
        assert_eq!(parse_schema(body).unwrap().schema_type.as_deref(), Some("PROTOBUF"));
    }

    #[test]
    fn the_human_message_is_pulled_out_of_an_error_body() {
        let body = r#"{"error_code": 40401, "message": "Subject 'nope' not found."}"#;
        assert_eq!(
            registry_error(body).as_deref(),
            Some("Subject 'nope' not found.")
        );
        // Не JSON и JSON без `message` — не повод падать: вызывающий покажет
        // тогда просто код статуса.
        assert_eq!(registry_error("<html>502</html>"), None);
        assert_eq!(registry_error("{}"), None);
    }

    #[test]
    fn an_empty_url_is_refused_before_any_request() {
        assert!(Registry::new("   ", None, None, None).is_err());
    }

    // --- Проверка по настоящему HTTP ------------------------------------------
    //
    // Разбор ответов проверяется выше на строках, но между ним и реестром лежит
    // всё то, на чём интеграции и ломаются: путь запроса, заголовок
    // авторизации, разрешение `references` вторым запросом. Ни одно из этого не
    // видно на фикстурах, поэтому здесь поднимается настоящий сервер на
    // loopback — он отвечает заготовленным и заодно ЗАПОМИНАЕТ, что у него
    // спросили.

    use std::io::{BufRead, BufReader, Write};
    use std::net::TcpListener;
    use std::sync::mpsc;

    /// Что сервер увидел: путь и заголовок авторизации.
    type Seen = (String, Option<String>);

    /// Поднимает сервер, отвечающий по таблице «путь → тело», и отдаёт его
    /// адрес вместе с каналом, куда падают полученные запросы.
    fn serve(routes: Vec<(&'static str, &'static str)>, count: usize) -> (String, mpsc::Receiver<Seen>) {
        // Порт 0 — «любой свободный»: занимать фиксированный в тестах нельзя.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let (tx, rx) = mpsc::channel();

        std::thread::spawn(move || {
            for _ in 0..count {
                let Ok((stream, _)) = listener.accept() else {
                    return;
                };
                let mut reader = BufReader::new(&stream);
                let mut request = String::new();
                reader.read_line(&mut request).unwrap();
                let path = request.split_whitespace().nth(1).unwrap_or("").to_string();

                // Заголовки до пустой строки: тела у GET нет.
                let mut auth = None;
                loop {
                    let mut line = String::new();
                    if reader.read_line(&mut line).unwrap_or(0) == 0 || line.trim().is_empty() {
                        break;
                    }
                    // Имена заголовков регистронезависимы, и клиенты пишут их
                    // по-разному: ureq шлёт их в нижнем регистре.
                    if let Some((name, value)) = line.split_once(':') {
                        if name.eq_ignore_ascii_case("authorization") {
                            auth = Some(value.trim().to_string());
                        }
                    }
                }
                let _ = tx.send((path.clone(), auth));

                let body = routes
                    .iter()
                    .find(|(route, _)| *route == path)
                    .map(|(_, body)| *body);
                let mut stream = &stream;
                let response = match body {
                    Some(body) => format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    ),
                    None => "HTTP/1.1 404 Not Found\r\nContent-Length: 47\r\nConnection: close\r\n\r\n{\"error_code\":40401,\"message\":\"Not found here\"}".to_string(),
                };
                let _ = stream.write_all(response.as_bytes());
                let _ = stream.flush();
            }
        });

        (url, rx)
    }

    const MONEY: &str = r#"{"type":"record","name":"Money","namespace":"common","fields":[{"name":"amount","type":"long"}]}"#;

    #[test]
    fn subjects_are_fetched_with_basic_auth() {
        let (url, seen) = serve(vec![("/subjects", r#"["orders-value","money-value"]"#)], 1);
        let registry = Registry::new(&url, Some("alice"), Some("secret"), None).unwrap();

        assert_eq!(registry.subjects().unwrap(), ["orders-value", "money-value"]);

        let (path, auth) = seen.recv().unwrap();
        assert_eq!(path, "/subjects");
        // base64("alice:secret") — иначе реестр за basic auth просто отказал бы.
        assert_eq!(auth.as_deref(), Some("Basic YWxpY2U6c2VjcmV0"));
    }

    /// Реестр без авторизации: заголовка быть НЕ должно. Пустой `Authorization`
    /// некоторые реализации считают неудачной попыткой входа и отвечают 401.
    #[test]
    fn a_registry_without_credentials_sends_no_authorization_header() {
        let (url, seen) = serve(vec![("/subjects", "[]")], 1);
        let registry = Registry::new(&url, None, None, None).unwrap();

        assert!(registry.subjects().unwrap().is_empty());
        assert_eq!(seen.recv().unwrap().1, None);
    }

    /// Главное, чего не видно на фикстурах: ссылка догружается ВТОРЫМ запросом,
    /// по своему subject и версии.
    #[test]
    fn references_are_resolved_by_a_second_request() {
        let (url, seen) = serve(
            vec![
                (
                    "/schemas/ids/7",
                    r#"{"schema":"{\"type\":\"record\",\"name\":\"Order\",\"namespace\":\"orders\",\"fields\":[{\"name\":\"total\",\"type\":\"common.Money\"}]}","references":[{"name":"common.Money","subject":"money-value","version":2}]}"#,
                ),
                (
                    "/subjects/money-value/versions/2",
                    r#"{"id":4,"version":2,"schema":"{\"type\":\"record\",\"name\":\"Money\",\"namespace\":\"common\",\"fields\":[{\"name\":\"amount\",\"type\":\"long\"}]}"}"#,
                ),
            ],
            2,
        );
        let registry = Registry::new(&url, None, None, None).unwrap();

        let fetched = registry.by_id(7).unwrap();
        assert_eq!(fetched.id, 7, "id берётся из запроса — в ответе его нет");
        assert_eq!(fetched.references.len(), 1);
        assert!(fetched.references[0].contains("Money"));

        assert_eq!(seen.recv().unwrap().0, "/schemas/ids/7");
        assert_eq!(seen.recv().unwrap().0, "/subjects/money-value/versions/2");

        // И собранного хватает, чтобы схема связалась: ради этого ссылки и
        // догружались.
        let linked = crate::schema::avro::Linked::parse_with_refs(
            &fetched.schema,
            &fetched.references,
        )
        .unwrap();
        assert_eq!(linked.root_name().as_deref(), Some("orders.Order"));
    }

    /// Сквозная проверка того, ради чего весь модуль: сообщение в
    /// confluent-формате разбирается схемой, добытой по id из его заголовка.
    #[test]
    fn a_confluent_message_is_decoded_through_the_registry() {
        let body = format!(r#"{{"id":11,"schema":{}}}"#, serde_json::to_string(MONEY).unwrap());
        let body: &'static str = Box::leak(body.into_boxed_str());
        let (url, _seen) = serve(vec![("/schemas/ids/11", body)], 1);

        let registry = std::sync::Arc::new(Registry::new(&url, None, None, None).unwrap());
        let decoder = crate::schema::avro::AvroDecoder::new(
            // Без каталога настроек: дисковый кэш здесь ни при чём, проверяем
            // именно поход в реестр.
            None,
            None,
            Some((url.clone(), registry)),
        );

        // Тело: заголовок confluent (маркер и id 11) плюс datum с amount = 5.
        let linked = crate::schema::avro::Linked::parse_files(&[MONEY.to_string()], None).unwrap();
        let datum = linked
            .encode(serde_json::from_str(r#"{"amount": 5}"#).unwrap())
            .unwrap();
        let mut framed = vec![0x00, 0x00, 0x00, 0x00, 0x0b];
        framed.extend_from_slice(&datum);

        assert_eq!(decoder.decode(&framed).unwrap(), r#"{"amount":5}"#);

        // Второй раз в сеть не ходим: сервер отвечает ровно на один запрос, и
        // без кэша это упало бы.
        assert_eq!(decoder.decode(&framed).unwrap(), r#"{"amount":5}"#);
    }

    /// Реестр ответил отказом: показать надо его слова, а не «http status 404».
    #[test]
    fn an_error_body_from_the_registry_reaches_the_caller() {
        let (url, _seen) = serve(vec![], 1);
        let registry = Registry::new(&url, None, None, None).unwrap();

        let error = registry.by_id(99).unwrap_err();
        assert!(error.contains("Not found here"), "{error}");
    }
}
