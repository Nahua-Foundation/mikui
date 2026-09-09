use std::time::Duration;

use crate::kafka::ClusterConnectPayload;
use rdkafka::ClientConfig;

/// Механизмы SASL, которые умеет эта сборка.
///
/// Список закрытый, и оба отсутствующих в нём механизма отсутствуют по делу.
///
/// **GSSAPI** требует Cyrus SASL (`sasl2-sys`), который не собирается под
/// Windows, — то есть пункт меню стоил бы кроссплатформенности.
///
/// **OAUTHBEARER** librdkafka без посторонней помощи не умеет вовсе: токен она
/// ждёт от приложения через `oauthbearer_token_refresh_cb`, rdkafka-rust ставит
/// этот колбэк только при `ClientContext::ENABLE_REFRESH_OAUTH_TOKEN`, а
/// встроенный OIDC-путь (`sasl.oauthbearer.method=oidc`) собирается лишь с
/// libcurl, которого здесь нет (rdkafka-sys без фичи `curl` конфигурирует
/// librdkafka с `--disable-curl`). Без всего этого librdkafka просто кладёт в
/// очередь просьбу выдать токен, обрабатывать её некому, и подключение молча
/// висит до таймаута. Пункт в меню обещал бы то, чего не бывает.
///
/// Тот же список продублирован в `ClusterConfigModal.tsx`, где наполняет
/// выпадашку. Расходиться им нельзя: здесь проверка, там меню.
pub const SASL_MECHANISMS: [&str; 3] = ["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"];

/// Механизм для записи, в которой он не указан.
///
/// Такие записи есть: `sasl_mechanism` появился в `clusters.json` не сразу.
/// Значение должно совпадать с начальным в форме подключения — иначе форма
/// показывала бы один механизм, а подключение шло бы другим.
pub const DEFAULT_SASL_MECHANISM: &str = "SCRAM-SHA-512";

/// Общая часть конфигурации: адреса, сеть, безопасность.
///
/// Отдельно от `consumer_config` потому, что отправка сообщения поднимает
/// продьюсера на том же подключении, а консьюмерские настройки (`fetch.*`,
/// `queued.*`, `auto.offset.reset`) для него не значат ничего: librdkafka
/// сложит их в конфиг, увидит чужой scope и на каждую напишет предупреждение.
/// Держать «куда и под кем подключаемся» в одном месте, а «читаем или пишем» —
/// в другом, заодно избавляет от риска, что продьюсер уедет мимо TLS.
pub fn base_config(conf: &ClusterConnectPayload) -> Result<ClientConfig, String> {
    let mut cc = ClientConfig::new();

    cc.set("bootstrap.servers", &conf.brokers);
    cc.set("client.id", "mikui");

    // group.id намеренно не задаём. Чтение идёт через legacy simple consumer
    // API (см. kafka::raw_consumer), а не через assign()/subscribe(): любой
    // непустой group.id заставляет librdkafka поднять объект консьюмер-группы
    // (cgrp), который в фоне пытается найти координатора — а это уже требует
    // ACL на саму группу. На кластерах со строгой ACL-политикой такой доступ
    // выдаётся точечно на конкретную строку и не обобщается ни на какую
    // придуманную "на лету" (проверено на реальном кластере: конкретный
    // топик падает с GroupAuthorizationFailed даже с group.id от другого,
    // рабочего проекта). Без group.id в конфиге cgrp не создаётся вообще —
    // группа не участвует в чтении никак, и это не костыль, а единственный
    // группонезависимый путь.

    // --- Метаданные -------------------------------------------------------
    // Было 10_000: метаданные всего кластера перезапрашивались каждые 10 секунд.
    // На кластере с тысячами топиков это мегабайты трафика в минуту на простое.
    cc.set("metadata.max.age.ms", "300000");

    // --- Сеть -------------------------------------------------------------
    // Таймаут запроса. Для консьюмера librdkafka считает его как
    // `fetch.wait.max.ms + socket.timeout.ms` (CONFIGURATION.md), то есть
    // здесь — 30.5 секунды.
    //
    // Раньше стояло 10000 «чтобы не висеть минуту», и это оказалось ровно той
    // настройкой, из-за которой чтение и висело. Kafka ограничивает задержку
    // по квоте окном `quota.window.num × quota.window.size.seconds` — по
    // умолчанию около 11 секунд. Наш таймаут получался 10.5 с, то есть чуть
    // НИЖЕ максимальной задержки брокера: стоило тому придержать ответ близко
    // к пределу, как срабатывал наш таймаут, запрос выбрасывался и посылался
    // заново. Десять с половиной секунд впустую — и так по кругу; в логах это
    // выглядело как раунды ровно по 10.4 секунды независимо от того, сколько
    // данных в них было (6.9 МБ и 0.67 МБ занимали одинаково).
    //
    // Побочный эффект был и в диагностике: `throttle_time_ms` едет в том самом
    // ответе, который мы не дожидались, поэтому большие задержки в статистику
    // не попадали вовсе и максимум застревал на пределе, укладывавшемся в
    // таймаут.
    //
    // Запас берём с трёхкратным перекрытием квотного окна. Зависнуть надолго
    // это не даёт: чтение всё равно ограничено `READ_DEADLINE`, а воркер
    // остаётся отзывчивым, потому что шаги кооперативные.
    cc.set("socket.timeout.ms", "30000");
    // session.timeout.ms не задаём: без group.id никакого heartbeat-потока
    // и координатора вообще нет, параметр просто не применяется.
    // Мелкие запросы уходят сразу.
    cc.set("socket.nagle.disable", "true");

    // Было 30_000: каждые 30 секунд простоя соединение рвалось, и следующий
    // запрос платил заново за TCP + TLS handshake. Держим чуть меньше
    // брокерского дефолта (600_000), чтобы закрывать первыми и без гонки.
    cc.set("connections.max.idle.ms", "540000");
    cc.set("reconnect.backoff.ms", "300");
    cc.set("reconnect.backoff.max.ms", "10000");

    apply_security(&mut cc, conf)?;

    Ok(cc)
}

/// Консьюмерская часть поверх уже настроенного подключения.
///
/// Берёт готовый `base_config` по той же причине, по которой его берёт
/// `producer_config`: подключение настраивается ровно один раз, и второй разбор
/// того же payload означал бы второе место, где безопасность может разъехаться.
pub fn consumer_config(base: &ClientConfig) -> ClientConfig {
    let mut cc = base.clone();

    // --- Fetch ------------------------------------------------------------
    // Пара (min.bytes=1, wait.max.ms=500) — дефолт librdkafka, и она лучше
    // прежней (min.bytes=1MB, wait.max.ms=50) сразу по двум осям:
    //   * быстрее: брокер отвечает немедленно, как только есть хоть какие-то
    //     данные, а не ждёт накопления мегабайта;
    //   * дешевле: на пустом топике опрос раз в 500 мс вместо 20 раз в секунду.
    cc.set("fetch.min.bytes", "1");
    cc.set("fetch.wait.max.ms", "500");
    // Было 50 МБ — слишком крупный разовый спайк памяти на ответ.
    cc.set("fetch.max.bytes", "10485760");
    // Потолок НА ПАРТИЦИЮ в одном ответе брокера. Дефолт (1 МБ) рассчитан на
    // потоковое чтение, а мы читаем окнами: набрали своё окно — и уходим
    // читать другой диапазон, оплатив при этом весь присланный мегабайт.
    // На кластере с квотой это прямые потери: лишний мегабайт на партицию —
    // данные, за которые квота списалась, а мы их выбросили.
    //
    // Было 64 КБ, и это оказалось слишком туго. Измеритель показал реальные
    // тела в этих топиках: 20–70 КБ на сообщение. При лимите в 64 КБ один
    // ответ брокера приносил одно-два сообщения, и окно в десяток офсетов
    // превращалось в десятки последовательных round-trip'ов. 256 КБ дают
    // 4–12 сообщений даже на самых крупных телах и сотни на мелких, оставаясь
    // вчетверо ниже дефолтного мегабайта. Сообщение крупнее лимита librdkafka
    // дотянет сама, постепенно поднимая его для этой партиции.
    cc.set("fetch.message.max.bytes", "262144");

    // --- Буферы потребителя ----------------------------------------------
    // queued.min.messages считается НА ПАРТИЦИЮ. Прежняя 1000 на топике
    // со 100 партициями означала до 100k сообщений в локальной очереди.
    // Здесь же — верхняя граница предвыборки: всё, что librdkafka успела
    // натаскать сверх окна раунда, оплачено квотой и выброшено.
    //
    // ПРОВЕРЕНО И ОТКАЧЕНО: 10 (и 1024 КБ) вместо 100/16384. Гипотеза была,
    // что раунды в 11 секунд держит именно предвыборка: 100 сообщений × 8
    // партиций × ~11.5 КБ = 9.2 МБ, которые библиотека тянет независимо от
    // размера окна. Численно это сходилось, и время раунда действительно не
    // зависело от размера окна. Но замер на боевом кластере гипотезу не
    // подтвердил: предвыборка ужалась в десять раз, а доля выброшенного
    // осталась прежней (waste 3.3–4.4× против 3.2–4.1×) — значит лишний
    // трафик приходит не отсюда. Худшая пауза при этом сократилась с 11.4 до
    // 7.5 с, но время до первых строк выросло с 2.0 до 3.4 с, а суммарная
    // пропускная способность не изменилась (36.6 → 39.4 сообщений/с, шум).
    // Менять эти числа снова имеет смысл только вместе с ответом на вопрос,
    // откуда берётся четырёхкратный перерасход трафика.
    cc.set("queued.min.messages", "100");
    cc.set("queued.max.messages.kbytes", "16384");

    // --- Офсеты -----------------------------------------------------------
    // Просмотрщику незачем создавать топики побочным эффектом.
    cc.set("allow.auto.create.topics", "false");
    // Нужно, чтобы понимать «партиция вычитана до конца» и прекращать опрос,
    // а не крутиться вхолостую.
    cc.set("enable.partition.eof", "true");
    // enable.auto.offset.store не нужен: коммитить некуда (нет группы).
    //
    // auto.offset.reset — НЕ про группы и коммиты, это ошибка в более раннем
    // варианте этого комментария. Он общий для клиента: срабатывает всегда,
    // когда запрошенный офсет оказался вне диапазона [low, high) партиции —
    // в том числе для офсетов, которые мы сами явно задаём в
    // consume_start_queue(). Воркер режет партицию на окна по watermarks
    // (см. kafka::worker), а retention тем временем двигает low вперёд: окно,
    // нарезанное секунду назад, может уже начинаться до начала лога. С
    // дефолтом (`latest`) librdkafka в такой ситуации молча откатывает курсор
    // на КОНЕЦ партиции, и та уходит в EOF с нулём прочитанных сообщений, хотя
    // данные есть (воспроизведено и залогировано на реальном кластере).
    // "earliest" откатывает на начало — то есть отдаёт всё, что от окна
    // реально уцелело.
    cc.set("auto.offset.reset", "earliest");

    cc
}

/// Конфигурация продьюсера поверх уже настроенного подключения.
///
/// Берёт готовый `base_config`, а не payload: пароль сохранённой учётки живёт
/// в keychain и подставляется один раз, при подключении. Второй раз ходить за
/// ним ради отправки одного сообщения незачем.
pub fn producer_config(base: &ClientConfig) -> ClientConfig {
    let mut cc = base.clone();

    // Java-совместимый хэш ключа. Дефолт librdkafka (`consistent_random`)
    // считает CRC32 и раскладывает ключи ИНАЧЕ, чем штатный продьюсер сервиса:
    // тестовое сообщение легло бы не в ту партицию, в которую ходят боевые, и
    // весь смысл отправки «как настоящее» пропал бы. Оба варианта одинаково
    // кладут сообщение без ключа в случайную партицию.
    cc.set("partitioner", "murmur2_random");

    // Подтверждение от ВСЕЙ ISR. Просмотрщик отправляет по одному сообщению
    // руками и сразу показывает партицию с офсетом — обещать это, не дождавшись
    // реплик, значит показать офсет, который может и не пережить смену лидера.
    cc.set("acks", "all");

    // Столько librdkafka пытается доставить сообщение, прежде чем сдаться. Та же
    // константа — потолок ожидания в `Worker::produce`: показать «отправлено»
    // без отчёта о доставке нельзя, а ждать дольше библиотеки бессмысленно.
    cc.set("message.timeout.ms", PRODUCE_TIMEOUT.as_millis().to_string());

    // Дефолт librdkafka — 5 мс накопления батча. Батчить тут нечего: сообщение
    // ровно одно, и эти миллисекунды были бы чистой задержкой ответа.
    cc.set("linger.ms", "0");

    // enable.idempotence НЕ включаем: она требует отдельного права
    // IdempotentWrite на кластере, которого у учётки для ручной отправки
    // обычно нет, — а без него продьюсер не создастся вовсе.

    cc
}

/// Потолок доставки одного сообщения — общий для librdkafka и для собственного
/// ожидания отчёта в `Worker::produce`. Разъехаться этим двум числам нельзя:
/// меньшее из них решало бы за большее, и либо воркер сдавался бы раньше
/// библиотеки, либо ждал впустую после того, как та уже всё бросила.
pub const PRODUCE_TIMEOUT: Duration = Duration::from_secs(30);

fn apply_security(cc: &mut ClientConfig, conf: &ClusterConnectPayload) -> Result<(), String> {
    match conf.security_protocol.trim().to_ascii_uppercase().as_str() {
        "PLAINTEXT" => {
            cc.set("security.protocol", "plaintext");
        }
        "SSL" => {
            cc.set("security.protocol", "ssl");
            apply_tls(cc, conf)?;
        }
        "SASL_PLAINTEXT" => {
            cc.set("security.protocol", "sasl_plaintext");
            cc.set("sasl.mechanism", mechanism(conf)?);
            apply_credentials(cc, conf);
        }
        "SASL_SSL" => {
            cc.set("security.protocol", "sasl_ssl");
            cc.set("sasl.mechanism", mechanism(conf)?);
            apply_tls(cc, conf)?;
            apply_credentials(cc, conf);
        }
        // Раньше сюда падало всё незнакомое, и подключение молча уходило
        // открытым текстом: опечатка в `clusters.json` или запись, сделанная
        // более новой сборкой, давала не отказ, а соединение не по тому
        // протоколу, о котором просили. Если брокер слушает PLAINTEXT-порт,
        // такое ещё и удаётся.
        other => {
            return Err(format!(
                "unknown security protocol \"{other}\": expected PLAINTEXT, SSL, \
                 SASL_PLAINTEXT or SASL_SSL"
            ))
        }
    }
    Ok(())
}

/// Механизм SASL: тот, что назван, либо умолчание для записей без него.
///
/// Незнакомый отвергаем, а не отдаём librdkafka: она ответит на него отказом в
/// создании клиента, но текстом про свой список механизмов, в котором есть и
/// те, что эта сборка не поддерживает. Сообщение про «this build» точнее.
/// Отдельно это важно для записей со времён, когда в меню были GSSAPI и
/// OAUTHBEARER: подключение с ними всё равно не работало.
fn mechanism(conf: &ClusterConnectPayload) -> Result<String, String> {
    let named = conf
        .sasl_mechanism
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());
    let Some(named) = named else {
        return Ok(DEFAULT_SASL_MECHANISM.to_string());
    };

    // Регистр librdkafka не важен, но приводим к своему: так значение в конфиге
    // совпадает с тем, что показано в форме.
    let upper = named.to_ascii_uppercase();
    if SASL_MECHANISMS.contains(&upper.as_str()) {
        Ok(upper)
    } else {
        Err(format!(
            "unsupported SASL mechanism \"{named}\": this build supports {}",
            SASL_MECHANISMS.join(", ")
        ))
    }
}

fn apply_credentials(cc: &mut ClientConfig, conf: &ClusterConnectPayload) {
    if let Some(user) = &conf.username {
        cc.set("sasl.username", user);
    }
    // Пароль намеренно не тримится: пробелы по краям — это часть пароля.
    if let Some(pwd) = &conf.password {
        cc.set("sasl.password", pwd);
    }
}

/// TLS: чем проверяем брокера, чем представляемся сами и от каких проверок
/// отказываемся.
fn apply_tls(cc: &mut ClientConfig, conf: &ClusterConnectPayload) -> Result<(), String> {
    // Раньше здесь всё было закомментировано, из-за чего режим SSL молча не
    // работал: CA-бандл из UI никуда не доезжал.
    //
    // Пустое значение не ставим вовсе, и это не то же самое, что поставить
    // пустую строку: без `ssl.ca.location` librdkafka ищет корни сама —
    // на macOS подставляет `probe`, на Windows читает Root store, на Linux при
    // статически слинкованном OpenSSL прощупывает стандартные пути. Публично
    // подписанному кластеру свой бандл поэтому не нужен.
    if let Some(path) = trimmed(&conf.ssl_ca_bundle_path) {
        cc.set("ssl.ca.location", path);
    }

    // Клиентская пара — это mTLS. Именно PEM-файлы, а не JKS: keystore и
    // truststore из мира Java librdkafka не читает.
    match (
        trimmed(&conf.ssl_certificate_path),
        trimmed(&conf.ssl_key_path),
    ) {
        (Some(cert), Some(key)) => {
            cc.set("ssl.certificate.location", cert);
            cc.set("ssl.key.location", key);
            // Пароль ключа, как и пароль SASL, не тримится.
            if let Some(pwd) = conf.ssl_key_password.as_deref().filter(|p| !p.is_empty()) {
                cc.set("ssl.key.password", pwd);
            }
        }
        (None, None) => {}
        // Половина пары бесполезна: librdkafka отвергла бы её сама, но уже в
        // недрах OpenSSL и соответствующим текстом.
        (Some(_), None) => {
            return Err("client certificate is set but the private key is missing: \
                        mutual TLS needs both"
                .into())
        }
        (None, Some(_)) => {
            return Err("private key is set but the client certificate is missing: \
                        mutual TLS needs both"
                .into())
        }
    }

    // Дальше — отказы от проверок. Оба выключателя существуют ради стендов и
    // кластеров, до которых иначе не дотянуться: имя в сертификате брокера
    // сплошь и рядом не совпадает с адресом, по которому к нему ходят
    // (bootstrap по IP, alias, проброшенный порт), а на самоподписанных
    // стендах бандла может не быть вовсе. По умолчанию оба выключателя
    // выключены, и это свойство закреплено тестом: настройка «не проверять»
    // обязана быть осознанной.
    if conf.ssl_skip_hostname_check {
        cc.set("ssl.endpoint.identification.algorithm", "none");
    }
    if conf.ssl_skip_certificate_verification {
        cc.set("enable.ssl.certificate.verification", "false");
    }

    Ok(())
}

/// Непустое значение поля-пути. Пустая строка приезжает из формы наравне с
/// отсутствием значения и означает ровно то же самое.
fn trimmed(value: &Option<String>) -> Option<&str> {
    value.as_deref().map(str::trim).filter(|s| !s.is_empty())
}

/// Сборка конфигурации — единственная часть подключения, которую можно
/// проверить без кластера: это чистая функция, а `ClientConfig::get` показывает
/// каждое выставленное значение. Всё остальное (доехало ли рукопожатие,
/// принял ли брокер механизм) проверяется только руками на живом кластере,
/// поэтому здесь закрыта хотя бы таблица «протокол × механизм × TLS».
#[cfg(test)]
mod tests {
    use super::*;

    fn payload(protocol: &str) -> ClusterConnectPayload {
        ClusterConnectPayload {
            id: None,
            user_id: None,
            brokers: "b:9092".into(),
            security_protocol: protocol.into(),
            sasl_mechanism: None,
            username: Some("alice".into()),
            password: Some("secret".into()),
            ssl_ca_bundle_path: None,
            ssl_certificate_path: None,
            ssl_key_path: None,
            ssl_key_password: None,
            ssl_skip_hostname_check: false,
            ssl_skip_certificate_verification: false,
        }
    }

    fn config(conf: &ClusterConnectPayload) -> ClientConfig {
        base_config(conf).expect("config should build")
    }

    // --- Протоколы -----------------------------------------------------------

    #[test]
    fn plaintext_carries_neither_credentials_nor_tls() {
        let cc = config(&payload("PLAINTEXT"));

        assert_eq!(cc.get("security.protocol"), Some("plaintext"));
        assert_eq!(cc.get("sasl.mechanism"), None);
        // Логин у записи вполне может быть — от прежнего протокола или потому,
        // что кластер смотрят и так, и так. Слать его в открытую нельзя.
        assert_eq!(cc.get("sasl.username"), None);
        assert_eq!(cc.get("sasl.password"), None);
    }

    #[test]
    fn ssl_authenticates_the_broker_but_sends_no_login() {
        let mut conf = payload("SSL");
        conf.ssl_ca_bundle_path = Some("/certs/ca.pem".into());
        let cc = config(&conf);

        assert_eq!(cc.get("security.protocol"), Some("ssl"));
        assert_eq!(cc.get("ssl.ca.location"), Some("/certs/ca.pem"));
        assert_eq!(cc.get("sasl.mechanism"), None);
        assert_eq!(cc.get("sasl.username"), None);
    }

    #[test]
    fn sasl_plaintext_sends_the_login_without_tls() {
        let mut conf = payload("SASL_PLAINTEXT");
        conf.sasl_mechanism = Some("SCRAM-SHA-256".into());
        let cc = config(&conf);

        assert_eq!(cc.get("security.protocol"), Some("sasl_plaintext"));
        assert_eq!(cc.get("sasl.mechanism"), Some("SCRAM-SHA-256"));
        assert_eq!(cc.get("sasl.username"), Some("alice"));
        assert_eq!(cc.get("sasl.password"), Some("secret"));
        assert_eq!(cc.get("ssl.ca.location"), None);
    }

    #[test]
    fn sasl_ssl_sets_both_halves() {
        let mut conf = payload("SASL_SSL");
        conf.sasl_mechanism = Some("SCRAM-SHA-512".into());
        conf.ssl_ca_bundle_path = Some("/certs/ca.pem".into());
        let cc = config(&conf);

        assert_eq!(cc.get("security.protocol"), Some("sasl_ssl"));
        assert_eq!(cc.get("sasl.mechanism"), Some("SCRAM-SHA-512"));
        assert_eq!(cc.get("sasl.username"), Some("alice"));
        assert_eq!(cc.get("ssl.ca.location"), Some("/certs/ca.pem"));
    }

    #[test]
    fn the_protocol_is_read_case_insensitively_and_untrimmed() {
        let cc = config(&payload("  sasl_ssl  "));
        assert_eq!(cc.get("security.protocol"), Some("sasl_ssl"));
    }

    /// Незнакомый протокол раньше молча становился PLAINTEXT — то есть
    /// опечатка давала не отказ, а подключение открытым текстом.
    #[test]
    fn an_unknown_protocol_is_refused_instead_of_silently_becoming_plaintext() {
        let e = base_config(&payload("SASL-SSL")).unwrap_err();
        assert!(e.contains("unknown security protocol"), "{e}");
        assert!(e.contains("SASL-SSL"), "{e}");
    }

    // --- Механизмы -----------------------------------------------------------

    /// Умолчание бэкенда и начальное значение формы — одно и то же число.
    /// Разъехавшись, они дали бы запись, которая подключается одним механизмом,
    /// а показывает другой.
    #[test]
    fn a_record_without_a_mechanism_falls_back_to_the_default() {
        let mut conf = payload("SASL_SSL");
        conf.sasl_mechanism = None;
        assert_eq!(
            config(&conf).get("sasl.mechanism"),
            Some(DEFAULT_SASL_MECHANISM)
        );

        // Пустая строка из формы — то же самое, что отсутствие значения.
        conf.sasl_mechanism = Some("   ".into());
        assert_eq!(
            config(&conf).get("sasl.mechanism"),
            Some(DEFAULT_SASL_MECHANISM)
        );
    }

    #[test]
    fn a_mechanism_is_normalised_to_upper_case() {
        let mut conf = payload("SASL_SSL");
        conf.sasl_mechanism = Some(" scram-sha-512 ".into());
        assert_eq!(config(&conf).get("sasl.mechanism"), Some("SCRAM-SHA-512"));
    }

    /// GSSAPI и OAUTHBEARER стояли в меню и вполне могли осесть в
    /// `clusters.json`. Ни тот, ни другой в этой сборке не работает, и молча
    /// отдавать их librdkafka значит менять внятный отказ на таймаут.
    #[test]
    fn mechanisms_this_build_cannot_do_are_refused_by_name() {
        for mech in ["GSSAPI", "OAUTHBEARER", "SCRAM-SHA-1"] {
            let mut conf = payload("SASL_SSL");
            conf.sasl_mechanism = Some(mech.into());
            let e = base_config(&conf).unwrap_err();
            assert!(e.contains("unsupported SASL mechanism"), "{mech}: {e}");
            assert!(e.contains(mech), "{mech}: {e}");
        }
    }

    // --- TLS -----------------------------------------------------------------

    #[test]
    fn a_client_pair_turns_on_mutual_tls() {
        let mut conf = payload("SSL");
        conf.ssl_certificate_path = Some("/certs/client.pem".into());
        conf.ssl_key_path = Some("/certs/client.key".into());
        conf.ssl_key_password = Some(" pass phrase ".into());
        let cc = config(&conf);

        assert_eq!(cc.get("ssl.certificate.location"), Some("/certs/client.pem"));
        assert_eq!(cc.get("ssl.key.location"), Some("/certs/client.key"));
        // Пароль ключа не тримится: пробелы по краям — часть пароля.
        assert_eq!(cc.get("ssl.key.password"), Some(" pass phrase "));
    }

    #[test]
    fn half_a_client_pair_is_refused() {
        let mut only_cert = payload("SASL_SSL");
        only_cert.ssl_certificate_path = Some("/certs/client.pem".into());
        let e = base_config(&only_cert).unwrap_err();
        assert!(e.contains("private key is missing"), "{e}");

        let mut only_key = payload("SASL_SSL");
        only_key.ssl_key_path = Some("/certs/client.key".into());
        let e = base_config(&only_key).unwrap_err();
        assert!(e.contains("client certificate is missing"), "{e}");
    }

    #[test]
    fn an_unencrypted_key_gets_no_password_line() {
        let mut conf = payload("SSL");
        conf.ssl_certificate_path = Some("/certs/client.pem".into());
        conf.ssl_key_path = Some("/certs/client.key".into());
        conf.ssl_key_password = Some(String::new());

        assert_eq!(config(&conf).get("ssl.key.password"), None);
    }

    /// Проверки выключаются только по прямой просьбе. Молчание в конфиге — это
    /// «проверять», и никакая миграция записи не должна это менять.
    #[test]
    fn verification_is_on_unless_switched_off_explicitly() {
        let mut conf = payload("SASL_SSL");
        let cc = config(&conf);
        assert_eq!(cc.get("ssl.endpoint.identification.algorithm"), None);
        assert_eq!(cc.get("enable.ssl.certificate.verification"), None);

        conf.ssl_skip_hostname_check = true;
        conf.ssl_skip_certificate_verification = true;
        let cc = config(&conf);
        assert_eq!(
            cc.get("ssl.endpoint.identification.algorithm"),
            Some("none")
        );
        assert_eq!(cc.get("enable.ssl.certificate.verification"), Some("false"));
    }

    /// Настройки TLS остаются в записи и после того, как кластер перевели на
    /// протокол без шифрования. Применять их там нечего.
    #[test]
    fn tls_settings_are_ignored_by_protocols_without_tls() {
        let mut conf = payload("SASL_PLAINTEXT");
        conf.ssl_ca_bundle_path = Some("/certs/ca.pem".into());
        conf.ssl_certificate_path = Some("/certs/client.pem".into());
        conf.ssl_key_path = Some("/certs/client.key".into());
        conf.ssl_skip_certificate_verification = true;
        let cc = config(&conf);

        assert_eq!(cc.get("ssl.ca.location"), None);
        assert_eq!(cc.get("ssl.certificate.location"), None);
        assert_eq!(cc.get("enable.ssl.certificate.verification"), None);
    }

    // --- Наследование --------------------------------------------------------

    /// Ради этого `base_config` и отделён от остальных: и чтение, и отправка
    /// обязаны идти под теми же кредами и по тому же TLS.
    #[test]
    fn both_the_consumer_and_the_producer_inherit_security() {
        let mut conf = payload("SASL_SSL");
        conf.ssl_ca_bundle_path = Some("/certs/ca.pem".into());
        let base = config(&conf);

        for cc in [consumer_config(&base), producer_config(&base)] {
            assert_eq!(cc.get("security.protocol"), Some("sasl_ssl"));
            assert_eq!(cc.get("sasl.mechanism"), Some(DEFAULT_SASL_MECHANISM));
            assert_eq!(cc.get("sasl.username"), Some("alice"));
            assert_eq!(cc.get("sasl.password"), Some("secret"));
            assert_eq!(cc.get("ssl.ca.location"), Some("/certs/ca.pem"));
        }
    }
}
