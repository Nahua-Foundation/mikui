//! Схемы топиков: чем показывать тела сообщений и чем кодировать отправляемые.
//!
//! Формат тела — свойство топика, а не приложения, поэтому выбор хранится на
//! диске рядом с самой схемой (`store`). Форматов со схемой два — protobuf и
//! Avro, — и общего у них больше, чем различий: привязка к паре кластер-топик,
//! копии файлов у себя, правило «не применять изменение, которое не
//! разбирается». Всё это живёт здесь, а специфика — в `proto` и `avro`.
//!
//! Здесь только переход от `AppHandle` к каталогу настроек. Вся работа — в
//! `store`, и она намеренно не знает про Tauri: иначе её нельзя было бы
//! прогнать на временном каталоге, а проверять раскладку файлов и откат
//! неудавшейся загрузки больше негде.

pub mod avro;
mod decoder;
mod files;
pub mod json;
pub mod proto;
/// Библиотека наборов .proto: источник — дерево, набор — входной файл в нём.
/// Заведена затем, чтобы файлы перестали копироваться на каждый топик и чтобы
/// их было откуда взять там, где чекаутов нет.
pub mod source;
/// Общий HTTP-клиент: доверие корпоративному CA настраивается в одном месте на
/// всех, кто ходит по сети, — реестр и GitLab за одним и тем же инспектором.
pub mod http;
/// Клиент Schema Registry. Реестр в Confluent один на все три формата схем, и
/// аврошного в HTTP-запросах к нему нет ничего — поэтому модуль общий, а не
/// внутри `avro`, где он жил, пока формат со схемой в реестре был один.
pub mod registry;
mod types;
/// Обёртки тела: confluent-заголовок и всё, что отличает один способ положить
/// схему рядом с сообщением от другого. Общий на avro, protobuf и JSON Schema.
pub mod wire;

// Видно всему крейту ради `crate::favorites`: сохранённое сообщение
// показывается той же схемой, что и открытый топик, а весь тот модуль — как и
// этот — работает по каталогу настроек, чтобы прогоняться на временном.
pub(crate) mod store;

use std::sync::Arc;

use tauri::AppHandle;

pub use decoder::Decoder;
pub use proto::guess::Guess;
pub use types::{BodyFormat, LensSetting, MessageForm, TopicSchemaView};

use crate::config;
use crate::config::SchemaRegistry;

pub fn view(
    app: &AppHandle,
    cluster: &str,
    topic: &str,
) -> Result<Option<TopicSchemaView>, String> {
    store::view(&config::config_dir(app)?, cluster, topic)
}

pub fn decoder(
    app: &AppHandle,
    cluster: &str,
    topic: &str,
) -> Result<Option<Arc<Decoder>>, String> {
    store::decoder(&config::config_dir(app)?, cluster, topic)
}

/// Декодер ключа — своей схемой, а не схемой тела. См. `store::key_decoder`.
pub fn key_decoder(
    app: &AppHandle,
    cluster: &str,
    topic: &str,
) -> Result<Option<Arc<Decoder>>, String> {
    store::key_decoder(&config::config_dir(app)?, cluster, topic)
}

pub fn add_files(
    app: &AppHandle,
    cluster: &str,
    topic: &str,
    sources: &[String],
) -> Result<TopicSchemaView, String> {
    store::add_files(&config::config_dir(app)?, cluster, topic, sources)
}

/// Контракт топика из вставленного текста. См. `store::add_text`.
pub fn add_text(
    app: &AppHandle,
    cluster: &str,
    topic: &str,
    text: &str,
) -> Result<TopicSchemaView, String> {
    store::add_text(&config::config_dir(app)?, cluster, topic, text)
}

/// Разбирает вставленный текст, ничего не сохраняя. См. `store::check_text`.
///
/// Каталог настроек ей не нужен вовсе: проверка идёт во временном каталоге и
/// топика не касается. Обёртка оставлена ради единообразия — все команды схем
/// ходят через этот модуль.
pub fn check_text(text: &str) -> Result<(), String> {
    store::check_text(text)
}

/// Какими типами может быть записан топик — по его же телам.
/// См. `store::guess_message`.
pub fn guess_message(
    app: &AppHandle,
    cluster: &str,
    topic: &str,
    samples: &[Vec<u8>],
) -> Result<Guess, String> {
    store::guess_message(&config::config_dir(app)?, cluster, topic, samples)
}

pub fn refresh(
    app: &AppHandle,
    cluster: &str,
    topic: &str,
    name: Option<&str>,
) -> Result<TopicSchemaView, String> {
    store::refresh(&config::config_dir(app)?, cluster, topic, name)
}

pub fn remove_file(
    app: &AppHandle,
    cluster: &str,
    topic: &str,
    name: &str,
) -> Result<Option<TopicSchemaView>, String> {
    store::remove_file(&config::config_dir(app)?, cluster, topic, name)
}

pub fn set_options(
    app: &AppHandle,
    cluster: &str,
    topic: &str,
    format: BodyFormat,
    message: Option<String>,
) -> Result<TopicSchemaView, String> {
    store::set_options(&config::config_dir(app)?, cluster, topic, format, message)
}

pub fn forget_cluster(app: &AppHandle, cluster: &str) -> Result<(), String> {
    store::forget_cluster(&config::config_dir(app)?, cluster)
}

/// Выбор линзы, назначенный топику. `None` — не выбирали.
pub fn lens_of(app: &AppHandle, cluster: &str, topic: &str) -> Option<LensSetting> {
    store::lens_of(&config::config_dir(app).ok()?, cluster, topic)
}

/// Сохраняет выбор линзы. `None` — вернуть распознаванию право решать.
pub fn set_lens(
    app: &AppHandle,
    cluster: &str,
    topic: &str,
    lens: Option<LensSetting>,
) -> Result<TopicSchemaView, String> {
    store::set_lens(&config::config_dir(app)?, cluster, topic, lens)
}

// --- Библиотека наборов .proto -------------------------------------------------

/// Ключ токена источника в системном хранилище.
///
/// С префиксом: хранилище у приложения одно на всё (`config::secrets`), и
/// ключи кластерных учёток — это их идентификаторы. Без префикса два разных
/// рода секретов делили бы одно пространство имён, и совпадение
/// идентификаторов стоило бы чужого пароля.
fn token_key(id: &str) -> String {
    format!("source:{id}")
}

/// Ключ токена, общего на весь ХОСТ.
///
/// Отдельно от токена источника, и это не дубль. Personal access token заводят
/// на человека и на инсталляцию, а не на проект: у владельца в GitLab десятки
/// тысяч репозиториев, и заводить токен заново на каждый добавленный источник
/// означало бы каждый раз идти в настройки GitLab. Токен источника остаётся
/// при этом на месте — его `refresh` и берёт, — а этот служит заготовкой для
/// СЛЕДУЮЩЕГО источника того же хоста.
///
/// Удаление источника его не трогает: он тем и ценен, что переживает источники.
/// Забыть его можно явно — `forget_host_token`.
///
/// Провайдер входит в ключ, а не только хост. На одном хосте двух сразу не
/// бывает, так что различать их по сути незачем, — но имя записи видно человеку
/// в системной связке ключей, и `github-host:github.com` там честнее, чем
/// `gitlab-host:github.com`. Заодно сохранённые раньше токены GitLab остаются
/// на своих местах: ключ у них тот же, что и был.
fn host_token_key(kind: source::SourceKind, host: &str) -> String {
    let prefix = match kind {
        source::SourceKind::Github => "github-host",
        _ => "gitlab-host",
    };
    format!("{prefix}:{host}")
}

/// Хост из URL проекта — то, чем в хранилище назван общий токен.
///
/// Разбирается грубо, без `url`-крейта: нужна одна часть, а зависимость ради
/// неё не окупается. Регистр приводится к нижнему — иначе `GitLab.example.com`
/// и `gitlab.example.com` завели бы два токена на один сервер.
fn host_of(url: &str) -> Option<String> {
    let rest = url
        .trim()
        .split_once("://")
        .map_or(url.trim(), |(_, rest)| rest);
    let host = rest.split(['/', '?', '#']).next()?.trim();
    // Порт отбрасывается: токен выдан сервером, а не портом, и `:8443` в ключе
    // развело бы один и тот же токен по двум записям.
    let host = host.split(':').next()?;
    (!host.is_empty()).then(|| host.to_ascii_lowercase())
}

/// Чем заполнить форму нового удалённого источника.
///
/// Заведение источника — это шесть полей, из которых пять одинаковы у всех
/// проектов одного сервера. Подставляются хост последнего заведённого источника
/// ТОГО ЖЕ провайдера и признак сохранённого на него токена; сам токен наружу не
/// уезжает, как и пароли кластеров.
///
/// Провайдера спрашивают отдельно потому, что хост у них разный: подставить
/// `gitlab.example.com` в форму GitHub значило бы предложить заготовку,
/// которую первым делом надо стереть.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub struct RepoDefaults {
    /// `https://<хост>/` — приставка, которую остаётся дополнить путём проекта.
    pub url_prefix: Option<String>,
    pub host: Option<String>,
    /// На этот хост в хранилище лежит токен, и поле можно оставить пустым.
    pub has_token: bool,
}

pub fn repo_defaults(app: &AppHandle, kind: source::SourceKind) -> Result<RepoDefaults, String> {
    let root = config::config_dir(app)?;
    // Последний по времени забора, а не по порядку в файле: добавляют источники
    // вперемешку, а подставить надо тот сервер, с которым работали только что.
    let host = source::list(&root)?
        .into_iter()
        .filter(|s| s.kind == kind)
        .max_by(|a, b| a.fetched_at.cmp(&b.fetched_at))
        .and_then(|s| host_of(&s.location));

    // У публичного GitHub хост один на всех, и ждать первого источника, чтобы
    // его подставить, незачем — это не догадка, а единственный возможный ответ.
    let host = host.or_else(|| match kind {
        source::SourceKind::Github => Some("github.com".to_string()),
        _ => None,
    });

    let has_token = match &host {
        Some(host) => config::secrets::read_password(&host_token_key(kind, host))
            .ok()
            .flatten()
            .is_some(),
        None => false,
    };
    Ok(RepoDefaults {
        url_prefix: host.as_ref().map(|host| format!("https://{host}/")),
        host,
        has_token,
    })
}

/// Забывает общий токен хоста. Токены самих источников остаются при них.
pub fn forget_host_token(kind: source::SourceKind, host: &str) -> Result<(), String> {
    config::secrets::delete_password(&host_token_key(kind, host))
}

pub fn source_views(app: &AppHandle) -> Result<Vec<source::SourceView>, String> {
    source::views(&config::config_dir(app)?)
}

/// Заводит источник-папку: .proto копируются к себе, как и выбранные руками.
pub fn add_folder_source(
    app: &AppHandle,
    path: &str,
) -> Result<source::SchemaSource, String> {
    let root = config::config_dir(app)?;
    let folder = std::path::Path::new(path);
    let record = source::folder_source(new_source_id(), folder);
    let files = source::read_folder(folder)?;
    source::add(&root, record, files)
}

/// Заводит удалённый источник — GitLab или GitHub — и сразу забирает дерево.
///
/// Токен кладётся в хранилище ДО похода за деревом (он для него и нужен), а
/// если поход не удался — убирается: источник не завёлся, и висящий секрет от
/// него остался бы сиротой.
///
/// Пустой токен — не «ходить без токена», а «взять сохранённый на этот хост»
/// (`host_token_key`). Приватный репозиторий без токена отвечает тем же 404,
/// что и несуществующий, и заставлять набирать один и тот же PAT на каждый из
/// десятков тысяч проектов сервера незачем. Заданный токен, наоборот,
/// запоминается на хост и становится заготовкой для следующего источника.
pub fn add_repo_source(
    app: &AppHandle,
    kind: source::SourceKind,
    url: &str,
    git_ref: Option<String>,
    subdir: Option<String>,
    ca_bundle_path: Option<String>,
    token: Option<String>,
) -> Result<source::SchemaSource, String> {
    if !kind.is_remote() {
        return Err("a folder source is added by path, not by URL".to_string());
    }
    let root = config::config_dir(app)?;
    let id = new_source_id();
    let host = host_of(url);
    let given = token.filter(|t| !t.trim().is_empty());
    // Сохранить на хост — до похода: токен нужен ему самому, а не отчёту об
    // успехе, и неудачный поход за деревом его не портит.
    if let (Some(token), Some(host)) = (&given, &host) {
        config::secrets::store_password(&host_token_key(kind, host), token)?;
    }
    let token = match given {
        Some(given) => Some(given),
        None => host.as_deref().and_then(|host| {
            config::secrets::read_password(&host_token_key(kind, host))
                .ok()
                .flatten()
        }),
    };
    if let Some(token) = &token {
        config::secrets::store_password(&token_key(&id), token)?;
    }

    let record = source::repo_source(
        id.clone(),
        kind,
        url,
        git_ref,
        subdir,
        ca_bundle_path,
        token.is_some(),
    );
    let outcome = source::fetch(&record, token.as_deref())
        .and_then(|files| source::add(&root, record, files));
    if outcome.is_err() && token.is_some() {
        let _ = config::secrets::delete_password(&token_key(&id));
    }
    outcome
}

/// Забирает дерево заново. `git_ref` задан — заодно переключает ветку.
pub fn refresh_source(
    app: &AppHandle,
    id: &str,
    git_ref: Option<String>,
) -> Result<source::SchemaSource, String> {
    let root = config::config_dir(app)?;
    // Метка пересчитывается на каждом перезаборе, а не только при заведении.
    //
    // Иначе починить её нечем: правило менялось (было «последний сегмент пути»,
    // стало «группа/репозиторий» — `contracts` в двух группах не различались
    // вовсе), а завести источник заново значит получить новый `id` и потерять
    // все привязки топиков. Переименования руками в приложении нет, так что
    // своего, набранного человеком имени мы тут не затираем.
    let record = source::update(&root, id, |source| {
        if let Some(git_ref) = git_ref {
            source.git_ref = Some(git_ref).filter(|r| !r.trim().is_empty());
        }
        if source.kind.is_remote() {
            source.label = source::repo_label(&source.location, source.kind);
        }
    })?;

    let token = match record.has_token {
        true => config::secrets::read_password(&token_key(id))?,
        false => None,
    };
    let files = source::fetch(&record, token.as_deref())?;
    source::replace_tree(&root, id, files)
}

/// Убирает источник вместе с деревом и токеном.
pub fn delete_source(app: &AppHandle, id: &str) -> Result<(), String> {
    let root = config::config_dir(app)?;
    // Секрет убирается даже если записи уже нет: осиротевший токен в хранилище
    // — это то, чего из приложения потом не видно вовсе.
    let _ = config::secrets::delete_password(&token_key(id));
    source::delete(&root, id)
}

/// Привязывает топик к набору из библиотеки. `None` — снять привязку.
pub fn bind_source(
    app: &AppHandle,
    cluster: &str,
    topic: &str,
    binding: Option<source::SourceBinding>,
) -> Result<TopicSchemaView, String> {
    store::bind_source(&config::config_dir(app)?, cluster, topic, binding)
}

/// Идентификатор источника: случайный и неизменный.
///
/// Им названы каталог дерева и ключ токена, поэтому он не должен зависеть ни от
/// URL (его правят), ни от метки (её переименовывают).
fn new_source_id() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

// --- Avro --------------------------------------------------------------------

pub fn add_avro_files(
    app: &AppHandle,
    cluster: &str,
    topic: &str,
    sources: &[String],
) -> Result<TopicSchemaView, String> {
    store::add_avro_files(&config::config_dir(app)?, cluster, topic, sources)
}

pub fn refresh_avro_files(
    app: &AppHandle,
    cluster: &str,
    topic: &str,
    name: Option<&str>,
) -> Result<TopicSchemaView, String> {
    store::refresh_avro_files(&config::config_dir(app)?, cluster, topic, name)
}

pub fn remove_avro_file(
    app: &AppHandle,
    cluster: &str,
    topic: &str,
    name: &str,
) -> Result<Option<TopicSchemaView>, String> {
    store::remove_avro_file(&config::config_dir(app)?, cluster, topic, name)
}

pub fn set_avro_subject(
    app: &AppHandle,
    cluster: &str,
    topic: &str,
    subject: Option<String>,
    version: Option<i32>,
) -> Result<TopicSchemaView, String> {
    store::set_avro_subject(&config::config_dir(app)?, cluster, topic, subject, version)
}

/// Забывает кэш схем реестра. Зовётся, когда реестр отвязывают от кластера.
pub fn forget_registry_cache(root: &std::path::Path, url: &str) {
    avro::cache::forget(root, url);
    json::cache::forget();
}

// --- JSON Schema --------------------------------------------------------------

pub fn add_json_files(
    app: &AppHandle,
    cluster: &str,
    topic: &str,
    sources: &[String],
) -> Result<TopicSchemaView, String> {
    store::add_json_files(&config::config_dir(app)?, cluster, topic, sources)
}

pub fn refresh_json_files(
    app: &AppHandle,
    cluster: &str,
    topic: &str,
    name: Option<&str>,
) -> Result<TopicSchemaView, String> {
    store::refresh_json_files(&config::config_dir(app)?, cluster, topic, name)
}

pub fn remove_json_file(
    app: &AppHandle,
    cluster: &str,
    topic: &str,
    name: &str,
) -> Result<Option<TopicSchemaView>, String> {
    store::remove_json_file(&config::config_dir(app)?, cluster, topic, name)
}

pub fn set_json_subject(
    app: &AppHandle,
    cluster: &str,
    topic: &str,
    subject: Option<String>,
    version: Option<i32>,
) -> Result<TopicSchemaView, String> {
    store::set_json_subject(&config::config_dir(app)?, cluster, topic, subject, version)
}

pub fn set_avro_record(
    app: &AppHandle,
    cluster: &str,
    topic: &str,
    record: Option<String>,
) -> Result<TopicSchemaView, String> {
    store::set_avro_record(&config::config_dir(app)?, cluster, topic, record)
}

/// Клиент реестра кластера. `None` — реестр не настроен либо подключение не
/// сохранено (у формы негде хранить настройки).
fn registry(
    app: &AppHandle,
    cluster: &str,
) -> Result<(String, std::sync::Arc<registry::Registry>), String> {
    let root = config::config_dir(app)?;
    store::registry_of(&root, cluster)
        .ok_or_else(|| "no schema registry is configured for this cluster".to_string())
}

/// Список subject реестра — содержимое выпадающего списка в настройках топика.
pub fn registry_subjects(app: &AppHandle, cluster: &str) -> Result<Vec<String>, String> {
    let (_, client) = registry(app, cluster)?;
    client.subjects()
}

/// Версии одного subject, по возрастанию.
pub fn registry_versions(
    app: &AppHandle,
    cluster: &str,
    subject: &str,
) -> Result<Vec<i32>, String> {
    let (_, client) = registry(app, cluster)?;
    client.versions(subject)
}

/// Проверяет настройки реестра, не сохраняя их.
///
/// Пароль приходит из формы, а не из keychain: проверяют как раз то, что
/// набрали, — в том числе ещё до первого сохранения. Пустой пароль у записи с
/// `has_password` означает «взять сохранённый», иначе проверка существующего
/// подключения требовала бы вводить пароль заново.
pub fn test_registry(
    cluster_id: Option<&str>,
    settings: &SchemaRegistry,
    password: Option<&str>,
) -> Result<usize, String> {
    let stored = match (password.filter(|p| !p.is_empty()), cluster_id) {
        (Some(_), _) | (None, None) => None,
        (None, Some(id)) => config::secrets::read_password(&SchemaRegistry::secret_key(id))?,
    };
    let password = password
        .filter(|p| !p.is_empty())
        .map(str::to_string)
        .or(stored);

    let client = registry::Registry::new(
        &settings.url,
        settings.username.as_deref(),
        password.as_deref(),
        settings.ssl_ca_bundle_path.as_deref(),
    )?;
    client.subjects().map(|s| s.len())
}

// --- Отправка ---------------------------------------------------------------
//
// Здесь схема работает в обратную сторону: не «байты из Kafka → JSON», а
// «JSON из формы → байты в Kafka». Обе стороны обязаны ходить через один и тот
// же descriptor, иначе приложение показывало бы одно, а отправляло другое.

/// Заготовка тела и имена enum-значений выбранного message.
///
/// `choice` — запись селектора отправки: либо имя message, либо `<Message>.<поле>`
/// для ветки `oneof` (см. `proto::linked::produce_choices`). В обоих случаях
/// кодировать будут ОБЁРТКОЙ, а ветка решает только, какое поле в заготовке
/// раскрыть: тело — это конверт целиком, и уезжает он как есть.
pub fn message_form(
    app: &AppHandle,
    cluster: &str,
    topic: &str,
    choice: &str,
) -> Result<MessageForm, String> {
    let (md, branch) = store::produce_target(&config::config_dir(app)?, cluster, topic, choice)?;
    let template = proto::template::skeleton(&md, branch.as_deref());
    // Имя снимается строкой, `ProtoDecoder::new` забирает descriptor себе.
    let message = md.full_name().to_string();
    // Список берётся у декодера, а не собирается здесь заново: он же красит
    // enum в модалке чтения. Второй способ решать, что здесь enum, а что просто
    // строка, рано или поздно разошёлся бы с первым — и одно и то же значение
    // подсвечивалось бы по-разному при отправке и при чтении.
    let enum_values = proto::ProtoDecoder::new(md).enum_values().to_vec();
    Ok(MessageForm {
        template,
        enum_values,
        subject: None,
        message: Some(message),
    })
}

/// Заготовка тела и символы enum для Avro.
///
/// `subject` — то же, что `message` у protobuf: чем именно кодировать. Пустой —
/// взять то, что назначено топику.
pub fn avro_message_form(
    app: &AppHandle,
    cluster: &str,
    topic: &str,
    subject: Option<&str>,
) -> Result<MessageForm, String> {
    let root = config::config_dir(app)?;
    let found = store::avro_for_produce(&root, cluster, topic, subject)?;
    Ok(MessageForm {
        template: avro::template::skeleton(&found.linked),
        enum_values: found.linked.enum_values().to_vec(),
        subject: found.subject,
        // Расхождение «выбрано одно, кодируется другим» бывает только у
        // protobuf: у Avro выбирают схему, и она же кодирует.
        message: None,
    })
}

/// Заготовка тела для JSON Schema.
///
/// `enum_values` пуст, и это не заглушка: `enum` в JSON Schema — перечень
/// допустимых литералов, а не именованные символы, и красить их отдельным
/// цветом значило бы красить обычные строки (см. `Decoder::decode_with_enums`).
pub fn json_message_form(
    app: &AppHandle,
    cluster: &str,
    topic: &str,
    subject: Option<&str>,
) -> Result<MessageForm, String> {
    let root = config::config_dir(app)?;
    let found = store::json_for_produce(&root, cluster, topic, subject)?;
    Ok(MessageForm {
        template: json::template::skeleton(found.compiled.schema()),
        enum_values: Vec::new(),
        subject: found.subject,
        // См. `avro_message_form`: расхождение бывает только у protobuf.
        message: None,
    })
}

/// Проверяет введённое тело по JSON-схеме топика и одевает его в
/// confluent-обёртку.
///
/// Отличие от двух других форматов принципиальное: кодировать тут нечего — тело
/// уже JSON, и в топик уезжает ровно то, что набрали. Схема нужна только чтобы
/// не дать положить в топик сообщение, которое его потребитель не прочитает; в
/// avro и protobuf ту же роль играет сам факт успешного кодирования.
///
/// Заголовок ставится только у схемы из реестра. Локальная схема id не имеет, и
/// тело уедет голым — ровно так, как его пишет обычный `JsonSerializer`.
pub fn encode_json(
    app: &AppHandle,
    cluster: &str,
    topic: &str,
    subject: Option<&str>,
    json: &str,
) -> Result<Vec<u8>, String> {
    let root = config::config_dir(app)?;
    let found = store::json_for_produce(&root, cluster, topic, subject)?;

    let value: serde_json::Value =
        serde_json::from_str(json).map_err(|e| format!("the body is not valid JSON: {e}"))?;
    found.compiled.validate(&value)?;

    // Отправляем НАБРАННОЕ, а не перепечатанное из `value`: пробелы и порядок
    // ключей — это тоже тело, и менять его за пользователя незачем.
    let body = json.as_bytes();
    Ok(match found.id {
        Some(id) => wire::frame(id, body),
        None => body.to_vec(),
    })
}

/// Кодирует введённый JSON в Avro.
///
/// Схема из реестра даёт confluent-обёртку: маркер и id впереди тела — ровно
/// то, чего ждёт от топика штатный потребитель со своим `KafkaAvroDeserializer`.
/// Локальная схема идентификатора не имеет, и тело уезжает голым datum'ом;
/// форма отправки об этой разнице пишет прямо, потому что видна она не нам, а
/// тем, кто топик читает.
pub fn encode_avro(
    app: &AppHandle,
    cluster: &str,
    topic: &str,
    subject: Option<&str>,
    json: &str,
) -> Result<Vec<u8>, String> {
    let root = config::config_dir(app)?;
    let found = store::avro_for_produce(&root, cluster, topic, subject)?;

    let value: serde_json::Value =
        serde_json::from_str(json).map_err(|e| format!("the body is not valid JSON: {e}"))?;
    let datum = found.linked.encode(value)?;

    Ok(match found.id {
        Some(id) => wire::frame(id, &datum),
        None => datum,
    })
}

/// Кодирует введённый JSON в protobuf по выбранному message.
///
/// Тело — конверт целиком, включая имя ветки `oneof` внутри него: обёртка тут не
/// додумывается. Иначе отправленный из формы `OrderChanged` уезжал бы байтами
/// самого `OrderChanged` — без `PublicEvent`, а значит и без признака того, чей
/// это вариант: `oneof` в конверте остался бы пустым, и потребитель, читающий
/// топик как `PublicEvent`, увидел бы пустое сообщение. Форма поэтому и
/// показывает заготовку с полем ветки внутри (см. `message_form`).
///
/// Неизвестные поля НЕ игнорируются (`ParseOptions` оставлены дефолтными):
/// опечатка в имени поля иначе молча уехала бы в топик пустым значением — а
/// это ровно та ошибка, ради обнаружения которой схему к топику и грузят.
pub fn encode(
    app: &AppHandle,
    cluster: &str,
    topic: &str,
    choice: &str,
    json: &str,
) -> Result<Vec<u8>, String> {
    let (md, _) = store::produce_target(&config::config_dir(app)?, cluster, topic, choice)?;
    let parsed = protobuf_json_mapping::parse_dyn_from_str(&md, json)
        .map_err(|e| format!("can't encode as {}: {e}", md.full_name()))?;
    parsed
        .write_to_bytes_dyn()
        .map_err(|e| format!("can't serialize {}: {e}", md.full_name()))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Хост из URL проекта. Нужен точным: им назван общий токен в хранилище, и
    /// разъехавшийся регистр или прилипший порт завели бы второй токен на тот
    /// же сервер — то есть вернули бы ровно ту работу, ради отмены которой он и
    /// сохраняется.
    #[test]
    fn the_host_is_taken_from_whatever_form_the_url_was_pasted_in() {
        for (url, expected) in [
            (
                "https://gitlab.example.com/group/contracts",
                "gitlab.example.com",
            ),
            (
                "http://gitlab.example.com/group/sub/contracts.git",
                "gitlab.example.com",
            ),
            // Без схемы — так URL и копируют из адресной строки половину раз.
            ("gitlab.example.com/group/contracts", "gitlab.example.com"),
            // Регистр и порт не должны разводить один сервер по двум записям.
            ("https://GitLab.Example.COM/group/x", "gitlab.example.com"),
            ("https://gitlab.example.com:8443/group/x", "gitlab.example.com"),
            // Хост сам по себе — тоже ответ: форму заполняют по нему.
            ("https://gitlab.example.com/", "gitlab.example.com"),
            ("  https://gitlab.example.com  ", "gitlab.example.com"),
        ] {
            assert_eq!(host_of(url).as_deref(), Some(expected), "{url}");
        }
    }

    /// Пустой URL хоста не даёт — иначе общий токен лёг бы под ключ
    /// `gitlab-host:`, то есть стал бы общим на все серверы сразу.
    #[test]
    fn an_empty_url_has_no_host() {
        for url in ["", "   ", "https://", "/group/contracts"] {
            assert_eq!(host_of(url), None, "{url:?}");
        }
    }

    /// Ключи двух токенов не пересекаются ни между собой, ни с кластерными
    /// учётками: хранилище у приложения одно на всё.
    #[test]
    fn the_two_token_keys_live_in_their_own_namespaces() {
        assert_eq!(token_key("abc"), "source:abc");
        assert_eq!(
            host_token_key(source::SourceKind::Gitlab, "gitlab.example.com"),
            "gitlab-host:gitlab.example.com"
        );
        assert_eq!(
            host_token_key(source::SourceKind::Github, "github.com"),
            "github-host:github.com"
        );
        assert_ne!(
            token_key("x"),
            host_token_key(source::SourceKind::Gitlab, "x")
        );
    }

    /// Ключ токена GitLab не менялся: сохранённые прошлыми версиями токены
    /// обязаны остаться на своих местах, иначе владельцу пришлось бы заводить
    /// PAT заново без всякой на то причины.
    #[test]
    fn the_gitlab_token_key_did_not_change_when_github_appeared() {
        assert_eq!(
            host_token_key(source::SourceKind::Gitlab, "gitlab.tcsbank.ru"),
            "gitlab-host:gitlab.tcsbank.ru"
        );
    }
}
