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
pub mod proto;
mod types;

// Видно всему крейту ради `crate::favorites`: сохранённое сообщение
// показывается той же схемой, что и открытый топик, а весь тот модуль — как и
// этот — работает по каталогу настроек, чтобы прогоняться на временном.
pub(crate) mod store;

use std::sync::Arc;

use tauri::AppHandle;

pub use decoder::Decoder;
pub use types::{BodyFormat, MessageForm, TopicSchemaView};

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
fn registry(app: &AppHandle, cluster: &str) -> Result<(String, std::sync::Arc<avro::Registry>), String> {
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

    let client = avro::Registry::new(
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
pub fn message_form(
    app: &AppHandle,
    cluster: &str,
    topic: &str,
    message: &str,
) -> Result<MessageForm, String> {
    let md = store::message(&config::config_dir(app)?, cluster, topic, message)?;
    let template = proto::template::skeleton(&md);
    // Список берётся у декодера, а не собирается здесь заново: он же красит
    // enum в модалке чтения. Второй способ решать, что здесь enum, а что просто
    // строка, рано или поздно разошёлся бы с первым — и одно и то же значение
    // подсвечивалось бы по-разному при отправке и при чтении.
    let enum_values = proto::ProtoDecoder::new(md).enum_values().to_vec();
    Ok(MessageForm {
        template,
        enum_values,
        subject: None,
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

    let Some(id) = found.id else {
        return Ok(datum);
    };
    let mut framed = Vec::with_capacity(datum.len() + 5);
    framed.push(0x00);
    framed.extend_from_slice(&id.to_be_bytes());
    framed.extend_from_slice(&datum);
    Ok(framed)
}

/// Кодирует введённый JSON в protobuf по выбранному message.
///
/// Неизвестные поля НЕ игнорируются (`ParseOptions` оставлены дефолтными):
/// опечатка в имени поля иначе молча уехала бы в топик пустым значением — а
/// это ровно та ошибка, ради обнаружения которой схему к топику и грузят.
pub fn encode(
    app: &AppHandle,
    cluster: &str,
    topic: &str,
    message: &str,
    json: &str,
) -> Result<Vec<u8>, String> {
    let md = store::message(&config::config_dir(app)?, cluster, topic, message)?;
    let parsed = protobuf_json_mapping::parse_dyn_from_str(&md, json)
        .map_err(|e| format!("can't encode as {}: {e}", md.full_name()))?;
    parsed
        .write_to_bytes_dyn()
        .map_err(|e| format!("can't serialize {}: {e}", md.full_name()))
}
