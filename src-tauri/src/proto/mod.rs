//! Protobuf-схемы топиков: загрузка .proto, их хранение и декодирование тел.
//!
//! Здесь только переход от `AppHandle` к каталогу настроек. Вся работа — в
//! `store`, и она намеренно не знает про Tauri: иначе её нельзя было бы
//! прогнать на временном каталоге, а проверять раскладку файлов и откат
//! неудавшейся загрузки больше негде.

mod decoder;
mod files;
mod schema;
mod template;
mod types;

// Видно всему крейту ради `crate::favorites`: сохранённое сообщение
// показывается той же схемой, что и открытый топик, а весь тот модуль — как и
// этот — работает по каталогу настроек, чтобы прогоняться на временном.
pub(crate) mod store;

use std::sync::Arc;

use tauri::AppHandle;

pub use decoder::ProtoDecoder;
pub use types::{BodyFormat, ProtoMessageForm, TopicSchemaView};

use crate::config;

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
) -> Result<Option<Arc<ProtoDecoder>>, String> {
    store::decoder(&config::config_dir(app)?, cluster, topic)
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
) -> Result<ProtoMessageForm, String> {
    let md = store::message(&config::config_dir(app)?, cluster, topic, message)?;
    let template = template::skeleton(&md);
    // Список берётся у декодера, а не собирается здесь заново: он же красит
    // enum в модалке чтения. Второй способ решать, что здесь enum, а что просто
    // строка, рано или поздно разошёлся бы с первым — и одно и то же значение
    // подсвечивалось бы по-разному при отправке и при чтении.
    let enum_values = ProtoDecoder::new(md).enum_values().to_vec();
    Ok(ProtoMessageForm {
        template,
        enum_values,
    })
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
