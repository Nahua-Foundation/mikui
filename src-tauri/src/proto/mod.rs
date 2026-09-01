//! Protobuf-схемы топиков: загрузка .proto, их хранение и декодирование тел.
//!
//! Здесь только переход от `AppHandle` к каталогу настроек. Вся работа — в
//! `store`, и она намеренно не знает про Tauri: иначе её нельзя было бы
//! прогнать на временном каталоге, а проверять раскладку файлов и откат
//! неудавшейся загрузки больше негде.

mod decoder;
mod files;
mod schema;
mod store;
mod types;

use std::sync::Arc;

use tauri::AppHandle;

pub use decoder::ProtoDecoder;
pub use types::{BodyFormat, TopicSchemaView};

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
