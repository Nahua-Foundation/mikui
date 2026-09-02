//! Сохранённые сообщения: то, что лежит у нас на диске и переживает и
//! перезапуск приложения, и retention на кластере.
//!
//! Здесь только переход от `AppHandle` к каталогу настроек. Вся работа — в
//! `store`, и она намеренно не знает про Tauri: иначе её нельзя было бы
//! прогнать на временном каталоге, а проверять раскладку файлов и учёт занятого
//! места больше негде. То же разделение, что у `proto`.

mod store;
mod types;

use tauri::AppHandle;

pub use types::{FavoritesView, SaveFavoriteRequest, SaveFavoriteResult, SavedMessage};

use crate::config;
use crate::kafka::RawBody;

pub fn list(app: &AppHandle) -> Result<FavoritesView, String> {
    store::list(&config::config_dir(app)?)
}

/// Сохранённое сообщение по идентификатору записи — по требованию, ровно как
/// `get_message_body` для строки таблицы. Тело собирается по сегодняшней схеме
/// топика: на диске лежат сырые байты.
pub fn message(app: &AppHandle, id: &str) -> Result<SavedMessage, String> {
    store::message(&config::config_dir(app)?, id)
}

pub fn save(
    app: &AppHandle,
    cluster: &str,
    cluster_name: &str,
    topic: &str,
    raw: &RawBody,
) -> Result<SaveFavoriteResult, String> {
    store::save(&config::config_dir(app)?, cluster, cluster_name, topic, raw)
}

pub fn delete(app: &AppHandle, id: &str) -> Result<FavoritesView, String> {
    store::delete(&config::config_dir(app)?, id)
}

pub fn clear(app: &AppHandle) -> Result<FavoritesView, String> {
    store::clear(&config::config_dir(app)?)
}
