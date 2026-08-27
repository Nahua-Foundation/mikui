use serde::{Deserialize, Serialize};

/// Версия схемы файла настроек. Пригодится, когда появятся миграции.
pub const SETTINGS_VERSION: u32 = 1;

/// Сохранённое подключение.
///
/// Пароля здесь нет и быть не должно: он лежит в системном хранилище секретов
/// (см. `config::secrets`). В JSON на диске оседают только неопасные поля.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ClusterConfig {
    pub id: String,
    pub name: String,
    pub brokers: String,
    pub security_protocol: String,
    #[serde(default)]
    pub sasl_mechanism: Option<String>,
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub ssl_ca_bundle_path: Option<String>,
    pub created_at: String,
    #[serde(default)]
    pub last_used: Option<String>,
    /// Лежит ли пароль в keychain. Это флаг, а не сам пароль: опрашивать
    /// хранилище на каждый показ списка дорого, а на macOS ещё и чревато
    /// диалогом доступа на каждый кластер.
    #[serde(default)]
    pub has_password: bool,
}

/// Файл настроек приложения.
///
/// Собственных полей пока нет — файл заводится заранее, чтобы потом не
/// разбираться со случаем «файла не существовало». Всё, чего эта сборка не
/// знает, сохраняется в `extra` и записывается обратно нетронутым: иначе
/// старая версия молча затёрла бы настройки, добавленные новой.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct Settings {
    pub version: u32,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            version: SETTINGS_VERSION,
            extra: serde_json::Map::new(),
        }
    }
}
