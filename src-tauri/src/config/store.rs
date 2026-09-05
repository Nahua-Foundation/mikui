//! Чтение и запись файлов конфигурации в каталоге приложения.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use tauri::{AppHandle, Manager};

use super::types::{ClusterConfig, Settings};

const CLUSTERS_FILE: &str = "clusters.json";
const SETTINGS_FILE: &str = "settings.json";

pub fn config_dir(app: &AppHandle) -> Result<PathBuf, String> {
    let dir = app
        .path()
        .app_config_dir()
        .map_err(|e| format!("can't resolve config directory: {e}"))?;
    fs::create_dir_all(&dir)
        .map_err(|e| format!("can't create {}: {e}", dir.display()))?;
    Ok(dir)
}

/// Запись через временный файл и переименование.
///
/// Прямая запись поверх существующего файла означает, что падение или
/// отключение питания на середине оставит обрезанный JSON и потерю всех
/// сохранённых подключений. `rename` в пределах одной ФС атомарен.
pub fn write_atomic(path: &Path, bytes: &[u8]) -> Result<(), String> {
    let tmp = path.with_extension("json.tmp");
    fs::write(&tmp, bytes).map_err(|e| format!("can't write {}: {e}", tmp.display()))?;
    fs::rename(&tmp, path).map_err(|e| format!("can't replace {}: {e}", path.display()))?;
    Ok(())
}

/// Читает JSON, если он есть.
///
/// Битый файл не роняет приложение и не молча теряется: он отодвигается в
/// `<имя>.bad`, а вызывающему возвращается ошибка с путём, чтобы можно было
/// посмотреть глазами. Следующий запуск стартует с чистого листа.
pub fn read_json<T: serde::de::DeserializeOwned + Default>(path: &Path) -> Result<T, String> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(T::default()),
        Err(e) => return Err(format!("can't read {}: {e}", path.display())),
    };

    match serde_json::from_slice(&bytes) {
        Ok(value) => Ok(value),
        Err(e) => {
            let salvaged = path.with_extension("json.bad");
            let _ = fs::rename(path, &salvaged);
            Err(format!(
                "{} is not valid JSON ({e}); moved it to {} and started fresh",
                path.display(),
                salvaged.display()
            ))
        }
    }
}

pub fn load_clusters(app: &AppHandle) -> Result<Vec<ClusterConfig>, String> {
    clusters_at(&config_dir(app)?)
}

/// То же самое по каталогу настроек, без `AppHandle`.
///
/// Нужно `crate::schema`: адрес Schema Registry живёт на кластере, а модуль
/// схем работает по каталогу — ровно затем, чтобы прогоняться на временном.
pub fn clusters_at(root: &Path) -> Result<Vec<ClusterConfig>, String> {
    let mut clusters: Vec<ClusterConfig> = read_json(&root.join(CLUSTERS_FILE))?;
    // Единственная точка входа для записей с диска — значит и единственное
    // место, где стоит приводить их к текущему формату. Остальной код имеет
    // дело только с мигрированными.
    for cluster in &mut clusters {
        cluster.migrate();
    }
    Ok(clusters)
}

pub fn save_clusters(app: &AppHandle, clusters: &[ClusterConfig]) -> Result<(), String> {
    let json = serde_json::to_vec_pretty(clusters)
        .map_err(|e| format!("can't serialize clusters: {e}"))?;
    write_atomic(&config_dir(app)?.join(CLUSTERS_FILE), &json)
}

pub fn load_settings(app: &AppHandle) -> Result<Settings, String> {
    read_json(&config_dir(app)?.join(SETTINGS_FILE))
}

pub fn save_settings(app: &AppHandle, settings: &Settings) -> Result<(), String> {
    let json = serde_json::to_vec_pretty(settings)
        .map_err(|e| format!("can't serialize settings: {e}"))?;
    write_atomic(&config_dir(app)?.join(SETTINGS_FILE), &json)
}
