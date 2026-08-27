// Learn more about Tauri commands at https://tauri.app/develop/calling-rust/

mod config;
mod helpers;
mod kafka;

use config::{ClusterConfig, Settings};
use kafka::*;

/// Подставляет пароль из keychain, если фронт его не прислал.
///
/// Для сохранённого кластера пароль вообще не пересекает границу IPC: фронт
/// шлёт только идентификатор, а значение подтягивается здесь.
fn resolve_password(payload: &mut ClusterConnectPayload) -> Result<(), String> {
    let already_provided = payload.password.as_deref().is_some_and(|p| !p.is_empty());
    if already_provided {
        return Ok(());
    }
    if let Some(id) = payload.id.clone() {
        payload.password = config::secrets::read_password(&id)?;
    }
    Ok(())
}

/// Все команды — тонкие обёртки: положить сообщение в очередь воркера и
/// дождаться ответа. Никакой блокирующей работы и никаких мьютексов здесь нет,
/// весь Kafka-код живёт на выделенном потоке.
#[tauri::command]
async fn cluster_connect(
    app: tauri::AppHandle,
    worker: tauri::State<'_, WorkerHandle>,
    mut payload: ClusterConnectPayload,
) -> Result<(), String> {
    resolve_password(&mut payload)?;
    let id = payload.id.clone();
    worker
        .call(|reply| Command::Connect(payload, reply))
        .await??;

    // Подключение удалось — отмечаем кластер как недавно использованный.
    // Не критично, поэтому ошибку записи не поднимаем наверх.
    if let Some(id) = id {
        if let Err(e) = touch_cluster(&app, &id) {
            eprintln!("can't update last_used for cluster {id}: {e}");
        }
    }
    Ok(())
}

fn touch_cluster(app: &tauri::AppHandle, id: &str) -> Result<(), String> {
    let mut clusters = config::load_clusters(app)?;
    let Some(cluster) = clusters.iter_mut().find(|c| c.id == id) else {
        return Ok(());
    };
    cluster.last_used = Some(chrono::Utc::now().to_rfc3339());
    config::save_clusters(app, &clusters)
}

#[tauri::command]
async fn cluster_test(
    worker: tauri::State<'_, WorkerHandle>,
    mut payload: ClusterConnectPayload,
) -> Result<(), String> {
    resolve_password(&mut payload)?;
    worker.call(|reply| Command::Test(payload, reply)).await?
}

#[tauri::command]
async fn list_clusters(app: tauri::AppHandle) -> Result<Vec<ClusterConfig>, String> {
    config::load_clusters(&app)
}

/// Сохраняет подключение. Пароль трактуется трояко:
///   `Some(непустой)` — записать в keychain;
///   `Some("")`       — удалить из keychain;
///   `None`           — не трогать то, что там уже лежит.
#[tauri::command]
async fn save_cluster(
    app: tauri::AppHandle,
    cluster: ClusterConfig,
    password: Option<String>,
) -> Result<ClusterConfig, String> {
    let mut cluster = cluster;
    let mut clusters = config::load_clusters(&app)?;

    match password.as_deref() {
        Some("") => {
            config::secrets::delete_password(&cluster.id)?;
            cluster.has_password = false;
        }
        Some(secret) => {
            config::secrets::store_password(&cluster.id, secret)?;
            cluster.has_password = true;
        }
        None => {
            // Сохраняем прежний флаг: форма могла прийти без пароля просто
            // потому, что пользователь его не менял.
            cluster.has_password = clusters
                .iter()
                .find(|c| c.id == cluster.id)
                .is_some_and(|c| c.has_password);
        }
    }

    match clusters.iter_mut().find(|c| c.id == cluster.id) {
        Some(existing) => *existing = cluster.clone(),
        None => clusters.push(cluster.clone()),
    }

    config::save_clusters(&app, &clusters)?;
    Ok(cluster)
}

#[tauri::command]
async fn delete_cluster(app: tauri::AppHandle, id: String) -> Result<(), String> {
    let mut clusters = config::load_clusters(&app)?;
    clusters.retain(|c| c.id != id);
    config::save_clusters(&app, &clusters)?;
    // Осиротевший пароль в keychain никому не нужен.
    config::secrets::delete_password(&id)
}

#[tauri::command]
async fn get_settings(app: tauri::AppHandle) -> Result<Settings, String> {
    config::load_settings(&app)
}

#[tauri::command]
async fn save_settings(app: tauri::AppHandle, settings: Settings) -> Result<(), String> {
    config::save_settings(&app, &settings)
}

#[tauri::command]
async fn cluster_disconnect(worker: tauri::State<'_, WorkerHandle>) -> Result<(), String> {
    worker.call(Command::Disconnect).await
}

/// Механизмы SASL, поддержанные этой сборкой: GSSAPI линкуется не всегда.
#[tauri::command]
fn sasl_mechanisms() -> Vec<&'static str> {
    helpers::supported_sasl_mechanisms()
}

#[tauri::command]
async fn get_topics(worker: tauri::State<'_, WorkerHandle>) -> Result<Vec<TopicInfo>, String> {
    worker.call(Command::ListTopics).await?
}

/// Вычитывает окно сообщений в буфер на стороне Rust и возвращает только
/// счётчики. Сами строки забираются через `get_window` по мере прокрутки.
#[tauri::command]
async fn open_topic(
    worker: tauri::State<'_, WorkerHandle>,
    params: OpenTopicParams,
) -> Result<OpenTopicResult, String> {
    worker
        .call(|reply| Command::OpenTopic(params, reply))
        .await?
}

/// Меняет фильтр и возвращает новое число видимых строк. Работает по буферу
/// в памяти, в сеть не ходит.
#[tauri::command]
async fn set_filter(
    worker: tauri::State<'_, WorkerHandle>,
    filter: MessageFilter,
) -> Result<usize, String> {
    worker
        .call(|reply| Command::SetFilter(filter, reply))
        .await?
}

/// Отдаёт ровно те строки, что видны на экране.
#[tauri::command]
async fn get_window(
    worker: tauri::State<'_, WorkerHandle>,
    start: usize,
    count: usize,
) -> Result<Vec<RowPreview>, String> {
    worker
        .call(|reply| Command::GetWindow {
            start,
            count,
            reply,
        })
        .await?
}

/// Полное тело сообщения — только когда его открыли.
#[tauri::command]
async fn get_message_body(
    worker: tauri::State<'_, WorkerHandle>,
    index: usize,
) -> Result<FullMessage, String> {
    worker.call(|reply| Command::GetBody(index, reply)).await?
}

#[tauri::command]
async fn close_topic(worker: tauri::State<'_, WorkerHandle>) -> Result<(), String> {
    worker.call(Command::CloseTopic).await
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_opener::init())
        .manage(WorkerHandle::spawn())
        .invoke_handler(tauri::generate_handler![
            cluster_connect,
            cluster_disconnect,
            cluster_test,
            sasl_mechanisms,
            list_clusters,
            save_cluster,
            delete_cluster,
            get_settings,
            save_settings,
            get_topics,
            open_topic,
            set_filter,
            get_window,
            get_message_body,
            close_topic,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
