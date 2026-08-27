// Learn more about Tauri commands at https://tauri.app/develop/calling-rust/

mod helpers;
mod kafka;

use kafka::*;

/// Все команды — тонкие обёртки: положить сообщение в очередь воркера и
/// дождаться ответа. Никакой блокирующей работы и никаких мьютексов здесь нет,
/// весь Kafka-код живёт на выделенном потоке.
#[tauri::command]
async fn cluster_connect(
    worker: tauri::State<'_, WorkerHandle>,
    payload: ClusterConnectPayload,
) -> Result<(), String> {
    worker.call(|reply| Command::Connect(payload, reply)).await?
}

#[tauri::command]
async fn cluster_test(
    worker: tauri::State<'_, WorkerHandle>,
    payload: ClusterConnectPayload,
) -> Result<(), String> {
    worker.call(|reply| Command::Test(payload, reply)).await?
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
