// Learn more about Tauri commands at https://tauri.app/develop/calling-rust/

mod config;
mod helpers;
mod kafka;
mod proto;

use config::{ClusterConfig, ClusterUser, Settings};
use kafka::*;
use proto::{BodyFormat, TopicSchemaView};

/// Подставляет пароль из keychain, если фронт его не прислал.
///
/// Для сохранённой учётки пароль вообще не пересекает границу IPC: фронт шлёт
/// только идентификатор пользователя, а значение подтягивается здесь.
fn resolve_password(payload: &mut ClusterConnectPayload) -> Result<(), String> {
    let already_provided = payload.password.as_deref().is_some_and(|p| !p.is_empty());
    if already_provided {
        return Ok(());
    }
    // `id` как запасной ключ — ради записей, которые ещё не пережили миграцию
    // на список пользователей: там ключом в keychain был идентификатор кластера.
    let key = payload.user_id.clone().or_else(|| payload.id.clone());
    if let Some(key) = key {
        payload.password = config::secrets::read_password(&key)?;
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
    let user_id = payload.user_id.clone();
    worker
        .call(|reply| Command::Connect(payload, reply))
        .await??;

    // Подключение удалось — отмечаем кластер как недавно использованный и
    // запоминаем учётку. Не критично, поэтому ошибку записи не поднимаем наверх.
    if let Some(id) = id {
        if let Err(e) = touch_cluster(&app, &id, user_id.as_deref()) {
            eprintln!("can't update last_used for cluster {id}: {e}");
        }
    }
    Ok(())
}

/// Отмечает кластер использованным и запоминает, под кем подключились: иначе
/// выбор пользователя не пережил бы перезапуск приложения.
fn touch_cluster(
    app: &tauri::AppHandle,
    id: &str,
    user_id: Option<&str>,
) -> Result<(), String> {
    let mut clusters = config::load_clusters(app)?;
    let Some(cluster) = clusters.iter_mut().find(|c| c.id == id) else {
        return Ok(());
    };
    cluster.last_used = Some(chrono::Utc::now().to_rfc3339());
    // Подключиться могли и из формы, руками введя логин, которого в списке нет
    // — такой выбор запоминать нечем и незачем.
    if let Some(user_id) = user_id.filter(|id| cluster.user(id).is_some()) {
        cluster.active_user_id = Some(user_id.to_string());
    }
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

/// Сохраняет параметры подключения.
///
/// Список учёток берётся из уже сохранённой записи, а не из присланной формы:
/// пользователями заведуют `save_cluster_user`/`delete_cluster_user`, и
/// разъехавшийся во вкладке список не должен молча затирать keychain.
#[tauri::command]
async fn save_cluster(
    app: tauri::AppHandle,
    cluster: ClusterConfig,
) -> Result<ClusterConfig, String> {
    let mut cluster = cluster;
    let mut clusters = config::load_clusters(&app)?;

    if let Some(existing) = clusters.iter().find(|c| c.id == cluster.id) {
        cluster.users = existing.users.clone();
        cluster.active_user_id = existing.active_user_id.clone();
    }
    cluster.migrate();

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
    let Some(position) = clusters.iter().position(|c| c.id == id) else {
        return Ok(());
    };
    let removed = clusters.remove(position);
    config::save_clusters(&app, &clusters)?;

    // Осиротевшие пароли в keychain никому не нужны. Ошибку одной учётки не
    // поднимаем наверх: кластер уже удалён, и падать после этого значило бы
    // показать пользователю сбой на успешной операции.
    for user in &removed.users {
        if let Err(e) = config::secrets::delete_password(&user.id) {
            eprintln!("can't delete password of user {}: {e}", user.id);
        }
    }
    // Схемы топиков привязаны к кластеру — вместе с ним они и уходят. Ошибку,
    // как и с паролями, наверх не поднимаем: кластер уже удалён.
    if let Err(e) = proto::forget_cluster(&app, &id) {
        eprintln!("can't delete proto schemas of cluster {id}: {e}");
    }
    Ok(())
}

/// Заводит или обновляет Kafka-пользователя кластера. Пароль трактуется трояко:
///   `Some(непустой)` — записать в keychain;
///   `Some("")`       — удалить из keychain;
///   `None`           — не трогать то, что там уже лежит.
///
/// `activate` — сделать эту учётку текущей для кластера. Заведение ещё одного
/// логина само по себе текущего не меняет (иначе список пользователей уводил бы
/// подключение из-под ног), а вот выбор учётки в настройках подключения — как
/// раз меняет, и должен пережить неудачную попытку подключиться.
#[tauri::command]
async fn save_cluster_user(
    app: tauri::AppHandle,
    cluster_id: String,
    user: ClusterUser,
    password: Option<String>,
    activate: Option<bool>,
) -> Result<ClusterConfig, String> {
    let mut user = user;
    let mut clusters = config::load_clusters(&app)?;
    let cluster = clusters
        .iter_mut()
        .find(|c| c.id == cluster_id)
        .ok_or_else(|| format!("unknown cluster {cluster_id}"))?;

    match password.as_deref() {
        Some("") => {
            config::secrets::delete_password(&user.id)?;
            user.has_password = false;
        }
        Some(secret) => {
            config::secrets::store_password(&user.id, secret)?;
            user.has_password = true;
        }
        // Форма могла прийти без пароля просто потому, что его не меняли.
        None => user.has_password = cluster.user(&user.id).is_some_and(|u| u.has_password),
    }

    match cluster.users.iter_mut().find(|u| u.id == user.id) {
        Some(existing) => *existing = user.clone(),
        None => cluster.users.push(user.clone()),
    }
    // Первая заведённая учётка становится текущей — иначе подключаться было бы
    // не под кем, пока пользователь не выберет её руками.
    if activate.unwrap_or(false) || cluster.active_user_id.is_none() {
        cluster.active_user_id = Some(user.id);
    }

    let updated = cluster.clone();
    config::save_clusters(&app, &clusters)?;
    Ok(updated)
}

#[tauri::command]
async fn delete_cluster_user(
    app: tauri::AppHandle,
    cluster_id: String,
    user_id: String,
) -> Result<ClusterConfig, String> {
    let mut clusters = config::load_clusters(&app)?;
    let cluster = clusters
        .iter_mut()
        .find(|c| c.id == cluster_id)
        .ok_or_else(|| format!("unknown cluster {cluster_id}"))?;

    cluster.users.retain(|u| u.id != user_id);
    if cluster.active_user_id.as_deref() == Some(user_id.as_str()) {
        cluster.active_user_id = cluster.users.first().map(|u| u.id.clone());
    }

    let updated = cluster.clone();
    config::save_clusters(&app, &clusters)?;
    config::secrets::delete_password(&user_id)?;
    Ok(updated)
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

/// Дочитывает ещё по `additional` сообщений на каждую ещё не исчерпанную
/// партицию текущего топика, продолжая с места, на котором остановилось
/// предыдущее чтение.
#[tauri::command]
async fn load_more(
    worker: tauri::State<'_, WorkerHandle>,
    params: LoadMoreParams,
) -> Result<OpenTopicResult, String> {
    worker
        .call(|reply| Command::LoadMore(params, reply))
        .await?
}

/// Дешёвый снимок хода ещё не завершённого `open_topic`/`load_more` — фронт
/// опрашивает эту команду по таймеру, пока идёт загрузка.
#[tauri::command]
async fn get_open_topic_progress(
    worker: tauri::State<'_, WorkerHandle>,
) -> Result<OpenTopicProgress, String> {
    worker.call(Command::GetOpenTopicProgress).await?
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

// --- Protobuf-схемы топиков --------------------------------------------------
//
// Все команды ниже возвращают схему целиком, а не подтверждение: форма настроек
// топика показывает список файлов, список message и выбранный из них, и любая
// операция меняет сразу несколько из них (см. `proto::store`). Отдавать
// «ок» и заставлять фронт досчитывать новое состояние самому — верный способ
// разъехаться с диском.

#[tauri::command]
async fn get_topic_schema(
    app: tauri::AppHandle,
    cluster: String,
    topic: String,
) -> Result<Option<TopicSchemaView>, String> {
    proto::view(&app, &cluster, &topic)
}

/// Добавляет .proto к топику. Невалидный набор не сохраняется вовсе — ошибка
/// уезжает наверх, а на диске остаётся то, что работало.
#[tauri::command]
async fn add_proto_files(
    app: tauri::AppHandle,
    cluster: String,
    topic: String,
    paths: Vec<String>,
) -> Result<TopicSchemaView, String> {
    proto::add_files(&app, &cluster, &topic, &paths)
}

/// Перечитывает .proto с диска: `name` — конкретный файл, `None` — все.
#[tauri::command]
async fn refresh_proto_files(
    app: tauri::AppHandle,
    cluster: String,
    topic: String,
    name: Option<String>,
) -> Result<TopicSchemaView, String> {
    proto::refresh(&app, &cluster, &topic, name.as_deref())
}

#[tauri::command]
async fn remove_proto_file(
    app: tauri::AppHandle,
    cluster: String,
    topic: String,
    name: String,
) -> Result<Option<TopicSchemaView>, String> {
    proto::remove_file(&app, &cluster, &topic, &name)
}

/// Сохраняет выбор из формы: формат тела и основной message.
#[tauri::command]
async fn save_topic_schema(
    app: tauri::AppHandle,
    cluster: String,
    topic: String,
    format: BodyFormat,
    message: Option<String>,
) -> Result<TopicSchemaView, String> {
    proto::set_options(&app, &cluster, &topic, format, message)
}

/// Сообщает воркеру, чем декодировать тела открытого топика, и возвращает
/// схему — фронту она нужна, чтобы знать, в каком виде приедет тело.
///
/// Зовётся перед каждым открытием топика и после каждого сохранения настроек.
/// Схема, которая перестала разбираться, не должна мешать смотреть топик:
/// ошибка возвращается, декодер при этом снимается, и тела едут текстом.
#[tauri::command]
async fn apply_topic_schema(
    app: tauri::AppHandle,
    worker: tauri::State<'_, WorkerHandle>,
    cluster: String,
    topic: String,
) -> Result<Option<TopicSchemaView>, String> {
    let built = proto::decoder(&app, &cluster, &topic);
    // Воркеру говорим в любом случае, в том числе и «декодера нет»: иначе на
    // сломавшейся схеме он продолжил бы разбирать прежней и показывать чужое.
    let decoder = built.as_ref().ok().and_then(Clone::clone);
    worker
        .call(|reply| Command::SetDecoder(decoder, reply))
        .await?;
    built?;
    proto::view(&app, &cluster, &topic)
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
            save_cluster_user,
            delete_cluster_user,
            get_settings,
            save_settings,
            get_topics,
            open_topic,
            load_more,
            get_open_topic_progress,
            set_filter,
            get_window,
            get_message_body,
            close_topic,
            get_topic_schema,
            add_proto_files,
            refresh_proto_files,
            remove_proto_file,
            save_topic_schema,
            apply_topic_schema,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
