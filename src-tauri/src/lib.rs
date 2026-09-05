// Learn more about Tauri commands at https://tauri.app/develop/calling-rust/

mod config;
mod favorites;
mod helpers;
mod kafka;
mod schema;

use config::{ClusterConfig, ClusterUser, SchemaRegistry, Settings};
use favorites::{FavoritesView, SaveFavoriteRequest, SaveFavoriteResult, SavedMessage};
use kafka::*;
use schema::{BodyFormat, MessageForm, TopicSchemaView};

/// Уводит блокирующую работу с исполнителя Tauri.
///
/// Всё, что ходит в Schema Registry, блокирует поток: клиент синхронный (см.
/// `schema::avro::registry`). В async-команде это заняло бы поток исполнителя
/// на все пять секунд таймаута, и на медленном реестре приложение перестало бы
/// отвечать целиком.
async fn blocking<T, F>(work: F) -> Result<T, String>
where
    F: FnOnce() -> Result<T, String> + Send + 'static,
    T: Send + 'static,
{
    tauri::async_runtime::spawn_blocking(work)
        .await
        .map_err(|e| format!("background task failed: {e}"))?
}

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
/// разъехавшийся во вкладке список не должен молча затирать keychain. Ровно то
/// же и с реестром: им заведуют `save_schema_registry`/`delete_schema_registry`,
/// и сохранение соседнего поля формы не должно его сносить.
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
        cluster.schema_registry = existing.schema_registry.clone();
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
    if let Err(e) = schema::forget_cluster(&app, &id) {
        eprintln!("can't delete proto schemas of cluster {id}: {e}");
    }
    // А вот избранное этого кластера остаётся, и это не забывчивость: схема без
    // кластера бесполезна, а сохранённое сообщение — сама ценность. Его для того
    // и сохраняли, чтобы оно пережило и retention, и само подключение.
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

/// Устройство и настройки топика: партиции, реплики и всё, что отдаёт
/// `DescribeConfigs`. К чтению отношения не имеет — открытый топик не меняет.
#[tauri::command]
async fn describe_topic(
    worker: tauri::State<'_, WorkerHandle>,
    topic: String,
) -> Result<TopicDetails, String> {
    worker
        .call(|reply| Command::DescribeTopic(topic, reply))
        .await?
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

/// Клик по заголовку колонки. Работает по буферу в памяти — как `set_filter`,
/// ни одного сетевого запроса.
#[tauri::command]
async fn set_sort(
    worker: tauri::State<'_, WorkerHandle>,
    sort: Option<SortSpec>,
) -> Result<usize, String> {
    worker.call(|reply| Command::SetSort(sort, reply)).await?
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
// операция меняет сразу несколько из них (см. `schema::store`). Отдавать
// «ок» и заставлять фронт досчитывать новое состояние самому — верный способ
// разъехаться с диском.
//
// Все они уходят в `blocking`, включая чтение: вид топика включает
// автоопределение формата, а оно спрашивает Schema Registry. Ответ кэширован и
// в подавляющем большинстве вызовов не стоит ничего, но первый на каждый топик
// — это сеть, и держать на ней исполнитель Tauri нельзя.

#[tauri::command]
async fn get_topic_schema(
    app: tauri::AppHandle,
    cluster: String,
    topic: String,
) -> Result<Option<TopicSchemaView>, String> {
    blocking(move || schema::view(&app, &cluster, &topic)).await
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
    blocking(move || schema::add_files(&app, &cluster, &topic, &paths)).await
}

/// Перечитывает .proto с диска: `name` — конкретный файл, `None` — все.
#[tauri::command]
async fn refresh_proto_files(
    app: tauri::AppHandle,
    cluster: String,
    topic: String,
    name: Option<String>,
) -> Result<TopicSchemaView, String> {
    blocking(move || schema::refresh(&app, &cluster, &topic, name.as_deref())).await
}

#[tauri::command]
async fn remove_proto_file(
    app: tauri::AppHandle,
    cluster: String,
    topic: String,
    name: String,
) -> Result<Option<TopicSchemaView>, String> {
    blocking(move || schema::remove_file(&app, &cluster, &topic, &name)).await
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
    blocking(move || schema::set_options(&app, &cluster, &topic, format, message)).await
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
    // Через `blocking`: у Avro сборка декодера может сходить в реестр за
    // схемой закреплённого subject. Оба декодера — одним заходом: у ключа
    // схема своя, но реестр и кэш общие, и второй поход обошёлся бы дороже
    // самой работы.
    let (built, key) = {
        let (app, cluster, topic) = (app.clone(), cluster.clone(), topic.clone());
        blocking(move || {
            let value = schema::decoder(&app, &cluster, &topic);
            // Ключ отдельно от тела и молча: не разобрался — поедет байтами,
            // как ездил всегда. Ошибка тела — новость (её показывают в
            // настройках топика), ошибка ключа — нет.
            let key = schema::key_decoder(&app, &cluster, &topic).ok().flatten();
            Ok((value, key))
        })
        .await?
    };
    // Воркеру говорим в любом случае, в том числе и «декодера нет»: иначе на
    // сломавшейся схеме он продолжил бы разбирать прежней и показывать чужое.
    let decoder = built.as_ref().ok().and_then(Clone::clone);
    worker
        .call(|reply| Command::SetDecoder {
            value: decoder,
            key,
            reply,
        })
        .await?;
    built?;
    blocking(move || schema::view(&app, &cluster, &topic)).await
}

// --- Avro-схемы топиков -------------------------------------------------------
//
// Возвращают, как и protobuf-команды, схему ЦЕЛИКОМ: список файлов, выбранная
// запись и subject меняются вместе, и досчитывать новое состояние на фронте
// значило бы разъезжаться с диском.

#[tauri::command]
async fn add_avro_files(
    app: tauri::AppHandle,
    cluster: String,
    topic: String,
    paths: Vec<String>,
) -> Result<TopicSchemaView, String> {
    blocking(move || schema::add_avro_files(&app, &cluster, &topic, &paths)).await
}

#[tauri::command]
async fn refresh_avro_files(
    app: tauri::AppHandle,
    cluster: String,
    topic: String,
    name: Option<String>,
) -> Result<TopicSchemaView, String> {
    blocking(move || schema::refresh_avro_files(&app, &cluster, &topic, name.as_deref())).await
}

#[tauri::command]
async fn remove_avro_file(
    app: tauri::AppHandle,
    cluster: String,
    topic: String,
    name: String,
) -> Result<Option<TopicSchemaView>, String> {
    blocking(move || schema::remove_avro_file(&app, &cluster, &topic, &name)).await
}

/// Привязывает топик к subject реестра. `subject` пуст — отвязать.
#[tauri::command]
async fn save_topic_avro_subject(
    app: tauri::AppHandle,
    cluster: String,
    topic: String,
    subject: Option<String>,
    version: Option<i32>,
) -> Result<TopicSchemaView, String> {
    let subject = subject.filter(|s| !s.is_empty());
    blocking(move || schema::set_avro_subject(&app, &cluster, &topic, subject, version)).await
}

/// Выбирает запись из загруженных .avsc.
#[tauri::command]
async fn save_topic_avro_record(
    app: tauri::AppHandle,
    cluster: String,
    topic: String,
    record: Option<String>,
) -> Result<TopicSchemaView, String> {
    blocking(move || {
        schema::set_avro_record(&app, &cluster, &topic, record.filter(|r| !r.is_empty()))
    })
    .await
}

// --- Schema Registry ----------------------------------------------------------
//
// Настройки реестра живут на КЛАСТЕРЕ, а не на топике: реестр в кластере один,
// и вводить его заново для каждого топика значило бы переписывать одно и то же
// по десять раз. Пароль, как и пароли учёток, в JSON не попадает и обратно
// через IPC не уезжает.

/// Сохраняет реестр кластера. Пароль трактуется так же, как у учёток:
/// `Some(непустой)` — записать, `Some("")` — удалить, `None` — не трогать.
#[tauri::command]
async fn save_schema_registry(
    app: tauri::AppHandle,
    cluster_id: String,
    registry: SchemaRegistry,
    password: Option<String>,
) -> Result<ClusterConfig, String> {
    let mut registry = registry;
    let mut clusters = config::load_clusters(&app)?;
    let cluster = clusters
        .iter_mut()
        .find(|c| c.id == cluster_id)
        .ok_or_else(|| format!("unknown cluster {cluster_id}"))?;

    let key = SchemaRegistry::secret_key(&cluster_id);
    match password.as_deref() {
        Some("") => {
            config::secrets::delete_password(&key)?;
            registry.has_password = false;
        }
        Some(secret) => {
            config::secrets::store_password(&key, secret)?;
            registry.has_password = true;
        }
        // Форма могла прийти без пароля просто потому, что его не меняли.
        None => {
            registry.has_password = cluster
                .schema_registry
                .as_ref()
                .is_some_and(|r| r.has_password)
        }
    }

    cluster.schema_registry = Some(registry);
    let updated = cluster.clone();
    config::save_clusters(&app, &clusters)?;
    Ok(updated)
}

/// Убирает реестр у кластера вместе с его паролем и кэшем схем.
#[tauri::command]
async fn delete_schema_registry(
    app: tauri::AppHandle,
    cluster_id: String,
) -> Result<ClusterConfig, String> {
    let mut clusters = config::load_clusters(&app)?;
    let cluster = clusters
        .iter_mut()
        .find(|c| c.id == cluster_id)
        .ok_or_else(|| format!("unknown cluster {cluster_id}"))?;

    let gone = cluster.schema_registry.take();
    let updated = cluster.clone();
    config::save_clusters(&app, &clusters)?;

    // Осиротевший кэш и пароль никому не нужны — как и при удалении кластера
    // целиком. Ошибки не поднимаем: реестр уже отвязан, и падать после
    // успешной операции значило бы показать сбой там, где его нет.
    if let Some(gone) = gone {
        if let Err(e) = config::secrets::delete_password(&SchemaRegistry::secret_key(&cluster_id)) {
            eprintln!("can't delete the schema registry password of {cluster_id}: {e}");
        }
        if let Ok(root) = config::config_dir(&app) {
            schema::forget_registry_cache(&root, &gone.url);
        }
    }
    Ok(updated)
}

/// Проверяет настройки реестра, не сохраняя их: сколько subject он отдал.
#[tauri::command]
async fn test_schema_registry(
    cluster_id: Option<String>,
    registry: SchemaRegistry,
    password: Option<String>,
) -> Result<usize, String> {
    blocking(move || {
        schema::test_registry(cluster_id.as_deref(), &registry, password.as_deref())
    })
    .await
}

#[tauri::command]
async fn list_registry_subjects(
    app: tauri::AppHandle,
    cluster: String,
) -> Result<Vec<String>, String> {
    blocking(move || schema::registry_subjects(&app, &cluster)).await
}

#[tauri::command]
async fn list_subject_versions(
    app: tauri::AppHandle,
    cluster: String,
    subject: String,
) -> Result<Vec<i32>, String> {
    blocking(move || schema::registry_versions(&app, &cluster, &subject)).await
}

// --- Отправка сообщения ------------------------------------------------------

/// Превращает введённое тело в байты.
///
/// JSON здесь НЕ проверяется намеренно: невалидный JSON уезжает в топик тем
/// самым текстом, который набрали, — предупредить о нём это дело
/// `check_produce_payload`. А вот proto, avro и hex либо кодируются, либо не
/// дают байтов вовсе, и притворяться, что дали, нельзя.
fn encode_payload(
    app: &tauri::AppHandle,
    cluster: Option<&str>,
    request: &ProduceRequest,
) -> Result<Vec<u8>, String> {
    match request.format {
        PayloadFormat::Text | PayloadFormat::Json => Ok(request.payload.as_bytes().to_vec()),
        PayloadFormat::Hex => kafka::decode_hex(&request.payload),
        PayloadFormat::Proto => {
            let cluster = cluster.ok_or("not connected to a cluster")?;
            let message = request
                .message
                .as_deref()
                .filter(|m| !m.is_empty())
                .ok_or("pick the message type to encode with")?;
            schema::encode(app, cluster, &request.topic, message, &request.payload)
        }
        PayloadFormat::Avro => {
            let cluster = cluster.ok_or("not connected to a cluster")?;
            schema::encode_avro(
                app,
                cluster,
                &request.topic,
                request.subject.as_deref(),
                &request.payload,
            )
        }
    }
}

/// Что показать под полем ввода тела, пока его набирают.
///
/// Отдельная команда, а не проверка на фронте, ровно затем, чтобы предупреждение
/// не могло разойтись с отправкой: и то, и другое считает `encode_payload`, то
/// есть один и тот же код. Проверка на фронте неизбежно разъехалась бы —
/// повторить разбор .proto в TypeScript нечем.
#[tauri::command]
async fn check_produce_payload(
    app: tauri::AppHandle,
    cluster: Option<String>,
    request: ProduceRequest,
) -> Result<Option<PayloadIssue>, String> {
    // Невалидный JSON — предупреждение, а не отказ: пользователь мог осознанно
    // класть в JSON-топик что-то другое, и запрещать ему это приложение для
    // отладки не должно.
    if request.format == PayloadFormat::Json {
        if request.payload.trim().is_empty() {
            return Ok(None);
        }
        return Ok(serde_json::from_str::<serde_json::Value>(&request.payload)
            .err()
            .map(|e| PayloadIssue {
                severity: IssueSeverity::Warning,
                message: e.to_string(),
            }));
    }

    // Через `blocking`: у avro проверка кодирует тело настоящей схемой, а за
    // ней ходят в реестр.
    blocking(move || {
        Ok(encode_payload(&app, cluster.as_deref(), &request)
            .err()
            .map(|message| PayloadIssue {
                severity: IssueSeverity::Error,
                message,
            }))
    })
    .await
}

/// Заготовка тела и имена enum-значений выбранного message — всё, что форме
/// отправки нужно знать про выбранный тип.
#[tauri::command]
async fn proto_message_form(
    app: tauri::AppHandle,
    cluster: String,
    topic: String,
    message: String,
) -> Result<MessageForm, String> {
    blocking(move || schema::message_form(&app, &cluster, &topic, &message)).await
}

/// То же самое для Avro. `subject` пуст — взять тот, что назначен топику.
#[tauri::command]
async fn avro_message_form(
    app: tauri::AppHandle,
    cluster: String,
    topic: String,
    subject: Option<String>,
) -> Result<MessageForm, String> {
    blocking(move || schema::avro_message_form(&app, &cluster, &topic, subject.as_deref())).await
}

/// Кладёт сообщение в топик.
///
/// Тело кодируется ЗДЕСЬ, а не в воркере: для protobuf нужны каталог настроек и
/// схема топика, а воркер про них не знает и знать не должен — так же, как
/// декодер для чтения собирается в `apply_topic_schema`.
#[tauri::command]
async fn produce_message(
    app: tauri::AppHandle,
    worker: tauri::State<'_, WorkerHandle>,
    cluster: Option<String>,
    request: ProduceRequest,
) -> Result<ProduceResult, String> {
    // Кодирование — через `blocking` по той же причине, что и проверка: для
    // avro за схемой идут в реестр.
    let (payload, request) = {
        let app = app.clone();
        let cluster = cluster.clone();
        blocking(move || {
            let payload = encode_payload(&app, cluster.as_deref(), &request)?;
            Ok((payload, request))
        })
        .await?
    };
    let record = ProduceRecord {
        topic: request.topic,
        partition: request.partition,
        // Пустой ключ — это ОТСУТСТВИЕ ключа, а не ключ нулевой длины: от
        // разницы зависит и партиционирование, и compaction.
        key: Some(request.key).filter(|k| !k.is_empty()).map(String::into_bytes),
        headers: request.headers,
        payload,
    };
    worker
        .call(|reply| Command::Produce(record, reply))
        .await?
}

// --- Сохранённые сообщения ----------------------------------------------------
//
// Всё, что меняет архив, возвращает его ЦЕЛИКОМ вместе с занятым местом — тот
// же приём, что у схем топиков: досчитывать новое состояние на фронте значило
// бы разъезжаться с диском, а размеры там всё равно неоткуда взять.
//
// И через `blocking` по той же причине: превью сохранённого сообщения строится
// схемой его топика, а за avro-схемой ходят в Schema Registry.

#[tauri::command]
async fn list_favorites(app: tauri::AppHandle) -> Result<FavoritesView, String> {
    blocking(move || favorites::list(&app)).await
}

/// Сохраняет сообщение так, чтобы оно пережило и перезапуск, и retention.
///
/// Тело берётся у воркера СЫРЫМИ байтами, а не приезжает с фронта готовой
/// строкой: на фронте оно уже прошло через нашу схему и `from_utf8_lossy`, а
/// схему к топику загружают когда угодно — в том числе после сохранения. Байты
/// разберутся и завтра, испорченный текст — уже нет.
///
/// `partition` и `offset` присылаются вместе с индексом строки и сверяются с
/// тем, что нашлось: индекс живёт ровно до следующей смены фильтра или
/// сортировки, а модалка к этому моменту может стоять открытой.
#[tauri::command]
async fn save_favorite(
    app: tauri::AppHandle,
    worker: tauri::State<'_, WorkerHandle>,
    request: SaveFavoriteRequest,
) -> Result<SaveFavoriteResult, String> {
    let raw = worker
        .call(|reply| Command::GetRaw(request.index, reply))
        .await??;
    if raw.partition != request.partition || raw.offset != request.offset {
        return Err("the message list has changed; reopen the message and try again".into());
    }
    blocking(move || {
        favorites::save(
            &app,
            &request.cluster,
            &request.cluster_name,
            &request.topic,
            &raw,
        )
    })
    .await
}

#[tauri::command]
async fn delete_favorite(app: tauri::AppHandle, id: String) -> Result<FavoritesView, String> {
    blocking(move || favorites::delete(&app, &id)).await
}

#[tauri::command]
async fn clear_favorites(app: tauri::AppHandle) -> Result<FavoritesView, String> {
    blocking(move || favorites::clear(&app)).await
}

/// Тело сохранённого сообщения — только когда его открыли, как и у строки
/// таблицы, и разобранное сегодняшней схемой топика.
#[tauri::command]
async fn get_favorite(app: tauri::AppHandle, id: String) -> Result<SavedMessage, String> {
    blocking(move || favorites::message(&app, &id)).await
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
            describe_topic,
            open_topic,
            load_more,
            get_open_topic_progress,
            set_filter,
            set_sort,
            get_window,
            get_message_body,
            close_topic,
            get_topic_schema,
            add_proto_files,
            refresh_proto_files,
            remove_proto_file,
            save_topic_schema,
            apply_topic_schema,
            add_avro_files,
            refresh_avro_files,
            remove_avro_file,
            save_topic_avro_subject,
            save_topic_avro_record,
            save_schema_registry,
            delete_schema_registry,
            test_schema_registry,
            list_registry_subjects,
            list_subject_versions,
            proto_message_form,
            avro_message_form,
            check_produce_payload,
            produce_message,
            list_favorites,
            save_favorite,
            delete_favorite,
            clear_favorites,
            get_favorite,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
