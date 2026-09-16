// Learn more about Tauri commands at https://tauri.app/develop/calling-rust/

mod config;
// Публичный ради интеграционного теста: обработчик паники глобальный и
// ставится один раз на процесс, поэтому проверить его можно только в отдельном
// тестовом бинарнике (`tests/panic_log.rs`), а тот видит лишь публичное.
pub mod diag;
mod favorites;
mod helpers;
mod kafka;
mod link;
mod schema;

use config::{ClusterConfig, ClusterUser, SchemaRegistry, Settings};
use favorites::{FavoritesView, SaveFavoriteRequest, SaveFavoriteResult, SavedMessage};
use kafka::*;
use schema::{BodyFormat, LensSetting, MessageForm, TopicSchemaView};

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

/// Подставляет пароли из keychain, если фронт их не прислал.
///
/// Для сохранённого кластера пароли вообще не пересекают границу IPC: фронт
/// шлёт только идентификаторы, а значения подтягиваются здесь.
fn resolve_secrets(payload: &mut ClusterConnectPayload) -> Result<(), String> {
    let sasl_provided = payload.password.as_deref().is_some_and(|p| !p.is_empty());
    if !sasl_provided {
        // `id` как запасной ключ — ради записей, которые ещё не пережили
        // миграцию на список пользователей: там ключом в keychain был
        // идентификатор кластера.
        let key = payload.user_id.clone().or_else(|| payload.id.clone());
        if let Some(key) = key {
            payload.password = config::secrets::read_password(&key)?;
        }
    }

    // Пароль приватного ключа — только когда ключ вообще задан. Лишний поход в
    // хранилище секретов не бесплатен: на macOS он в худшем случае показывает
    // диалог доступа, и делать это на каждом подключении к кластеру без mTLS
    // незачем.
    let key_provided = payload
        .ssl_key_password
        .as_deref()
        .is_some_and(|p| !p.is_empty());
    let uses_client_key = payload
        .ssl_key_path
        .as_deref()
        .is_some_and(|p| !p.trim().is_empty());
    if !key_provided && uses_client_key {
        if let Some(id) = payload.id.as_deref() {
            let key = ClusterConfig::key_password_secret_key(id);
            payload.ssl_key_password = config::secrets::read_password(&key)?;
        }
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
    resolve_secrets(&mut payload)?;
    let id = payload.id.clone();
    let user_id = payload.user_id.clone();
    worker
        .call(|reply| Command::Connect(payload, reply))
        .await??;

    // Чем кластер представился. Спрашиваем сразу после подключения, потому что
    // это единственный момент, когда его можно узнать: сохранённая запись
    // своего `cluster.id` не содержит, пока к кластеру хоть раз не подключились
    // (см. `crate::link`), и именно из-за этого ссылка на сообщение может не
    // открыться при полностью настроенном подключении.
    let cluster_id = worker.call(Command::ClusterId).await?;

    // Подключение удалось — отмечаем кластер как недавно использованный и
    // запоминаем учётку. Не критично, поэтому ошибку записи не поднимаем наверх.
    if let Some(id) = id {
        if let Err(e) = touch_cluster(&app, &id, user_id.as_deref(), cluster_id) {
            eprintln!("can't update last_used for cluster {id}: {e}");
        }
    }
    Ok(())
}

/// Отмечает кластер использованным и запоминает, под кем подключились: иначе
/// выбор пользователя не пережил бы перезапуск приложения.
///
/// Заодно записывает `cluster.id`, которым кластер представился. Место то же по
/// той же причине: и то, и другое известно только после успешного подключения и
/// должно пережить перезапуск.
fn touch_cluster(
    app: &tauri::AppHandle,
    id: &str,
    user_id: Option<&str>,
    kafka_cluster_id: Option<String>,
) -> Result<(), String> {
    let mut clusters = config::load_clusters(app)?;
    let Some(cluster) = clusters.iter_mut().find(|c| c.id == id) else {
        return Ok(());
    };
    cluster.last_used = Some(chrono::Utc::now().to_rfc3339());
    // Только если кластер его назвал: молчание — это «спросить не удалось», а
    // не «идентификатора нет», и затирать им уже записанный нельзя.
    if kafka_cluster_id.is_some() {
        cluster.kafka_cluster_id = kafka_cluster_id;
    }
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
    resolve_secrets(&mut payload)?;
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
///
/// `key_password` — пароль приватного ключа для mTLS, по тем же правилам, что и
/// пароли учёток: непустая строка — записать в keychain, пустая — удалить
/// оттуда, `None` — не трогать сохранённый. Отдельным аргументом, а не полем
/// записи, ровно потому, что в саму запись секретам путь закрыт.
#[tauri::command]
async fn save_cluster(
    app: tauri::AppHandle,
    cluster: ClusterConfig,
    key_password: Option<String>,
) -> Result<ClusterConfig, String> {
    let mut cluster = cluster;
    let mut clusters = config::load_clusters(&app)?;

    if let Some(existing) = clusters.iter().find(|c| c.id == cluster.id) {
        cluster.users = existing.users.clone();
        cluster.active_user_id = existing.active_user_id.clone();
        cluster.schema_registry = existing.schema_registry.clone();
        cluster.has_key_password = existing.has_key_password;
    } else {
        // Новой записи наследовать нечего, а форма о содержимом keychain не
        // знает: признак считается ниже, по тому, что пришло вместе с ней.
        cluster.has_key_password = false;
    }

    let secret_key = ClusterConfig::key_password_secret_key(&cluster.id);
    match key_password.as_deref() {
        Some("") => {
            config::secrets::delete_password(&secret_key)?;
            cluster.has_key_password = false;
        }
        Some(secret) => {
            config::secrets::store_password(&secret_key, secret)?;
            cluster.has_key_password = true;
        }
        // Поле пришло пустым просто потому, что пароль не меняли.
        None => {}
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
    // Секреты самого кластера — реестра и приватного ключа — лежат там же, под
    // ключами с суффиксом. Раньше они переживали удаление кластера и оставались
    // в хранилище навсегда: ссылок на них после этого нет ни у кого.
    for key in [
        SchemaRegistry::secret_key(&id),
        ClusterConfig::key_password_secret_key(&id),
    ] {
        if let Err(e) = config::secrets::delete_password(&key) {
            eprintln!("can't delete secret {key}: {e}");
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

/// Самое свежее записанное падение, если оно было.
///
/// `None` — падений не было, и показывать пользователю нечего. Именно по
/// файлам, а не по флагу в памяти: упавшее в прошлый запуск важно не меньше, а
/// обычно больше — как раз его и просят прислать.
#[tauri::command]
async fn panic_log() -> Option<String> {
    diag::latest_crash().map(|path| path.display().to_string())
}

/// Показывает журнал паник в файловом менеджере.
///
/// Открывается КАТАЛОГ с выделенным файлом, а не сам файл: `.log` в системе
/// обычно ни на что не назначен, и «открыть» его — это либо диалог выбора
/// программы, либо ничего. А из каталога файл можно перетащить в переписку —
/// и заодно видно остальные падения, если пригодятся.
#[tauri::command]
async fn reveal_panic_log(app: tauri::AppHandle) -> Result<(), String> {
    use tauri_plugin_opener::OpenerExt;

    let path =
        diag::latest_crash().ok_or("nothing has crashed yet — there is no log".to_string())?;
    app.opener()
        .reveal_item_in_dir(&path)
        .map_err(|e| format!("can't open the folder with the log: {e}"))
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

/// Читает топик до конца, оставляя в буфере только сообщения, попавшие под
/// фильтр. Долгая операция: она и задумана такой — см. `ReadDepth::WholeTopic`.
///
/// Параметры те же, что у `open_topic` (кроме `limit`, который здесь не при
/// чём): поиск заново открывает топик с того же конца и в тех же границах.
#[tauri::command]
async fn deep_search(
    worker: tauri::State<'_, WorkerHandle>,
    params: OpenTopicParams,
) -> Result<OpenTopicResult, String> {
    worker
        .call(|reply| Command::DeepSearch(params, reply))
        .await?
}

/// Останавливает глубокий поиск и сбрасывает буфер. Топик после этого положено
/// перечитать обычным `open_topic` — иначе в таблице останутся одни находки.
///
/// `false` — останавливать было нечего: поиск успел закончиться сам, и его
/// результат в буфере законный. Перечитывать топик в этом случае не надо.
#[tauri::command]
async fn stop_search(worker: tauri::State<'_, WorkerHandle>) -> Result<bool, String> {
    worker.call(Command::StopSearch).await
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

/// Где в таблице стоит сообщение с такими координатами. `None` — его нет в
/// буфере. Нужно переходу по ссылке: она называет партицию и офсет, а таблица
/// и тело работают по индексу строки.
#[tauri::command]
async fn find_message(
    worker: tauri::State<'_, WorkerHandle>,
    partition: i32,
    offset: i64,
) -> Result<Option<usize>, String> {
    worker
        .call(|reply| Command::FindMessage {
            partition,
            offset,
            reply,
        })
        .await?
}

#[tauri::command]
async fn close_topic(worker: tauri::State<'_, WorkerHandle>) -> Result<(), String> {
    worker.call(Command::CloseTopic).await
}

// --- Ссылка на сообщение ------------------------------------------------------
//
// Формат ссылки и её разбор живут в `crate::link` и ничего не знают ни про
// Tauri, ни про Kafka. Здесь то, чего они знать не могут: чем мы подключены
// прямо сейчас и какие подключения сохранены на этой машине.

/// Ссылка на сообщение — то, что кладётся в буфер обмена.
///
/// `cluster` — ключ схем (идентификатор сохранённого подключения либо имя
/// подключения из формы), нужен только ради подсказки о формате тела. Сам
/// кластер в ссылке назван не им, а идентификатором, который выдал брокер:
/// локальный ключ на чужой машине не значит ничего.
#[tauri::command]
async fn build_message_link(
    app: tauri::AppHandle,
    worker: tauri::State<'_, WorkerHandle>,
    request: link::ShareRequest,
) -> Result<String, String> {
    let link::ShareRequest {
        cluster,
        cluster_name,
        topic,
        partition,
        offset,
        timestamp,
    } = request;

    let cluster_id = worker.call(Command::ClusterId).await?.ok_or_else(|| {
        "this cluster does not report a cluster id, so there is nothing a link could point at"
            .to_string()
    })?;

    // Чем отправитель читает этот топик. Схема по ссылке не едет — локальные
    // .proto лежат у отправителя на диске, — но сказать, чего получателю не
    // хватает, стоит ровно ничего.
    //
    // Сломанная или недоступная схема здесь не ошибка: не поделиться ссылкой
    // из-за того, что реестр не отвечает, было бы куда обиднее, чем поделиться
    // ею без подсказки.
    let hint = match cluster {
        Some(cluster) => {
            let topic = topic.clone();
            blocking(move || Ok(schema::view(&app, &cluster, &topic).ok().flatten())).await?
        }
        None => None,
    };
    let (format, type_name) = hint.map(schema_hint).unwrap_or_default();

    Ok(link::MessageLink {
        cluster_id,
        cluster_name: cluster_name.filter(|n| !n.is_empty()),
        topic,
        partition,
        offset,
        // Отрицательное время означает, что его не выставил ни продюсер, ни
        // брокер. Врать им в ссылке незачем — подсказки просто не будет.
        timestamp: (timestamp > 0).then_some(timestamp),
        format,
        type_name,
    }
    .to_url())
}

/// Формат тела и имя типа внутри схемы — то, что уезжает в ссылку подсказкой.
///
/// Имя берётся оттуда, где оно у формата живёт: у protobuf это выбранный
/// message, у avro — subject реестра либо запись из .avsc, у JSON Schema —
/// subject. У `json`, `text` и `hex` имени нет и быть не может: там нет схемы.
fn schema_hint(view: TopicSchemaView) -> (Option<String>, Option<String>) {
    let format = view.format;
    let type_name = match format {
        BodyFormat::Proto => view.message,
        BodyFormat::Avro => view.avro.and_then(|a| a.subject.or(a.record)),
        BodyFormat::JsonSchema => view.json.and_then(|j| j.subject),
        BodyFormat::Json | BodyFormat::Text | BodyFormat::Hex => None,
    };
    (Some(format.as_str().to_string()), type_name)
}

/// Куда ведёт ссылка на этой машине. Ошибка отсюда показывается человеку как
/// есть — см. `link::no_such_connection`.
#[tauri::command]
async fn resolve_message_link(app: tauri::AppHandle, url: String) -> Result<link::Target, String> {
    blocking(move || link::resolve(&url, &config::load_clusters(&app)?)).await
}

/// Ссылка, с которой приложение запустили или которую ему передала система,
/// пока оно уже работало. Забирается ровно один раз — см. `PendingLink`.
#[tauri::command]
fn take_pending_link(pending: tauri::State<'_, PendingLink>) -> Option<String> {
    pending.take()
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
    let (built, key, lens) = {
        let (app, cluster, topic) = (app.clone(), cluster.clone(), topic.clone());
        blocking(move || {
            let value = schema::decoder(&app, &cluster, &topic);
            // Ключ отдельно от тела и молча: не разобрался — поедет байтами,
            // как ездил всегда. Ошибка тела — новость (её показывают в
            // настройках топика), ошибка ключа — нет.
            let key = schema::key_decoder(&app, &cluster, &topic).ok().flatten();
            // Линза от схемы не зависит совсем, но приезжает тем же заходом:
            // относится она к тому же топику и разъехаться с декодерами во
            // времени не должна.
            let lens = schema::lens_of(&app, &cluster, &topic);
            Ok((value, key, lens))
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
            lens: lens.into(),
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

/// Сохраняет выбор линзы. `lens` пуст — вернуть распознаванию право решать.
///
/// Отдельно от `save_topic_schema`, хотя лежат они в одной записи: формат
/// меняют в модалке настроек и применяют кнопкой, а линзу — селектором в шапке,
/// и она обязана примениться сразу. Воркеру о ней говорит `apply_topic_schema`,
/// который фронт зовёт следом.
#[tauri::command]
async fn save_topic_lens(
    app: tauri::AppHandle,
    cluster: String,
    topic: String,
    lens: Option<LensSetting>,
) -> Result<TopicSchemaView, String> {
    blocking(move || schema::set_lens(&app, &cluster, &topic, lens)).await
}

// --- JSON-схемы топиков --------------------------------------------------------
//
// Тот же набор операций, что у avro, минус выбор записи: у JSON Schema корень
// один, и выбирать не из чего.

#[tauri::command]
async fn add_json_files(
    app: tauri::AppHandle,
    cluster: String,
    topic: String,
    paths: Vec<String>,
) -> Result<TopicSchemaView, String> {
    blocking(move || schema::add_json_files(&app, &cluster, &topic, &paths)).await
}

#[tauri::command]
async fn refresh_json_files(
    app: tauri::AppHandle,
    cluster: String,
    topic: String,
    name: Option<String>,
) -> Result<TopicSchemaView, String> {
    blocking(move || schema::refresh_json_files(&app, &cluster, &topic, name.as_deref())).await
}

#[tauri::command]
async fn remove_json_file(
    app: tauri::AppHandle,
    cluster: String,
    topic: String,
    name: String,
) -> Result<Option<TopicSchemaView>, String> {
    blocking(move || schema::remove_json_file(&app, &cluster, &topic, &name)).await
}

/// Привязывает топик к subject реестра. `subject` пуст — отвязать.
#[tauri::command]
async fn save_topic_json_subject(
    app: tauri::AppHandle,
    cluster: String,
    topic: String,
    subject: Option<String>,
    version: Option<i32>,
) -> Result<TopicSchemaView, String> {
    let subject = subject.filter(|s| !s.is_empty());
    blocking(move || schema::set_json_subject(&app, &cluster, &topic, subject, version)).await
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
    blocking(move || schema::test_registry(cluster_id.as_deref(), &registry, password.as_deref()))
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
        PayloadFormat::JsonSchema => {
            let cluster = cluster.ok_or("not connected to a cluster")?;
            schema::encode_json(
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

/// И для JSON Schema — на тех же условиях, что и avro.
#[tauri::command]
async fn json_message_form(
    app: tauri::AppHandle,
    cluster: String,
    topic: String,
    subject: Option<String>,
) -> Result<MessageForm, String> {
    blocking(move || schema::json_message_form(&app, &cluster, &topic, subject.as_deref())).await
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
        key: Some(request.key)
            .filter(|k| !k.is_empty())
            .map(String::into_bytes),
        headers: request.headers,
        payload,
    };
    worker.call(|reply| Command::Produce(record, reply)).await?
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
/// Ссылка, которую система принесла приложению.
///
/// Слот, а не событие, из-за холодного старта: когда по ссылке приложение
/// ЗАПУСКАЮТ, URL приезжает раньше, чем появляется webview, — событие уходить
/// тогда некому. Поэтому URL всегда кладётся сюда, а фронт забирает его сам:
/// один раз при старте и потом на каждый звонок в дверь (`LINK_EVENT`).
///
/// Забирается ровно один раз («take»), и это важнее, чем кажется: у ссылки
/// должен быть единственный потребитель, иначе один и тот же переход выполнялся
/// бы дважды — событием и опросом.
#[derive(Default)]
struct PendingLink(std::sync::Mutex<Option<String>>);

impl PendingLink {
    fn put(&self, url: String) {
        if let Ok(mut slot) = self.0.lock() {
            *slot = Some(url);
        }
    }

    fn take(&self) -> Option<String> {
        self.0.lock().ok().and_then(|mut slot| slot.take())
    }
}

/// Звонок в дверь: «система принесла ссылку, забери её».
///
/// Самого URL в событии нет намеренно — см. `PendingLink`.
const LINK_EVENT: &str = "mikui://link";

/// Показывает окно и забирает фокус.
///
/// Нужно на каждый переход по ссылке. Открывая `mikui://…`, система запускает
/// приложение, но НЕ выводит его вперёд, если оно уже работало: браузер (или
/// что там было) остаётся активным, а mikui показывает диалог перехода где-то
/// позади, и человек ищет его сам. Приложение обязано выйти вперёд само.
///
/// Три шага, а не один: `set_focus` на macOS ничего не делает у свёрнутого или
/// спрятанного окна (см. `tao`), поэтому сначала развернуть и показать.
fn bring_to_front(app: &tauri::AppHandle) {
    use tauri::Manager;
    if let Some(window) = app.get_webview_window("main") {
        let _ = window.unminimize();
        let _ = window.show();
        let _ = window.set_focus();
    }
}

pub fn run() {
    let builder = tauri::Builder::default();

    // Единственный экземпляр — и он обязан подключаться ПЕРВЫМ плагином, иначе
    // вторая копия успеет подняться до проверки. macOS этого не требует: там
    // система сама доставляет `mikui://…` уже запущенному приложению.
    #[cfg(any(windows, target_os = "linux"))]
    let builder = builder.plugin(tauri_plugin_single_instance::init(|app, _argv, _cwd| {
        // Ссылку из аргументов второй копии передаст плагину ссылок сам
        // single-instance (фича `deep-link`), и она приедет в тот же
        // `on_open_url`, что и на macOS, — вместе с выходом окна вперёд.
        // Здесь то же самое ради запуска второй копии БЕЗ ссылки: человек
        // кликнул по иконке, а мы должны показать уже работающее окно.
        bring_to_front(app);
    }));

    builder
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_opener::init())
        .plugin(tauri_plugin_deep_link::init())
        .manage(WorkerHandle::spawn())
        .manage(PendingLink::default())
        .setup(|app| {
            use tauri::{Emitter, Manager};
            use tauri_plugin_deep_link::DeepLinkExt;

            // Первым делом: до этой строки паника уходит только в stderr,
            // которого у собранного бандла нет. Раньше, чем здесь, поставить
            // обработчик не получится — каталог настроек разрешает `app`.
            match config::config_dir(app.handle()) {
                Ok(dir) => diag::install(&dir),
                Err(e) => eprintln!("can't set up the panic log: {e}"),
            }

            // Ручка воркера живёт в state с момента сборки билдера, то есть
            // раньше, чем появляется `AppHandle`. Отдаём его сюда, чтобы
            // перезапуск воркера мог позвать фронт.
            app.state::<WorkerHandle>().attach(app.handle().clone());

            // Схему регистрирует система, а не приложение: на macOS — по
            // Info.plist собранного бандла, на Windows и Linux — установщик.
            // В отладочной сборке ни того, ни другого нет, поэтому под
            // Windows и Linux регистрируем схему на себя сами. На macOS так
            // нельзя, и проверять ссылки там приходится либо собранным
            // бандлом, либо через «Open link…» в самом приложении.
            #[cfg(all(debug_assertions, any(windows, target_os = "linux")))]
            if let Err(e) = app.deep_link().register_all() {
                eprintln!("can't register the mikui:// scheme: {e}");
            }

            // Приложение ЗАПУСТИЛИ ссылкой: URL уже здесь, и события по нему
            // не будет.
            if let Ok(Some(urls)) = app.deep_link().get_current() {
                if let Some(url) = urls.into_iter().next() {
                    app.state::<PendingLink>().put(url.to_string());
                }
            }

            let handle = app.handle().clone();
            app.deep_link().on_open_url(move |event| {
                // Ссылок в событии может быть несколько (так бывает, когда
                // система отдаёт пачку). Открыть можно только одну — берём
                // первую, а не молча делаем вид, что их не было.
                let Some(url) = event.urls().into_iter().next() else {
                    return;
                };
                handle.state::<PendingLink>().put(url.to_string());
                let _ = handle.emit(LINK_EVENT, ());
                bring_to_front(&handle);
            });

            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            cluster_connect,
            cluster_disconnect,
            cluster_test,
            list_clusters,
            save_cluster,
            delete_cluster,
            save_cluster_user,
            delete_cluster_user,
            get_settings,
            save_settings,
            panic_log,
            reveal_panic_log,
            get_topics,
            describe_topic,
            open_topic,
            load_more,
            deep_search,
            stop_search,
            get_open_topic_progress,
            set_filter,
            set_sort,
            get_window,
            get_message_body,
            find_message,
            close_topic,
            build_message_link,
            resolve_message_link,
            take_pending_link,
            get_topic_schema,
            add_proto_files,
            refresh_proto_files,
            remove_proto_file,
            save_topic_schema,
            save_topic_lens,
            apply_topic_schema,
            add_avro_files,
            refresh_avro_files,
            remove_avro_file,
            save_topic_avro_subject,
            save_topic_avro_record,
            add_json_files,
            refresh_json_files,
            remove_json_file,
            save_topic_json_subject,
            save_schema_registry,
            delete_schema_registry,
            test_schema_registry,
            list_registry_subjects,
            list_subject_versions,
            proto_message_form,
            avro_message_form,
            json_message_form,
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
