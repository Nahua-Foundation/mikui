/**
 * Единственное место, где фронт разговаривает с Rust.
 *
 * Раньше `invoke` был раскидан по компонентам, из-за чего, например, клик по
 * кластеру в архиве только красил строку и показывал «Connected», не подключаясь
 * на самом деле. Один типизированный слой такие расхождения исключает.
 */
import { invoke } from '@tauri-apps/api/core';
import {
  BodyFormat,
  ClusterConnectPayload,
  ClusterUser,
  FavoritesView,
  FavoriteTopics,
  FAVORITE_TOPICS_KEY,
  FullMessage,
  KafkaCluster,
  LoadMoreParams,
  MessageFilter,
  MessageLinkTarget,
  OpenTopicParams,
  OpenTopicProgress,
  OpenTopicResult,
  LensSetting,
  PayloadIssue,
  ProduceRequest,
  ProduceResult,
  MessageForm,
  RowPreview,
  SavedMessage,
  SchemaRegistryConfig,
  SaveFavoriteResult,
  Settings,
  parseFavoriteTopics,
  ShareRequest,
  Topic,
  TopicDetails,
  TopicSchema,
} from './types';

/**
 * Текст ошибки, пришедшей из Rust.
 *
 * Пустое или невнятное `e` превратилось бы в тост «Failed to …: », который
 * ничего не сообщает и выглядит как поломка самого приложения.
 */
export function describeError(e: unknown): string {
  const text = e instanceof Error ? e.message : String(e ?? '');
  return text.trim() || 'unknown error';
}

// --- Подключение ------------------------------------------------------------

/**
 * Payload для сохранённого кластера: пароли не передаём, Rust возьмёт их из
 * keychain — пароль учётки по `user_id`, пароль ключа по `id` кластера.
 */
export function clusterToPayload(
  cluster: KafkaCluster,
  user?: ClusterUser | null,
): ClusterConnectPayload {
  return {
    id: cluster.id,
    user_id: user?.id,
    brokers: cluster.brokers,
    security_protocol: cluster.security_protocol,
    sasl_mechanism: cluster.sasl_mechanism,
    username: user?.username,
    ssl_ca_bundle_path: cluster.ssl_ca_bundle_path,
    ssl_certificate_path: cluster.ssl_certificate_path,
    ssl_key_path: cluster.ssl_key_path,
    ssl_skip_hostname_check: cluster.ssl_skip_hostname_check,
    ssl_skip_certificate_verification: cluster.ssl_skip_certificate_verification,
  };
}

/**
 * Под кем подключаться к кластеру: та учётка, что использовалась в прошлый раз,
 * иначе первая известная. `null` — логинов нет вовсе (PLAINTEXT-кластер).
 */
export function activeUser(cluster: KafkaCluster): ClusterUser | null {
  const users = cluster.users ?? [];
  return users.find((u) => u.id === cluster.active_user_id) ?? users[0] ?? null;
}

export const clusterConnect = (payload: ClusterConnectPayload) =>
  invoke<void>('cluster_connect', { payload });

export const clusterTest = (payload: ClusterConnectPayload) =>
  invoke<void>('cluster_test', { payload });

export const clusterDisconnect = () => invoke<void>('cluster_disconnect');

// --- Сохранённые подключения ------------------------------------------------

export const listClusters = () => invoke<KafkaCluster[]>('list_clusters');

/**
 * Сохраняет параметры подключения. Список пользователей бэкенд берёт из уже
 * сохранённой записи, а не отсюда: им заведуют `saveClusterUser`/`deleteClusterUser`.
 *
 * `keyPassword` — пароль приватного ключа для mTLS, по общему для всех паролей
 * правилу: непустая строка — записать в keychain, пустая — удалить,
 * `undefined` — не трогать сохранённый.
 */
export const saveCluster = (cluster: KafkaCluster, keyPassword?: string) =>
  invoke<KafkaCluster>('save_cluster', { cluster, keyPassword });

export const deleteCluster = (id: string) => invoke<void>('delete_cluster', { id });

// --- Kafka-пользователи кластера --------------------------------------------

/**
 * `password`: непустая строка — записать в keychain; пустая — удалить оттуда;
 * `undefined` — не трогать сохранённый. Возвращает кластер целиком.
 *
 * `activate` — сделать учётку текущей для кластера. Нужно там, где логин
 * ВЫБИРАЮТ (настройки подключения), и не нужно там, где его просто заводят.
 */
export const saveClusterUser = (
  clusterId: string,
  user: ClusterUser,
  password?: string,
  activate?: boolean,
) => invoke<KafkaCluster>('save_cluster_user', { clusterId, user, password, activate });

export const deleteClusterUser = (clusterId: string, userId: string) =>
  invoke<KafkaCluster>('delete_cluster_user', { clusterId, userId });

// --- Настройки --------------------------------------------------------------

export const getSettings = () => invoke<Settings>('get_settings');
export const saveSettings = (settings: Settings) => invoke<void>('save_settings', { settings });

export const loadFavoriteTopics = async (): Promise<FavoriteTopics> =>
  parseFavoriteTopics((await getSettings())[FAVORITE_TOPICS_KEY]);

/**
 * Очередь записей избранного. Каждая запись — это «прочитать файл, подменить
 * один ключ, записать обратно», и две таких внахлёст разъезжаются: звезду
 * ставят кликом, клики идут подряд, а какая из двух записей ляжет на диск
 * последней, порядок кликов не решает. Вторая ждёт первую, и читает уже
 * записанное ею.
 */
let favoritesWrite: Promise<void> = Promise.resolve();

/**
 * Записывает избранное, сохраняя остальные настройки.
 *
 * Файл перечитывается прямо перед записью, а не берётся из того, что фронт
 * прочитал при старте: `save_settings` пишет файл целиком, и снимок недельной
 * давности затёр бы всё, что появилось в нём после.
 */
export const saveFavoriteTopics = (favorites: FavoriteTopics): Promise<void> => {
  // Хвост очереди — всегда успешный: неудача одной записи не должна ронять
  // следующую, у той свои данные и своя причина не получиться.
  const write = favoritesWrite.then(async () => {
    const settings = await getSettings();
    await saveSettings({ ...settings, [FAVORITE_TOPICS_KEY]: favorites });
  });
  favoritesWrite = write.catch(() => {});
  return write;
};

// --- Топики и сообщения -----------------------------------------------------

export const getTopics = () => invoke<Topic[]>('get_topics');

/** Устройство и настройки топика. Открытый топик не трогает. */
export const describeTopic = (topic: string) =>
  invoke<TopicDetails>('describe_topic', { topic });

export const openTopic = (params: OpenTopicParams) =>
  invoke<OpenTopicResult>('open_topic', { params });

export const loadMore = (params: LoadMoreParams) =>
  invoke<OpenTopicResult>('load_more', { params });

/**
 * Прочитать топик до конца, оставив в буфере только совпадения с фильтром.
 *
 * Параметры те же, что у `openTopic`, и это не совпадение: поиск ЗАНОВО
 * открывает топик с того же конца и в тех же границах — продолжить с места,
 * где встало обычное чтение, нельзя (в буфере лежит непросеянное, и места под
 * находки в нём может не остаться). `limit` бэкенд здесь игнорирует.
 *
 * Долгая операция; отменяется `stopSearch`. Отмена приезжает сюда ошибкой
 * `SEARCH_STOPPED` — это не сбой.
 */
export const deepSearch = (params: OpenTopicParams) =>
  invoke<OpenTopicResult>('deep_search', { params });

/**
 * Остановить глубокий поиск. Буфер после этого пуст: в нём лежали одни находки,
 * и показывать их как «весь топик» было бы обманом — топик положено перечитать
 * обычным `openTopic`.
 *
 * `false` — останавливать было нечего: поиск успел закончиться сам, пока летела
 * команда. Тогда буфер не тронут и перечитывать НЕ надо — иначе клик по «стоп»
 * в последнюю секунду стирал бы только что найденное.
 */
export const stopSearch = () => invoke<boolean>('stop_search');

export const getOpenTopicProgress = () =>
  invoke<OpenTopicProgress>('get_open_topic_progress');

export const setFilter = (filter: MessageFilter) => invoke<number>('set_filter', { filter });

export const getWindow = (start: number, count: number) =>
  invoke<RowPreview[]>('get_window', { start, count });

export const getMessageBody = (index: number) =>
  invoke<FullMessage>('get_message_body', { index });

/**
 * Где в таблице стоит сообщение с такими координатами. `null` — его нет в
 * загруженном буфере (не вычитали либо отфильтровали).
 *
 * Нужно переходу по ссылке: она называет партицию и офсет, а таблица и тело
 * работают по индексу строки, и посчитать его на фронте нечем — ни буфера, ни
 * текущего фильтра здесь нет.
 */
export const findMessage = (partition: number, offset: number) =>
  invoke<number | null>('find_message', { partition, offset });

export const closeTopic = () => invoke<void>('close_topic');

// --- Ссылка на сообщение --------------------------------------------------------

/** Ссылка на сообщение открытого топика — та, что кладётся в буфер обмена. */
export const buildMessageLink = (request: ShareRequest) =>
  invoke<string>('build_message_link', { request });

/** Куда ссылка ведёт на этой машине. Ошибку показываем как есть: она объясняет
 *  и почему подключение не нашлось. */
export const resolveMessageLink = (url: string) =>
  invoke<MessageLinkTarget>('resolve_message_link', { url });

/**
 * Ссылка, которую система принесла приложению: с ней его запустили либо
 * передали уже работающему.
 *
 * Забирается один раз — второй вызов вернёт `null`. Так и задумано: у ссылки
 * должен быть единственный потребитель, иначе один и тот же переход выполнялся
 * бы дважды (см. `PendingLink` в lib.rs).
 */
export const takePendingLink = () => invoke<string | null>('take_pending_link');

// --- Сохранённые сообщения ----------------------------------------------------
//
// Всё, что меняет архив, возвращает его ЦЕЛИКОМ вместе с занятым местом: тот же
// приём, что у схем топиков. Досчитывать размеры на фронте всё равно нечем —
// их знает только диск.

export const listFavorites = () => invoke<FavoritesView>('list_favorites');

/**
 * Сохраняет сообщение на диск.
 *
 * Тело не передаётся: бэкенд берёт его у воркера СЫРЫМИ байтами по индексу
 * строки. То, что лежит здесь, уже прошло через схему и `from_utf8_lossy` — а
 * схему к топику загружают когда угодно, в том числе после сохранения, и
 * разбирать испорченный текст назавтра было бы нечем. `partition` и `offset`
 * едут для сверки: индекс живёт до ближайшей смены фильтра или сортировки.
 *
 * Повторное сохранение того же сообщения обновляет запись, а не заводит вторую
 * — об этом говорит `updated` в ответе.
 */
export const saveFavorite = (request: {
  cluster: string;
  cluster_name: string;
  topic: string;
  index: number;
  partition: number;
  offset: number;
}) => invoke<SaveFavoriteResult>('save_favorite', { request });

export const deleteFavorite = (id: string) => invoke<FavoritesView>('delete_favorite', { id });

export const clearFavorites = () => invoke<FavoritesView>('clear_favorites');

/** Тело сохранённого сообщения — по требованию, как `getMessageBody` у строки
 *  таблицы: в списке его нет. Разбирается сегодняшней схемой того топика,
 *  откуда сообщение, а не того, что открыт сейчас. */
export const getFavorite = (id: string) => invoke<SavedMessage>('get_favorite', { id });

// --- Protobuf-схемы топиков --------------------------------------------------
//
// Каждая операция возвращает схему ЦЕЛИКОМ: список файлов, список message и
// выбранный из них меняются вместе, и досчитывать новое состояние на фронте
// значило бы разъезжаться с диском. Неудача не меняет на диске ничего —
// достаточно показать ошибку и оставить показанное как есть.

export const getTopicSchema = (cluster: string, topic: string) =>
  invoke<TopicSchema | null>('get_topic_schema', { cluster, topic });

/** Добавляет .proto. Невалидный набор не сохраняется вовсе — прилетит ошибка. */
export const addProtoFiles = (cluster: string, topic: string, paths: string[]) =>
  invoke<TopicSchema>('add_proto_files', { cluster, topic, paths });

/** Перечитывает .proto с диска: `name` — конкретный файл, иначе все. */
export const refreshProtoFiles = (cluster: string, topic: string, name?: string) =>
  invoke<TopicSchema>('refresh_proto_files', { cluster, topic, name: name ?? null });

export const removeProtoFile = (cluster: string, topic: string, name: string) =>
  invoke<TopicSchema | null>('remove_proto_file', { cluster, topic, name });

/** Сохраняет выбор из формы: формат тела и основной message. */
export const saveTopicSchema = (
  cluster: string,
  topic: string,
  format: BodyFormat,
  message: string | null,
) => invoke<TopicSchema>('save_topic_schema', { cluster, topic, format, message });

/**
 * Сообщает бэкенду, чем декодировать тела открытого топика.
 *
 * Зовётся перед каждым открытием топика и после каждой правки схемы. Смена
 * схемы не требует перечитывать топик из Kafka: декодирование происходит на
 * выдаче окна, а буфер в Rust хранит сырые байты.
 */
export const applyTopicSchema = (cluster: string, topic: string) =>
  invoke<TopicSchema | null>('apply_topic_schema', { cluster, topic });

// --- Avro-схемы топиков --------------------------------------------------------
//
// Возвращают, как и protobuf-команды, схему ЦЕЛИКОМ: файлы, выбранная запись и
// subject меняются вместе.

/** Добавляет .avsc. Невалидный набор не сохраняется вовсе. */
export const addAvroFiles = (cluster: string, topic: string, paths: string[]) =>
  invoke<TopicSchema>('add_avro_files', { cluster, topic, paths });

export const refreshAvroFiles = (cluster: string, topic: string, name?: string) =>
  invoke<TopicSchema>('refresh_avro_files', { cluster, topic, name: name ?? null });

export const removeAvroFile = (cluster: string, topic: string, name: string) =>
  invoke<TopicSchema | null>('remove_avro_file', { cluster, topic, name });

/** Привязывает топик к subject реестра. `null` — отвязать. Загруженные .avsc
 *  при этом убираются: два источника схемы у одного топика — это два разных
 *  ответа на вопрос «чем декодировать». */
export const saveTopicAvroSubject = (
  cluster: string,
  topic: string,
  subject: string | null,
  version: number | null,
) => invoke<TopicSchema>('save_topic_avro_subject', { cluster, topic, subject, version });

/** Выбирает запись из загруженных .avsc. */
export const saveTopicAvroRecord = (cluster: string, topic: string, record: string | null) =>
  invoke<TopicSchema>('save_topic_avro_record', { cluster, topic, record });

/**
 * Сохраняет выбор линзы. `null` — вернуть распознаванию право решать.
 *
 * Отдельно от `saveTopicSchema`, хотя лежат они в одной записи: формат меняют
 * в модалке и применяют кнопкой, а линзу — селектором в шапке, и она обязана
 * примениться сразу. Воркеру о ней говорит `applyTopicSchema`, который надо
 * позвать следом.
 */
export const saveTopicLens = (cluster: string, topic: string, lens: LensSetting | null) =>
  invoke<TopicSchema>('save_topic_lens', { cluster, topic, lens });

// --- JSON-схемы топиков ---------------------------------------------------------
//
// Тот же набор, что у avro, минус выбор записи: у JSON Schema корень один.

/** Добавляет файлы схемы. Набор, который не компилируется, не сохраняется. */
export const addJsonFiles = (cluster: string, topic: string, paths: string[]) =>
  invoke<TopicSchema>('add_json_files', { cluster, topic, paths });

export const refreshJsonFiles = (cluster: string, topic: string, name?: string) =>
  invoke<TopicSchema>('refresh_json_files', { cluster, topic, name: name ?? null });

export const removeJsonFile = (cluster: string, topic: string, name: string) =>
  invoke<TopicSchema | null>('remove_json_file', { cluster, topic, name });

/** Привязывает топик к subject реестра. `null` — отвязать. */
export const saveTopicJsonSubject = (
  cluster: string,
  topic: string,
  subject: string | null,
  version: number | null,
) => invoke<TopicSchema>('save_topic_json_subject', { cluster, topic, subject, version });

// --- Schema Registry ------------------------------------------------------------
//
// Настройки живут на КЛАСТЕРЕ. Пароль ведёт себя как пароли учёток: непустая
// строка — записать в keychain, пустая — удалить, `undefined` — не трогать.

export const saveSchemaRegistry = (
  clusterId: string,
  registry: SchemaRegistryConfig,
  password?: string,
) => invoke<KafkaCluster>('save_schema_registry', { clusterId, registry, password });

export const deleteSchemaRegistry = (clusterId: string) =>
  invoke<KafkaCluster>('delete_schema_registry', { clusterId });

/** Проверяет настройки, не сохраняя их: сколько subject отдал реестр. */
export const testSchemaRegistry = (
  clusterId: string | null,
  registry: SchemaRegistryConfig,
  password?: string,
) => invoke<number>('test_schema_registry', { clusterId, registry, password });

export const listRegistrySubjects = (cluster: string) =>
  invoke<string[]>('list_registry_subjects', { cluster });

export const listSubjectVersions = (cluster: string, subject: string) =>
  invoke<number[]>('list_subject_versions', { cluster, subject });

// --- Отправка сообщения -------------------------------------------------------

/** Заготовка тела и имена enum-значений выбранного message. */
export const protoMessageForm = (cluster: string, topic: string, message: string) =>
  invoke<MessageForm>('proto_message_form', { cluster, topic, message });

/** То же для Avro. `subject` пуст — взять тот, что назначен топику. */
export const avroMessageForm = (cluster: string, topic: string, subject: string | null) =>
  invoke<MessageForm>('avro_message_form', { cluster, topic, subject });

/** И для JSON Schema — на тех же условиях. `enum_values` там всегда пуст. */
export const jsonMessageForm = (cluster: string, topic: string, subject: string | null) =>
  invoke<MessageForm>('json_message_form', { cluster, topic, subject });

/**
 * Что показать под полем ввода тела, пока его набирают.
 *
 * Проверяет Rust, а не фронт, ровно затем, чтобы предупреждение не могло
 * разойтись с отправкой: и то, и другое считает один и тот же код. Повторить
 * разбор .proto в TypeScript всё равно нечем, а расходиться этим двум местам
 * нельзя — иначе форма разрешала бы отправить то, что бэкенд отвергнет.
 *
 * `null` — претензий нет.
 */
export const checkProducePayload = (cluster: string | null, request: ProduceRequest) =>
  invoke<PayloadIssue | null>('check_produce_payload', { cluster, request });

/** Кладёт сообщение в топик и ждёт отчёта о доставке: партиция и офсет в
 *  ответе — те, что подтвердил брокер. */
export const produceMessage = (cluster: string | null, request: ProduceRequest) =>
  invoke<ProduceResult>('produce_message', { cluster, request });
