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
  FullMessage,
  KafkaCluster,
  LoadMoreParams,
  MessageFilter,
  OpenTopicProgress,
  OpenTopicResult,
  PayloadIssue,
  ProduceRequest,
  ProduceResult,
  ProtoMessageForm,
  ReadRange,
  RowPreview,
  Settings,
  StartFrom,
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
 * Payload для сохранённого кластера: пароль не передаём, Rust возьмёт его из
 * keychain по `user_id`.
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

export const saslMechanisms = () => invoke<string[]>('sasl_mechanisms');

// --- Сохранённые подключения ------------------------------------------------

export const listClusters = () => invoke<KafkaCluster[]>('list_clusters');

/**
 * Сохраняет параметры подключения. Список пользователей бэкенд берёт из уже
 * сохранённой записи, а не отсюда: им заведуют `saveClusterUser`/`deleteClusterUser`.
 */
export const saveCluster = (cluster: KafkaCluster) =>
  invoke<KafkaCluster>('save_cluster', { cluster });

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

// --- Топики и сообщения -----------------------------------------------------

export const getTopics = () => invoke<Topic[]>('get_topics');

/** Устройство и настройки топика. Открытый топик не трогает. */
export const describeTopic = (topic: string) =>
  invoke<TopicDetails>('describe_topic', { topic });

export const openTopic = (params: {
  topic: string;
  start_from: StartFrom;
  limit: number;
  /** null — все партиции топика. */
  partitions: number[] | null;
  filter: MessageFilter;
  /** Границы чтения. Пустые — весь топик с конца, названного в `start_from`. */
  range: ReadRange;
}) => invoke<OpenTopicResult>('open_topic', { params });

export const loadMore = (params: LoadMoreParams) =>
  invoke<OpenTopicResult>('load_more', { params });

export const getOpenTopicProgress = () =>
  invoke<OpenTopicProgress>('get_open_topic_progress');

export const setFilter = (filter: MessageFilter) => invoke<number>('set_filter', { filter });

export const getWindow = (start: number, count: number) =>
  invoke<RowPreview[]>('get_window', { start, count });

export const getMessageBody = (index: number) =>
  invoke<FullMessage>('get_message_body', { index });

export const closeTopic = () => invoke<void>('close_topic');

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

// --- Отправка сообщения -------------------------------------------------------

/** Заготовка тела и имена enum-значений выбранного message. */
export const protoMessageForm = (cluster: string, topic: string, message: string) =>
  invoke<ProtoMessageForm>('proto_message_form', { cluster, topic, message });

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
