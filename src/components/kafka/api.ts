/**
 * Единственное место, где фронт разговаривает с Rust.
 *
 * Раньше `invoke` был раскидан по компонентам, из-за чего, например, клик по
 * кластеру в архиве только красил строку и показывал «Connected», не подключаясь
 * на самом деле. Один типизированный слой такие расхождения исключает.
 */
import { invoke } from '@tauri-apps/api/core';
import {
  ClusterConnectPayload,
  ClusterUser,
  FullMessage,
  KafkaCluster,
  LoadMoreParams,
  MessageFilter,
  OpenTopicProgress,
  OpenTopicResult,
  ReadRange,
  RowPreview,
  Settings,
  StartFrom,
  Topic,
} from './types';

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
