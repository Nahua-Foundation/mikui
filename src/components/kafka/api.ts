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
  FullMessage,
  KafkaCluster,
  MessageFilter,
  OpenTopicResult,
  RowPreview,
  Settings,
  StartFrom,
  Topic,
} from './types';

// --- Подключение ------------------------------------------------------------

/** Payload для сохранённого кластера: пароль не передаём, Rust возьмёт его из keychain. */
export function clusterToPayload(cluster: KafkaCluster): ClusterConnectPayload {
  return {
    id: cluster.id,
    brokers: cluster.brokers,
    security_protocol: cluster.security_protocol,
    sasl_mechanism: cluster.sasl_mechanism,
    username: cluster.username,
    ssl_ca_bundle_path: cluster.ssl_ca_bundle_path,
  };
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
 * `password`: непустая строка — записать в keychain; пустая — удалить оттуда;
 * `undefined` — не трогать сохранённый.
 */
export const saveCluster = (cluster: KafkaCluster, password?: string) =>
  invoke<KafkaCluster>('save_cluster', { cluster, password });

export const deleteCluster = (id: string) => invoke<void>('delete_cluster', { id });

// --- Настройки --------------------------------------------------------------

export const getSettings = () => invoke<Settings>('get_settings');
export const saveSettings = (settings: Settings) => invoke<void>('save_settings', { settings });

// --- Топики и сообщения -----------------------------------------------------

export const getTopics = () => invoke<Topic[]>('get_topics');

export const openTopic = (params: {
  topic: string;
  start_from: StartFrom;
  limit: number;
  partition: number | null;
  filter: MessageFilter;
}) => invoke<OpenTopicResult>('open_topic', { params });

export const setFilter = (filter: MessageFilter) => invoke<number>('set_filter', { filter });

export const getWindow = (start: number, count: number) =>
  invoke<RowPreview[]>('get_window', { start, count });

export const getMessageBody = (index: number) =>
  invoke<FullMessage>('get_message_body', { index });

export const closeTopic = () => invoke<void>('close_topic');
