// DTO, пересекающие границу IPC, названы в snake_case — ровно так, как их
// сериализует Rust. Промежуточный слой переименований только добавил бы место,
// где можно ошибиться.

export interface Topic {
  name: string;
  partitions: number;
}

/** Строка таблицы. Тело обрезано бэкендом; полное берётся по `index`. */
export interface RowPreview {
  /** Индекс в текущем отфильтрованном представлении на стороне Rust. */
  index: number;
  partition: number;
  offset: number;
  /** Unix millis. Форматируется лениво и только для видимых строк. */
  timestamp: number;
  key: string;
  preview: string;
  value_size: number;
  /** Тело не является валидным UTF-8: Avro, Protobuf, произвольные байты. */
  binary: boolean;
}

export interface MessageHeader {
  key: string;
  value: string;
}

/** Полное сообщение. Запрашивается только когда пользователь его открыл. */
export interface FullMessage {
  partition: number;
  offset: number;
  timestamp: number;
  key: string;
  value: string;
  value_size: number;
  binary: boolean;
  headers: MessageHeader[];
}

export interface FavoriteMessage extends FullMessage {
  topicName: string;
  savedAt: string;
}

/** Применяется в Rust по сырым байтам, до пересечения границы IPC. */
export interface MessageFilter {
  key: string;
  value: string;
  case_sensitive: boolean;
}

export const EMPTY_FILTER: MessageFilter = {
  key: '',
  value: '',
  case_sensitive: false,
};

export type StartFrom = 'oldest' | 'newest';

export interface OpenTopicResult {
  /** Число видимых строк с учётом фильтра — это totalCount для списка. */
  total: number;
  /** Сколько всего вычитано в буфер до фильтрации. */
  loaded: number;
  /** Размер арены в байтах. */
  buffer_bytes: number;
  /** Упёрлись в лимит или таймаут — в топике есть ещё. */
  truncated: boolean;
}

/**
 * Сохранённое подключение — ровно то, что лежит в clusters.json.
 *
 * Поля пароля здесь нет намеренно: он живёт в системном keychain, и при
 * подключении к сохранённому кластеру фронт передаёт только `id`.
 * `has_password` — признак того, что пароль там есть, а не сам пароль.
 */
export interface KafkaCluster {
  id: string;
  name: string;
  brokers: string;
  security_protocol: string;
  sasl_mechanism?: string;
  username?: string;
  ssl_ca_bundle_path?: string;
  created_at: string;
  last_used?: string;
  has_password: boolean;
}

/** Параметры подключения. `password` заполняется только для несохранённой формы. */
export interface ClusterConnectPayload {
  id?: string;
  brokers: string;
  security_protocol: string;
  sasl_mechanism?: string;
  username?: string;
  password?: string;
  ssl_ca_bundle_path?: string;
}

/** Файл настроек. Своих полей пока нет — see config/types.rs. */
export interface Settings {
  version: number;
  [key: string]: unknown;
}
