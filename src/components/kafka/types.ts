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

/** С какого конца читать. То, что понимает бэкенд, когда границ не задано. */
export type StartFrom = 'oldest' | 'newest';

/**
 * Что выбрано в селекторе порядка чтения.
 *
 * `offset` и `timestamp` — это не «ещё два порядка», а чтение куска топика:
 * направление у них следует из того, какая граница задана, и считает его
 * бэкенд (см. `ReadRange::newest_first`), чтобы сортировка и обход окон не
 * разъехались.
 */
export type ReadMode = StartFrom | 'offset' | 'timestamp';

/** Границы чтения. `null` — граница не задана. Обе inclusive. */
export interface ReadRange {
  from_offset: number | null;
  to_offset: number | null;
  /** Unix millis. */
  from_timestamp: number | null;
  to_timestamp: number | null;
}

export const EMPTY_RANGE: ReadRange = {
  from_offset: null,
  to_offset: null,
  from_timestamp: null,
  to_timestamp: null,
};

/**
 * Что кластер реально даёт по скорости — измеряется косвенно, по статистике
 * librdkafka (см. kafka/quota.rs). Настроенную квоту спросить нельзя.
 *
 * Нужна в UI потому, что без неё медленное чтение неотличимо от зависшего
 * приложения: пользователь видит замерший экран и винит mikui, хотя это брокер
 * придерживает ответы по квоте.
 */
export interface QuotaInfo {
  /** null — измерений пока недостаточно. Это СУММА по всем брокерам. */
  read_bytes_per_sec: number | null;
  /** Самая длинная задержка, наложенная брокером, мс. 0 — не придерживал. */
  peak_throttle_ms: number;
  /**
   * Со скольких брокеров шли данные. `consumer_byte_rate` применяет каждый
   * брокер самостоятельно, поэтому суммарная скорость кратна их числу — без
   * этой цифры измеренные мегабайты выглядят невозможными под квотой в
   * сотни килобайт.
   */
  active_brokers: number;
  /**
   * Сколько брокеров всего в кластере — знаменатель к `active_brokers`.
   * «Семь» правдоподобно при любом составе кластера, «семь из десяти»
   * проверяемо.
   */
  known_brokers: number;
}

/**
 * Сколько памяти держит буфер сообщений и сколько ему позволено.
 *
 * Приложение обещает быть экономным на топиках, где сообщения весят десятки
 * килобайт, — шкала в футере делает это обещание проверяемым на глаз. Заодно
 * из неё видно, почему выдача оказалась усечённой: упёрлись в потолок, а не
 * «приложение сломалось».
 */
export interface MemoryUsage {
  /** Занято по-настоящему: арена с запасом ёмкости плюс индекс сообщений. */
  memory_bytes: number;
  memory_limit: number;
}

export interface OpenTopicResult extends QuotaInfo, MemoryUsage {
  /** Число видимых строк с учётом фильтра — это totalCount для списка. */
  total: number;
  /** Сколько всего вычитано в буфер до фильтрации. */
  loaded: number;
  /** Размер арены в байтах. */
  buffer_bytes: number;
  /** Упёрлись в лимит или таймаут — в топике есть ещё. */
  truncated: boolean;
}

/** Сколько ещё сообщений вычитать на каждую ещё не исчерпанную партицию. */
export interface LoadMoreParams {
  additional: number;
}

/** Снимок хода ещё не завершённого `open_topic`/`load_more` — опрашивается
 *  по таймеру, пока идёт загрузка. */
export interface OpenTopicProgress extends QuotaInfo, MemoryUsage {
  /** Топик, к которому относится снимок: ответ опроса может разминуться со
   *  сменой топика, и без этой проверки счётчики перепутались бы. */
  topic: string | null;
  loaded: number;
  buffer_bytes: number;
  total: number;
  truncated: boolean;
  /** true — чтения в фоне уже нет, снимок финальный. */
  done: boolean;
}

/**
 * Kafka-пользователь кластера.
 *
 * Один кластер обычно смотрят из-под нескольких учёток с разными ACL, поэтому
 * логины живут списком, а переключение между ними — обычная операция в шапке,
 * а не перенастройка подключения.
 *
 * Пароля здесь нет намеренно: он живёт в системном keychain под ключом `id`.
 * `has_password` — признак того, что пароль там есть, а не сам пароль.
 */
export interface ClusterUser {
  id: string;
  username: string;
  has_password: boolean;
}

/**
 * Сохранённое подключение — ровно то, что лежит в clusters.json.
 *
 * Ни пароля, ни логина в самом кластере нет: логины — в `users`, пароли —
 * в keychain. При подключении фронт передаёт только идентификаторы.
 */
export interface KafkaCluster {
  id: string;
  name: string;
  brokers: string;
  security_protocol: string;
  sasl_mechanism?: string;
  ssl_ca_bundle_path?: string;
  created_at: string;
  last_used?: string;
  users: ClusterUser[];
  /** Под кем подключались в прошлый раз — с него и начинаем следующий раз. */
  active_user_id?: string;
}

/**
 * Параметры подключения.
 *
 * `user_id` — сохранённая учётка: пароль подтянет Rust из keychain, через IPC
 * он не поедет. `password` заполняется только для несохранённой формы.
 */
export interface ClusterConnectPayload {
  id?: string;
  user_id?: string;
  brokers: string;
  security_protocol: string;
  sasl_mechanism?: string;
  username?: string;
  password?: string;
  ssl_ca_bundle_path?: string;
}

/** Требуют ли выбранные настройки безопасности SASL-логина. */
export function needsSasl(securityProtocol: string): boolean {
  return securityProtocol === 'SASL_PLAINTEXT' || securityProtocol === 'SASL_SSL';
}

/**
 * Идентификатор кластера или учётки. У учётки он же служит ключом пароля в
 * keychain, поэтому уникален и неизменен: переименование пользователя не
 * должно осиротить его пароль.
 */
export function newId(): string {
  return `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

/** Файл настроек. Своих полей пока нет — see config/types.rs. */
export interface Settings {
  version: number;
  [key: string]: unknown;
}
