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
  /** Тело не является валидным UTF-8: Avro, Protobuf, произвольные байты.
   *  У декодированного protobuf — false: приехал JSON, а не байты. */
  binary: boolean;
  /** Схема к топику загружена, но это сообщение по ней не разобралось.
   *  В `preview` тогда обычный текст: одно битое сообщение не повод
   *  перестать показывать топик. */
  decode_error: string | null;
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
  /** Декодированный protobuf приезжает сюда компактным JSON — модалка
   *  разложит его отступами тем же кодом, каким печатает JSON-топики. */
  value: string;
  /** Размер тела НА ПРОВОДЕ, а не длина `value`. */
  value_size: number;
  binary: boolean;
  /** См. `RowPreview.decode_error`. */
  decode_error: string | null;
  /** Имена enum-значений схемы, применённой к телу — модалка красит их
   *  отдельным цветом вместо того, чтобы гадать по формату строки. Пусто,
   *  если тело не декодировано protobuf'ом. */
  enum_values: string[];
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

/** Столбцы, по которым можно кликнуть заголовок и отсортировать таблицу. */
export type SortColumn = 'partition' | 'offset' | 'key' | 'timestamp';
export type SortDirection = 'asc' | 'desc';

/** Сортировка поверх обычного порядка чтения. `null` — обычный порядок,
 *  заданный `ReadMode` (см. `ReadRange.newest_first` в Rust). */
export interface SortSpec {
  column: SortColumn;
  direction: SortDirection;
}

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

// --- Схемы топиков ----------------------------------------------------------

/**
 * Как показывать тело сообщений топика.
 *
 * `json` — исходное поведение: тело едет текстом, а форматирует его фронт.
 * `proto` включает декодирование по загруженной схеме ещё в Rust.
 * `avro` хранится, но пока ничего не меняет: схем-реестра в приложении нет.
 */
export type BodyFormat = 'json' | 'text' | 'proto' | 'avro';

/** Один .proto, привязанный к топику. */
export interface ProtoFile {
  /** Путь внутри каталога схемы — он же имя, под которым файл виден в
   *  `import`. Служит идентификатором в операциях обновления и удаления. */
  name: string;
  /** Откуда файл взяли. По нему работает «перечитать с диска». */
  source: string;
}

/** Схема топика вместе с результатом её разбора. */
export interface TopicSchema {
  cluster: string;
  topic: string;
  format: BodyFormat;
  files: ProtoFile[];
  /** Полное имя message, которым декодируется тело. */
  message: string | null;
  /** Все message из добавленных файлов, по алфавиту — содержимое селектора. */
  messages: string[];
  /** Схема на диске есть, но не разбирается. Файлы всё равно показываем:
   *  иначе пользователю нечего чинить. */
  error: string | null;
}

/**
 * Ключ кластера в привязке схемы.
 *
 * Топики с одинаковыми именами в dev и prod несут разные контракты, поэтому
 * привязка идёт по паре кластер-топик. У сохранённого кластера ключ — его id,
 * у подключения из формы — то имя, под которым оно показано в шапке: другого
 * устойчивого признака у него нет.
 */
export function clusterKey(clusterId: string | null, clusterName: string | null): string | null {
  return clusterId ?? clusterName ?? null;
}

// --- Отправка сообщения ------------------------------------------------------

/**
 * В каком виде пользователь ввёл тело отправляемого сообщения.
 *
 * Шире `BodyFormat` намеренно: тот описывает, КАК ПОКАЗЫВАТЬ прочитанный топик,
 * и hex ему не нужен — двоичное тело там видно меткой `[binary]`. А при
 * отправке hex единственный способ положить в топик байты, которых не выражает
 * ни текст, ни схема.
 */
export type PayloadFormat = 'text' | 'json' | 'proto' | 'avro' | 'hex';

/** Форма отправки в том виде, в каком её заполнили. Тело — строкой: в байты
 *  его превращает Rust, потому что для proto нужна схема топика. */
export interface ProduceRequest {
  topic: string;
  /** null — партицию выбирает партишенер по ключу (murmur2, как в Java). */
  partition: number | null;
  /** Пустая строка — ключа НЕТ. Это не ключ нулевой длины: от наличия ключа
   *  зависит и партиционирование, и compaction. */
  key: string;
  headers: MessageHeader[];
  format: PayloadFormat;
  payload: string;
  /** Полное имя message для кодирования. Только для `proto`; может отличаться
   *  от выбранного в настройках топика — отправляют и не тот тип, которым
   *  топик читают. */
  message: string | null;
}

/**
 * Всё, что форме отправки нужно знать про выбранный message.
 *
 * Одной структурой, а не двумя запросами: обе половины описывают один и тот же
 * тип и приезжают на одно событие — подсветка обязана относиться ровно к той
 * схеме, по которой построена заготовка.
 */
export interface ProtoMessageForm {
  /** JSON-заготовка: все поля на месте, значения нулевые. */
  template: string;
  /** Имена enum-значений этого типа. Тот же список, которым красит enum
   *  модалка чтения (`FullMessage.enum_values`). */
  enum_values: string[];
}

/** Куда легло отправленное сообщение — из отчёта о доставке, то есть то, что
 *  подтвердил брокер, а не то, что мы попросили. */
export interface ProduceResult {
  partition: number;
  offset: number;
}

/**
 * Что показать под полем ввода тела.
 *
 * `warning` — отправить всё равно можно: байты получаются, просто это не то,
 * чем притворяется формат (невалидный JSON уедет набранным текстом).
 * `error` — байтов не получить вовсе, отправка заблокирована.
 */
export interface PayloadIssue {
  severity: 'warning' | 'error';
  message: string;
}

/** Файл настроек. Своих полей пока нет — see config/types.rs. */
export interface Settings {
  version: number;
  [key: string]: unknown;
}
