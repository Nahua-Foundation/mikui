// DTO, пересекающие границу IPC, названы в snake_case — ровно так, как их
// сериализует Rust. Промежуточный слой переименований только добавил бы место,
// где можно ошибиться.

export interface Topic {
  name: string;
  partitions: number;
}

/**
 * Одна настройка топика, как её отдал брокер.
 *
 * `is_default` не украшение: в ответе приезжают ВСЕ параметры, включая те,
 * что топику никто не задавал, и без этого признака унаследованное от кластера
 * значение неотличимо от выставленного руками — а интересна обычно разница.
 */
export interface TopicConfigEntry {
  name: string;
  /** null — брокер значения не отдал (так приезжают чувствительные). */
  value: string | null;
  is_default: boolean;
  is_read_only: boolean;
  is_sensitive: boolean;
}

/** Одна партиция топика: как устроена и что в ней лежит. */
export interface PartitionDetails {
  id: number;
  /** Брокер-лидер. -1 — лидера нет, партиция недоступна. */
  leader: number;
  replicas: number;
  /** Реплик в синхронизации. Меньше `replicas` — партиция under-replicated. */
  in_sync: number;
  /** Первый доступный офсет: что было до него, съел retention. null —
   *  границы снять не успели. */
  low: number | null;
  /** Офсет следующего записанного сообщения (exclusive). */
  high: number | null;
}

/**
 * Всё, что известно про топик помимо его сообщений.
 *
 * Устройство (партиции, реплики) приезжает из метаданных, границы партиций —
 * из `ListOffsets` (записей не переносит, квоту на чтение не тратит), а
 * настройки — из `DescribeConfigs` под отдельным правом. Последнее может быть
 * закрыто, и тогда `config` пуст, а `config_error` объясняет почему: показать
 * «партиций 12, настройки недоступны» полезнее, чем не показать ничего.
 *
 * Времени самого старого и самого нового сообщения здесь нет: кластер такого
 * не сообщает, а вычитывать ради него по батчу с каждой партиции — это уже
 * квота на чтение. Дешёвая правда об этом — офсеты `low`/`high`.
 *
 * Размера топика на диске тоже нет: Kafka отдаёт его запросом
 * `DescribeLogDirs`, которого librdkafka не реализует вовсе. Вместо него —
 * сырьё для оценки в `sample_*`, измеренное на уже перекачанном.
 */
export interface TopicDetails {
  name: string;
  partitions: PartitionDetails[];
  /** null — партиций нет. */
  replication_factor: number | null;
  /** У скольких партиций ISR меньше числа реплик. */
  under_replicated: number;
  /** Сумма `high - low` там, где границы известны. Оценка сверху: в офсеты
   *  попадают и транзакционные маркеры, и дырки от compaction. */
  messages: number | null;
  /** Границы известны не у всех партиций — `messages` тогда нижняя граница. */
  offsets_partial: boolean;
  /** Сколько СЖАТЫХ байт пришло по сети за уже прочитанное этого топика — то
   *  есть данные в том виде, в каком они лежат у брокера. Все три `sample_*`
   *  заполнены только у ОТКРЫТОГО топика и только когда выборки набралось
   *  достаточно, чтобы ей верить. */
  sample_wire_bytes: number | null;
  /** Сколько сообщений из них распаковалось — включая выброшенные нами за
   *  границей окна: за них тоже заплачено, и без них в оценку вошла бы наша
   *  собственная предвыборка. */
  sample_messages: number | null;
  /** Их же размер в разжатом виде: рядом с `sample_wire_bytes` показывает,
   *  во сколько раз топик сжат — и сжат ли вообще. */
  sample_bytes: number | null;
  /** По алфавиту. Пусто, когда настройки не удалось получить. */
  config: TopicConfigEntry[];
  config_error: string | null;
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
  /** Метка операции CDC: `c`, `u`, `d`, `r`, `t`, `m`. Заполнена только там,
   *  где сработала линза Debezium; у Connect-конверта операции нет. */
  lens_tag: string | null;
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

/**
 * Строка списка сохранённых сообщений.
 *
 * Тела здесь нет намеренно: сохраняют как раз тяжёлое, и грузить десятки
 * мегабайт только чтобы нарисовать список, незачем — оно лежит отдельным файлом
 * и берётся по `getFavorite`, ровно как полное тело строки таблицы.
 */
export interface FavoriteInfo {
  id: string;
  /** Ключ кластера — тот же, что у схем топиков (`clusterKey`). */
  cluster: string;
  /** Как кластер назывался в момент сохранения: список общий на все кластеры,
   *  и по идентификатору его не узнать. */
  cluster_name: string;
  topic: string;
  partition: number;
  offset: number;
  /** Unix millis самого сообщения, а не момента сохранения. */
  timestamp: number;
  key: string;
  preview: string;
  /** Размер тела НА ПРОВОДЕ. С `bytes` не совпадает: на диске лежит UTF-8
   *  текст в JSON, а в Kafka — сжатые байты. */
  value_size: number;
  binary: boolean;
  /** RFC 3339. */
  saved_at: string;
  /** Сколько запись занимает на диске прямо сейчас. */
  bytes: number;
  /** Файла тела нет — открыть запись нечем, удалить можно. */
  missing: boolean;
}

/** Весь архив: и список, и то, во что он обходится. */
export interface FavoritesView {
  items: FavoriteInfo[];
  /** Тела плюс сам индекс. */
  total_bytes: number;
}

export const EMPTY_FAVORITES: FavoritesView = { items: [], total_bytes: 0 };

/** Ответ на сохранение. `updated` отличает «сохранили» от «обновили уже
 *  сохранённое»: записей после повторного клика по звезде по-прежнему одна. */
export interface SaveFavoriteResult extends FavoritesView {
  id: string;
  updated: boolean;
}

/**
 * Открытое сохранённое сообщение.
 *
 * Тело собрано бэкендом ПРЯМО СЕЙЧАС и по той схеме, которая привязана к топику
 * сегодня: на диске лежат сырые байты из Kafka, а не наш вчерашний способ их
 * показать. Поэтому схема, загруженная уже после сохранения, разбирает и то,
 * что сохранено до неё.
 */
export interface SavedMessage extends FullMessage {
  /** Формат тела ТОГО топика, откуда сообщение, — не того, что открыт сейчас. */
  format: BodyFormat;
}

/** Применяется в Rust по сырым байтам, до пересечения границы IPC. */
export interface MessageFilter {
  key: string;
  value: string;
  case_sensitive: boolean;
  /** Искать ли по разобранному телу, а не только по сырым байтам. Осмысленно
   *  лишь у топика со схемой, и стоит разбора каждого тела — поэтому отдельным
   *  флагом, а не всегда. */
  search_decoded: boolean;
}

export const EMPTY_FILTER: MessageFilter = {
  key: '',
  value: '',
  case_sensitive: false,
  search_decoded: false,
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

export interface OpenTopicResult extends QuotaInfo, MemoryUsage, ReadScope {
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
 * Сколько топика просмотрено и сколько его всего.
 *
 * Отдельно от `loaded` (сколько лежит в буфере): при глубоком поиске эти числа
 * расходятся принципиально — просмотрено 50 000, в буфере три находки, в топике
 * около 92 000.
 */
export interface ReadScope {
  /** Сколько сообщений просмотрено с момента открытия топика — включая
   *  выброшенные фильтром при глубоком поиске. */
  scanned: number;
  /**
   * Сколько сообщений в читаемых партициях ПРИМЕРНО. `null` — границы партиций
   * ещё не сняты.
   *
   * Это число офсетов, а не сообщений: на компактированных партициях офсетов
   * больше, часть из них уже никому не отдадут. Поэтому оценка сверху, и в UI
   * она обязана называться приблизительной.
   */
  approx_total: number | null;
}

/**
 * Чем открывать топик. Один тип на `open_topic` и на `deep_search`: поиск
 * заново открывает тот же топик с того же конца и в тех же границах, и
 * разъехаться этим двум наборам нельзя — иначе поиск пойдёт не по тому, что
 * показано в таблице.
 */
export interface OpenTopicParams {
  topic: string;
  start_from: StartFrom;
  /** Сколько сообщений вычитать на каждую партицию. `deep_search` игнорирует:
   *  он идёт до конца топика. */
  limit: number;
  /** null — все партиции топика. */
  partitions: number[] | null;
  filter: MessageFilter;
  /** Границы чтения. Пустые — весь топик с конца, названного в `start_from`. */
  range: ReadRange;
  /** Сортировка по столбцу. null — обычный порядок чтения. */
  sort: SortSpec | null;
}

/** Сколько ещё сообщений вычитать на каждую ещё не исчерпанную партицию. */
export interface LoadMoreParams {
  additional: number;
}

/** Снимок хода ещё не завершённого `open_topic`/`load_more` — опрашивается
 *  по таймеру, пока идёт загрузка. */
export interface OpenTopicProgress extends QuotaInfo, MemoryUsage, ReadScope {
  /** Топик, к которому относится снимок: ответ опроса может разминуться со
   *  сменой топика, и без этой проверки счётчики перепутались бы. */
  topic: string | null;
  loaded: number;
  buffer_bytes: number;
  total: number;
  truncated: boolean;
  /** true — чтения в фоне уже нет, снимок финальный. */
  done: boolean;
  /** Идёт глубокий поиск, а не обычное чтение. У обычного чтения есть свой
   *  конец, и оно закончится само; поиск идёт до конца топика. */
  searching: boolean;
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
  /** Клиентская пара для mTLS, оба файла PEM. Осмысленны только вместе. */
  ssl_certificate_path?: string;
  ssl_key_path?: string;
  /** Пароль приватного ключа лежит в keychain — здесь только признак, что он
   *  там есть, как и у паролей учёток. */
  has_key_password?: boolean;
  /** Отказы от проверок TLS. Отсутствие поля означает «проверять». */
  ssl_skip_hostname_check?: boolean;
  ssl_skip_certificate_verification?: boolean;
  created_at: string;
  last_used?: string;
  users: ClusterUser[];
  /** Под кем подключались в прошлый раз — с него и начинаем следующий раз. */
  active_user_id?: string;
  /** Schema Registry кластера. На кластере, а не на топике: реестр в кластере
   *  один, и вводить его для каждого топика заново незачем. */
  schema_registry?: SchemaRegistryConfig | null;
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
  ssl_certificate_path?: string;
  ssl_key_path?: string;
  /** Заполняется только для несохранённой формы — как и `password`. */
  ssl_key_password?: string;
  ssl_skip_hostname_check?: boolean;
  ssl_skip_certificate_verification?: boolean;
}

/**
 * Механизмы SASL, которые умеет бэкенд.
 *
 * Список продублирован из `helpers::SASL_MECHANISMS`: здесь он наполняет
 * выпадашку, там проверяет пришедшее. Расходиться им нельзя — механизм, которого
 * нет в бэкенде, форма предложила бы, а подключение отвергло.
 *
 * GSSAPI и OAUTHBEARER отсюда убраны: Kerberos требует Cyrus SASL, который не
 * собирается под Windows, а OAUTHBEARER без libcurl и колбэка выдачи токена
 * молча висел бы до таймаута. Подробности — в комментарии к константе в Rust.
 */
export const SASL_MECHANISMS = ['PLAIN', 'SCRAM-SHA-256', 'SCRAM-SHA-512'];

/** Механизм для новой записи и для записи, в которой его нет. Должен совпадать
 *  с `helpers::DEFAULT_SASL_MECHANISM`, иначе форма показывала бы один
 *  механизм, а подключение шло бы другим. */
export const DEFAULT_SASL_MECHANISM = 'SCRAM-SHA-512';

/** Требуют ли выбранные настройки безопасности SASL-логина. */
export function needsSasl(securityProtocol: string): boolean {
  return securityProtocol === 'SASL_PLAINTEXT' || securityProtocol === 'SASL_SSL';
}

/** Требуют ли выбранные настройки безопасности TLS. */
export function needsTls(securityProtocol: string): boolean {
  return securityProtocol === 'SSL' || securityProtocol === 'SASL_SSL';
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
 * `proto` и `avro` включают декодирование по схеме ещё в Rust.
 *
 * `jsonschema` отличается от `json` не разбором тела — тело там такой же JSON,
 * — а тем, что перед ним стоит confluent-заголовок (Rust его снимает; раньше
 * пять его байт ехали в UI мусором перед `{`) и что у топика есть контракт в
 * реестре, по которому проверяется отправляемое.
 *
 * `hex` — противоположность `text`: тот показывает то, что прочиталось как
 * UTF-8, подставляя символы замены вместо остального, а этот — сами байты, все
 * до единого. Для тела, которое не рассматривают, а забирают — чтобы переслать
 * дальше или отправить в другой топик формой отправки, принимающей ровно такую
 * запись.
 */
export type BodyFormat = 'json' | 'text' | 'proto' | 'avro' | 'jsonschema' | 'hex';

/** Показывается ли тело этого формата как есть, без попытки разложить его по
 *  строкам как JSON. Вопрос возникает в двух местах — в окне сообщения и при
 *  разборе конверта, — и ответ у них обязан быть один. */
export function showsRawBody(format: BodyFormat): boolean {
  return format === 'text' || format === 'hex';
}

/**
 * Что делать с конвертом Debezium / Kafka Connect.
 *
 * Настройка независимая от `BodyFormat`: тот отвечает на вопрос «как превратить
 * байты в JSON», линза — «что из этого JSON показать». Debezium с
 * Avro-сериализатором обычное дело, и линза работает поверх любого формата.
 *
 * `null` в схеме топика — «не выбирали»: конверт распознаётся по самому телу.
 * `off` — это ОТВЕТ пользователя, и распознавание его не отменяет.
 */
export type LensSetting = 'off' | 'connect' | 'debezium';

/** Один файл схемы, привязанный к топику: .proto или .avsc. */
export interface SchemaFile {
  /** Путь внутри каталога схемы — он же имя, под которым файл виден в
   *  `import`. Служит идентификатором в операциях обновления и удаления. */
  name: string;
  /** Откуда файл взяли. По нему работает «перечитать с диска». */
  source: string;
  /**
   * Файл не выбирали — его нашли по `import` в выбранном.
   *
   * Показывается отдельно от выбранного и без кнопок: удалять и перечитывать
   * по одному можно то, что добавляли, а зависимости пересчитываются целиком
   * при каждом изменении набора. Ключа может не быть — так выглядят схемы,
   * сохранённые до появления поиска импортов, и там все файлы выбраны руками.
   */
  auto?: boolean;
}

/**
 * Avro-половина схемы топика.
 *
 * Источника схемы два, и они взаимоисключающи: либо локальные .avsc, либо
 * subject реестра. Выбор одного очищает другой — иначе непонятно, чем именно
 * приложение декодирует.
 *
 * Пустые `files` и `subject` при `registry: true` — рабочее состояние, а не
 * недонастроенное: в confluent-формате id схемы едет в каждом сообщении, и
 * закреплять топику ещё и свою схему незачем.
 */
export interface AvroView {
  files: SchemaFile[];
  /** Полное имя записи, которой декодируется тело, когда схема из файлов. */
  record: string | null;
  /** Subject реестра. */
  subject: string | null;
  /** Версия subject. null — последняя, перечитывается при каждом открытии. */
  version: number | null;
  /** Имена записей из добавленных .avsc — содержимое селектора. */
  records: string[];
  /** Настроен ли у кластера реестр. */
  registry: boolean;
  /**
   * Subject, который реестр держит для этого топика САМ — `<topic>-value`,
   * имя по умолчанию у любого штатного сериализатора Kafka. Отдельно от
   * `subject`: тот выбрали руками, а этот найден, и на диск он не пишется.
   *
   * Именно он и переводит топик в avro без единого клика: пришёл непустым —
   * значит `TopicSchema.format` приехал avro не потому, что так решили, а
   * потому, что реестр знает про этот топик.
   */
  detected: string | null;
  /** Avro-схема есть, но не разбирается или не добывается. Отдельно от
   *  `TopicSchema.error`: у топика могут быть загружены обе схемы, и чинить
   *  надо ту, которая сломалась. */
  error: string | null;
}

/**
 * JSON-Schema-половина схемы топика.
 *
 * Как `AvroView` минус список записей: у JSON Schema корень один, сама схема им
 * и является, и выбирать не из чего.
 */
export interface JsonView {
  files: SchemaFile[];
  /** Subject реестра. */
  subject: string | null;
  /** Версия subject. null — последняя. */
  version: number | null;
  /** Настроен ли у кластера реестр. */
  registry: boolean;
  /** Subject, который реестр держит для топика сам И который при этом
   *  действительно JSON-схема. См. `AvroView.detected`. */
  detected: string | null;
  /** Схема есть, но не компилируется или не добывается. */
  error: string | null;
}

/** Схема топика вместе с результатом её разбора. */
export interface TopicSchema {
  cluster: string;
  topic: string;
  format: BodyFormat;
  files: SchemaFile[];
  /** Полное имя message, которым декодируется тело. */
  message: string | null;
  /** Все message из добавленных файлов, по алфавиту — содержимое селектора. */
  messages: string[];
  /** Схема на диске есть, но не разбирается. Файлы всё равно показываем:
   *  иначе пользователю нечего чинить. */
  error: string | null;
  /** Avro-половина. null — топика она не касается. */
  avro: AvroView | null;
  /** JSON-Schema-половина. null — топика она не касается. */
  json: JsonView | null;
  /** Выбранная линза. null — не выбирали: конверт распознаётся по телу. */
  lens: LensSetting | null;
  /**
   * Каким форматом реестр называет схему, которую держит для топика САМ:
   * `AVRO` / `PROTOBUF` / `JSON`. null — не держит или реестра нет.
   *
   * Нужно ровно ради PROTOBUF. Схему protobuf из реестра приложение пока не
   * тянет, поэтому автоопределение на таком топике ничего не переключает — и
   * сказать, что контракт всё-таки есть и надо загрузить .proto, больше нечем.
   */
  detected_kind: string | null;
}

/** Настройки Schema Registry кластера. Пароль здесь, как и у учёток, только
 *  признаком: сам он живёт в системном keychain. */
export interface SchemaRegistryConfig {
  url: string;
  username?: string | null;
  has_password: boolean;
  /** Свой CA-бандл: реестр стоит за своим фронтом и может быть подписан не тем
   *  же центром, что брокеры. */
  ssl_ca_bundle_path?: string | null;
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
export type PayloadFormat = 'text' | 'json' | 'proto' | 'avro' | 'jsonschema' | 'hex';

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
  /** Subject реестра для кодирования. То же самое, но для `avro`; пустой —
   *  взять тот, что назначен топику. */
  subject: string | null;
}

/**
 * Всё, что форме отправки нужно знать про выбранный message.
 *
 * Одной структурой, а не двумя запросами: обе половины описывают один и тот же
 * тип и приезжают на одно событие — подсветка обязана относиться ровно к той
 * схеме, по которой построена заготовка.
 */
export interface MessageForm {
  /** JSON-заготовка: все поля на месте, значения нулевые. */
  template: string;
  /** Имена enum-значений этого типа. Тот же список, которым красит enum
   *  модалка чтения (`FullMessage.enum_values`). */
  enum_values: string[];
  /** Subject, которым заготовка построена на самом деле. null у protobuf и у
   *  локальных .avsc. Нужен там, где subject никто не выбирал: бэкенд нашёл его
   *  в реестре сам, и форма обязана показать, чем собралась кодировать. */
  subject: string | null;
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

/** Файл настроек. Rust о его содержимом ничего не знает и возвращает
 *  незнакомые ключи нетронутыми — see config/types.rs. */
export interface Settings {
  version: number;
  [key: string]: unknown;
}

/** Ключ в settings.json, под которым лежит избранное. */
export const FAVORITE_TOPICS_KEY = 'favorite_topics';

/**
 * Избранные топики: ключ кластера (`clusterKey`) → имена топиков.
 *
 * По кластерам, а не общим списком: одноимённые топики в dev и prod — разные
 * топики, и отметка на одном не имеет отношения к другому. Тот же довод, что
 * и у привязки схем.
 */
export type FavoriteTopics = Record<string, string[]>;

/**
 * Разбирает избранное из настроек, отбрасывая всё, что не похоже на список
 * имён.
 *
 * Файл лежит на диске рядом с подключениями, его правят руками, и одна
 * испорченная запись не должна лишать избранного остальные кластеры.
 */
export function parseFavoriteTopics(value: unknown): FavoriteTopics {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return {};
  const parsed: FavoriteTopics = {};
  for (const [cluster, names] of Object.entries(value as Record<string, unknown>)) {
    if (!Array.isArray(names)) continue;
    parsed[cluster] = names.filter((name): name is string => typeof name === 'string');
  }
  return parsed;
}

// --- Ссылка на сообщение -------------------------------------------------------

/**
 * Что превратить в ссылку. Координаты сообщения плюс то, чем описывается
 * контекст: кластер (ради подсказки о схеме) и его имя.
 *
 * Сам кластер в ссылку уезжает НЕ этим ключом, а идентификатором, который выдал
 * брокер: локальный ключ на чужой машине не значит ничего. Спрашивает его Rust
 * у своего же подключения — см. `link.rs`.
 */
export interface ShareRequest {
  cluster: string | null;
  cluster_name: string | null;
  topic: string;
  partition: number;
  offset: number;
  /** Unix millis. Неположительное — времени у сообщения нет. */
  timestamp: number;
}

/**
 * Куда ведёт ссылка на ЭТОЙ машине: кластер уже сопоставлен с сохранённым
 * подключением, и назван он локальным идентификатором.
 */
export interface MessageLinkTarget {
  /** Идентификатор сохранённого подключения. */
  cluster: string;
  /** Как оно называется здесь, а не у отправителя. */
  cluster_name: string;
  topic: string;
  partition: number;
  offset: number;
  /** Unix millis. null — отправитель поделился ссылкой без времени. */
  timestamp: number | null;
  /** Каким форматом читает топик ОТПРАВИТЕЛЬ. Схема по ссылке не едет, и
   *  расхождение с нашим форматом — повод сказать, чего не хватает. */
  format: string | null;
  /** Имя типа внутри схемы: message у protobuf, subject или запись у
   *  остальных. */
  type_name: string | null;
}
