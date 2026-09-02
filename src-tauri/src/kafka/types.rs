use serde::{Deserialize, Serialize};

/// Параметры подключения. Незнакомые поля serde игнорирует, поэтому фронт
/// может слать больше, чем бэкенду нужно.
///
/// Полей keystore/truststore здесь намеренно нет: это понятия из мира Java
/// (JKS), а librdkafka их не читает. Для mTLS ей нужны PEM-файлы —
/// `ssl.certificate.location`, `ssl.key.location`, `ssl.key.password`.
/// Когда дойдут руки до mTLS, добавляем именно их, а не JKS-пути.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ClusterConnectPayload {
    /// Идентификатор сохранённого кластера, если подключаемся к такому.
    #[serde(default)]
    pub id: Option<String>,
    /// Идентификатор Kafka-пользователя, под которым подключаемся. Нужен, чтобы
    /// достать пароль из keychain, не гоняя его через IPC: у сохранённой учётки
    /// пароль вообще не покидает бэкенд.
    #[serde(default)]
    pub user_id: Option<String>,
    pub brokers: String,
    pub security_protocol: String,
    pub sasl_mechanism: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub ssl_ca_bundle_path: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TopicInfo {
    pub name: String,
    pub partitions: usize,
}

/// Одна настройка топика в том виде, в каком её отдал брокер.
///
/// `is_default` здесь не украшение: в ответе `DescribeConfigs` приезжают ВСЕ
/// параметры, включая те, что топику никто не задавал, и без этого признака
/// унаследованное от кластера значение неотличимо от выставленного руками
/// — а именно эта разница обычно и интересна.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct TopicConfigEntry {
    pub name: String,
    /// `None` — брокер значения не отдал; так приезжают, в частности,
    /// параметры, помеченные им как чувствительные.
    pub value: Option<String>,
    /// Значение унаследовано (от кластера или из дефолта), а не задано топику.
    pub is_default: bool,
    pub is_read_only: bool,
    pub is_sensitive: bool,
}

/// Одна партиция топика: как устроена и что в ней лежит.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct PartitionDetails {
    pub id: i32,
    /// Брокер-лидер. `-1` — лидера нет (партиция недоступна).
    pub leader: i32,
    pub replicas: usize,
    /// Сколько реплик в синхронизации. Меньше `replicas` — партиция
    /// under-replicated.
    pub in_sync: usize,
    /// Первый доступный офсет: всё, что было до него, съел retention.
    /// `None` — границы снять не успели (см. `OFFSETS_BUDGET`).
    pub low: Option<i64>,
    /// Офсет, который получит следующее записанное сообщение (exclusive).
    pub high: Option<i64>,
}

/// Всё, что известно про топик помимо его сообщений.
///
/// Собирается из трёх источников, и они не равнозначны:
///
/// * метаданные — устройство топика (партиции, реплики, ISR); в
///   `DescribeConfigs` этого нет вовсе;
/// * `ListOffsets` — границы партиций, из которых считается число сообщений.
///   Записей не переносит, то есть квоту на чтение не тратит;
/// * `DescribeConfigs` — настройки. Закрыт отдельным правом, которого у
///   учётки может и не быть.
///
/// Неудача любого из последних двух не отменяет первого: показать «партиций
/// 12, настройки недоступны» полезнее, чем не показать ничего.
///
/// Времени самого старого и самого нового сообщения здесь нет намеренно.
/// Кластер такого не сообщает: чтобы узнать время сообщения, его надо
/// вычитать, а фетч приносит целый батч на партицию и списывается с квоты на
/// чтение. Дешёвая правда об этом — офсеты `low`/`high` ниже.
///
/// Размера топика на диске здесь тоже нет — но не потому, что его нельзя
/// оценить. Kafka его знает и отдаёт запросом `DescribeLogDirs` (тем самым,
/// которым работает `kafka-log-dirs.sh`), а librdkafka этот запрос не
/// реализует вовсе: в `rdkafka.h` и `rdkafka_admin.h` нет ни одного
/// упоминания, имя API-ключа встречается только в таблице названий протокола.
/// Собирать его по проводу руками — мимо TLS, SASL и согласования версий —
/// несоразмерно задаче. Вместо точного числа отсюда уезжает сырьё для оценки
/// (`sample_*`), измеренное на уже перекачанном.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct TopicDetails {
    pub name: String,
    pub partitions: Vec<PartitionDetails>,
    /// Сколько реплик у партиций топика. `None` — партиций нет.
    pub replication_factor: Option<usize>,
    /// У скольких партиций ISR меньше числа реплик. Ненулевое значение — это
    /// состояние кластера, а не свойство топика, и заметить его хочется сразу.
    pub under_replicated: usize,
    /// Сумма `high - low` по партициям, у которых границы известны.
    /// `None` — не известны ни у одной.
    ///
    /// Оценка сверху, а не точный счёт: в офсеты попадают и транзакционные
    /// маркеры, и дырки, оставшиеся от compaction.
    pub messages: Option<i64>,
    /// Границы известны не у всех партиций — значит `messages` считает не
    /// весь топик и является нижней границей.
    pub offsets_partial: bool,
    /// Сколько СЖАТЫХ байт пришло по сети за уже прочитанное этого топика.
    /// Это данные в том же виде, в каком они лежат у брокера, поэтому
    /// `wire / messages` и есть цена одного сообщения на диске.
    ///
    /// Все три `sample_*` заполнены только для ОТКРЫТОГО топика и только
    /// когда выборки набралось достаточно, чтобы ей верить (см.
    /// `quota::TransferSample`). У соседнего топика выборки нет, а заводить
    /// её значило бы читать его — чего от окна информации никто не просил.
    pub sample_wire_bytes: Option<u64>,
    /// Сколько сообщений из этих байт распаковалось — включая те, что мы
    /// выбросили за границей окна: за них тоже заплачено.
    pub sample_messages: Option<u64>,
    /// Их же размер в разжатом виде. Рядом с `sample_wire_bytes` показывает,
    /// во сколько раз топик сжат — и сжат ли вообще.
    pub sample_bytes: Option<u64>,
    /// Настройки по алфавиту. Пусто, когда их не удалось получить.
    pub config: Vec<TopicConfigEntry>,
    /// Почему настроек нет. `None` — они приехали.
    pub config_error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StartFrom {
    Oldest,
    Newest,
}

/// Столбец, по которому таблица сортирована поверх обычного порядка чтения.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SortColumn {
    Partition,
    Offset,
    Key,
    Timestamp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SortDirection {
    Asc,
    Desc,
}

/// Клик по заголовку колонки. `None` (нет активной сортировки) — обычный
/// порядок чтения из `ReadRange::newest_first`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct SortSpec {
    pub column: SortColumn,
    pub direction: SortDirection,
}

/// Фильтр применяется в Rust по сырым байтам, до того как что-либо пересечёт
/// границу IPC. Пустые поля означают «не фильтровать».
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct MessageFilter {
    #[serde(default)]
    pub key: String,
    #[serde(default)]
    pub value: String,
    #[serde(default)]
    pub case_sensitive: bool,
}

impl MessageFilter {
    pub fn is_empty(&self) -> bool {
        self.key.is_empty() && self.value.is_empty()
    }
}

/// Границы чтения — «не всё, а вот отсюда досюда».
///
/// Мода не задаётся отдельным полем: она однозначно следует из того, какие
/// границы заполнены. Пустая структура — обычное чтение с конца или с начала.
///
/// Офсет и время здесь не смешиваются: фронт заполняет либо пару офсетов, либо
/// пару отметок времени. Офсеты свои в каждой партиции, поэтому диапазон по ним
/// осмыслен только при ОДНОЙ выбранной партиции — это проверяет фронт, где
/// видно, что именно выбрано в селекторе. Время сквозное, и по нему диапазон
/// осмыслен на любом их числе.
#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ReadRange {
    /// Офсет первого нужного сообщения, inclusive.
    #[serde(default)]
    pub from_offset: Option<i64>,
    /// Офсет последнего нужного сообщения, inclusive.
    #[serde(default)]
    pub to_offset: Option<i64>,
    /// Unix millis. Первое сообщение со временем не раньше этого.
    #[serde(default)]
    pub from_timestamp: Option<i64>,
    /// Unix millis. Последнее сообщение со временем не позже этого —
    /// сам момент входит в диапазон.
    #[serde(default)]
    pub to_timestamp: Option<i64>,
}

impl ReadRange {
    pub fn is_empty(&self) -> bool {
        !self.has_from() && !self.has_to()
    }

    pub fn has_from(&self) -> bool {
        self.from_offset.is_some() || self.from_timestamp.is_some()
    }

    pub fn has_to(&self) -> bool {
        self.to_offset.is_some() || self.to_timestamp.is_some()
    }

    pub fn by_timestamp(&self) -> bool {
        self.from_timestamp.is_some() || self.to_timestamp.is_some()
    }

    /// В какую сторону читать с такими границами.
    ///
    /// Названа только верхняя граница — значит пользователь указал точку, ОТ
    /// которой хочет посмотреть НАЗАД, и порядок должен быть по убыванию.
    /// Названа нижняя — читаем вперёд от неё. Границ нет — как выбрано в
    /// селекторе.
    ///
    /// Правило живёт здесь, а не на фронте: иначе направление сортировки и
    /// направление обхода окон определялись бы в двух местах и могли разойтись.
    pub fn newest_first(&self, start_from: StartFrom) -> bool {
        match (self.has_from(), self.has_to()) {
            (false, true) => true,
            (true, _) => false,
            (false, false) => start_from == StartFrom::Newest,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct OpenTopicParams {
    pub topic: String,
    pub start_from: StartFrom,
    /// Сколько сообщений вычитать на КАЖДУЮ партицию (не общий бюджет на
    /// топик — при большом числе партиций итоговый объём в буфере кратно
    /// больше). Не путать с размером окна выдачи.
    pub limit: usize,
    /// Из каких партиций читать. `None` (как и пустой список) — из всех.
    ///
    /// Список, а не одна партиция: выбрать «партиции 3 и 7» — обычная задача
    /// при разборе инцидента, а читать ради неё весь топик значит платить за
    /// это квотой на чтение.
    #[serde(default)]
    pub partitions: Option<Vec<i32>>,
    #[serde(default)]
    pub filter: MessageFilter,
    /// Границы чтения. Пустые — читаем топик целиком с того конца, который
    /// назван в `start_from`.
    #[serde(default)]
    pub range: ReadRange,
    /// Сортировка по столбцу, выбранная в таблице до открытия этого топика.
    /// `None` — обычный порядок чтения.
    #[serde(default)]
    pub sort: Option<SortSpec>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct LoadMoreParams {
    /// Сколько ещё сообщений вычитать на каждую партицию, которая ещё не
    /// упёрлась в реальный EOF/начало топика.
    pub additional: usize,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct OpenTopicResult {
    /// Сколько строк видно с текущим фильтром — это и есть totalCount для UI.
    pub total: usize,
    /// Сколько всего лежит в буфере до фильтрации.
    pub loaded: usize,
    /// Сколько байт занимает арена. Приложение обещает быть экономным —
    /// пусть эта цифра будет наблюдаемой, а не на словах.
    pub buffer_bytes: usize,
    #[serde(flatten)]
    pub memory: MemoryUsage,
    /// true — упёрлись в лимит или таймаут, в топике есть ещё.
    pub truncated: bool,
    #[serde(flatten)]
    pub quota: QuotaInfo,
}

/// Сколько памяти держит буфер сообщений и сколько ему позволено.
///
/// Показывается шкалой в футере. Приложение обещает быть экономным на топиках,
/// где сообщения весят десятки килобайт, — и это обещание должно быть
/// проверяемым на глаз, а не на словах. Заодно из шкалы видно, почему выдача
/// оказалась усечённой: упёрлись в потолок, а не «приложение сломалось».
#[derive(Debug, Clone, Copy, Default, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct MemoryUsage {
    /// Занято по-настоящему: арена вместе с запасом ёмкости плюс индекс.
    /// Больше `buffer_bytes`, который считает только полезные байты.
    pub memory_bytes: usize,
    /// Потолок, на котором чтение останавливается.
    pub memory_limit: usize,
}

/// Что кластер реально даёт по скорости. Измеряется косвенно — см.
/// `kafka::quota`. В UI нужна потому, что без неё медленное чтение неотличимо
/// от зависшего приложения: пользователь видит замерший экран и винит mikui,
/// хотя это брокер придерживает ответы по квоте.
#[derive(Debug, Clone, Copy, Default, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct QuotaInfo {
    /// None — измерений пока недостаточно. Это СУММА по всем брокерам, с
    /// которых шли данные, — см. `active_brokers`.
    pub read_bytes_per_sec: Option<u64>,
    /// Самая длинная задержка, наложенная брокером, мс. 0 — не придерживал.
    pub peak_throttle_ms: u64,
    /// Со скольких брокеров шли данные. Нужно затем, что `consumer_byte_rate`
    /// применяет каждый брокер самостоятельно: под квотой 200 КБ/с чтение с
    /// семи брокеров законно даёт около 1.4 МБ/с суммарно, и без этого числа
    /// измеренная скорость выглядит невозможной.
    pub active_brokers: u32,
    /// Сколько брокеров всего в кластере. Знаменатель к `active_brokers`:
    /// «семь» правдоподобно при любом составе, «семь из десяти» — проверяемо.
    pub known_brokers: u32,
}

/// Снимок хода ещё не завершённого чтения — опрашивается фронтом по таймеру,
/// пока `open_topic`/`load_more` не ответили финальным `OpenTopicResult`.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct OpenTopicProgress {
    /// Топик, к которому относится снимок. Опрос идёт по таймеру и его ответ
    /// вполне может разминуться со сменой топика — без этого поля счётчики
    /// ушедшего топика на мгновение подменяли бы счётчики нового.
    pub topic: Option<String>,
    pub loaded: usize,
    pub total: usize,
    /// Размер арены. Раньше его тут не было, и шапка во время загрузки честно
    /// показывала «0 B» — единственное поле, которого не хватало.
    pub buffer_bytes: usize,
    #[serde(flatten)]
    pub memory: MemoryUsage,
    pub truncated: bool,
    /// true — чтения в фоне уже нет, снимок финальный.
    pub done: bool,
    #[serde(flatten)]
    pub quota: QuotaInfo,
}

/// Строка таблицы. Тело обрезано: таблица всё равно показывает его в одну
/// строку с многоточием. Полное тело отдаёт `get_message_body`.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct RowPreview {
    /// Индекс в текущем отфильтрованном представлении — по нему запрашивается тело.
    pub index: usize,
    pub partition: i32,
    pub offset: i64,
    /// Unix millis. Форматируется в JS лениво и только для видимых строк.
    pub timestamp: i64,
    pub key: String,
    pub preview: String,
    pub value_size: usize,
    /// Тело не является валидным UTF-8 (Avro, Protobuf, произвольные байты).
    /// У декодированного protobuf это false: наружу уехал JSON, а не байты.
    pub binary: bool,
    /// Схема к топику загружена, но это сообщение по ней не разобралось.
    /// В `preview` тогда лежит обычное текстовое представление: одно битое
    /// сообщение не повод перестать показывать топик.
    pub decode_error: Option<String>,
}

/// `Deserialize` здесь нужен ровно затем, что заголовки не только показываются
/// у прочитанного сообщения, но и вводятся руками у отправляемого.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct MessageHeader {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct FullMessage {
    pub partition: i32,
    pub offset: i64,
    pub timestamp: i64,
    pub key: String,
    /// Декодированный protobuf приезжает сюда компактным JSON — модалка
    /// разложит его отступами тем же кодом, каким давно печатает JSON-топики.
    pub value: String,
    /// Размер тела НА ПРОВОДЕ, а не длина `value`: модалка показывает, сколько
    /// сообщение весит в Kafka, и декодирование этого числа не меняет.
    pub value_size: usize,
    pub binary: bool,
    /// См. `RowPreview::decode_error`.
    pub decode_error: Option<String>,
    /// Имена enum-значений схемы, применённой к этому телу — модалка красит
    /// их отдельным цветом. Пусто, если тело не декодировано protobuf'ом: у
    /// произвольного JSON или текста никакой схемы, с которой можно было бы
    /// сверяться, попросту нет.
    pub enum_values: Vec<String>,
    pub headers: Vec<MessageHeader>,
}

// --- Отправка сообщения ------------------------------------------------------

/// В каком виде пользователь ввёл тело отправляемого сообщения.
///
/// Шире, чем `proto::BodyFormat`, намеренно. Тот описывает, КАК ПОКАЗЫВАТЬ уже
/// прочитанный топик, и hex ему не нужен: двоичное тело там и так видно меткой
/// `[binary]`. А при отправке hex — единственный способ положить в топик байты,
/// которых не выражает ни текст, ни схема.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum PayloadFormat {
    Text,
    Json,
    Proto,
    /// Схем-реестра в приложении пока нет: тело уезжает текстом, как `Text`.
    Avro,
    Hex,
}

/// Форма отправки ровно в том виде, в каком её заполнили.
///
/// Тело едет строкой, а не байтами: превращать его в байты — дело `lib.rs`,
/// потому что для `proto` для этого нужны и каталог настроек, и схема топика.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ProduceRequest {
    pub topic: String,
    /// `None` — партицию выбирает партишенер по ключу (murmur2, как в Java;
    /// см. `helpers::producer_config`).
    #[serde(default)]
    pub partition: Option<i32>,
    /// Пустая строка — ключа НЕТ, и это не то же самое, что ключ нулевой длины:
    /// от наличия ключа зависит и партиционирование, и compaction.
    #[serde(default)]
    pub key: String,
    #[serde(default)]
    pub headers: Vec<MessageHeader>,
    pub format: PayloadFormat,
    #[serde(default)]
    pub payload: String,
    /// Полное имя message, которым кодировать тело. Только для `proto`, и
    /// вполне может отличаться от сохранённого в схеме топика: отправляют и не
    /// тот тип, которым топик читают.
    #[serde(default)]
    pub message: Option<String>,
}

/// Готовая к отправке запись: тело уже в байтах.
///
/// Воркер не знает ни про схемы, ни про каталог настроек — к нему приезжает
/// уже закодированное.
#[derive(Debug, Clone)]
pub struct ProduceRecord {
    pub topic: String,
    pub partition: Option<i32>,
    pub key: Option<Vec<u8>>,
    pub headers: Vec<MessageHeader>,
    pub payload: Vec<u8>,
}

/// Куда легло отправленное сообщение. Берётся из отчёта о доставке, то есть
/// это то, что подтвердил брокер, а не то, что мы попросили.
#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct ProduceResult {
    pub partition: i32,
    pub offset: i64,
}

/// Насколько плохо то, что ввели в поле тела.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum IssueSeverity {
    /// Отправить всё равно можно: байты из введённого получаются, просто это
    /// не то, чем притворяется выбранный формат. Так ведёт себя невалидный
    /// JSON — он уедет в топик тем самым текстом, который набрали.
    Warning,
    /// Байтов из введённого не получить вовсе, отправка заблокирована.
    Error,
}

/// Что показать под полем ввода тела.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct PayloadIssue {
    pub severity: IssueSeverity,
    pub message: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `#[serde(flatten)]` легко даёт вложенный объект вместо плоских полей, а
    /// заметно это только по пустому месту в шапке приложения. Проверяем, что
    /// измеренная квота доезжает до фронта именно там, где её ждут.
    #[test]
    fn quota_fields_are_flattened_to_the_top_level() {
        let json = serde_json::to_value(OpenTopicResult {
            total: 10,
            loaded: 10,
            buffer_bytes: 4096,
            memory: MemoryUsage {
                memory_bytes: 8192,
                memory_limit: 256 * 1024 * 1024,
            },
            truncated: true,
            quota: QuotaInfo {
                read_bytes_per_sec: Some(204_800),
                peak_throttle_ms: 9_000,
                active_brokers: 7,
                known_brokers: 10,
            },
        })
        .unwrap();

        assert_eq!(json["read_bytes_per_sec"], 204_800);
        assert_eq!(json["peak_throttle_ms"], 9_000);
        assert_eq!(json["buffer_bytes"], 4096);
        // Шкала памяти читает эти два поля с верхнего уровня.
        assert_eq!(json["memory_bytes"], 8192);
        assert_eq!(json["memory_limit"], 256 * 1024 * 1024);
        assert!(json.get("quota").is_none(), "поля должны быть плоскими");
        assert!(json.get("memory").is_none(), "поля должны быть плоскими");
    }

    #[test]
    fn progress_carries_the_same_fields_the_header_reads() {
        let json = serde_json::to_value(OpenTopicProgress {
            topic: Some("t".into()),
            loaded: 5,
            total: 5,
            buffer_bytes: 128,
            memory: MemoryUsage::default(),
            truncated: false,
            done: false,
            quota: QuotaInfo::default(),
        })
        .unwrap();

        // Ни одного измерения — поле обязано быть null, а не отсутствовать:
        // иначе на фронте `p.read_bytes_per_sec` был бы undefined.
        assert!(json["read_bytes_per_sec"].is_null());
        assert_eq!(json["peak_throttle_ms"], 0);
        assert_eq!(json["buffer_bytes"], 128);
    }
}
