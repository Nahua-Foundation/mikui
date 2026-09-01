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

#[derive(Debug, Clone, Serialize)]
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
