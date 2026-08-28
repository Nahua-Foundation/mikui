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
    /// Нужен, чтобы достать пароль из keychain, не гоняя его через IPC.
    #[serde(default)]
    pub id: Option<String>,
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

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct OpenTopicParams {
    pub topic: String,
    pub start_from: StartFrom,
    /// Сколько сообщений вычитать на КАЖДУЮ партицию (не общий бюджет на
    /// топик — при большом числе партиций итоговый объём в буфере кратно
    /// больше). Не путать с размером окна выдачи.
    pub limit: usize,
    /// None — все партиции топика.
    pub partition: Option<i32>,
    #[serde(default)]
    pub filter: MessageFilter,
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
    /// true — упёрлись в лимит или таймаут, в топике есть ещё.
    pub truncated: bool,
}

/// Снимок хода ещё не завершённого чтения — опрашивается фронтом по таймеру,
/// пока `open_topic`/`load_more` не ответили финальным `OpenTopicResult`.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct OpenTopicProgress {
    pub loaded: usize,
    pub total: usize,
    pub truncated: bool,
    /// true — чтения в фоне уже нет, снимок финальный.
    pub done: bool,
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
    pub binary: bool,
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
    pub value: String,
    pub value_size: usize,
    pub binary: bool,
    pub headers: Vec<MessageHeader>,
}
