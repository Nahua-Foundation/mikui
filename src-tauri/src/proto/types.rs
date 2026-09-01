use serde::{Deserialize, Serialize};

/// Как показывать тело сообщений топика.
///
/// `Json` — исходное поведение приложения: тело едет как текст, а фронт сам
/// решает, красиво ли его печатать. `Proto` включает декодирование по
/// загруженной схеме ещё в Rust, до пересечения границы IPC.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BodyFormat {
    #[default]
    Json,
    Text,
    Proto,
    /// Схем-реестра в приложении пока нет; значение хранится, но ничего не меняет.
    Avro,
}

/// Один .proto, привязанный к топику.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ProtoFile {
    /// Путь внутри каталога схемы — он же то имя, под которым файл виден в
    /// `import`. У корневых файлов это просто имя файла, а у того, который
    /// кто-то импортирует как `common/types.proto`, — этот самый путь целиком:
    /// иначе импорт не разрешился бы (см. `files::layout`).
    pub name: String,
    /// Откуда файл взяли. По нему работает «перечитать с диска».
    pub source: String,
}

/// Привязка схемы к топику. Ровно то, что лежит в `proto.json`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct TopicSchema {
    /// Идентификатор сохранённого кластера, а для подключения из формы — его
    /// отображаемое имя. Топики с одинаковыми именами в dev и prod несут разные
    /// контракты, поэтому привязка не может быть просто по имени топика.
    pub cluster: String,
    pub topic: String,
    #[serde(default)]
    pub format: BodyFormat,
    #[serde(default)]
    pub files: Vec<ProtoFile>,
    /// Полное имя message, которым декодируется тело. `None` — пользователь
    /// ещё не выбрал его из нескольких.
    #[serde(default)]
    pub message: Option<String>,
    /// Имя каталога с копиями .proto внутри `<config>/proto/`. Хранится, а не
    /// вычисляется каждый раз: так переименование правила вычисления не
    /// осиротит уже сохранённые схемы.
    pub dir: String,
}

impl TopicSchema {
    /// Есть ли чем декодировать. Одних файлов мало: пока не выбран message,
    /// декодеру не за что зацепиться.
    pub fn decodes(&self) -> bool {
        self.format == BodyFormat::Proto && self.message.is_some() && !self.files.is_empty()
    }
}

/// Что видит UI. Отдельно от `TopicSchema` затем, что `messages` и `error` —
/// результат разбора, а не содержимое файла настроек: писать их на диск значило
/// бы хранить то, что устаревает при первом же изменении .proto.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct TopicSchemaView {
    pub cluster: String,
    pub topic: String,
    pub format: BodyFormat,
    pub files: Vec<ProtoFile>,
    pub message: Option<String>,
    /// Все message из добавленных файлов, по алфавиту — это и есть содержимое
    /// выпадающего списка.
    pub messages: Vec<String>,
    /// Схема лежит на диске, но не разбирается. Файлы всё равно показываем:
    /// иначе пользователю нечего чинить.
    pub error: Option<String>,
}

impl TopicSchemaView {
    pub fn new(schema: &TopicSchema, messages: Vec<String>, error: Option<String>) -> Self {
        Self {
            cluster: schema.cluster.clone(),
            topic: schema.topic.clone(),
            format: schema.format,
            files: schema.files.clone(),
            message: schema.message.clone(),
            messages,
            error,
        }
    }
}
