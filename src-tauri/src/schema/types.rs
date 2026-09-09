use serde::{Deserialize, Serialize};

/// Как показывать тело сообщений топика.
///
/// `Json` — исходное поведение приложения: тело едет как текст, а фронт сам
/// решает, красиво ли его печатать. `Proto` и `Avro` включают декодирование по
/// схеме ещё в Rust, до пересечения границы IPC.
///
/// `JsonSchema` стоит особняком и по причине, которую стоит назвать: от `Json`
/// он отличается НЕ разбором тела — тело там такой же JSON, — а тем, что перед
/// ним стоит confluent-заголовок, и тем, что у топика есть контракт в реестре.
/// Первое чинит показ (пять байт заголовка ехали в UI мусором перед `{`),
/// второе даёт проверку при отправке. Свести их в один формат было бы нельзя:
/// голому JSON-топику заголовок снимать не с чего, а `Json` — это ещё и
/// осознанный выбор «показывай как есть», который обязан отменять
/// автоопределение.
///
/// `Hex` — тоже показ без всякой схемы, но противоположный `Text`: тот
/// показывает то, что удалось прочитать как UTF-8, подставляя символы замены
/// вместо остального, а этот — сами байты, все до единого. Нужен там, где тело
/// не рассматривают, а забирают: чтобы переслать его дальше или воспроизвести
/// в другом месте, и метка `[binary]` для этого бесполезна. Печатается ровно в
/// том виде, который форма отправки принимает обратно (`kafka::hex`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BodyFormat {
    #[default]
    Json,
    Text,
    Proto,
    Avro,
    #[serde(rename = "jsonschema")]
    JsonSchema,
    Hex,
}

impl BodyFormat {
    /// Название формата тем же словом, каким он записан на диске и каким его
    /// знает фронт. Нужно ссылке на сообщение: она несёт подсказку «этот топик
    /// читается как protobuf», и слово в ней обязано быть тем же самым —
    /// получатель сравнивает его со своим форматом (см. `crate::link`).
    ///
    /// Совпадение с serde закреплено тестом ниже, а не устным договором.
    pub fn as_str(self) -> &'static str {
        match self {
            BodyFormat::Json => "json",
            BodyFormat::Text => "text",
            BodyFormat::Proto => "proto",
            BodyFormat::Avro => "avro",
            BodyFormat::JsonSchema => "jsonschema",
            BodyFormat::Hex => "hex",
        }
    }
}

/// Что делать с конвертом Debezium / Kafka Connect в строке таблицы.
///
/// Живёт рядом с `BodyFormat` и хранится там же — в записи топика, — потому что
/// это ровно такая же настройка ПОКАЗА. Но настройка независимая: формат
/// отвечает на вопрос «как превратить байты в JSON», линза — «что из этого JSON
/// показать». Debezium с Avro-сериализатором обычное дело, и линза обязана
/// работать поверх любого формата.
///
/// `None` в записи — «не выбирали», и тогда конверт распознаётся по самому телу
/// (`kafka::lens`). Ровно то же правило, что у `format` с автоопределением по
/// реестру, включая то, что `Off` — это ОТВЕТ пользователя, а не отсутствие
/// ответа: распознавание его отменять не должно.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LensSetting {
    Off,
    Connect,
    Debezium,
}

/// Откуда брать Avro-схему топика.
///
/// Источника два, и они взаимоисключающи: два ответа на вопрос «чем
/// декодировать» — это способ показывать то одно, то другое в зависимости от
/// того, какой из них проверили раньше.
///
/// Заметное отличие от protobuf: реестр может быть настроен, а `subject` — нет,
/// и это рабочее состояние, а не недонастроенное. В confluent-формате id схемы
/// едет в каждом сообщении, и закреплять топику ещё и subject незачем; он нужен
/// голому datum'у, у которого подсказок в теле нет, и форме отправки.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct AvroBinding {
    /// Локальные .avsc. Лежат в `<config>/avro/<dir>` — тот же `dir`, что и у
    /// .proto, только каталог соседний.
    #[serde(default)]
    pub files: Vec<SchemaFile>,
    /// Полное имя записи, которой декодируется тело, когда схема взята из
    /// файлов. `None` — в наборе она одна и выбирать не из чего.
    #[serde(default)]
    pub record: Option<String>,
    /// Subject в реестре кластера.
    #[serde(default)]
    pub subject: Option<String>,
    /// Версия subject. `None` — последняя; она перечитывается при каждом
    /// применении схемы, так что топик сам подхватывает новый контракт.
    #[serde(default)]
    pub version: Option<i32>,
}

impl AvroBinding {
    /// Пустая привязка — это отсутствие привязки, и хранить её незачем.
    pub fn is_empty(&self) -> bool {
        self.files.is_empty() && self.subject.is_none()
    }
}

/// Откуда брать JSON-схему топика.
///
/// Устроено как `AvroBinding`, и по тем же причинам: источника два, они
/// взаимоисключающи, а пустая привязка при настроенном реестре — рабочее
/// состояние, потому что id схемы едет в заголовке каждого сообщения.
///
/// Чего здесь нет по сравнению с Avro — выбора корневой записи: у JSON Schema
/// корень один, сама схема им и является, и выбирать не из чего.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct JsonBinding {
    /// Локальные файлы схем. Лежат в `<config>/jsonschema/<dir>` — тот же
    /// `dir`, что у .proto и .avsc, только каталог третий.
    #[serde(default)]
    pub files: Vec<SchemaFile>,
    /// Subject в реестре кластера.
    #[serde(default)]
    pub subject: Option<String>,
    /// Версия subject. `None` — последняя; перечитывается при каждом обращении,
    /// так что топик сам подхватывает новый контракт.
    #[serde(default)]
    pub version: Option<i32>,
}

impl JsonBinding {
    /// Пустая привязка — это отсутствие привязки, и хранить её незачем.
    pub fn is_empty(&self) -> bool {
        self.files.is_empty() && self.subject.is_none()
    }
}

/// Один файл схемы, привязанный к топику: .proto или .avsc.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct SchemaFile {
    /// Путь внутри каталога схемы — он же то имя, под которым файл виден в
    /// `import`. У корневых файлов это просто имя файла, а у того, который
    /// кто-то импортирует как `common/types.proto`, — этот самый путь целиком:
    /// иначе импорт не разрешился бы (см. `proto::imports::layout`). У .avsc
    /// импортов нет, и имя там всегда basename.
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
    /// Чем показывать тела. `None` — пользователь формат не выбирал, и решает
    /// автоопределение: реестр кластера спрашивают, держит ли он схему для
    /// этого топика (см. `store::detected_subject`). Отличать «не выбирали» от
    /// «выбрали json» обязательно — иначе выбор json на автоопределённом топике
    /// было бы негде хранить, и топик возвращался бы в avro сам собой.
    ///
    /// Отсутствие ключа в файле и есть «не выбирали»: так `proto.json`,
    /// написанный прошлыми версиями без всякого автоопределения, читается без
    /// миграции — топики, которым формат не назначали, там ровно такие.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub format: Option<BodyFormat>,
    #[serde(default)]
    pub files: Vec<SchemaFile>,
    /// Полное имя message, которым декодируется тело. `None` — пользователь
    /// ещё не выбрал его из нескольких.
    #[serde(default)]
    pub message: Option<String>,
    /// Имя каталога с копиями файлов схемы внутри `<config>/proto/` и
    /// `<config>/avro/`. Хранится, а не вычисляется каждый раз: так
    /// переименование правила вычисления не осиротит уже сохранённые схемы.
    pub dir: String,
    /// Avro-половина настроек. `None` у топиков, которых она не касается, — то
    /// есть у всех, сохранённых до её появления: старый `proto.json` читается
    /// без миграции.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub avro: Option<AvroBinding>,
    /// JSON-Schema-половина. Ровно на тех же условиях, что и `avro`: ключа нет
    /// — топика она не касается, и `proto.json`, написанный прошлыми версиями,
    /// читается без миграции.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub json: Option<JsonBinding>,
    /// Выбор линзы. `None` — не выбирали, решает распознавание по телу.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lens: Option<LensSetting>,
}

impl TopicSchema {
    /// Есть ли чем декодировать.
    ///
    /// У protobuf одних файлов мало: пока не выбран message, декодеру не за что
    /// зацепиться. У Avro условие другое — хватает ЛИБО своей схемы (файлы или
    /// subject), ЛИБО одного лишь настроенного реестра: в confluent-формате
    /// схема приедет с сообщением. Настроен ли реестр, здесь не видно — он
    /// живёт на кластере, — поэтому решение принимает `store::decoder`, а тут
    /// остаётся то, что видно по самой записи.
    ///
    /// Формат приходит аргументом, а не берётся из записи: он мог не выбираться
    /// вовсе, и тогда его назначило автоопределение.
    pub fn decodes(&self, format: BodyFormat) -> bool {
        match format {
            BodyFormat::Proto => self.message.is_some() && !self.files.is_empty(),
            BodyFormat::Avro => true,
            // JSON Schema не требует вообще ничего: снять confluent-заголовок
            // декодер умеет и без схемы, а больше для показа ничего и не надо.
            BodyFormat::JsonSchema => true,
            // Hex тем более: он печатает те самые байты, что пришли, и схема
            // ему не нужна по определению.
            BodyFormat::Hex => true,
            _ => false,
        }
    }

    pub fn avro(&self) -> Option<&AvroBinding> {
        self.avro.as_ref()
    }

    pub fn json(&self) -> Option<&JsonBinding> {
        self.json.as_ref()
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
    pub files: Vec<SchemaFile>,
    pub message: Option<String>,
    /// Все message из добавленных файлов, по алфавиту — это и есть содержимое
    /// выпадающего списка.
    pub messages: Vec<String>,
    /// Схема лежит на диске, но не разбирается. Файлы всё равно показываем:
    /// иначе пользователю нечего чинить.
    pub error: Option<String>,
    /// Avro-половина. `None` — топика она не касается.
    pub avro: Option<AvroView>,
    /// JSON-Schema-половина. `None` — топика она не касается.
    pub json: Option<JsonView>,
    /// Выбранная линза. `None` — не выбирали: конверт распознаётся по телу.
    pub lens: Option<LensSetting>,
    /// Каким форматом реестр называет схему, которую держит для этого топика
    /// САМ (`AVRO` / `PROTOBUF` / `JSON`). `None` — не держит или реестра нет.
    ///
    /// Отдельно от `detected` в половинах: тем приезжает уже отфильтрованное по
    /// своему типу, а форме нужен сырой ответ. Нужен он ровно ради одного
    /// случая — PROTOBUF. Схему protobuf из реестра приложение пока не тянет,
    /// поэтому автоопределение на нём НИЧЕГО не переключает, и объяснить, что
    /// контракт у топика всё-таки есть и надо загрузить .proto, больше нечем.
    pub detected_kind: Option<String>,
}

/// JSON-Schema-половина того, что видит форма настроек топика.
///
/// Устроена как `AvroView` минус то, чего у формата нет: списка записей, потому
/// что корень у схемы один.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct JsonView {
    pub files: Vec<SchemaFile>,
    pub subject: Option<String>,
    pub version: Option<i32>,
    /// Настроен ли у кластера реестр.
    pub registry: bool,
    /// Subject, который реестр держит для этого топика сам и который при этом
    /// и правда JSON-схема. См. `AvroView::detected`.
    pub detected: Option<String>,
    /// Схема есть, но не разбирается или не добывается.
    pub error: Option<String>,
}

/// Avro-половина того, что видит форма настроек топика.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct AvroView {
    pub files: Vec<SchemaFile>,
    pub record: Option<String>,
    pub subject: Option<String>,
    pub version: Option<i32>,
    /// Имена записей из добавленных .avsc, по алфавиту — содержимое селектора.
    pub records: Vec<String>,
    /// Настроен ли у кластера реестр. Форма по нему решает, предлагать ли
    /// выбор subject или сначала отправить в настройки подключения.
    pub registry: bool,
    /// Subject, который реестр держит для этого топика САМ, без выбора
    /// пользователя, — `<topic>-value` по умолчанию любого confluent-
    /// сериализатора. Отдельно от `subject`: тот выбрали руками, а этот
    /// найден, и записывать догадку на диск нельзя — завтра ответ реестра
    /// может стать другим. Форме он нужен, чтобы сказать, откуда взялся
    /// формат, а форме отправки — чтобы было чем строить заготовку.
    pub detected: Option<String>,
    /// Схема есть, но не разбирается или не добывается. Отдельно от
    /// `TopicSchemaView::error`: у топика могут быть загружены и .proto, и
    /// .avsc, и сломаться может любая из двух половин, а чинить надо ту,
    /// которая сломалась.
    pub error: Option<String>,
}

/// Всё, что форме отправки нужно знать про выбранный тип.
///
/// Одной структурой, а не двумя командами: обе половины описывают один и тот же
/// тип, приезжают на одно и то же событие (пользователь выбрал message или
/// subject) и разъехаться не должны — подсветка обязана относиться ровно к той
/// схеме, по которой построена заготовка. Общая на оба формата: у Avro тоже
/// есть enum, и красить его надо тем же способом.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct MessageForm {
    /// JSON-заготовка: все поля на месте, значения нулевые.
    pub template: String,
    /// Имена enum-значений этого типа — форма красит их отдельным цветом,
    /// как это делает модалка чтения.
    pub enum_values: Vec<String>,
    /// Subject, которым заготовка построена на самом деле. Пусто у protobuf и
    /// у локальных .avsc — там subject'а нет вовсе.
    ///
    /// Нужен ради случая «пользователь не выбирал ничего»: subject нашёлся сам,
    /// в реестре, и показать его надо — иначе форма молчит о том, каким
    /// контрактом она собралась кодировать.
    pub subject: Option<String>,
}

impl TopicSchemaView {
    /// `format` — ДЕЙСТВУЮЩИЙ формат, а не тот, что лежит в записи: он мог не
    /// выбираться вовсе, и тогда его назначило автоопределение. Наружу уезжает
    /// всегда конкретный: фронту не за что зацепиться в «не выбран».
    pub fn new(
        schema: &TopicSchema,
        format: BodyFormat,
        messages: Vec<String>,
        error: Option<String>,
    ) -> Self {
        Self {
            cluster: schema.cluster.clone(),
            topic: schema.topic.clone(),
            format,
            files: schema.files.clone(),
            message: schema.message.clone(),
            messages,
            error,
            avro: None,
            json: None,
            lens: schema.lens,
            detected_kind: None,
        }
    }

    /// Дополняет вид avro-половиной. Отдельным шагом, а не аргументом `new`:
    /// половина эта считается по кластеру (настроен ли реестр), а `new` зовут и
    /// оттуда, где кластера под рукой нет.
    pub fn with_avro(mut self, avro: Option<AvroView>) -> Self {
        self.avro = avro;
        self
    }

    /// То же для JSON-Schema-половины и по той же причине.
    pub fn with_json(mut self, json: Option<JsonView>) -> Self {
        self.json = json;
        self
    }

    pub fn with_detected_kind(mut self, kind: Option<String>) -> Self {
        self.detected_kind = kind;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `as_str` пишет формат тем же словом, что и serde.
    ///
    /// Проверяется, а не подразумевается: этим словом подписана схема топика на
    /// диске, им же приезжает подсказка в ссылке на сообщение, и разойдись эти
    /// два места — ссылка на protobuf-топик молча перестала бы узнавать
    /// protobuf, ничего при этом не сломав на вид.
    #[test]
    fn as_str_matches_what_serde_writes() {
        for format in [
            BodyFormat::Json,
            BodyFormat::Text,
            BodyFormat::Proto,
            BodyFormat::Avro,
            BodyFormat::JsonSchema,
            BodyFormat::Hex,
        ] {
            let serialized = serde_json::to_string(&format).unwrap();
            assert_eq!(serialized, format!("\"{}\"", format.as_str()));
        }
    }
}
