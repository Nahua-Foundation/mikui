//! Привязки схем к топикам: файл настроек, каталоги с копиями .proto и все
//! операции над ними.
//!
//! Общее правило на весь модуль: изменение применяется ТОЛЬКО если после него
//! схема разбирается. Каталог собирается рядом, проверяется разбором и лишь
//! потом встаёт на место (см. `files::Staged`), а `proto.json` переписывается
//! последним. Невалидный .proto не должен ни сохраниться, ни испортить схему,
//! которая до него работала.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::decoder::ProtoDecoder;
use super::files::{self, Pending};
use super::schema;
use super::types::{BodyFormat, ProtoFile, TopicSchema, TopicSchemaView};
use crate::config;

const SCHEMAS_FILE: &str = "proto.json";
const SCHEMAS_DIR: &str = "proto";

// --- Файл настроек ----------------------------------------------------------

/// Каталог настроек приложения передаётся аргументом, а не достаётся из
/// `AppHandle`: так весь модуль прогоняется на временном каталоге в тестах, а
/// не только внутри живого Tauri. Обёртки над `AppHandle` — в `proto::mod`.
pub fn list(root: &Path) -> Result<Vec<TopicSchema>, String> {
    config::read_json(&root.join(SCHEMAS_FILE))
}

fn save(root: &Path, schemas: &[TopicSchema]) -> Result<(), String> {
    let json = serde_json::to_vec_pretty(schemas)
        .map_err(|e| format!("can't serialize proto schemas: {e}"))?;
    config::write_atomic(&root.join(SCHEMAS_FILE), &json)
}

/// Каталог с копиями .proto этой схемы.
fn dir_of(root: &Path, schema: &TopicSchema) -> PathBuf {
    root.join(SCHEMAS_DIR).join(&schema.dir)
}

/// Имя каталога схемы: FNV-1a от пары кластер-топик.
///
/// Хэш, а не имена как есть: в имени топика бывает что угодно, включая знаки,
/// которые файловая система не примет, а длина легко перерастает лимит пути.
/// Алгоритм зафиксирован здесь и не меняется — иначе уже сохранённые схемы
/// потеряли бы свои каталоги. Впрочем, имя каталога и хранится в записи, так
/// что вычисляется оно ровно один раз, при заведении.
fn new_dir_name(cluster: &str, topic: &str) -> String {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    let bytes = cluster
        .as_bytes()
        .iter()
        .chain(b"\0".iter())
        .chain(topic.as_bytes().iter());
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("{hash:016x}")
}

fn position(schemas: &[TopicSchema], cluster: &str, topic: &str) -> Option<usize> {
    schemas
        .iter()
        .position(|s| s.cluster == cluster && s.topic == topic)
}

/// Запись, в которой нечего хранить: держать её в файле — только копить мусор.
fn is_trivial(schema: &TopicSchema) -> bool {
    schema.files.is_empty() && schema.message.is_none() && schema.format == BodyFormat::default()
}

// --- Чтение -----------------------------------------------------------------

/// Схема топика вместе со списком message. Разбор ленивый и кэшированный;
/// не разобравшаяся схема возвращается с `error`, а не проглатывается: файлы
/// надо показать, иначе пользователю нечего чинить.
pub fn view(
    root: &Path,
    cluster: &str,
    topic: &str,
) -> Result<Option<TopicSchemaView>, String> {
    let schemas = list(root)?;
    let Some(index) = position(&schemas, cluster, topic) else {
        return Ok(None);
    };
    Ok(Some(describe(root, &schemas[index])?))
}

fn describe(root: &Path, schema: &TopicSchema) -> Result<TopicSchemaView, String> {
    if schema.files.is_empty() {
        return Ok(TopicSchemaView::new(schema, Vec::new(), None));
    }
    let dir = dir_of(root, schema);
    match schema::linked(&dir, &schema.files) {
        Ok(linked) => Ok(TopicSchemaView::new(schema, linked.messages.clone(), None)),
        Err(e) => Ok(TopicSchemaView::new(schema, Vec::new(), Some(e))),
    }
}

/// Декодер для топика, если он настроен и схема разбирается.
pub fn decoder(
    root: &Path,
    cluster: &str,
    topic: &str,
) -> Result<Option<Arc<ProtoDecoder>>, String> {
    let schemas = list(root)?;
    let Some(index) = position(&schemas, cluster, topic) else {
        return Ok(None);
    };
    let schema = &schemas[index];
    if !schema.decodes() {
        return Ok(None);
    }

    let dir = dir_of(root, schema);
    let linked = schema::linked(&dir, &schema.files)?;
    let name = schema.message.as_deref().unwrap_or_default();
    let message = linked
        .message(name)
        .ok_or_else(|| format!("message {name} is not in the loaded .proto files"))?;
    Ok(Some(Arc::new(ProtoDecoder::new(message))))
}

/// Descriptor одного message схемы — по имени, а не по тому, что выбрано в
/// настройках топика.
///
/// Имя приходит отдельным аргументом ради отправки: класть в топик руками
/// приходится и не тот тип, которым топик читают (команду, а не событие;
/// сообщение старой версии контракта), и заставлять ради этого переключать
/// настройки показа значило бы менять то, как выглядит уже открытая таблица.
pub fn message(
    root: &Path,
    cluster: &str,
    topic: &str,
    name: &str,
) -> Result<protobuf::reflect::MessageDescriptor, String> {
    let schemas = list(root)?;
    let index = position(&schemas, cluster, topic)
        .ok_or_else(|| format!("no .proto schema for topic {topic}"))?;
    let schema = &schemas[index];
    if schema.files.is_empty() {
        return Err(format!("no .proto files loaded for topic {topic}"));
    }

    let dir = dir_of(root, schema);
    let linked = schema::linked(&dir, &schema.files)?;
    linked
        .message(name)
        .ok_or_else(|| format!("message {name} is not in the loaded .proto files"))
}

// --- Изменение --------------------------------------------------------------

/// Добавляет .proto к топику.
///
/// Разбирается вся схема целиком, вместе с уже добавленным: файл, который сам
/// по себе валиден, но конфликтует с соседом, — такая же поломка, и принимать
/// его нельзя. Успех переводит топик в PROTO-формат: ради этого файл и грузили.
pub fn add_files(
    root: &Path,
    cluster: &str,
    topic: &str,
    sources: &[String],
) -> Result<TopicSchemaView, String> {
    if sources.is_empty() {
        return Err("no .proto files selected".to_string());
    }

    let mut schemas = list(root)?;
    let index = ensure_record(&mut schemas, cluster, topic);
    let dir = dir_of(root, &schemas[index]);

    let mut pending = keep_existing(&dir, &schemas[index].files)?;

    // Байты новых файлов нужны раньше их имён: имя берётся из импортов, а те
    // объявлены в тексте — и старых файлов, и новых.
    let mut incoming = Vec::with_capacity(sources.len());
    for source in sources {
        let bytes = std::fs::read(source).map_err(|e| format!("can't read {source}: {e}"))?;
        incoming.push((source.clone(), bytes));
    }

    let imports = files::imports_in(
        pending
            .iter()
            .map(|p| p.bytes.as_slice())
            .chain(incoming.iter().map(|(_, b)| b.as_slice())),
    );

    for (source, bytes) in incoming {
        let name = files::layout(Path::new(&source), &imports);
        // Тот же файл, добавленный повторно, — это «перечитать», а не дубликат.
        pending.retain(|p| p.name != name);
        pending.push(Pending {
            name,
            source,
            bytes,
        });
    }

    let messages = commit(&mut schemas[index], &dir, &pending)?;
    schemas[index].format = BodyFormat::Proto;
    settle_message(&mut schemas[index], &messages);

    let view = TopicSchemaView::new(&schemas[index], messages, None);
    save(root, &schemas)?;
    Ok(view)
}

/// Перечитывает файлы с диска — по одному имени или все сразу.
pub fn refresh(
    root: &Path,
    cluster: &str,
    topic: &str,
    name: Option<&str>,
) -> Result<TopicSchemaView, String> {
    let mut schemas = list(root)?;
    let index = position(&schemas, cluster, topic)
        .ok_or_else(|| format!("no .proto schema for topic {topic}"))?;
    let dir = dir_of(root, &schemas[index]);

    if let Some(name) = name {
        if !schemas[index].files.iter().any(|f| f.name == name) {
            return Err(format!("{name} is not part of this schema"));
        }
    }

    let mut pending = Vec::with_capacity(schemas[index].files.len());
    for file in &schemas[index].files {
        let stale = name.is_none_or(|wanted| wanted == file.name);
        let bytes = if stale {
            std::fs::read(&file.source)
                .map_err(|e| format!("can't re-read {}: {e}", file.source))?
        } else {
            read_copy(&dir, file)?
        };
        pending.push(Pending {
            name: file.name.clone(),
            source: file.source.clone(),
            bytes,
        });
    }

    let messages = commit(&mut schemas[index], &dir, &pending)?;
    settle_message(&mut schemas[index], &messages);

    let view = TopicSchemaView::new(&schemas[index], messages, None);
    save(root, &schemas)?;
    Ok(view)
}

/// Убирает один .proto. Оставшееся обязано по-прежнему разбираться: удалить
/// файл, на который кто-то ссылается импортом, — это сломать схему, и такое
/// изменение не применяется.
pub fn remove_file(
    root: &Path,
    cluster: &str,
    topic: &str,
    name: &str,
) -> Result<Option<TopicSchemaView>, String> {
    let mut schemas = list(root)?;
    let Some(index) = position(&schemas, cluster, topic) else {
        return Ok(None);
    };
    let dir = dir_of(root, &schemas[index]);

    let mut pending = keep_existing(&dir, &schemas[index].files)?;
    let before = pending.len();
    pending.retain(|p| p.name != name);
    if pending.len() == before {
        return Err(format!("{name} is not part of this schema"));
    }

    // Последний файл ушёл — вместе с ним уходят и каталог, и выбор message,
    // и PROTO-формат: декодировать больше нечем.
    if pending.is_empty() {
        schema::invalidate(&dir);
        let _ = std::fs::remove_dir_all(&dir);
        schemas[index].files.clear();
        schemas[index].message = None;
        schemas[index].format = BodyFormat::default();

        let view = TopicSchemaView::new(&schemas[index], Vec::new(), None);
        if is_trivial(&schemas[index]) {
            schemas.remove(index);
        }
        save(root, &schemas)?;
        return Ok(Some(view));
    }

    let messages = commit(&mut schemas[index], &dir, &pending)?;
    settle_message(&mut schemas[index], &messages);

    let view = TopicSchemaView::new(&schemas[index], messages, None);
    save(root, &schemas)?;
    Ok(Some(view))
}

/// Сохраняет выбор из формы: формат тела и основной message.
///
/// Файлов не касается — их правят операции выше, каждая со своей проверкой.
pub fn set_options(
    root: &Path,
    cluster: &str,
    topic: &str,
    format: BodyFormat,
    message: Option<String>,
) -> Result<TopicSchemaView, String> {
    let mut schemas = list(root)?;
    let index = ensure_record(&mut schemas, cluster, topic);
    let described = describe(root, &schemas[index])?;

    if let Some(name) = message.as_deref() {
        if !described.messages.iter().any(|m| m == name) {
            return Err(format!("unknown message {name}"));
        }
    }
    schemas[index].format = format;
    schemas[index].message = message;

    let view = TopicSchemaView::new(&schemas[index], described.messages, described.error);
    if is_trivial(&schemas[index]) {
        schemas.remove(index);
    }
    save(root, &schemas)?;
    Ok(view)
}

/// Забывает все схемы кластера вместе с копиями .proto.
///
/// Зовётся при удалении подключения: осиротевшие каталоги контрактов никому не
/// нужны ровно так же, как осиротевшие пароли в keychain.
pub fn forget_cluster(root: &Path, cluster: &str) -> Result<(), String> {
    let mut schemas = list(root)?;
    let doomed: Vec<TopicSchema> = schemas
        .iter()
        .filter(|s| s.cluster == cluster)
        .cloned()
        .collect();
    if doomed.is_empty() {
        return Ok(());
    }

    for victim in &doomed {
        let dir = dir_of(root, victim);
        schema::invalidate(&dir);
        let _ = std::fs::remove_dir_all(&dir);
    }
    schemas.retain(|s| s.cluster != cluster);
    save(root, &schemas)
}

// --- Общая механика ---------------------------------------------------------

fn ensure_record(schemas: &mut Vec<TopicSchema>, cluster: &str, topic: &str) -> usize {
    if let Some(index) = position(schemas, cluster, topic) {
        return index;
    }
    schemas.push(TopicSchema {
        cluster: cluster.to_string(),
        topic: topic.to_string(),
        format: BodyFormat::default(),
        files: Vec::new(),
        message: None,
        dir: new_dir_name(cluster, topic),
    });
    schemas.len() - 1
}

/// Байты уже добавленных файлов — из своей копии.
///
/// Копии может не оказаться, если каталог настроек чистили руками; тогда
/// выручает исходный путь. Это не «перечитать с диска»: там перечитывание
/// затребовано пользователем, а здесь оно последнее средство.
fn keep_existing(dir: &Path, files: &[ProtoFile]) -> Result<Vec<Pending>, String> {
    files
        .iter()
        .map(|file| {
            Ok(Pending {
                name: file.name.clone(),
                source: file.source.clone(),
                bytes: read_copy(dir, file)?,
            })
        })
        .collect()
}

fn read_copy(dir: &Path, file: &ProtoFile) -> Result<Vec<u8>, String> {
    match std::fs::read(dir.join(&file.name)) {
        Ok(bytes) => Ok(bytes),
        Err(_) => std::fs::read(&file.source)
            .map_err(|e| format!("{} is gone and {} can't be read: {e}", file.name, file.source)),
    }
}

/// Складывает набор файлов, проверяет его разбором и, только если он удался,
/// ставит каталог на место и переписывает список файлов схемы.
///
/// Возвращает message, которые в этом наборе объявлены.
fn commit(
    schema: &mut TopicSchema,
    dir: &Path,
    pending: &[Pending],
) -> Result<Vec<String>, String> {
    let staged = files::Staged::write(dir.to_path_buf(), pending)?;

    let listing: Vec<ProtoFile> = pending
        .iter()
        .map(|p| ProtoFile {
            name: p.name.clone(),
            source: p.source.clone(),
        })
        .collect();

    // Разбираем ещё во временном каталоге: неудача не должна оставить следа.
    let linked = schema::parse(staged.path(), &listing)?;
    let messages = linked.messages.clone();

    // Прошлый разбор относится к прежнему содержимому каталога и с этого
    // момента врёт.
    schema::invalidate(dir);
    staged.commit()?;
    schema.files = listing;
    Ok(messages)
}

/// Приводит выбранный message в соответствие с тем, что теперь есть в схеме.
///
/// Единственный message подставляется сам — выбирать не из чего, а требовать
/// выбор значило бы требовать лишний клик. Исчезнувший из схемы выбор
/// сбрасывается: молча декодировать не тем, что показано в форме, нельзя.
fn settle_message(schema: &mut TopicSchema, messages: &[String]) {
    if let [only] = messages {
        schema.message = Some(only.clone());
        return;
    }
    if let Some(chosen) = &schema.message {
        if !messages.iter().any(|m| m == chosen) {
            schema.message = None;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn directory_name_is_stable_and_separates_clusters() {
        assert_eq!(new_dir_name("c1", "orders"), new_dir_name("c1", "orders"));
        assert_ne!(new_dir_name("c1", "orders"), new_dir_name("c2", "orders"));
        // Разделитель не даёт склейке «c1» + «x.orders» совпасть с «c1x» + «orders».
        assert_ne!(new_dir_name("c1", "x.orders"), new_dir_name("c1x", "orders"));
        assert_eq!(new_dir_name("c1", "orders").len(), 16);
    }

    fn schema_with(message: Option<&str>) -> TopicSchema {
        TopicSchema {
            cluster: "c".into(),
            topic: "t".into(),
            format: BodyFormat::Proto,
            files: Vec::new(),
            message: message.map(str::to_string),
            dir: "d".into(),
        }
    }

    #[test]
    fn a_single_message_is_selected_without_asking() {
        let mut schema = schema_with(None);
        settle_message(&mut schema, &["demo.Only".to_string()]);
        assert_eq!(schema.message.as_deref(), Some("demo.Only"));
    }

    #[test]
    fn a_single_message_overrides_a_stale_choice() {
        let mut schema = schema_with(Some("demo.Gone"));
        settle_message(&mut schema, &["demo.Only".to_string()]);
        assert_eq!(schema.message.as_deref(), Some("demo.Only"));
    }

    #[test]
    fn several_messages_leave_the_valid_choice_alone() {
        let mut schema = schema_with(Some("demo.B"));
        settle_message(&mut schema, &["demo.A".to_string(), "demo.B".to_string()]);
        assert_eq!(schema.message.as_deref(), Some("demo.B"));
    }

    /// Выбор, которого в схеме больше нет, обязан исчезнуть и из формы: иначе
    /// селектор показывал бы тип, которым уже ничего не декодируется.
    #[test]
    fn a_choice_that_vanished_from_the_schema_is_dropped() {
        let mut schema = schema_with(Some("demo.Gone"));
        settle_message(&mut schema, &["demo.A".to_string(), "demo.B".to_string()]);
        assert_eq!(schema.message, None);
    }

    #[test]
    fn a_schema_without_files_or_choices_is_not_worth_storing() {
        let mut schema = schema_with(None);
        schema.format = BodyFormat::default();
        assert!(is_trivial(&schema));
        schema.format = BodyFormat::Text;
        assert!(!is_trivial(&schema), "выбор Text — тоже настройка");
    }

    // --- Сквозные проверки поверх настоящей файловой системы -----------------
    //
    // Всё, что ниже, прогоняет полный путь: выбрали файл — скопировали —
    // разобрали — записали. Именно здесь и живут ошибки, которых не видно на
    // отдельных функциях: раскладка импортов, откат неудачной загрузки,
    // выживание схемы между «перезапусками».

    const CLUSTER: &str = "prod";
    const TOPIC: &str = "orders.stream";

    /// Каталог настроек и каталог «пользовательских» .proto рядом.
    struct Sandbox {
        root: PathBuf,
        outside: PathBuf,
    }

    impl Sandbox {
        fn new(name: &str) -> Self {
            let base = std::env::temp_dir().join(format!("mikui-store-test-{name}"));
            let _ = std::fs::remove_dir_all(&base);
            let sandbox = Self {
                root: base.join("config"),
                outside: base.join("user"),
            };
            std::fs::create_dir_all(&sandbox.root).unwrap();
            std::fs::create_dir_all(&sandbox.outside).unwrap();
            sandbox
        }

        /// Кладёт .proto туда, где он лежал бы у пользователя, и отдаёт путь.
        fn author(&self, relative: &str, text: &str) -> String {
            let path = self.outside.join(relative);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(&path, text).unwrap();
            path.to_string_lossy().into_owned()
        }

        fn add(&self, sources: &[String]) -> Result<TopicSchemaView, String> {
            add_files(&self.root, CLUSTER, TOPIC, sources)
        }

        fn stored(&self) -> Vec<TopicSchema> {
            list(&self.root).unwrap()
        }

        /// Каталог схемы единственного сохранённого топика.
        fn schema_dir(&self) -> PathBuf {
            dir_of(&self.root, &self.stored()[0])
        }
    }

    impl Drop for Sandbox {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(self.root.parent().unwrap());
        }
    }

    const ORDERS: &str = r#"
        syntax = "proto3";
        package orders;
        import "common/types.proto";
        message Order { string id = 1; common.Money total = 2; }
    "#;

    const MONEY: &str = r#"
        syntax = "proto3";
        package common;
        message Money { int64 amount = 1; }
    "#;

    #[test]
    fn a_single_file_becomes_a_working_proto_topic() {
        let sandbox = Sandbox::new("single");
        let path = sandbox.author(
            "events.proto",
            "syntax = \"proto3\"; package demo; message Event { string id = 1; }",
        );

        let view = sandbox.add(&[path.clone()]).unwrap();
        // Загрузили схему — значит топик показываем протобафом, и единственный
        // message подставился сам.
        assert_eq!(view.format, BodyFormat::Proto);
        assert_eq!(view.message.as_deref(), Some("demo.Event"));
        assert_eq!(view.messages, ["demo.Event"]);
        assert_eq!(view.files.len(), 1);
        assert_eq!(view.files[0].source, path);
        // И уже есть чем декодировать.
        assert!(decoder(&sandbox.root, CLUSTER, TOPIC).unwrap().is_some());
    }

    /// Схема обязана пережить перезапуск приложения и переезд исходников.
    #[test]
    fn the_schema_survives_the_original_files_disappearing() {
        let sandbox = Sandbox::new("survives");
        let path = sandbox.author(
            "events.proto",
            "syntax = \"proto3\"; package demo; message Event { string id = 1; }",
        );
        sandbox.add(&[path.clone()]).unwrap();

        // Пользователь снёс каталог, из которого брал контракт.
        std::fs::remove_file(&path).unwrap();
        schema::invalidate(&sandbox.schema_dir());

        let view = view(&sandbox.root, CLUSTER, TOPIC).unwrap().unwrap();
        assert_eq!(view.error, None, "своя копия должна разбираться и без оригинала");
        assert!(decoder(&sandbox.root, CLUSTER, TOPIC).unwrap().is_some());
    }

    /// Ради этого случая и заведена вся раскладка: два файла, между ними
    /// импорт по пути, а лежат они в произвольном каталоге пользователя.
    #[test]
    fn two_files_with_an_import_resolve_against_each_other() {
        let sandbox = Sandbox::new("imports");
        let orders = sandbox.author("orders.proto", ORDERS);
        let money = sandbox.author("common/types.proto", MONEY);

        let view = sandbox.add(&[orders, money]).unwrap();
        assert_eq!(view.messages, ["common.Money", "orders.Order"]);
        // Импортируемый файл лёг под тем именем, под которым его импортируют.
        let names: Vec<&str> = view.files.iter().map(|f| f.name.as_str()).collect();
        assert!(names.contains(&"common/types.proto"), "{names:?}");
        assert!(sandbox.schema_dir().join("common/types.proto").is_file());
    }

    /// Порядок выбора в диалоге пользователь не контролирует, и импорт должен
    /// разрешиться, даже когда зависимость названа раньше корневого файла.
    #[test]
    fn import_resolution_does_not_depend_on_the_order_files_were_picked() {
        let sandbox = Sandbox::new("order");
        let money = sandbox.author("common/types.proto", MONEY);
        let orders = sandbox.author("orders.proto", ORDERS);

        let view = sandbox.add(&[money, orders]).unwrap();
        assert_eq!(view.messages, ["common.Money", "orders.Order"]);
    }

    /// Требование в чистом виде: невалидный файл не сохраняется, а то, что
    /// работало, продолжает работать.
    #[test]
    fn a_broken_file_is_rejected_and_changes_nothing() {
        let sandbox = Sandbox::new("broken");
        let good = sandbox.author(
            "events.proto",
            "syntax = \"proto3\"; package demo; message Event { string id = 1; }",
        );
        sandbox.add(&[good]).unwrap();

        let bad = sandbox.author("bad.proto", "syntax = \"proto3\"; message { oops");
        assert!(sandbox.add(&[bad]).is_err());

        let view = view(&sandbox.root, CLUSTER, TOPIC).unwrap().unwrap();
        assert_eq!(view.files.len(), 1, "битый файл не должен был попасть в схему");
        assert_eq!(view.message.as_deref(), Some("demo.Event"));
        assert_eq!(view.error, None, "прежняя схема обязана остаться рабочей");
        assert!(!sandbox.schema_dir().join("bad.proto").exists());
        // И временный каталог за собой прибран.
        let staging = sandbox.schema_dir().with_extension("staging");
        assert!(!staging.exists(), "черновик не убран: {}", staging.display());
    }

    /// Файл, который импортируют, сам по себе валиден — но без корневого он
    /// схему не образует. Принимать такое нельзя.
    #[test]
    fn a_file_whose_import_is_missing_is_rejected() {
        let sandbox = Sandbox::new("dangling");
        let orders = sandbox.author("orders.proto", ORDERS);
        let error = sandbox.add(&[orders]).unwrap_err();
        assert!(error.contains("common/types.proto"), "{error}");
        assert!(sandbox.stored().is_empty(), "запись не должна была появиться");
    }

    #[test]
    fn refresh_picks_up_an_edited_file() {
        let sandbox = Sandbox::new("refresh");
        let path = sandbox.author(
            "events.proto",
            "syntax = \"proto3\"; package demo; message Before { string id = 1; }",
        );
        let view = sandbox.add(&[path.clone()]).unwrap();
        assert_eq!(view.messages, ["demo.Before"]);

        std::fs::write(
            &path,
            "syntax = \"proto3\"; package demo; message After { string id = 1; }",
        )
        .unwrap();

        let view = refresh(&sandbox.root, CLUSTER, TOPIC, None).unwrap();
        assert_eq!(view.messages, ["demo.After"]);
        // Единственный message — снова подставился сам.
        assert_eq!(view.message.as_deref(), Some("demo.After"));
    }

    #[test]
    fn refresh_of_a_file_that_became_invalid_keeps_the_old_one() {
        let sandbox = Sandbox::new("refresh-broken");
        let path = sandbox.author(
            "events.proto",
            "syntax = \"proto3\"; package demo; message Event { string id = 1; }",
        );
        sandbox.add(&[path.clone()]).unwrap();

        std::fs::write(&path, "syntax = \"proto3\"; message { oops").unwrap();
        assert!(refresh(&sandbox.root, CLUSTER, TOPIC, None).is_err());

        let view = view(&sandbox.root, CLUSTER, TOPIC).unwrap().unwrap();
        assert_eq!(view.error, None, "на диске обязана была остаться рабочая копия");
        assert_eq!(view.messages, ["demo.Event"]);
    }

    #[test]
    fn removing_the_last_file_takes_the_proto_format_with_it() {
        let sandbox = Sandbox::new("remove-last");
        let path = sandbox.author(
            "events.proto",
            "syntax = \"proto3\"; package demo; message Event { string id = 1; }",
        );
        let dir = {
            sandbox.add(&[path]).unwrap();
            sandbox.schema_dir()
        };

        let view = remove_file(&sandbox.root, CLUSTER, TOPIC, "events.proto")
            .unwrap()
            .unwrap();
        assert!(view.files.is_empty());
        assert_eq!(view.format, BodyFormat::Json);
        assert_eq!(view.message, None);
        assert!(!dir.exists(), "копии .proto должны были уйти вместе со схемой");
        assert!(sandbox.stored().is_empty(), "пустая запись хранению не подлежит");
        assert!(decoder(&sandbox.root, CLUSTER, TOPIC).unwrap().is_none());
    }

    /// Удалить файл, на который ссылается импорт, — это сломать схему. Такое
    /// изменение не применяется, как и любое другое, ломающее разбор.
    #[test]
    fn removing_a_file_that_is_still_imported_is_refused() {
        let sandbox = Sandbox::new("remove-needed");
        let orders = sandbox.author("orders.proto", ORDERS);
        let money = sandbox.author("common/types.proto", MONEY);
        sandbox.add(&[orders, money]).unwrap();

        assert!(remove_file(&sandbox.root, CLUSTER, TOPIC, "common/types.proto").is_err());

        let view = view(&sandbox.root, CLUSTER, TOPIC).unwrap().unwrap();
        assert_eq!(view.files.len(), 2);
        assert_eq!(view.error, None);
    }

    #[test]
    fn one_topic_schema_does_not_leak_into_another_cluster() {
        let sandbox = Sandbox::new("scoped");
        let path = sandbox.author(
            "events.proto",
            "syntax = \"proto3\"; package demo; message Event { string id = 1; }",
        );
        sandbox.add(&[path]).unwrap();

        assert!(view(&sandbox.root, "dev", TOPIC).unwrap().is_none());
        assert!(decoder(&sandbox.root, "dev", TOPIC).unwrap().is_none());
    }

    #[test]
    fn saving_the_form_rejects_a_message_the_schema_does_not_have() {
        let sandbox = Sandbox::new("save");
        let path = sandbox.author(
            "events.proto",
            r#"
                syntax = "proto3";
                package demo;
                message Event { string id = 1; }
                message Other { string id = 1; }
            "#,
        );
        sandbox.add(&[path]).unwrap();

        assert!(set_options(
            &sandbox.root,
            CLUSTER,
            TOPIC,
            BodyFormat::Proto,
            Some("demo.Nope".into())
        )
        .is_err());

        let view = set_options(
            &sandbox.root,
            CLUSTER,
            TOPIC,
            BodyFormat::Proto,
            Some("demo.Other".into()),
        )
        .unwrap();
        assert_eq!(view.message.as_deref(), Some("demo.Other"));
    }

    /// Пока message не выбран, декодировать нечем — и подсовывать первый
    /// попавшийся нельзя: разобралось бы молча и не тем.
    #[test]
    fn no_decoder_until_a_message_is_chosen() {
        let sandbox = Sandbox::new("unchosen");
        let path = sandbox.author(
            "events.proto",
            r#"
                syntax = "proto3";
                package demo;
                message Event { string id = 1; }
                message Other { string id = 1; }
            "#,
        );
        let view = sandbox.add(&[path]).unwrap();
        assert_eq!(view.message, None, "из двух сам не выбирается");
        assert!(decoder(&sandbox.root, CLUSTER, TOPIC).unwrap().is_none());
    }

    /// Формат тела — тоже настройка топика, и она обязана пережить перезапуск
    /// даже там, где никакого protobuf нет.
    #[test]
    fn a_plain_format_choice_is_remembered_on_its_own() {
        let sandbox = Sandbox::new("format-only");
        set_options(&sandbox.root, CLUSTER, TOPIC, BodyFormat::Text, None).unwrap();

        let view = view(&sandbox.root, CLUSTER, TOPIC).unwrap().unwrap();
        assert_eq!(view.format, BodyFormat::Text);
        assert!(view.files.is_empty());
    }

    #[test]
    fn deleting_a_cluster_takes_its_schemas_and_leaves_the_others() {
        let sandbox = Sandbox::new("forget");
        let path = sandbox.author(
            "events.proto",
            "syntax = \"proto3\"; package demo; message Event { string id = 1; }",
        );
        sandbox.add(&[path.clone()]).unwrap();
        add_files(&sandbox.root, "dev", TOPIC, &[path]).unwrap();
        let doomed_dir = dir_of(&sandbox.root, &sandbox.stored()[0]);

        forget_cluster(&sandbox.root, CLUSTER).unwrap();

        assert!(view(&sandbox.root, CLUSTER, TOPIC).unwrap().is_none());
        assert!(!doomed_dir.exists(), "копии .proto удалённого кластера остались");
        // Соседний кластер не тронут.
        assert!(view(&sandbox.root, "dev", TOPIC).unwrap().is_some());
        assert!(decoder(&sandbox.root, "dev", TOPIC).unwrap().is_some());
    }
}
