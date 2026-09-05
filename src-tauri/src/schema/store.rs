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

use super::avro::{self, AvroDecoder, Linked};
use super::decoder::Decoder;
use super::files::{Pending, Staged};
use super::json::{self, Compiled, JsonDecoder};
use super::proto::{imports, linked, ProtoDecoder};
use super::registry::{Registry, SchemaKind, SubjectAnswer};
use super::types::{
    AvroBinding, AvroView, BodyFormat, JsonBinding, JsonView, SchemaFile, TopicSchema,
    TopicSchemaView,
};
use crate::config;
use crate::config::SchemaRegistry;

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
    schema.files.is_empty()
        && schema.message.is_none()
        && schema.format.is_none()
        && schema.avro.as_ref().is_none_or(AvroBinding::is_empty)
        && schema.json.as_ref().is_none_or(JsonBinding::is_empty)
}

/// Запись «ничего не назначено».
///
/// Нужна затем, чтобы остальной код не различал топик без настроек и топик с
/// пустыми настройками: у первого тоже есть что показать, если реестр кластера
/// знает про него схему. На диск такая запись не попадает — её отсеет
/// `is_trivial`.
fn blank(cluster: &str, topic: &str) -> TopicSchema {
    TopicSchema {
        cluster: cluster.to_string(),
        topic: topic.to_string(),
        format: None,
        files: Vec::new(),
        message: None,
        dir: new_dir_name(cluster, topic),
        avro: None,
        json: None,
    }
}

/// Что реестр знает про топик сам, без единого клика пользователя.
///
/// Не просто subject: формат схемы виден только в ней самой, а реестр держит
/// все три. Пока сюда ехало одно лишь имя, JSON-Schema-топик автоопределялся
/// как avro и показывался мусором — тела к аврошному декодеру отношения не
/// имеют.
#[derive(Debug, Clone)]
struct Detected {
    subject: String,
    kind: SchemaKind,
}

/// Формат, которым топик показывается на самом деле.
///
/// Выбор пользователя сильнее автоопределения всегда, включая выбор json:
/// «показывай как есть» — это тоже ответ, и переспрашивать реестр вопреки ему
/// значило бы не слушать.
fn effective_format(schema: &TopicSchema, detected: Option<SchemaKind>) -> BodyFormat {
    match (schema.format, detected) {
        (Some(chosen), _) => chosen,
        (None, Some(SchemaKind::Avro)) => BodyFormat::Avro,
        (None, Some(SchemaKind::Json)) => BodyFormat::JsonSchema,
        // PROTOBUF формат НЕ переключает. Схему protobuf из реестра мы пока не
        // тянем — только из локальных .proto, — и топик оказался бы переведён в
        // формат, для которого декодера не собрать: `TopicSchema::decodes`
        // ответит `false`, тела поедут текстом, а пользователь останется гадать,
        // почему выбранный за него формат ничего не показывает. Честнее оставить
        // как есть и сказать в форме настроек, что реестр держит protobuf.
        (None, Some(SchemaKind::Protobuf)) | (None, None) => BodyFormat::default(),
    }
}

// --- Чтение -----------------------------------------------------------------

/// Схема топика вместе со списком message. Разбор ленивый и кэшированный;
/// не разобравшаяся схема возвращается с `error`, а не проглатывается: файлы
/// надо показать, иначе пользователю нечего чинить.
///
/// `None` — про топик сказать нечего вовсе: ни своих настроек, ни реестра у
/// кластера. Один лишь настроенный реестр уже повод вернуть вид: топику может
/// быть чем декодировать и чем кодировать отправляемое, даже если руками ему
/// не назначали ничего.
pub fn view(
    root: &Path,
    cluster: &str,
    topic: &str,
) -> Result<Option<TopicSchemaView>, String> {
    let schemas = list(root)?;
    if let Some(index) = position(&schemas, cluster, topic) {
        return Ok(Some(describe(root, &schemas[index])?));
    }

    let described = describe(root, &blank(cluster, topic))?;
    // Avro-половина есть ровно тогда, когда у кластера есть реестр, — а без
    // него у топика без записи и правда нет ничего.
    Ok(described.avro.is_some().then_some(described))
}

fn describe(root: &Path, schema: &TopicSchema) -> Result<TopicSchemaView, String> {
    let detected = detected_subject(root, schema);
    let format = effective_format(schema, detected.as_ref().map(|d| d.kind));
    let avro = avro_view(root, schema, detected.as_ref());
    let json = json_view(root, schema, detected.as_ref());
    // Формат схемы, которую реестр держит для топика САМ, — какой бы он ни был.
    // Половинам он приезжает уже отфильтрованным по своему типу, а форме нужен
    // сырой: только по нему она может сказать «реестр держит protobuf, загрузите
    // .proto» — единственный случай, когда автоопределение ничего не переключает
    // и объяснить это больше нечем.
    let detected_kind = detected.as_ref().map(|d| d.kind.as_str().to_string());

    if schema.files.is_empty() {
        return Ok(TopicSchemaView::new(schema, format, Vec::new(), None)
            .with_avro(avro)
            .with_json(json)
            .with_detected_kind(detected_kind));
    }
    let dir = dir_of(root, schema);
    Ok(match linked::linked(&dir, &schema.files) {
        Ok(linked) => TopicSchemaView::new(schema, format, linked.messages.clone(), None),
        Err(e) => TopicSchemaView::new(schema, format, Vec::new(), Some(e)),
    }
    .with_avro(avro)
    .with_json(json)
    .with_detected_kind(detected_kind))
}

/// Декодер для топика, если он настроен и схема разбирается.
pub fn decoder(
    root: &Path,
    cluster: &str,
    topic: &str,
) -> Result<Option<Arc<Decoder>>, String> {
    let schemas = list(root)?;
    let stored = position(&schemas, cluster, topic).map(|i| schemas[i].clone());
    // Записи может не быть вовсе, а декодер всё равно найтись: реестр кластера
    // знает про топик схему, и спрашивать об этом пользователя незачем.
    let schema = stored.unwrap_or_else(|| blank(cluster, topic));

    let detected = detected_subject(root, &schema);
    let format = effective_format(&schema, detected.as_ref().map(|d| d.kind));
    if !schema.decodes(format) {
        return Ok(None);
    }

    match format {
        BodyFormat::Proto => {
            let dir = dir_of(root, &schema);
            let linked = linked::linked(&dir, &schema.files)?;
            let name = schema.message.as_deref().unwrap_or_default();
            let message = linked
                .message(name)
                .ok_or_else(|| format!("message {name} is not in the loaded .proto files"))?;
            Ok(Some(Arc::new(Decoder::Proto(ProtoDecoder::new(message)))))
        }
        BodyFormat::Avro => {
            let registry = registry_of(root, cluster);
            let detected = detected_of_kind(&detected, SchemaKind::Avro);
            let pinned = avro_linked(root, &schema, registry.as_ref(), detected)?;
            // Ни своей схемы, ни реестра — декодировать нечем, и это не ошибка:
            // формат выбрали, а схему ещё не назначили. Тела поедут текстом.
            if pinned.is_none() && registry.is_none() {
                return Ok(None);
            }
            Ok(Some(Arc::new(Decoder::Avro(AvroDecoder::new(
                Some(root),
                pinned,
                registry,
            )))))
        }
        // Ни схемы, ни реестра этому декодеру не нужно вовсе: за
        // confluent-заголовком лежит обычный JSON, и вся его работа — снять
        // пять байт. Ровно из-за них такой топик и выглядел мусором. Схема у
        // формата стоит только на пути отправки, см. `json_for_produce`.
        BodyFormat::JsonSchema => Ok(Some(Arc::new(Decoder::Json(JsonDecoder::new())))),
        _ => Ok(None),
    }
}

/// Найденный subject, если он того формата, о котором спрашивают.
///
/// Реестр общий на три формата, и подсовывать аврошному декодеру subject с JSON
/// Schema (или наоборот) значит показать правдоподобный мусор.
fn detected_of_kind(detected: &Option<Detected>, kind: SchemaKind) -> Option<&str> {
    detected
        .as_ref()
        .filter(|d| d.kind == kind)
        .map(|d| d.subject.as_str())
}

/// Декодер КЛЮЧА топика.
///
/// Отдельный от `decoder` затем, что ключ сериализуется отдельно от тела и
/// схема у него своя: в реестре она лежит под `<topic>-key`, а не под
/// `<topic>-value`. Разобрать ключ схемой значения — это показать
/// правдоподобный мусор, а именно мусором ключ и выглядел, пока его печатали
/// сырыми байтами: перед «TCBR» стояла длина строки и признак union'а.
///
/// Только для Avro и только через реестр. У protobuf ключу негде взять свой
/// message — в настройках топика выбирают один, и он про тело. Ключи же в
/// подавляющем большинстве топиков либо строка, либо число, и трогать их без
/// доказательства обратного нельзя.
///
/// `None` — разбирать нечем или незачем, и ключ поедет байтами, как раньше.
pub fn key_decoder(
    root: &Path,
    cluster: &str,
    topic: &str,
) -> Result<Option<Arc<Decoder>>, String> {
    let schemas = list(root)?;
    let stored = position(&schemas, cluster, topic).map(|i| schemas[i].clone());
    let schema = stored.unwrap_or_else(|| blank(cluster, topic));

    let detected = detected_subject(root, &schema);
    if effective_format(&schema, detected.as_ref().map(|d| d.kind)) != BodyFormat::Avro {
        return Ok(None);
    }
    let Some((url, client)) = registry_of(root, cluster) else {
        return Ok(None);
    };

    // Закрепляем `<topic>-key`, если реестр её держит. Схема ключа тем самым
    // ищется ровно так же, как схема тела, — и находится ровно на тех топиках,
    // где ключ и правда avro-шный. Ключ-строка от `StringSerializer` своего
    // subject'а не заводит, и декодер для него не соберётся: показывать его
    // надо как раньше, байтами.
    // Тип проверяется и здесь: у топика с аврошным телом ключ вполне может быть
    // зарегистрирован JSON-схемой, и разобрать его аврошным декодером значило бы
    // поставить перед ключом пару нечитаемых знаков — ровно то, ради чего эта
    // ветка и заводилась.
    let pinned = subject_in_registry(root, cluster, &subject_of_key(topic))
        .filter(|d| d.kind == SchemaKind::Avro)
        .and_then(|d| avro::cache::by_subject(Some(root), &client, &url, &d.subject, None).ok());
    // Ни своей схемы, ни confluent-заголовка ждать неоткуда — собирать декодер,
    // который на каждом ключе будет отвечать «нечем», незачем. Реестр при этом
    // оставляем: ключ вполне может приехать с id в заголовке даже там, где
    // subject назван не по умолчанию.
    Ok(Some(Arc::new(Decoder::Avro(AvroDecoder::new(
        Some(root),
        pinned,
        Some((url, client)),
    )))))
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
    let linked = linked::linked(&dir, &schema.files)?;
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

    let imports = imports::imports_in(
        pending
            .iter()
            .map(|p| p.bytes.as_slice())
            .chain(incoming.iter().map(|(_, b)| b.as_slice())),
    );

    for (source, bytes) in incoming {
        let name = imports::layout(Path::new(&source), &imports);
        // Тот же файл, добавленный повторно, — это «перечитать», а не дубликат.
        pending.retain(|p| p.name != name);
        pending.push(Pending {
            name,
            source,
            bytes,
        });
    }

    let messages = commit(&mut schemas[index], &dir, &pending)?;
    schemas[index].format = Some(BodyFormat::Proto);
    settle_message(&mut schemas[index], &messages);

    let view = describe(root, &schemas[index])?;
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

    let view = describe(root, &schemas[index])?;
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

    // Последний файл ушёл — вместе с ним уходят и каталог, и выбор message, и
    // выбор формата: декодировать этой схемой больше нечем. Формат именно
    // сбрасывается в «не выбран», а не выставляется в json — с этого момента
    // топик снова вправе определиться сам.
    if pending.is_empty() {
        linked::invalidate(&dir);
        let _ = std::fs::remove_dir_all(&dir);
        schemas[index].files.clear();
        schemas[index].message = None;
        schemas[index].format = None;

        let view = describe(root, &schemas[index])?;
        if is_trivial(&schemas[index]) {
            schemas.remove(index);
        }
        save(root, &schemas)?;
        return Ok(Some(view));
    }

    let messages = commit(&mut schemas[index], &dir, &pending)?;
    settle_message(&mut schemas[index], &messages);

    let view = describe(root, &schemas[index])?;
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
    // Именно `Some`: выбор json на топике, который автоопределение считает
    // avro-шным, — это выбор, и он обязан пережить перезапуск. Хранись он как
    // «не выбран», топик возвращался бы в avro сам собой.
    schemas[index].format = Some(format);
    schemas[index].message = message;

    let view = describe(root, &schemas[index])?;
    if is_trivial(&schemas[index]) {
        schemas.remove(index);
    }
    save(root, &schemas)?;
    Ok(view)
}

/// Забывает все схемы кластера вместе с копиями файлов и кэшем реестра.
///
/// Зовётся при удалении подключения: осиротевшие каталоги контрактов никому не
/// нужны ровно так же, как осиротевшие пароли в keychain.
pub fn forget_cluster(root: &Path, cluster: &str) -> Result<(), String> {
    // Кэш реестра адресуется его URL, а не кластером, поэтому снять его надо
    // раньше, чем запись о кластере исчезнет: потом узнать адрес будет негде.
    if let Some(url) = registry_url(root, cluster) {
        avro::cache::forget(root, &url);
    }
    // У JSON-схем кэш только в памяти и без разбивки по реестрам: он мал,
    // живёт до первой правки настроек и стоит копейки — заводить в нём ключ
    // ради точечной чистки было бы дороже, чем сбросить его целиком.
    json::cache::forget();

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
        linked::invalidate(&dir);
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(avro_dir_of(root, victim));
        let _ = std::fs::remove_dir_all(json_dir_of(root, victim));
    }
    schemas.retain(|s| s.cluster != cluster);
    save(root, &schemas)
}

// --- Avro --------------------------------------------------------------------
//
// Записью заведует тот же `proto.json` и тот же `TopicSchema`: формат тела —
// одна настройка топика, а не две, и разложить её по двум файлам значило бы
// завести способ их рассогласовать. Копии .avsc лежат в соседнем каталоге под
// тем же именем, что и копии .proto: `dir` у записи один.

const AVRO_SCHEMAS_DIR: &str = "avro";

fn avro_dir_of(root: &Path, schema: &TopicSchema) -> PathBuf {
    root.join(AVRO_SCHEMAS_DIR).join(&schema.dir)
}

/// Адрес реестра кластера — без похода в keychain.
///
/// Отдельно от `registry_of` затем, что «настроен ли реестр» спрашивают на
/// каждое открытие формы настроек, а построение клиента читает пароль, и на
/// macOS это диалог доступа к хранилищу секретов.
fn registry_url(root: &Path, cluster: &str) -> Option<String> {
    config::clusters_at(root)
        .ok()?
        .into_iter()
        .find(|c| c.id == cluster)
        .and_then(|c| c.schema_registry)
        .map(|r| r.url)
}

/// Как реестр называет схему value этого топика по умолчанию.
///
/// TopicNameStrategy — стратегия имён, с которой работает любой штатный
/// сериализатор Kafka, пока его не переучили: схема значения регистрируется под
/// `<topic>-value`. Других стратегий здесь намеренно нет. RecordNameStrategy
/// даёт имя `<topic>-<полное имя записи>`, и угадывать его пришлось бы по
/// префиксу — а под префикс `orders-` попадает и `orders-extra-value`, схема
/// СОСЕДНЕГО топика. Ошибиться в эту сторону нельзя: чужая схема разберёт тело
/// в правдоподобный мусор. Впрочем, такому топику автоопределение и не нужно —
/// тела его сообщений называют свою схему id в заголовке, и формат ему
/// назначают руками один раз.
fn subject_of(topic: &str) -> String {
    format!("{topic}-value")
}

/// То же для ключа. Ключ сериализуется отдельно от тела, и subject у него свой.
fn subject_of_key(topic: &str) -> String {
    format!("{topic}-key")
}

/// Автоопределение: держит ли реестр кластера схему для этого топика.
///
/// Порядок проверок — от бесплатного к дорогому. Сначала «есть ли у кластера
/// реестр вообще» (файл настроек), потом уже полученный ответ (память) и лишь
/// потом живой клиент: его построение читает пароль из keychain, а macOS на
/// это может показать диалог. Топик, которому формат назначен руками, сюда не
/// доходит вовсе — за него отвечает `effective_format`.
fn subject_in_registry(root: &Path, cluster: &str, subject: &str) -> Option<Detected> {
    let url = registry_url(root, cluster)?;

    let answer = match avro::cache::subject_known(&url, subject) {
        Some(answer) => answer,
        None => {
            let (_, client) = registry_of(root, cluster)?;
            avro::cache::probe_subject(Some(root), &client, &url, subject)
        }
    };
    // Молчание реестра (`None`) и «нет такого subject» (`Absent`) ведут сюда
    // одинаково, но по разным причинам: первое — потому что недоступный на
    // минуту реестр не повод показать схемный топик текстом и тем более не
    // повод это запомнить; второе — потому что схемы и правда нет.
    match answer? {
        SubjectAnswer::Kind(kind) => Some(Detected {
            subject: subject.to_string(),
            kind,
        }),
        SubjectAnswer::Absent => None,
    }
}

/// То же самое, но только там, где ответ на что-то влияет.
///
/// А влияет он, пока формат не выбран (он-то и решается) и у двух форматов,
/// которые схему берут в реестре: найденным subject'ом разбирается голый datum
/// и строится заготовка для отправки. Топику, показанному json'ом, текстом или
/// протобафом, реестр не нужен — и ходить в него ради никому не нужного ответа
/// тоже. Отправка мимо этой развилки: там формат выбирают прямо в форме, и показ
/// топика ей не указ (см. `avro_for_produce`).
fn detected_subject(root: &Path, schema: &TopicSchema) -> Option<Detected> {
    let asks_registry = match schema.format {
        // Формат не выбран — он-то автоопределением и решается.
        None => true,
        Some(BodyFormat::Avro) | Some(BodyFormat::JsonSchema) => true,
        Some(_) => false,
    };
    if !asks_registry {
        return None;
    }
    subject_in_registry(root, &schema.cluster, &subject_of(&schema.topic))
}

/// Клиент реестра того кластера, к которому привязан топик.
///
/// `None` и когда реестр не настроен, и когда подключение вообще не сохранено:
/// у подключения из формы ключ кластера — его отображаемое имя, записи с таким
/// `id` в `clusters.json` нет, и хранить настройки реестра негде. Тогда avro
/// работает только по локальным .avsc, и это честный режим, а не поломка.
pub fn registry_of(root: &Path, cluster: &str) -> Option<(String, Arc<Registry>)> {
    let found = config::clusters_at(root)
        .ok()?
        .into_iter()
        .find(|c| c.id == cluster)?;
    let registry = found.schema_registry.clone()?;

    let password = match registry.has_password {
        true => config::secrets::read_password(&SchemaRegistry::secret_key(&found.id))
            .ok()
            .flatten(),
        false => None,
    };
    let client = Registry::new(
        &registry.url,
        registry.username.as_deref(),
        password.as_deref(),
        registry.ssl_ca_bundle_path.as_deref(),
    )
    .ok()?;
    Some((registry.url, Arc::new(client)))
}

/// Схема, закреплённая за топиком: из локальных .avsc или из subject реестра.
///
/// `Ok(None)` — своей схемы нет, и это рабочее состояние: в confluent-формате
/// она приедет с сообщением. `Err` — схема назначена, но её не собрать, и об
/// этом надо сказать.
fn avro_linked(
    root: &Path,
    schema: &TopicSchema,
    registry: Option<&(String, Arc<Registry>)>,
    detected: Option<&str>,
) -> Result<Option<Arc<Linked>>, String> {
    let Some(binding) = schema.avro() else {
        return Ok(detected_linked(root, registry, detected));
    };

    if !binding.files.is_empty() {
        let dir = avro_dir_of(root, schema);
        let texts = avro_texts(&dir, &binding.files)?;
        let names = names_of(&texts)?;

        let chosen = match binding.record.as_deref() {
            Some(name) => name,
            // Единственная запись — она и корень: выбирать не из чего.
            None if names.len() == 1 => &names[0],
            // Записей несколько, а выбор не сделан. Это НЕ ошибка, а «пока
            // нечем»: подставлять первую попавшуюся нельзя — разобралось бы
            // молча и не тем. Ровно то же правило, что у proto без message.
            None => return Ok(None),
        };
        return Ok(Some(Arc::new(Linked::parse_files(&texts, Some(chosen))?)));
    }

    let Some(subject) = binding.subject.as_deref() else {
        return Ok(detected_linked(root, registry, detected));
    };
    let Some((url, client)) = registry else {
        return Err(format!(
            "topic is bound to subject {subject}, but this cluster has no schema registry \
             configured"
        ));
    };
    avro::cache::by_subject(Some(root), client, url, subject, binding.version).map(Some)
}

/// Схема автоопределённого subject — та, которой разбирается ГОЛЫЙ datum.
///
/// Отдельно от `avro_linked` ради одного: неудача здесь не ошибка. Схему
/// назначил не пользователь, а догадка, и завалить из-за неё показ топика
/// нельзя — тела в confluent-формате называют свою схему сами, и без
/// закреплённой они прекрасно читаются.
fn detected_linked(
    root: &Path,
    registry: Option<&(String, Arc<Registry>)>,
    detected: Option<&str>,
) -> Option<Arc<Linked>> {
    let (url, client) = registry?;
    avro::cache::by_subject(Some(root), client, url, detected?, None).ok()
}

/// Схема для ОТПРАВКИ и id, который надо поставить в заголовок.
///
/// `Some(id)` — схему дал реестр, и тело поедет в confluent-формате: маркер
/// плюс id. `None` — схема своя, локальная, и заголовку взяться неоткуда:
/// уедет голый datum. Разница видна потребителю топика, поэтому форма о ней и
/// пишет.
///
/// `subject` приходит отдельным аргументом ради того же, ради чего у protobuf
/// отдельным аргументом идёт имя message: отправляют и не тем, чем читают.
pub struct ProduceSchema {
    pub linked: Arc<Linked>,
    /// id для confluent-заголовка. `None` — схема локальная, и тело поедет
    /// голым datum'ом.
    pub id: Option<u32>,
    /// Subject, которым в итоге кодируем. Форме он нужен, чтобы показать, что
    /// именно она выбрала за пользователя, когда тот не выбирал ничего.
    pub subject: Option<String>,
}

pub fn avro_for_produce(
    root: &Path,
    cluster: &str,
    topic: &str,
    subject: Option<&str>,
) -> Result<ProduceSchema, String> {
    let schemas = list(root)?;
    let stored = position(&schemas, cluster, topic).map(|i| schemas[i].clone());
    let schema = stored.unwrap_or_else(|| blank(cluster, topic));
    let binding = schema.avro();

    // Порядок старшинства: выбранное в форме сильнее настроек топика, настройки
    // сильнее автоопределения. Загруженные .avsc отменяют догадку целиком —
    // ими и кодируем, за тем их и грузили.
    let chosen = subject
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .or_else(|| binding.and_then(|a| a.subject.clone()));
    let has_files = binding.is_some_and(|a| !a.files.is_empty());
    let subject = match (chosen, has_files) {
        (Some(chosen), _) => Some(chosen),
        (None, true) => None,
        // `subject_in_registry`, а не `detected_subject`: формат отправки выбран
        // прямо в форме, и то, чем топик ПОКАЗЫВАЮТ, к нему отношения не имеет
        // — положить avro в топик, который смотрят текстом, никто не запрещал.
        // Тип при этом обязан совпасть: догадка «в реестре есть схема» без
        // оговорки про формат закодировала бы тело avro по JSON-схеме.
        (None, false) => subject_in_registry(root, cluster, &subject_of(topic))
            .filter(|d| d.kind == SchemaKind::Avro)
            .map(|d| d.subject),
    };

    if let Some(subject) = subject {
        let (url, client) = registry_of(root, cluster)
            .ok_or("no schema registry is configured for this cluster")?;
        let version = binding
            .and_then(|a| a.version.filter(|_| a.subject.as_deref() == Some(&subject)));

        // Выбранный руками subject тоже проверяется: пользователь мог назвать
        // тот, что зарегистрирован protobuf'ом, и молча закодировать по нему
        // значило бы положить в топик заведомый мусор.
        let fetched = client.by_subject(&subject, version)?.expect(SchemaKind::Avro)?;
        let linked = Arc::new(Linked::parse_with_refs(
            &fetched.schema,
            &fetched.references,
        )?);
        avro::cache::remember_fetched(Some(root), &url, &fetched, Arc::clone(&linked));
        return Ok(ProduceSchema {
            linked,
            id: Some(fetched.id),
            subject: Some(subject),
        });
    }

    let linked = avro_linked(root, &schema, None, None)?
        .ok_or("pick a subject or load an .avsc file for this topic first")?;
    Ok(ProduceSchema {
        linked,
        id: None,
        subject: None,
    })
}

/// Тексты .avsc из копий в каталоге схемы.
fn avro_texts(dir: &Path, files: &[SchemaFile]) -> Result<Vec<String>, String> {
    files
        .iter()
        .map(|file| {
            let bytes = read_copy(dir, file)?;
            String::from_utf8(bytes)
                .map_err(|e| format!("{} is not valid UTF-8: {e}", file.name))
        })
        .collect()
}

/// Avro-половина того, что видит форма настроек топика.
///
/// Появляется и у топика без всякой привязки, если у кластера настроен реестр:
/// такому топику есть чем декодировать (схема приедет в заголовке сообщения) и
/// есть чем кодировать отправляемое. `None` остаётся ровно за случаем «avro тут
/// вообще не при чём» — ни привязки, ни реестра.
fn avro_view(root: &Path, schema: &TopicSchema, detected: Option<&Detected>) -> Option<AvroView> {
    // Найденный subject показываем, только если он и правда аврошный. Реестр
    // общий на три формата, и «нашли схему» без оговорки про её тип означало бы
    // в аврошной форме предложить декодировать avro то, что записано JSON'ом.
    let detected = detected
        .filter(|d| d.kind == SchemaKind::Avro)
        .map(|d| d.subject.clone());
    let registry = registry_url(root, &schema.cluster).is_some();
    let binding = schema.avro();
    if binding.is_none() && !registry {
        return None;
    }
    let files = binding.map(|b| b.files.as_slice()).unwrap_or_default();

    // Список записей нужен только у файлов: у subject корень задан самим
    // subject'ом, выбирать не из чего. Разбираем без реестра — в сеть за
    // содержимым выпадающего списка не ходят.
    //
    // Через `names_of`, а не `parse_files(.., record)`: с невыбранным корнем
    // второй отказывается разбирать набор вовсе, и список, ИЗ КОТОРОГО корень
    // и выбирают, оказался бы пустым ровно тогда, когда он нужнее всего.
    let (records, error) = if files.is_empty() {
        (Vec::new(), None)
    } else {
        match avro_texts(&avro_dir_of(root, schema), files).and_then(|texts| names_of(&texts)) {
            Ok(names) => (names, None),
            Err(e) => (Vec::new(), Some(e)),
        }
    };

    Some(AvroView {
        files: files.to_vec(),
        record: binding.and_then(|b| b.record.clone()),
        subject: binding.and_then(|b| b.subject.clone()),
        version: binding.and_then(|b| b.version),
        records,
        registry,
        detected,
        error,
    })
}

/// Добавляет .avsc к топику.
///
/// Как и у .proto: разбирается весь набор целиком, и не разобравшийся не
/// сохраняется вовсе. Имена внутри каталога — просто basename: импортов у Avro
/// нет, ссылки идут по именам типов, а не по путям файлов.
pub fn add_avro_files(
    root: &Path,
    cluster: &str,
    topic: &str,
    sources: &[String],
) -> Result<TopicSchemaView, String> {
    if sources.is_empty() {
        return Err("no .avsc files selected".to_string());
    }

    let mut schemas = list(root)?;
    let index = ensure_record(&mut schemas, cluster, topic);
    let dir = avro_dir_of(root, &schemas[index]);
    let binding = schemas[index].avro.clone().unwrap_or_default();

    let mut pending = Vec::new();
    for file in &binding.files {
        pending.push(Pending {
            name: file.name.clone(),
            source: file.source.clone(),
            bytes: read_copy(&dir, file)?,
        });
    }
    for source in sources {
        let bytes = std::fs::read(source).map_err(|e| format!("can't read {source}: {e}"))?;
        let name = base_name(source);
        // Тот же файл, добавленный повторно, — это «перечитать», а не дубликат.
        pending.retain(|p| p.name != name);
        pending.push(Pending {
            name,
            source: source.clone(),
            bytes,
        });
    }

    let listing = commit_avro(&dir, &pending)?;
    let records = listing.1;
    let mut updated = binding;
    updated.files = listing.0;
    // Файлы и subject — два ответа на один вопрос. Загрузили файлы — значит
    // ими и декодируем; сказать об этом честнее, чем молча предпочесть один
    // источник другому в неочевидном порядке.
    updated.subject = None;
    updated.version = None;
    settle_record(&mut updated, &records);

    schemas[index].avro = Some(updated);
    // Успех переводит топик в AVRO-формат: ради этого файл и грузили.
    schemas[index].format = Some(BodyFormat::Avro);

    avro::cache::forget_subjects();
    let view = describe(root, &schemas[index])?;
    save(root, &schemas)?;
    Ok(view)
}

/// Перечитывает .avsc с диска — по одному имени или все сразу.
pub fn refresh_avro_files(
    root: &Path,
    cluster: &str,
    topic: &str,
    name: Option<&str>,
) -> Result<TopicSchemaView, String> {
    let mut schemas = list(root)?;
    let index = position(&schemas, cluster, topic)
        .ok_or_else(|| format!("no schema for topic {topic}"))?;
    let dir = avro_dir_of(root, &schemas[index]);
    let mut binding = schemas[index]
        .avro
        .clone()
        .ok_or_else(|| format!("no .avsc files loaded for topic {topic}"))?;

    if let Some(name) = name {
        if !binding.files.iter().any(|f| f.name == name) {
            return Err(format!("{name} is not part of this schema"));
        }
    }

    let mut pending = Vec::with_capacity(binding.files.len());
    for file in &binding.files {
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

    let (files, records) = commit_avro(&dir, &pending)?;
    binding.files = files;
    settle_record(&mut binding, &records);
    schemas[index].avro = Some(binding);

    avro::cache::forget_subjects();
    let view = describe(root, &schemas[index])?;
    save(root, &schemas)?;
    Ok(view)
}

/// Убирает один .avsc. Оставшееся обязано по-прежнему разбираться.
pub fn remove_avro_file(
    root: &Path,
    cluster: &str,
    topic: &str,
    name: &str,
) -> Result<Option<TopicSchemaView>, String> {
    let mut schemas = list(root)?;
    let Some(index) = position(&schemas, cluster, topic) else {
        return Ok(None);
    };
    let dir = avro_dir_of(root, &schemas[index]);
    let Some(mut binding) = schemas[index].avro.clone() else {
        return Ok(None);
    };

    let mut pending = Vec::new();
    for file in &binding.files {
        if file.name == name {
            continue;
        }
        pending.push(Pending {
            name: file.name.clone(),
            source: file.source.clone(),
            bytes: read_copy(&dir, file)?,
        });
    }
    if pending.len() == binding.files.len() {
        return Err(format!("{name} is not part of this schema"));
    }

    if pending.is_empty() {
        // Последний файл ушёл — вместе с ним уходят каталог и выбор записи.
        // Формат при этом остаётся: реестр вполне мог быть настроен, и топик
        // продолжит читаться по id из сообщений.
        let _ = std::fs::remove_dir_all(&dir);
        binding.files.clear();
        binding.record = None;
    } else {
        let (files, records) = commit_avro(&dir, &pending)?;
        binding.files = files;
        settle_record(&mut binding, &records);
    }

    schemas[index].avro = Some(binding).filter(|b| !b.is_empty());
    if schemas[index].avro.is_none() && schemas[index].format == Some(BodyFormat::Avro) {
        // Своей схемы больше нет — и выбор формата вместе с ней: то же правило,
        // что у последнего удалённого .proto. Дальше решает автоопределение,
        // и если реестр держит схему топика, avro так и останется.
        schemas[index].format = None;
    }

    avro::cache::forget_subjects();
    let view = describe(root, &schemas[index])?;
    if is_trivial(&schemas[index]) {
        schemas.remove(index);
    }
    save(root, &schemas)?;
    Ok(Some(view))
}

/// Привязывает топик к subject реестра.
///
/// `subject: None` — отвязать. Проверять существование subject здесь не надо:
/// список для селектора приезжает из того же реестра, а промах всё равно
/// вскроется при сборке схемы — с текстом от самого реестра, который понятнее
/// нашего пересказа.
pub fn set_avro_subject(
    root: &Path,
    cluster: &str,
    topic: &str,
    subject: Option<String>,
    version: Option<i32>,
) -> Result<TopicSchemaView, String> {
    let mut schemas = list(root)?;
    let index = ensure_record(&mut schemas, cluster, topic);
    let mut binding = schemas[index].avro.clone().unwrap_or_default();

    if subject.is_some() && !binding.files.is_empty() {
        // Взаимоисключающи: выбрали реестр — копии .avsc больше не при чём.
        let _ = std::fs::remove_dir_all(avro_dir_of(root, &schemas[index]));
        binding.files.clear();
        binding.record = None;
    }
    binding.subject = subject;
    binding.version = version;

    schemas[index].avro = Some(binding).filter(|b| !b.is_empty());
    avro::cache::forget_subjects();

    let view = describe(root, &schemas[index])?;
    if is_trivial(&schemas[index]) {
        schemas.remove(index);
    }
    save(root, &schemas)?;
    Ok(view)
}

/// Выбирает запись, которой декодируется тело, когда схема взята из файлов.
pub fn set_avro_record(
    root: &Path,
    cluster: &str,
    topic: &str,
    record: Option<String>,
) -> Result<TopicSchemaView, String> {
    let mut schemas = list(root)?;
    let index = position(&schemas, cluster, topic)
        .ok_or_else(|| format!("no schema for topic {topic}"))?;
    let mut binding = schemas[index]
        .avro
        .clone()
        .ok_or_else(|| format!("no .avsc files loaded for topic {topic}"))?;

    if let Some(name) = record.as_deref() {
        let texts = avro_texts(&avro_dir_of(root, &schemas[index]), &binding.files)?;
        // `names_of`, а не разбор с этим корнем: список имён от выбора не
        // зависит, и спрашивать его надо тем же способом, каким его показали в
        // форме, — иначе форма предлагает то, что сама же и отвергает.
        if !names_of(&texts)?.iter().any(|n| n == name) {
            return Err(format!("unknown record {name}"));
        }
    }
    binding.record = record;
    schemas[index].avro = Some(binding);

    let view = describe(root, &schemas[index])?;
    save(root, &schemas)?;
    Ok(view)
}

/// Складывает набор .avsc, проверяет разбором и ставит каталог на место.
/// Возвращает список файлов и имена записей, которые в наборе объявлены.
fn commit_avro(
    dir: &Path,
    pending: &[Pending],
) -> Result<(Vec<SchemaFile>, Vec<String>), String> {
    let staged = Staged::write(dir.to_path_buf(), pending)?;

    let texts: Vec<String> = pending
        .iter()
        .map(|p| {
            String::from_utf8(p.bytes.clone())
                .map_err(|e| format!("{} is not valid UTF-8: {e}", p.name))
        })
        .collect::<Result<_, _>>()?;

    // Проверка набора и список имён — одним разбором, ещё во временном
    // каталоге: неудача не должна оставить следа.
    let names = names_of(&texts)?;

    staged.commit()?;
    let files = pending
        .iter()
        .map(|p| SchemaFile {
            name: p.name.clone(),
            source: p.source.clone(),
        })
        .collect();
    Ok((files, names))
}

/// Имена всех типов набора — они же содержимое селектора «чем декодировать».
///
/// Корень для разбора берётся произвольный, из первого же файла: `parse_files`
/// требует его только затем, чтобы знать, ЧЕМ декодировать, а связность набора
/// и список имён от этого выбора не зависят. Иначе набор из нескольких записей
/// нельзя было бы даже проверить, пока пользователь не выбрал одну из них, —
/// а выбирать ему как раз из этого списка.
fn names_of(texts: &[String]) -> Result<Vec<String>, String> {
    let probe = texts
        .iter()
        .find_map(|t| serde_json::from_str::<serde_json::Value>(t).ok())
        .and_then(|v| full_name_of(&v))
        .ok_or("none of the .avsc files declares a named type")?;
    Linked::parse_files(texts, Some(&probe)).map(|l| l.names())
}

/// Полное имя типа прямо из текста .avsc — до всякого разбора схемы.
fn full_name_of(value: &serde_json::Value) -> Option<String> {
    let name = value.get("name")?.as_str()?;
    match value.get("namespace").and_then(|n| n.as_str()) {
        Some(ns) if !name.contains('.') => Some(format!("{ns}.{name}")),
        _ => Some(name.to_string()),
    }
}

/// Приводит выбранную запись в соответствие с тем, что теперь есть в наборе.
/// То же правило, что у `settle_message` для protobuf.
fn settle_record(binding: &mut AvroBinding, records: &[String]) {
    if let [only] = records {
        binding.record = Some(only.clone());
        return;
    }
    if let Some(chosen) = &binding.record {
        if !records.iter().any(|r| r == chosen) {
            binding.record = None;
        }
    }
}

/// Имя файла внутри каталога схемы. У .avsc это просто basename: ссылки между
/// схемами идут по именам ТИПОВ, а не по путям файлов, — раскладка, которой
/// требует `import` в protobuf, здесь не нужна.
fn base_name(source: &str) -> String {
    Path::new(source)
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| "schema.avsc".to_string())
}

// --- JSON Schema --------------------------------------------------------------
//
// Той же записью и тем же `proto.json`, что и две другие половины. Копии файлов
// — в третьем соседнем каталоге под тем же именем `dir`.
//
// Половина заметно короче аврошной, и вот чего в ней нет. Выбора корневой записи
// — у JSON Schema корень один, сама схема им и является. Разбора при загрузке
// файла «чтобы узнать имена» — узнавать нечего. И главное: схема здесь вообще не
// нужна для ПОКАЗА, только для отправки, поэтому её не приходится собирать при
// каждом открытии топика.

const JSON_SCHEMAS_DIR: &str = "jsonschema";

fn json_dir_of(root: &Path, schema: &TopicSchema) -> PathBuf {
    root.join(JSON_SCHEMAS_DIR).join(&schema.dir)
}

/// Тексты локальных схем из копий в каталоге.
fn json_texts(dir: &Path, files: &[SchemaFile]) -> Result<Vec<String>, String> {
    files
        .iter()
        .map(|file| {
            let bytes = read_copy(dir, file)?;
            String::from_utf8(bytes).map_err(|e| format!("{} is not valid UTF-8: {e}", file.name))
        })
        .collect()
}

/// JSON-Schema-половина того, что видит форма настроек топика.
///
/// Появляется на тех же условиях, что и аврошная: либо у топика есть привязка,
/// либо у кластера настроен реестр — тогда топику есть чем проверять
/// отправляемое, даже если руками ему не назначали ничего.
fn json_view(root: &Path, schema: &TopicSchema, detected: Option<&Detected>) -> Option<JsonView> {
    let registry = registry_url(root, &schema.cluster).is_some();
    let binding = schema.json();
    if binding.is_none() && !registry {
        return None;
    }
    let files = binding.map(|b| b.files.as_slice()).unwrap_or_default();

    // Схема разбирается только чтобы сказать, разбирается ли она: показывать из
    // неё в форме нечего — ни имён записей, ни выбора корня у формата нет.
    let error = match files.is_empty() {
        true => None,
        false => json_texts(&json_dir_of(root, schema), files)
            .and_then(|texts| json_compile(&texts).map(|_| ()))
            .err(),
    };

    Some(JsonView {
        files: files.to_vec(),
        subject: binding.and_then(|b| b.subject.clone()),
        version: binding.and_then(|b| b.version),
        registry,
        detected: detected
            .filter(|d| d.kind == SchemaKind::Json)
            .map(|d| d.subject.clone()),
        error,
    })
}

/// Компилирует набор локальных файлов.
///
/// Первый файл — основная схема, остальные идут в ссылки. Порядок, а не выбор
/// корня: у JSON Schema корневой тип один, и «какой из файлов главный» — это
/// вопрос не к схеме, а к тому, в каком порядке их загрузили.
fn json_compile(texts: &[String]) -> Result<Arc<Compiled>, String> {
    let (first, rest) = texts
        .split_first()
        .ok_or("no JSON schema files loaded for this topic")?;
    Compiled::parse(first, rest).map(Arc::new)
}

/// Добавляет файлы JSON-схемы к топику.
pub fn add_json_files(
    root: &Path,
    cluster: &str,
    topic: &str,
    sources: &[String],
) -> Result<TopicSchemaView, String> {
    if sources.is_empty() {
        return Err("no JSON schema files selected".to_string());
    }

    let mut schemas = list(root)?;
    let index = ensure_record(&mut schemas, cluster, topic);
    let dir = json_dir_of(root, &schemas[index]);
    let binding = schemas[index].json.clone().unwrap_or_default();

    let mut pending = keep_existing(&dir, &binding.files)?;
    for source in sources {
        let bytes = std::fs::read(source).map_err(|e| format!("can't read {source}: {e}"))?;
        let name = base_name(source);
        // Тот же файл, добавленный повторно, — это «перечитать», а не дубликат.
        pending.retain(|p| p.name != name);
        pending.push(Pending {
            name,
            source: source.clone(),
            bytes,
        });
    }

    let files = commit_json(&dir, &pending)?;
    let mut updated = binding;
    updated.files = files;
    // Файлы и subject — два ответа на один вопрос, как и у Avro.
    updated.subject = None;
    updated.version = None;

    schemas[index].json = Some(updated);
    // Успех переводит топик в JSON-SCHEMA-формат: ради этого файл и грузили.
    schemas[index].format = Some(BodyFormat::JsonSchema);

    json::cache::forget();
    let view = describe(root, &schemas[index])?;
    save(root, &schemas)?;
    Ok(view)
}

/// Перечитывает файлы JSON-схемы с диска — по одному имени или все сразу.
pub fn refresh_json_files(
    root: &Path,
    cluster: &str,
    topic: &str,
    name: Option<&str>,
) -> Result<TopicSchemaView, String> {
    let mut schemas = list(root)?;
    let index =
        position(&schemas, cluster, topic).ok_or_else(|| format!("no schema for topic {topic}"))?;
    let dir = json_dir_of(root, &schemas[index]);
    let mut binding = schemas[index]
        .json
        .clone()
        .ok_or_else(|| format!("no JSON schema files loaded for topic {topic}"))?;

    if let Some(name) = name {
        if !binding.files.iter().any(|f| f.name == name) {
            return Err(format!("{name} is not part of this schema"));
        }
    }

    let mut pending = Vec::with_capacity(binding.files.len());
    for file in &binding.files {
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

    binding.files = commit_json(&dir, &pending)?;
    schemas[index].json = Some(binding);

    json::cache::forget();
    let view = describe(root, &schemas[index])?;
    save(root, &schemas)?;
    Ok(view)
}

/// Убирает один файл JSON-схемы. Оставшееся обязано по-прежнему компилироваться.
pub fn remove_json_file(
    root: &Path,
    cluster: &str,
    topic: &str,
    name: &str,
) -> Result<Option<TopicSchemaView>, String> {
    let mut schemas = list(root)?;
    let Some(index) = position(&schemas, cluster, topic) else {
        return Ok(None);
    };
    let dir = json_dir_of(root, &schemas[index]);
    let Some(mut binding) = schemas[index].json.clone() else {
        return Ok(None);
    };

    let kept: Vec<SchemaFile> = binding
        .files
        .iter()
        .filter(|f| f.name != name)
        .cloned()
        .collect();
    if kept.len() == binding.files.len() {
        return Err(format!("{name} is not part of this schema"));
    }

    if kept.is_empty() {
        // Последний файл ушёл — вместе с ним уходит каталог. Формат при этом
        // остаётся: реестр вполне мог быть настроен, и топик продолжит
        // читаться, а отправка — проверяться по subject.
        let _ = std::fs::remove_dir_all(&dir);
        binding.files.clear();
    } else {
        let pending = keep_existing(&dir, &kept)?;
        binding.files = commit_json(&dir, &pending)?;
    }

    schemas[index].json = Some(binding).filter(|b| !b.is_empty());
    if schemas[index].json.is_none() && schemas[index].format == Some(BodyFormat::JsonSchema) {
        // Своей схемы больше нет — и выбор формата вместе с ней: то же правило,
        // что у последнего удалённого .proto и .avsc.
        schemas[index].format = None;
    }

    json::cache::forget();
    let view = describe(root, &schemas[index])?;
    if is_trivial(&schemas[index]) {
        schemas.remove(index);
    }
    save(root, &schemas)?;
    Ok(Some(view))
}

/// Привязывает топик к subject реестра. `subject: None` — отвязать.
pub fn set_json_subject(
    root: &Path,
    cluster: &str,
    topic: &str,
    subject: Option<String>,
    version: Option<i32>,
) -> Result<TopicSchemaView, String> {
    let mut schemas = list(root)?;
    let index = ensure_record(&mut schemas, cluster, topic);
    let mut binding = schemas[index].json.clone().unwrap_or_default();

    if subject.is_some() && !binding.files.is_empty() {
        // Взаимоисключающи: выбрали реестр — локальные копии больше не при чём.
        let _ = std::fs::remove_dir_all(json_dir_of(root, &schemas[index]));
        binding.files.clear();
    }
    binding.subject = subject;
    binding.version = version;

    schemas[index].json = Some(binding).filter(|b| !b.is_empty());
    json::cache::forget();

    let view = describe(root, &schemas[index])?;
    if is_trivial(&schemas[index]) {
        schemas.remove(index);
    }
    save(root, &schemas)?;
    Ok(view)
}

/// Складывает набор файлов, проверяет компиляцией и ставит каталог на место.
fn commit_json(dir: &Path, pending: &[Pending]) -> Result<Vec<SchemaFile>, String> {
    let staged = Staged::write(dir.to_path_buf(), pending)?;

    let texts: Vec<String> = pending
        .iter()
        .map(|p| {
            String::from_utf8(p.bytes.clone())
                .map_err(|e| format!("{} is not valid UTF-8: {e}", p.name))
        })
        .collect::<Result<_, _>>()?;

    // Проверка ещё во временном каталоге: неудача не должна оставить следа.
    json_compile(&texts)?;

    staged.commit()?;
    Ok(pending
        .iter()
        .map(|p| SchemaFile {
            name: p.name.clone(),
            source: p.source.clone(),
        })
        .collect())
}

/// Схема для ОТПРАВКИ и id, который надо поставить в заголовок.
///
/// Всё то же самое, что у `avro_for_produce`, включая порядок старшинства
/// «выбранное в форме > настройки топика > автоопределение». Отличие одно:
/// схема нужна не чтобы закодировать тело — тело и так JSON, — а чтобы
/// проверить его и построить заготовку.
pub struct JsonProduceSchema {
    pub compiled: Arc<Compiled>,
    /// id для confluent-заголовка. `None` — схема локальная, и тело поедет без
    /// заголовка, как его пишет обычный `JsonSerializer`.
    pub id: Option<u32>,
    pub subject: Option<String>,
}

pub fn json_for_produce(
    root: &Path,
    cluster: &str,
    topic: &str,
    subject: Option<&str>,
) -> Result<JsonProduceSchema, String> {
    let schemas = list(root)?;
    let stored = position(&schemas, cluster, topic).map(|i| schemas[i].clone());
    let schema = stored.unwrap_or_else(|| blank(cluster, topic));
    let binding = schema.json();

    let chosen = subject
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .or_else(|| binding.and_then(|b| b.subject.clone()));
    let has_files = binding.is_some_and(|b| !b.files.is_empty());
    let subject = match (chosen, has_files) {
        (Some(chosen), _) => Some(chosen),
        (None, true) => None,
        // Тип обязан совпасть: догадка «в реестре есть схема» без оговорки про
        // формат проверяла бы JSON по avro-контракту.
        (None, false) => subject_in_registry(root, cluster, &subject_of(topic))
            .filter(|d| d.kind == SchemaKind::Json)
            .map(|d| d.subject),
    };

    if let Some(subject) = subject {
        let (url, client) =
            registry_of(root, cluster).ok_or("no schema registry is configured for this cluster")?;
        let version = binding.and_then(|b| b.version.filter(|_| b.subject.as_deref() == Some(&subject)));

        // id приезжает вместе со схемой: спрашивать его вторым запросом значило
        // бы ходить в реестр дважды на каждое нажатие клавиши в форме отправки.
        let found = json::cache::by_subject(&client, &url, &subject, version)?;
        return Ok(JsonProduceSchema {
            compiled: found.compiled,
            id: Some(found.id),
            subject: Some(subject),
        });
    }

    let files = binding.map(|b| b.files.as_slice()).unwrap_or_default();
    let texts = json_texts(&json_dir_of(root, &schema), files)?;
    let compiled = json_compile(&texts)
        .map_err(|_| "pick a subject or load a JSON schema file for this topic first".to_string())?;
    Ok(JsonProduceSchema {
        compiled,
        id: None,
        subject: None,
    })
}

// --- Общая механика ---------------------------------------------------------

fn ensure_record(schemas: &mut Vec<TopicSchema>, cluster: &str, topic: &str) -> usize {
    if let Some(index) = position(schemas, cluster, topic) {
        return index;
    }
    schemas.push(blank(cluster, topic));
    schemas.len() - 1
}

/// Байты уже добавленных файлов — из своей копии.
///
/// Копии может не оказаться, если каталог настроек чистили руками; тогда
/// выручает исходный путь. Это не «перечитать с диска»: там перечитывание
/// затребовано пользователем, а здесь оно последнее средство.
fn keep_existing(dir: &Path, files: &[SchemaFile]) -> Result<Vec<Pending>, String> {
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

fn read_copy(dir: &Path, file: &SchemaFile) -> Result<Vec<u8>, String> {
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
    let staged = Staged::write(dir.to_path_buf(), pending)?;

    let listing: Vec<SchemaFile> = pending
        .iter()
        .map(|p| SchemaFile {
            name: p.name.clone(),
            source: p.source.clone(),
        })
        .collect();

    // Разбираем ещё во временном каталоге: неудача не должна оставить следа.
    let linked = linked::parse(staged.path(), &listing)?;
    let messages = linked.messages.clone();

    // Прошлый разбор относится к прежнему содержимому каталога и с этого
    // момента врёт.
    linked::invalidate(dir);
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
            format: Some(BodyFormat::Proto),
            files: Vec::new(),
            message: message.map(str::to_string),
            dir: "d".into(),
            avro: None,
            json: None,
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
        schema.format = None;
        assert!(is_trivial(&schema));
        schema.format = Some(BodyFormat::Text);
        assert!(!is_trivial(&schema), "выбор Text — тоже настройка");
        // И выбор json тоже: он отменяет автоопределение, а отменять его
        // нечему, если запись не сохранить.
        schema.format = Some(BodyFormat::Json);
        assert!(!is_trivial(&schema), "выбор json — тоже настройка");
    }

    /// Автоопределение действует, только пока формат не выбран, и уступает
    /// любому выбору — в том числе выбору json.
    #[test]
    fn a_chosen_format_wins_over_detection() {
        let mut schema = schema_with(None);
        schema.format = None;
        assert_eq!(
            effective_format(&schema, Some(SchemaKind::Avro)),
            BodyFormat::Avro
        );
        assert_eq!(effective_format(&schema, None), BodyFormat::Json);

        schema.format = Some(BodyFormat::Json);
        assert_eq!(
            effective_format(&schema, Some(SchemaKind::Avro)),
            BodyFormat::Json
        );
    }

    /// Формат схемы в реестре решает, каким форматом показывать топик. Ровно
    /// этого различия и не было, пока автоопределение отвечало «да/нет»:
    /// JSON-Schema-топик становился avro и показывался мусором.
    #[test]
    fn the_registry_kind_picks_the_format() {
        let mut schema = schema_with(None);
        schema.format = None;
        assert_eq!(
            effective_format(&schema, Some(SchemaKind::Json)),
            BodyFormat::JsonSchema
        );
    }

    /// PROTOBUF в реестре формат НЕ переключает: схему оттуда мы пока не тянем,
    /// и топик оказался бы в формате, для которого нечем декодировать.
    #[test]
    fn a_protobuf_subject_does_not_switch_the_format_on_its_own() {
        let mut schema = schema_with(None);
        schema.format = None;
        assert_eq!(
            effective_format(&schema, Some(SchemaKind::Protobuf)),
            BodyFormat::Json
        );
    }

    /// Запись с одной лишь JSON-привязкой хранить надо: иначе выбранный subject
    /// не пережил бы перезапуск.
    #[test]
    fn a_json_binding_alone_is_worth_storing() {
        let mut schema = schema_with(None);
        schema.format = None;
        assert!(is_trivial(&schema));

        schema.json = Some(JsonBinding {
            subject: Some("orders-value".to_string()),
            ..JsonBinding::default()
        });
        assert!(!is_trivial(&schema));

        // Пустая привязка — это отсутствие привязки.
        schema.json = Some(JsonBinding::default());
        assert!(is_trivial(&schema));
    }

    /// Имя subject по умолчанию у любого штатного сериализатора Kafka.
    #[test]
    fn the_default_subject_is_the_topic_name_strategy() {
        assert_eq!(subject_of("orders.stream"), "orders.stream-value");
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

        fn add_avsc(&self, sources: &[String]) -> Result<TopicSchemaView, String> {
            add_avro_files(&self.root, CLUSTER, TOPIC, sources)
        }

        fn stored(&self) -> Vec<TopicSchema> {
            list(&self.root).unwrap()
        }

        /// Каталог схемы единственного сохранённого топика.
        fn schema_dir(&self) -> PathBuf {
            dir_of(&self.root, &self.stored()[0])
        }

        fn avro_schema_dir(&self) -> PathBuf {
            avro_dir_of(&self.root, &self.stored()[0])
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
        linked::invalidate(&sandbox.schema_dir());

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

    /// Выбор json — это ВЫБОР, а не отсутствие настроек, и хранить его надо
    /// так же, как остальные. Иначе он не пережил бы даже перезапуска, а на
    /// топике, который автоопределение считает avro-шным, не пережил бы и
    /// следующего открытия: формат вернулся бы в avro сам собой.
    #[test]
    fn choosing_json_is_stored_and_not_mistaken_for_no_choice() {
        let sandbox = Sandbox::new("explicit-json");
        set_options(&sandbox.root, CLUSTER, TOPIC, BodyFormat::Json, None).unwrap();

        assert_eq!(sandbox.stored().len(), 1, "выбор json обязан лечь на диск");
        assert_eq!(sandbox.stored()[0].format, Some(BodyFormat::Json));
        let view = view(&sandbox.root, CLUSTER, TOPIC).unwrap().unwrap();
        assert_eq!(view.format, BodyFormat::Json);
    }

    /// Топик, которому не назначали ничего, на кластере без реестра — это
    /// отсутствие настроек, а не пустая форма: автоопределению неоткуда взяться,
    /// и придумывать топику запись незачем.
    #[test]
    fn a_topic_nobody_touched_has_no_view_without_a_registry() {
        let sandbox = Sandbox::new("untouched");
        assert!(view(&sandbox.root, CLUSTER, TOPIC).unwrap().is_none());
        assert!(decoder(&sandbox.root, CLUSTER, TOPIC).unwrap().is_none());
        assert!(sandbox.stored().is_empty());
    }

    /// Ушёл последний .proto — ушёл и выбор формата, а не сменился на json:
    /// с этого момента топик снова вправе определиться сам.
    #[test]
    fn removing_the_last_file_clears_the_choice_rather_than_forcing_json() {
        let sandbox = Sandbox::new("format-released");
        let proto = sandbox.author(
            "events.proto",
            "syntax = \"proto3\"; package demo; message Event { string id = 1; }",
        );
        sandbox.add(&[proto]).unwrap();
        // Второй топик того же кластера — чтобы запись не исчезла целиком и
        // было на что посмотреть.
        let avsc = sandbox.author("event.avsc", EVENT_AVSC);
        add_avro_files(&sandbox.root, CLUSTER, TOPIC, &[avsc]).unwrap();
        assert_eq!(sandbox.stored()[0].format, Some(BodyFormat::Avro));

        remove_avro_file(&sandbox.root, CLUSTER, TOPIC, "event.avsc").unwrap();
        assert_eq!(
            sandbox.stored()[0].format,
            None,
            "выбор формата должен был уйти вместе со схемой, которая его задала"
        );
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

    // --- Avro -----------------------------------------------------------------
    //
    // Те же требования, что и к .proto: набор проверяется разбором целиком,
    // невалидное не сохраняется, схема переживает исчезновение оригиналов. Плюс
    // одно своё: файлы и subject взаимоисключающи.

    const EVENT_AVSC: &str = r#"{
        "type": "record", "name": "Event", "namespace": "demo",
        "fields": [{"name": "id", "type": "string"}]
    }"#;

    const MONEY_AVSC: &str = r#"{
        "type": "record", "name": "Money", "namespace": "common",
        "fields": [{"name": "amount", "type": "long"}]
    }"#;

    const ORDER_AVSC: &str = r#"{
        "type": "record", "name": "Order", "namespace": "orders",
        "fields": [{"name": "total", "type": "common.Money"}]
    }"#;

    #[test]
    fn a_single_avsc_becomes_a_working_avro_topic() {
        let sandbox = Sandbox::new("avro-single");
        let path = sandbox.author("event.avsc", EVENT_AVSC);

        let view = sandbox.add_avsc(&[path.clone()]).unwrap();
        // Загрузили схему — значит топик показываем как Avro, и единственная
        // запись подставилась сама.
        assert_eq!(view.format, BodyFormat::Avro);
        let avro = view.avro.expect("avro-половина обязана появиться");
        assert_eq!(avro.record.as_deref(), Some("demo.Event"));
        assert_eq!(avro.records, ["demo.Event"]);
        assert_eq!(avro.files.len(), 1);
        assert_eq!(avro.files[0].source, path);
        assert!(!avro.registry, "реестра у этого кластера нет");
        // И уже есть чем декодировать.
        assert!(decoder(&sandbox.root, CLUSTER, TOPIC).unwrap().is_some());
    }

    #[test]
    fn the_avro_schema_survives_the_original_files_disappearing() {
        let sandbox = Sandbox::new("avro-survives");
        let path = sandbox.author("event.avsc", EVENT_AVSC);
        sandbox.add_avsc(&[path.clone()]).unwrap();

        std::fs::remove_file(&path).unwrap();

        let view = view(&sandbox.root, CLUSTER, TOPIC).unwrap().unwrap();
        assert_eq!(view.avro.and_then(|a| a.error), None, "своя копия должна разбираться");
        assert!(decoder(&sandbox.root, CLUSTER, TOPIC).unwrap().is_some());
    }

    /// Ссылка между .avsc разрешается только тогда, когда парсеру подали весь
    /// набор, — ровно как импорт между .proto.
    #[test]
    fn two_avsc_files_referencing_each_other_resolve() {
        let sandbox = Sandbox::new("avro-refs");
        let order = sandbox.author("order.avsc", ORDER_AVSC);
        let money = sandbox.author("money.avsc", MONEY_AVSC);

        let view = sandbox.add_avsc(&[order, money]).unwrap();
        let avro = view.avro.unwrap();
        assert_eq!(avro.records, ["common.Money", "orders.Order"]);
        // Из двух сам не выбирается: декодировать молча не тем нельзя.
        assert_eq!(avro.record, None);
        assert!(decoder(&sandbox.root, CLUSTER, TOPIC).unwrap().is_none());

        set_avro_record(&sandbox.root, CLUSTER, TOPIC, Some("orders.Order".into())).unwrap();
        assert!(decoder(&sandbox.root, CLUSTER, TOPIC).unwrap().is_some());
    }

    #[test]
    fn a_broken_avsc_is_rejected_and_changes_nothing() {
        let sandbox = Sandbox::new("avro-broken");
        let good = sandbox.author("event.avsc", EVENT_AVSC);
        sandbox.add_avsc(&[good]).unwrap();

        let bad = sandbox.author("bad.avsc", "{ \"type\": \"record\", oops");
        assert!(sandbox.add_avsc(&[bad]).is_err());

        let avro = view(&sandbox.root, CLUSTER, TOPIC).unwrap().unwrap().avro.unwrap();
        assert_eq!(avro.files.len(), 1, "битый файл не должен был попасть в схему");
        assert_eq!(avro.error, None, "прежняя схема обязана остаться рабочей");
        assert!(!sandbox.avro_schema_dir().join("bad.avsc").exists());
    }

    /// Файл, ссылающийся на тип, которого в наборе нет, схемы не образует —
    /// как и .proto с неразрешённым импортом.
    #[test]
    fn an_avsc_with_a_dangling_reference_is_rejected() {
        let sandbox = Sandbox::new("avro-dangling");
        let order = sandbox.author("order.avsc", ORDER_AVSC);
        let error = sandbox.add_avsc(&[order]).unwrap_err();
        assert!(error.contains("Money"), "{error}");
        assert!(sandbox.stored().is_empty(), "запись не должна была появиться");
    }

    #[test]
    fn removing_the_last_avsc_takes_the_avro_format_with_it() {
        let sandbox = Sandbox::new("avro-remove-last");
        let path = sandbox.author("event.avsc", EVENT_AVSC);
        let dir = {
            sandbox.add_avsc(&[path]).unwrap();
            sandbox.avro_schema_dir()
        };

        let view = remove_avro_file(&sandbox.root, CLUSTER, TOPIC, "event.avsc")
            .unwrap()
            .unwrap();
        assert_eq!(view.format, BodyFormat::Json);
        assert!(view.avro.is_none());
        assert!(!dir.exists(), "копии .avsc должны были уйти вместе со схемой");
        assert!(sandbox.stored().is_empty(), "пустая запись хранению не подлежит");
        assert!(decoder(&sandbox.root, CLUSTER, TOPIC).unwrap().is_none());
    }

    /// Требование в чистом виде: два источника схемы у одного топика — это два
    /// разных ответа на вопрос «чем декодировать», и держать оба нельзя.
    #[test]
    fn binding_a_subject_drops_the_loaded_files() {
        let sandbox = Sandbox::new("avro-exclusive");
        let path = sandbox.author("event.avsc", EVENT_AVSC);
        sandbox.add_avsc(&[path]).unwrap();
        let dir = sandbox.avro_schema_dir();

        let view =
            set_avro_subject(&sandbox.root, CLUSTER, TOPIC, Some("orders-value".into()), None)
                .unwrap();
        let avro = view.avro.unwrap();
        assert_eq!(avro.subject.as_deref(), Some("orders-value"));
        assert!(avro.files.is_empty());
        assert_eq!(avro.record, None);
        assert!(!dir.exists(), "копии .avsc должны были уйти вместе с выбором реестра");
    }

    /// Привязка обязана пережить перезапуск — как и любая другая настройка
    /// топика.
    #[test]
    fn a_subject_binding_is_remembered() {
        let sandbox = Sandbox::new("avro-subject");
        set_options(&sandbox.root, CLUSTER, TOPIC, BodyFormat::Avro, None).unwrap();
        set_avro_subject(&sandbox.root, CLUSTER, TOPIC, Some("orders-value".into()), Some(3))
            .unwrap();

        let avro = view(&sandbox.root, CLUSTER, TOPIC).unwrap().unwrap().avro.unwrap();
        assert_eq!(avro.subject.as_deref(), Some("orders-value"));
        assert_eq!(avro.version, Some(3));
    }

    /// Subject есть, а реестра у кластера нет: сказать об этом надо словами, а
    /// не молча показывать тела текстом.
    #[test]
    fn a_subject_without_a_registry_is_an_explicit_error() {
        let sandbox = Sandbox::new("avro-no-registry");
        set_options(&sandbox.root, CLUSTER, TOPIC, BodyFormat::Avro, None).unwrap();
        set_avro_subject(&sandbox.root, CLUSTER, TOPIC, Some("orders-value".into()), None)
            .unwrap();

        let error = decoder(&sandbox.root, CLUSTER, TOPIC)
            .err()
            .expect("привязка к subject без реестра обязана быть ошибкой");
        assert!(error.contains("no schema registry"), "{error}");
    }

    #[test]
    fn deleting_a_cluster_takes_its_avsc_copies_too() {
        let sandbox = Sandbox::new("avro-forget");
        let path = sandbox.author("event.avsc", EVENT_AVSC);
        sandbox.add_avsc(&[path]).unwrap();
        let dir = sandbox.avro_schema_dir();

        forget_cluster(&sandbox.root, CLUSTER).unwrap();

        assert!(view(&sandbox.root, CLUSTER, TOPIC).unwrap().is_none());
        assert!(!dir.exists(), "копии .avsc удалённого кластера остались");
    }

    /// Оба формата у одного топика: .proto и .avsc не мешают друг другу, а
    /// показывается тот, который выбран форматом.
    #[test]
    fn proto_and_avro_can_coexist_on_one_topic() {
        let sandbox = Sandbox::new("both");
        let proto = sandbox.author(
            "events.proto",
            "syntax = \"proto3\"; package demo; message Event { string id = 1; }",
        );
        sandbox.add(&[proto]).unwrap();
        let avsc = sandbox.author("event.avsc", EVENT_AVSC);
        let view = sandbox.add_avsc(&[avsc]).unwrap();

        // Загрузка .avsc перевела топик в Avro, но .proto никуда не делся.
        assert_eq!(view.format, BodyFormat::Avro);
        assert_eq!(view.messages, ["demo.Event"]);
        assert_eq!(view.message.as_deref(), Some("demo.Event"));
        assert!(matches!(
            decoder(&sandbox.root, CLUSTER, TOPIC).unwrap().as_deref(),
            Some(Decoder::Avro(_))
        ));

        // Переключили формат обратно — и декодирует снова protobuf.
        set_options(
            &sandbox.root,
            CLUSTER,
            TOPIC,
            BodyFormat::Proto,
            Some("demo.Event".into()),
        )
        .unwrap();
        assert!(matches!(
            decoder(&sandbox.root, CLUSTER, TOPIC).unwrap().as_deref(),
            Some(Decoder::Proto(_))
        ));
    }
}
