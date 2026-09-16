//! Архив сохранённых сообщений: индекс, тела и всё, что с ними делают.
//!
//! Раскладка на диске — индекс отдельно, тело каждой записи отдельным файлом:
//!
//! ```text
//! favorites.json                     метаданные всех записей, килобайты
//! favorites/1735…-a3f2….bin          тело одной записи, СЫРЫЕ байты из Kafka
//! ```
//!
//! Одним файлом это делать нельзя: сохраняют как раз тяжёлое, и клик по звезде
//! на сообщении в пару мегабайт переписывал бы тогда весь архив целиком, а
//! открытие списка грузило бы все тела в память. Здесь список стоит одного
//! чтения килобайтного индекса, а «сколько занимает эта запись» — честный
//! размер файла, а не оценка по длине строки.
//!
//! Тело хранится СЫРЫМ, а не разобранным, и это главное решение модуля. Схему к
//! топику загружают когда угодно — в том числе назавтра после сохранения, — и
//! записанный текст `from_utf8_lossy` разобрать было бы уже нечем: невалидные
//! последовательности он заменяет безвозвратно. С байтами же сохранённое
//! сообщение показывается ровно так, как показал бы его открытый топик со
//! своей сегодняшней схемой.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::types::{FavoriteRecord, FavoriteView, FavoritesView, SaveFavoriteResult, SavedMessage};
use crate::config;
use crate::kafka::{text, FullMessage, RawBody};
use crate::schema::{self, Decoder};

const INDEX_FILE: &str = "favorites.json";
const BODIES_DIR: &str = "favorites";

/// Каталог настроек передаётся аргументом, а не достаётся из `AppHandle`: так
/// весь модуль прогоняется на временном каталоге в тестах. Обёртки над
/// `AppHandle` — в `favorites::mod`, ровно как у `proto`.
fn index(root: &Path) -> Result<Vec<FavoriteRecord>, String> {
    config::read_json(&root.join(INDEX_FILE))
}

fn save_index(root: &Path, records: &[FavoriteRecord]) -> Result<(), String> {
    let json = serde_json::to_vec_pretty(records)
        .map_err(|e| format!("can't serialize favorites: {e}"))?;
    config::write_atomic(&root.join(INDEX_FILE), &json)
}

fn body_path(root: &Path, id: &str) -> PathBuf {
    root.join(BODIES_DIR).join(format!("{id}.bin"))
}

/// Идентификатор записи: время сохранения и хэш от того, что делает сообщение
/// тем же самым сообщением.
///
/// FNV-1a — тот же приём, что у имён каталогов схем (`schema::store`): в имени
/// топика бывает что угодно, включая знаки, которых файловая система не примет.
/// Миллисекунды впереди дают уникальность (одно и то же сообщение опознаётся по
/// индексу раньше, чем дойдёт сюда) и заодно читаемый порядок в каталоге.
fn new_id(millis: i64, cluster: &str, topic: &str, partition: i32, offset: i64) -> String {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    let tail = format!("\0{topic}\0{partition}\0{offset}");
    for byte in cluster.as_bytes().iter().chain(tail.as_bytes()) {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("{millis:x}-{hash:016x}")
}

/// Размер файла; отсутствующий файл — не ошибка, а `None`.
fn file_size(path: &Path) -> Option<u64> {
    fs::metadata(path).ok().map(|m| m.len())
}

/// Декодеры топиков, собранные не чаще одного раза на топик.
///
/// Список общий на все кластеры, и записей одного топика в нём обычно
/// несколько; собирать декодер каждой значило бы перечитывать .proto по разу
/// на строку.
#[derive(Default)]
struct Decoders {
    cache: HashMap<(String, String), Option<Arc<Decoder>>>,
}

impl Decoders {
    /// Сломанная схема — то же, что её отсутствие: открытый топик в этом случае
    /// тоже показывает тела текстом (см. `apply_topic_schema`), и архив не
    /// должен показывать их иначе.
    fn get(&mut self, root: &Path, cluster: &str, topic: &str) -> Option<Arc<Decoder>> {
        let key = (cluster.to_string(), topic.to_string());
        self.cache
            .entry(key)
            .or_insert_with(|| schema::store::decoder(root, cluster, topic).unwrap_or(None))
            .clone()
    }
}

fn describe(root: &Path, records: Vec<FavoriteRecord>) -> FavoritesView {
    // Индекс тоже занимает место, и в ответ на «сколько занимает избранное»
    // честнее назвать весь архив, а не только его тела.
    let mut total_bytes = file_size(&root.join(INDEX_FILE)).unwrap_or(0);
    let items = records
        .into_iter()
        .map(|record| {
            let size = file_size(&body_path(root, &record.id));
            total_bytes += size.unwrap_or(0);
            FavoriteView {
                record,
                bytes: size.unwrap_or(0),
                missing: size.is_none(),
            }
        })
        .collect();
    FavoritesView { items, total_bytes }
}

// --- Чтение -----------------------------------------------------------------

/// Список записей вместе с занятым местом.
///
/// Заодно приводит превью в соответствие с сегодняшними схемами: тело хранится
/// сырым, и топик, к которому вчера схемы не было, сегодня показывается иначе.
/// Пересчёт стоит чтения файла, поэтому делается только для записей, у которых
/// наличие схемы разошлось с тем, что было при сохранении, — и результат
/// оседает в индексе, так что второй раз за то же не платят.
pub fn list(root: &Path) -> Result<FavoritesView, String> {
    let mut records = index(root)?;
    if refresh_previews(root, &mut records) {
        // Не критично: превью уже пересчитаны и уедут наверх правильными, а не
        // записались — значит пересчитаются в следующий раз.
        if let Err(e) = save_index(root, &records) {
            eprintln!("can't refresh the favorites index: {e}");
        }
    }
    Ok(describe(root, records))
}

/// Возвращает true, если хоть одну запись пришлось пересчитать.
fn refresh_previews(root: &Path, records: &mut [FavoriteRecord]) -> bool {
    let mut decoders = Decoders::default();
    let mut changed = false;

    for record in records {
        let decoder = decoders.get(root, &record.cluster, &record.topic);
        if record.with_schema == decoder.is_some() {
            continue;
        }
        changed = true;
        // Отметку двигаем в любом случае, даже если тела на диске уже нет:
        // иначе список пытался бы пересчитать пропавшую запись при каждом
        // открытии и никогда не сходился.
        record.with_schema = decoder.is_some();

        let Ok(raw) = fs::read(body_path(root, &record.id)) else {
            continue;
        };
        let shown = text::render(decoder.as_deref(), &raw);
        record.preview = match &shown.decoded {
            Some(json) => text::preview(json.as_bytes(), text::PREVIEW_BYTES),
            None => text::preview(&raw, text::PREVIEW_BYTES),
        };
        record.binary = shown.binary(&raw);
    }
    changed
}

/// Полное сообщение — собранное из записи индекса и её тела ПРЯМО СЕЙЧАС, по
/// той схеме, которая привязана к топику сегодня.
///
/// Отдаётся по требованию, ровно как `get_message_body` для строки таблицы: в
/// списке тела нет и быть не должно.
pub fn message(root: &Path, id: &str) -> Result<SavedMessage, String> {
    let records = index(root)?;
    let record = records
        .iter()
        .find(|r| r.id == id)
        .ok_or_else(|| format!("no saved message {id}"))?;

    let path = body_path(root, id);
    let raw = fs::read(&path)
        .map_err(|e| format!("can't read the saved body {}: {e}", path.display()))?;

    let decoder = schema::store::decoder(root, &record.cluster, &record.topic).unwrap_or(None);
    let shown = text::body(decoder.as_deref(), &raw);
    // Формат — тот же, каким топик показывает таблица; сломанная схема формата
    // не отменяет, поэтому берётся из записи на диске, а не из декодера.
    let format = schema::store::view(root, &record.cluster, &record.topic)
        .unwrap_or(None)
        .map(|view| view.format)
        .unwrap_or_default();

    Ok(SavedMessage {
        message: FullMessage {
            partition: record.partition,
            offset: record.offset,
            timestamp: record.timestamp,
            key: record.key.clone(),
            value: shown.value,
            value_size: raw.len(),
            binary: shown.binary,
            decode_error: shown.decode_error,
            enum_values: shown.enum_values,
            headers: record.headers.clone(),
        },
        format,
    })
}

// --- Изменение --------------------------------------------------------------

/// Сохраняет сообщение.
///
/// То же сообщение (кластер, топик, партиция, офсет), сохранённое повторно, —
/// это обновление, а не вторая копия: идентификатор остаётся прежним, тело
/// перезаписывается. Иначе повторный клик по звезде тихо удваивал бы занятое
/// место.
///
/// Тело пишется раньше индекса. Обратный порядок оставил бы на сбое запись,
/// у которой тела нет вовсе, — а так в худшем случае остаётся файл, на который
/// никто не ссылается.
pub fn save(
    root: &Path,
    cluster: &str,
    cluster_name: &str,
    topic: &str,
    raw: &RawBody,
) -> Result<SaveFavoriteResult, String> {
    let mut records = index(root)?;
    let now = chrono::Utc::now();

    let existing = records
        .iter()
        .position(|r| r.same_message(cluster, topic, raw.partition, raw.offset));
    let id = match existing {
        Some(at) => records[at].id.clone(),
        None => new_id(
            now.timestamp_millis(),
            cluster,
            topic,
            raw.partition,
            raw.offset,
        ),
    };

    let dir = root.join(BODIES_DIR);
    fs::create_dir_all(&dir).map_err(|e| format!("can't create {}: {e}", dir.display()))?;
    config::write_atomic(&body_path(root, &id), &raw.value)?;

    // Превью считается тем же кодом и той же схемой, что и строка таблицы:
    // сохранённое сообщение обязано выглядеть в списке так же, как выглядело в
    // топике.
    let decoder = schema::store::decoder(root, cluster, topic).unwrap_or(None);
    let shown = text::render(decoder.as_deref(), &raw.value);

    let record = FavoriteRecord {
        id: id.clone(),
        cluster: cluster.to_string(),
        cluster_name: cluster_name.to_string(),
        topic: topic.to_string(),
        partition: raw.partition,
        offset: raw.offset,
        timestamp: raw.timestamp,
        key: raw.key.clone(),
        preview: match &shown.decoded {
            Some(json) => text::preview(json.as_bytes(), text::PREVIEW_BYTES),
            None => text::preview(&raw.value, text::PREVIEW_BYTES),
        },
        with_schema: decoder.is_some(),
        value_size: raw.value.len(),
        binary: shown.binary(&raw.value),
        headers: raw.headers.clone(),
        saved_at: now.to_rfc3339(),
    };

    match existing {
        Some(at) => records[at] = record,
        None => records.push(record),
    }
    save_index(root, &records)?;

    Ok(SaveFavoriteResult {
        favorites: describe(root, records),
        id,
        updated: existing.is_some(),
    })
}

/// Убирает одну запись вместе с её телом.
///
/// Индекс переписывается раньше файла: запись обязана исчезнуть из списка даже
/// там, где файл почему-то не удаляется. Осиротевший файл невидим и уходит с
/// `clear`.
pub fn delete(root: &Path, id: &str) -> Result<FavoritesView, String> {
    let mut records = index(root)?;
    let Some(at) = records.iter().position(|r| r.id == id) else {
        return Ok(describe(root, records));
    };
    records.remove(at);
    save_index(root, &records)?;

    if let Err(e) = fs::remove_file(body_path(root, id)) {
        if e.kind() != std::io::ErrorKind::NotFound {
            eprintln!("can't delete the body of favorite {id}: {e}");
        }
    }
    Ok(describe(root, records))
}

/// Чистит архив целиком — вместе с каталогом тел, включая осиротевшие файлы.
pub fn clear(root: &Path) -> Result<FavoritesView, String> {
    save_index(root, &[])?;
    let dir = root.join(BODIES_DIR);
    if let Err(e) = fs::remove_dir_all(&dir) {
        if e.kind() != std::io::ErrorKind::NotFound {
            return Err(format!("can't delete {}: {e}", dir.display()));
        }
    }
    Ok(describe(root, Vec::new()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::MessageHeader;
    use crate::schema::BodyFormat;

    const CLUSTER: &str = "prod";

    /// Каталог настроек на время одного теста.
    struct Sandbox {
        root: PathBuf,
    }

    impl Sandbox {
        fn new(name: &str) -> Self {
            let root = std::env::temp_dir().join(format!("mikui-favorites-test-{name}"));
            let _ = fs::remove_dir_all(&root);
            fs::create_dir_all(&root).unwrap();
            Self { root }
        }

        fn save(&self, topic: &str, partition: i32, offset: i64, value: &[u8]) -> String {
            super::save(
                &self.root,
                CLUSTER,
                "prod",
                topic,
                &raw(partition, offset, value),
            )
            .unwrap()
            .id
        }

        fn list(&self) -> FavoritesView {
            super::list(&self.root).unwrap()
        }
    }

    impl Drop for Sandbox {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    fn raw(partition: i32, offset: i64, value: &[u8]) -> RawBody {
        RawBody {
            partition,
            offset,
            timestamp: 1_700_000_000_000,
            key: "k".into(),
            value: value.to_vec(),
            headers: vec![MessageHeader {
                key: "trace".into(),
                value: "abc".into(),
            }],
        }
    }

    /// Ради этого всё и затевалось: сообщения нет ни в приложении, ни в Kafka —
    /// а оно на месте.
    #[test]
    fn a_saved_message_survives_a_restart() {
        let sandbox = Sandbox::new("restart");
        let id = sandbox.save("orders", 3, 42, br#"{"id":"a-1"}"#);

        // «Перезапуск»: ничего не кэшируем, всё читается с диска заново.
        let view = sandbox.list();
        assert_eq!(view.items.len(), 1);
        let item = &view.items[0];
        assert_eq!(item.record.topic, "orders");
        assert_eq!(item.record.partition, 3);
        assert_eq!(item.record.offset, 42);
        assert_eq!(item.record.preview, r#"{"id":"a-1"}"#);
        assert!(!item.missing);

        let saved = message(&sandbox.root, &id).unwrap();
        assert_eq!(saved.message.value, r#"{"id":"a-1"}"#);
        assert_eq!(saved.message.headers.len(), 1);
        assert_eq!(saved.message.partition, 3);
        assert_eq!(saved.format, BodyFormat::Json);
    }

    /// Тело обязано лежать на диске БАЙТ В БАЙТ таким, каким пришло: только так
    /// его сможет разобрать схема, загруженная уже после сохранения.
    #[test]
    fn the_body_is_stored_as_the_bytes_that_came_from_kafka() {
        let sandbox = Sandbox::new("raw");
        // Валидным UTF-8 это не является — ровно тот случай, на котором
        // сохранение текстом теряло бы данные безвозвратно.
        let bytes: &[u8] = &[0x0a, 0x03, 0xff, 0xfe, 0x00, 0x42];
        let id = sandbox.save("orders", 0, 1, bytes);

        let on_disk = fs::read(body_path(&sandbox.root, &id)).unwrap();
        assert_eq!(on_disk, bytes);

        let saved = message(&sandbox.root, &id).unwrap();
        assert!(saved.message.binary, "без схемы это двоичное тело");
        assert_eq!(saved.message.value_size, bytes.len());
    }

    /// Повторный клик по звезде не должен ни удваивать список, ни удваивать
    /// занятое место.
    #[test]
    fn saving_the_same_message_twice_updates_it_in_place() {
        let sandbox = Sandbox::new("dedup");
        let first = sandbox.save("orders", 0, 7, b"before");

        let result = super::save(
            &sandbox.root,
            CLUSTER,
            "prod",
            "orders",
            &raw(0, 7, b"after"),
        )
        .unwrap();
        assert!(result.updated);
        assert_eq!(result.id, first, "идентификатор обязан остаться прежним");

        let view = sandbox.list();
        assert_eq!(view.items.len(), 1);
        assert_eq!(
            message(&sandbox.root, &first).unwrap().message.value,
            "after"
        );
    }

    /// Регрессия: удаление раньше шло по паре партиция-офсет без учёта топика и
    /// уносило заодно одноимённую пару из соседнего.
    #[test]
    fn deleting_one_message_leaves_the_same_offset_in_another_topic() {
        let sandbox = Sandbox::new("scoped-delete");
        let doomed = sandbox.save("orders", 0, 7, b"orders body");
        let bystander = sandbox.save("payments", 0, 7, b"payments body");

        let view = delete(&sandbox.root, &doomed).unwrap();
        assert_eq!(view.items.len(), 1);
        assert_eq!(view.items[0].record.topic, "payments");
        assert_eq!(
            message(&sandbox.root, &bystander).unwrap().message.value,
            "payments body"
        );
        assert!(!body_path(&sandbox.root, &doomed).exists());
    }

    /// Один и тот же топик в dev и в prod — разные топики, и сохранённое из них
    /// не должно склеиваться.
    #[test]
    fn the_same_topic_in_two_clusters_gives_two_records() {
        let sandbox = Sandbox::new("clusters");
        sandbox.save("orders", 0, 7, b"prod body");
        super::save(
            &sandbox.root,
            "dev",
            "dev",
            "orders",
            &raw(0, 7, b"dev body"),
        )
        .unwrap();

        assert_eq!(sandbox.list().items.len(), 2);
    }

    #[test]
    fn the_reported_size_follows_what_is_actually_on_disk() {
        let sandbox = Sandbox::new("size");
        let heavy = vec![b'x'; 50_000];
        let id = sandbox.save("orders", 0, 1, &heavy);
        let view = sandbox.list();
        assert_eq!(view.items[0].bytes, 50_000);
        assert!(
            view.total_bytes > view.items[0].bytes,
            "индекс тоже занимает место"
        );

        let after = delete(&sandbox.root, &id).unwrap();
        assert!(after.items.is_empty());
        assert!(after.total_bytes < view.total_bytes - 50_000);
    }

    /// Каталог настроек могли почистить руками. Список из-за этого не должен
    /// перестать открываться — иначе повисшую запись нечем даже удалить.
    #[test]
    fn a_body_deleted_behind_our_back_is_reported_not_fatal() {
        let sandbox = Sandbox::new("missing");
        let id = sandbox.save("orders", 0, 1, b"body");
        fs::remove_file(body_path(&sandbox.root, &id)).unwrap();

        let view = sandbox.list();
        assert_eq!(view.items.len(), 1);
        assert!(view.items[0].missing);
        assert_eq!(view.items[0].bytes, 0);
        // А вот открыть такую запись нечем, и врать об этом не надо.
        assert!(message(&sandbox.root, &id).is_err());
        // Удалить — можно.
        assert!(delete(&sandbox.root, &id).unwrap().items.is_empty());
    }

    #[test]
    fn clear_takes_the_bodies_with_it() {
        let sandbox = Sandbox::new("clear");
        sandbox.save("orders", 0, 1, b"a");
        sandbox.save("orders", 0, 2, b"b");

        let view = clear(&sandbox.root).unwrap();
        assert!(view.items.is_empty());
        assert!(!sandbox.root.join(BODIES_DIR).exists());
        assert!(sandbox.list().items.is_empty());
    }

    #[test]
    fn deleting_something_that_is_already_gone_is_not_an_error() {
        let sandbox = Sandbox::new("gone");
        sandbox.save("orders", 0, 1, b"a");
        let view = delete(&sandbox.root, "nope").unwrap();
        assert_eq!(view.items.len(), 1);
    }

    #[test]
    fn an_empty_archive_reads_as_empty() {
        let sandbox = Sandbox::new("empty");
        let view = sandbox.list();
        assert!(view.items.is_empty());
        assert_eq!(view.total_bytes, 0);
    }

    #[test]
    fn identifiers_separate_messages_that_only_look_alike() {
        // Разделитель не даёт склейке «c1» + «x.orders» совпасть с «c1x» + «orders».
        assert_ne!(
            new_id(1, "c1", "x.orders", 0, 0),
            new_id(1, "c1x", "orders", 0, 0)
        );
        assert_ne!(
            new_id(1, "c1", "orders", 0, 1),
            new_id(1, "c1", "orders", 1, 0)
        );
        assert_eq!(
            new_id(1, "c1", "orders", 0, 1),
            new_id(1, "c1", "orders", 0, 1)
        );
    }

    // --- Схема, загруженная после сохранения ---------------------------------
    //
    // То, ради чего тело и хранится сырым. Настоящий .proto здесь не нужен:
    // достаточно проверить, что появление и исчезновение схемы у топика доходит
    // до уже сохранённых записей.

    const PROTO: &str = r#"
        syntax = "proto3";
        package demo;
        message Event { string id = 1; }
    "#;

    /// Кодирует `Event { id }` руками: поле 1, wire type 2 (length-delimited).
    fn event_bytes(id: &str) -> Vec<u8> {
        let mut out = vec![0x0a, id.len() as u8];
        out.extend_from_slice(id.as_bytes());
        out
    }

    /// Привязывает схему к топику так же, как это делает форма настроек.
    fn attach_schema(root: &Path, topic: &str) {
        let source = root.join("event.proto");
        fs::write(&source, PROTO).unwrap();
        schema::store::add_files(
            root,
            CLUSTER,
            topic,
            &[source.to_string_lossy().into_owned()],
        )
        .unwrap();
    }

    #[test]
    fn a_schema_loaded_after_saving_decodes_the_stored_bytes() {
        let sandbox = Sandbox::new("late-schema");
        let id = sandbox.save("orders", 0, 1, &event_bytes("a-1"));

        // Пока схемы нет — это просто байты, показанные как есть.
        let before = sandbox.list();
        assert!(!before.items[0].record.with_schema);
        assert!(!message(&sandbox.root, &id)
            .unwrap()
            .message
            .value
            .contains("\"id\""));

        attach_schema(&sandbox.root, "orders");

        // Сообщение открывается уже разобранным — байты-то никуда не делись.
        let saved = message(&sandbox.root, &id).unwrap();
        assert_eq!(saved.message.value, r#"{"id": "a-1"}"#);
        assert!(!saved.message.binary);
        assert_eq!(saved.format, BodyFormat::Proto);

        // И список догоняет: превью пересчитано и записано обратно в индекс.
        let after = sandbox.list();
        assert!(after.items[0].record.preview.contains("a-1"));
        assert!(after.items[0].record.with_schema);
        assert!(!after.items[0].record.binary);
        // Второе открытие списка уже ничего не пересчитывает — отметка сошлась.
        let again = sandbox.list();
        assert_eq!(again.items[0].record.preview, after.items[0].record.preview);
    }

    /// И обратно: схему убрали — сохранённое обязано снова показываться байтами,
    /// а не остатком вчерашнего разбора.
    #[test]
    fn removing_the_schema_puts_the_preview_back_to_bytes() {
        let sandbox = Sandbox::new("schema-gone");
        attach_schema(&sandbox.root, "orders");
        sandbox.save("orders", 0, 1, &event_bytes("a-1"));
        assert!(sandbox.list().items[0].record.preview.contains("a-1"));

        schema::store::remove_file(&sandbox.root, CLUSTER, "orders", "event.proto").unwrap();

        let view = sandbox.list();
        assert!(
            !view.items[0].record.preview.contains("\"id\""),
            "разбор должен был уйти"
        );
        assert!(!view.items[0].record.with_schema);
    }

    // --- То же самое с Avro ---------------------------------------------------
    //
    // Архив про формат схемы не знает вовсе: он хранит сырые байты и зовёт тот
    // декодер, который сегодня привязан к топику. Проверяем, что это правда, а
    // не совпадение, — на втором формате.

    const AVSC: &str = r#"{
        "type": "record", "name": "Event", "namespace": "demo",
        "fields": [{"name": "id", "type": "string"}]
    }"#;

    /// Кодирует `Event { id }` в голый avro-datum.
    fn avro_bytes(id: &str) -> Vec<u8> {
        let linked = crate::schema::avro::Linked::parse_files(&[AVSC.to_string()], None).unwrap();
        let json: serde_json::Value =
            serde_json::from_str(&format!(r#"{{"id": "{id}"}}"#)).unwrap();
        linked.encode(json).unwrap()
    }

    fn attach_avro(root: &Path, topic: &str) {
        let source = root.join("event.avsc");
        fs::write(&source, AVSC).unwrap();
        schema::store::add_avro_files(
            root,
            CLUSTER,
            topic,
            &[source.to_string_lossy().into_owned()],
        )
        .unwrap();
    }

    #[test]
    fn an_avro_schema_loaded_after_saving_decodes_the_stored_bytes() {
        let sandbox = Sandbox::new("late-avro");
        let id = sandbox.save("orders", 0, 1, &avro_bytes("a-1"));

        // Пока схемы нет — это просто байты. Avro без схемы не разбирается
        // вообще никак, в отличие от protobuf, где видны хотя бы номера полей.
        let before = sandbox.list();
        assert!(!before.items[0].record.with_schema);

        attach_avro(&sandbox.root, "orders");

        let saved = message(&sandbox.root, &id).unwrap();
        assert_eq!(saved.message.value, r#"{"id":"a-1"}"#);
        assert!(!saved.message.binary);
        assert_eq!(saved.format, BodyFormat::Avro);

        let after = sandbox.list();
        assert!(after.items[0].record.preview.contains("a-1"));
        assert!(after.items[0].record.with_schema);
    }
}
