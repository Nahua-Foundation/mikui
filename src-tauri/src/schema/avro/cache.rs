//! Схемы, добытые из реестра: в памяти и на диске.
//!
//! Кэш здесь не оптимизация, а условие работоспособности. В confluent-формате
//! схема адресуется id из заголовка КАЖДОГО сообщения, то есть за ней надо
//! идти прямо во время выдачи окна таблицы — а это путь, на котором сети быть
//! не должно. Кэш убирает её оттуда: запрос уходит один раз на id, которого мы
//! ещё не видели.
//!
//! Дисковая половина решает вторую задачу — сохранённые сообщения. Их для того
//! и сохраняли, чтобы они пережили и retention, и само подключение; открывать
//! их, требуя живого реестра, значило бы отменить это обещание. Ровно тем же
//! соображением объясняются копии .proto у себя в каталоге.
//!
//! Отрицательная половина — id, по которым реестр отказал. Без неё одно чужое
//! сообщение посреди топика стоило бы сетевого запроса на каждую прокрутку
//! мимо него. Забывается она при сборке нового декодера (`forget_failures`),
//! так что «переоткрыть топик» — это и есть жест «попробовать ещё раз».

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use super::linked::Linked;
use super::registry::{RegisteredSchema, Registry};

const CACHE_DIR: &str = "avro-cache";

/// Сколько разобранных схем держать в памяти.
///
/// Топик, переживший десяток версий контракта, — обычное дело; сотня разных id
/// в одном окне выдачи — уже нет.
const MEMORY_LIMIT: usize = 64;

/// Разобранные схемы. Ключ — реестр и id: схемы разных реестров нумеруются
/// независимо, и смешать их значило бы показать чужой контракт.
static MEMORY: Mutex<Vec<(Key, Arc<Linked>)>> = Mutex::new(Vec::new());

/// Чем кончилась неудачная попытка. Текст хранится, чтобы показать причину, а
/// не просто «не смогли».
static FAILURES: Mutex<Option<HashMap<Key, String>>> = Mutex::new(None);

/// Схемы, добытые по subject. Ключ — реестр и `subject@версия`.
static SUBJECTS: Mutex<Vec<(SubjectKey, Arc<Linked>)>> = Mutex::new(Vec::new());

/// Ответы на вопрос «есть ли в реестре такой subject» — под автоопределение
/// формата. Ключ тот же, что у схем по subject; значение — ответ и когда он
/// получен. `None` в ответе — реестр промолчал: это НЕ «нет такого subject», и
/// путать их нельзя.
static KNOWN: Mutex<Option<HashMap<SubjectKey, (Instant, Option<bool>)>>> = Mutex::new(None);

/// Сколько верить ответу про существование subject.
///
/// Не навсегда: топик заводят и снабжают схемой прямо во время работы с
/// приложением, и требовать ради этого перезапуска было бы странно. Не
/// секунды: ответ спрашивают на каждое открытие топика, а меняется он раз в
/// жизни контракта.
const KNOWN_TTL: Duration = Duration::from_secs(300);

/// Сколько не спрашивать снова после молчания реестра.
///
/// Короче обычного: недоступность — состояние временное, и топик обязан сам
/// стать avro-шным, как только реестр поднимут. Но и не ноль — иначе каждое
/// открытие топика при лежащем реестре стоило бы пяти секунд таймаута.
const SILENCE_TTL: Duration = Duration::from_secs(30);

/// Реестр и id схемы в нём. Реестр в ключе потому, что нумерация у каждого
/// своя, и смешать их значило бы показать чужой контракт.
type Key = (String, u32);

/// Реестр и `subject@версия`.
type SubjectKey = (String, String);

/// То, что лежит в файле кэша: схема и всё, на что она ссылается.
///
/// Ссылки сохраняются вместе с ней, а не докачиваются при чтении: иначе
/// офлайновое открытие сохранённого сообщения упиралось бы в реестр ровно там,
/// где кэш и должен был его заменить.
#[derive(Serialize, Deserialize)]
struct Cached {
    schema: String,
    #[serde(default)]
    references: Vec<String>,
}

/// Схема по id: из памяти, с диска или из реестра — в этом порядке.
///
/// `root` каталога настроек нужен ради дисковой половины; без него (в тестах)
/// работают только память и сеть.
pub fn by_id(
    root: Option<&Path>,
    registry: &Registry,
    url: &str,
    id: u32,
) -> Result<Arc<Linked>, String> {
    let key = (url.to_string(), id);

    if let Some(hit) = remembered(&key) {
        return Ok(hit);
    }
    if let Some(why) = failed(&key) {
        return Err(why);
    }

    let built = build(root, registry, url, id);
    match built {
        Ok(linked) => {
            memorize(key, Arc::clone(&linked));
            Ok(linked)
        }
        Err(e) => {
            remember_failure(key, &e);
            Err(e)
        }
    }
}

fn build(
    root: Option<&Path>,
    registry: &Registry,
    url: &str,
    id: u32,
) -> Result<Arc<Linked>, String> {
    if let Some(cached) = root.and_then(|root| read_disk(root, url, id)) {
        return Ok(Arc::new(Linked::parse_with_refs(
            &cached.schema,
            &cached.references,
        )?));
    }

    let fetched = registry.by_id(id)?;
    let linked = Linked::parse_with_refs(&fetched.schema, &fetched.references)?;
    // Пишем только то, что разобралось: класть на диск текст, который мы сами
    // не смогли прочитать, — это отложить ту же ошибку до следующего запуска,
    // уже без шанса перезапросить.
    if let Some(root) = root {
        write_disk(root, url, id, &fetched);
    }
    Ok(Arc::new(linked))
}

/// Кладёт в кэш уже разобранную схему, добытую вызывающим.
///
/// Нужно там, где схему берут не ради показа, а ради отправки: сообщение,
/// которое мы только что положили в топик, приедет обратно с этим же id, и без
/// этой записи первое же чтение пошло бы в сеть за тем, что минуту назад
/// держали в руках.
pub fn remember_fetched(
    root: Option<&Path>,
    url: &str,
    fetched: &RegisteredSchema,
    linked: Arc<Linked>,
) {
    if fetched.id == 0 {
        return;
    }
    memorize((url.to_string(), fetched.id), linked);
    if let Some(root) = root {
        write_disk(root, url, fetched.id, fetched);
    }
}

/// Схема subject'а, закреплённого за топиком.
///
/// Кэш здесь нужен по другой причине, чем у id: за subject'ом ходят не на
/// каждое сообщение, а на каждую сборку декодера — то есть на открытие топика
/// и на каждое обновление списка сохранённых, где декодер строится для каждого
/// топика архива. Без памяти список из десяти топиков означал бы десять
/// блокирующих запросов, а с недоступным реестром — десять таймаутов подряд.
///
/// Дисковой половины у subject'а нет намеренно: на диске схемы лежат по id, а
/// какой id у «последней версии» — знает только реестр. Офлайн выручает путь
/// по id: именно он и работает у сообщений confluent-формата, а это тот самый
/// случай, ради которого сохранённые сообщения и открывают без сети.
pub fn by_subject(
    root: Option<&Path>,
    registry: &Registry,
    url: &str,
    subject: &str,
    version: Option<i32>,
) -> Result<Arc<Linked>, String> {
    let key = format!(
        "{subject}@{}",
        version.map_or_else(|| "latest".to_string(), |v| v.to_string())
    );
    if let Some(hit) = remembered_subject(url, &key) {
        return Ok(hit);
    }

    let fetched = registry.by_subject(subject, version)?;
    let linked = Arc::new(Linked::parse_with_refs(
        &fetched.schema,
        &fetched.references,
    )?);

    // Сообщения такого топика приезжают с этим же id в заголовке — положив
    // схему и под ним, мы избавляем первое из них от похода в сеть за уже
    // добытым.
    if fetched.id != 0 {
        memorize((url.to_string(), fetched.id), Arc::clone(&linked));
        if let Some(root) = root {
            write_disk(root, url, fetched.id, &fetched);
        }
    }
    memorize_subject(url, key, Arc::clone(&linked));
    Ok(linked)
}

// --- Автоопределение формата --------------------------------------------------
//
// Вопрос «держит ли реестр схему для этого топика» задаётся на КАЖДОЕ его
// открытие, а ответ на него меняется раз в жизни контракта. Поэтому две
// функции, а не одна: `subject_known` отвечает по памяти и не стоит ничего, а
// `probe_subject` идёт в сеть — и вызывающий сначала спрашивает первую, потому
// что второй нужен живой клиент, а его построение читает пароль из keychain.

/// Готовый ответ, если он ещё не протух. `None` — спрашивать заново.
pub fn subject_known(url: &str, subject: &str) -> Option<Option<bool>> {
    let known = KNOWN.lock().ok()?;
    let (asked, answer) = known.as_ref()?.get(&key_of(url, subject))?;
    let ttl = match answer {
        Some(_) => KNOWN_TTL,
        None => SILENCE_TTL,
    };
    (asked.elapsed() < ttl).then_some(*answer)
}

/// Спрашивает реестр и запоминает ответ.
///
/// `None` — реестр не ответил. Наружу это уезжает как «не знаем», а не как
/// «нет»: автоопределение обязано молчать там, где оно не уверено, иначе
/// недоступный на минуту реестр превратил бы avro-топик в текстовый.
pub fn probe_subject(registry: &Registry, url: &str, subject: &str) -> Option<bool> {
    let answer = registry.has_subject(subject).ok();
    if let Ok(mut known) = KNOWN.lock() {
        known
            .get_or_insert_with(HashMap::new)
            .insert(key_of(url, subject), (Instant::now(), answer));
    }
    answer
}

fn key_of(url: &str, subject: &str) -> SubjectKey {
    (url.to_string(), subject.to_string())
}

/// Забывает ответы автоопределения.
///
/// Зовётся оттуда же, откуда и `forget_subjects`: пользователь, тронувший
/// настройки схемы, ждёт, что приложение посмотрит на реестр свежим взглядом.
fn forget_known(url: Option<&str>) {
    let Ok(mut known) = KNOWN.lock() else {
        return;
    };
    match url {
        Some(url) => {
            if let Some(known) = known.as_mut() {
                known.retain(|(cached, _), _| cached != url);
            }
        }
        None => *known = None,
    }
}

/// Забывает неудачи. Зовётся при сборке декодера: пользователь, переоткрывший
/// топик, вправе рассчитывать на новую попытку.
pub fn forget_failures() {
    if let Ok(mut failures) = FAILURES.lock() {
        *failures = None;
    }
}

/// Забывает схемы, добытые по subject.
///
/// Зовётся там, где пользователь сам сказал «схема поменялась»: сменил subject,
/// перечитал файлы. Автоматически это не делается — иначе кэш subject'а не
/// пережил бы и одного открытия топика, ради чего он и заведён.
pub fn forget_subjects() {
    if let Ok(mut subjects) = SUBJECTS.lock() {
        subjects.clear();
    }
    forget_known(None);
}

fn remembered(key: &Key) -> Option<Arc<Linked>> {
    let memory = MEMORY.lock().ok()?;
    memory
        .iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| Arc::clone(v))
}

fn memorize(key: Key, value: Arc<Linked>) {
    let Ok(mut memory) = MEMORY.lock() else {
        return;
    };
    memory.retain(|(k, _)| *k != key);
    memory.push((key, value));
    if memory.len() > MEMORY_LIMIT {
        memory.remove(0);
    }
}

fn remembered_subject(url: &str, key: &str) -> Option<Arc<Linked>> {
    let subjects = SUBJECTS.lock().ok()?;
    subjects
        .iter()
        .find(|((u, k), _)| u == url && k == key)
        .map(|(_, v)| Arc::clone(v))
}

fn memorize_subject(url: &str, key: String, value: Arc<Linked>) {
    let Ok(mut subjects) = SUBJECTS.lock() else {
        return;
    };
    let key = (url.to_string(), key);
    subjects.retain(|(k, _)| *k != key);
    subjects.push((key, value));
    if subjects.len() > MEMORY_LIMIT {
        subjects.remove(0);
    }
}

fn failed(key: &Key) -> Option<String> {
    let failures = FAILURES.lock().ok()?;
    failures.as_ref()?.get(key).cloned()
}

fn remember_failure(key: Key, why: &str) {
    if let Ok(mut failures) = FAILURES.lock() {
        failures
            .get_or_insert_with(HashMap::new)
            .insert(key, why.to_string());
    }
}

// --- Диск --------------------------------------------------------------------

fn path_of(root: &Path, url: &str, id: u32) -> PathBuf {
    root.join(CACHE_DIR).join(fingerprint(url)).join(format!("{id}.json"))
}

fn read_disk(root: &Path, url: &str, id: u32) -> Option<Cached> {
    let bytes = std::fs::read(path_of(root, url, id)).ok()?;
    serde_json::from_slice(&bytes).ok()
}

/// Ошибки записи глотаются намеренно: кэш — ускорение и запас на офлайн, а не
/// источник правды. Не сумели записать — работаем дальше по сети.
fn write_disk(root: &Path, url: &str, id: u32, fetched: &RegisteredSchema) {
    let path = path_of(root, url, id);
    let Some(parent) = path.parent() else {
        return;
    };
    if std::fs::create_dir_all(parent).is_err() {
        return;
    }
    let cached = Cached {
        schema: fetched.schema.clone(),
        references: fetched.references.clone(),
    };
    if let Ok(bytes) = serde_json::to_vec(&cached) {
        let _ = crate::config::write_atomic(&path, &bytes);
    }
}

/// Имя каталога по адресу реестра: FNV-1a, тот же приём, что у каталогов схем
/// топиков. В URL встречается что угодно, включая символы, которых файловая
/// система не примет, а длина легко перерастает лимит пути.
fn fingerprint(url: &str) -> String {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in url.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("{hash:016x}")
}

/// Забывает кэш реестра целиком — и на диске, и в памяти.
///
/// Зовётся, когда подключение удаляют: осиротевшие схемы никому не нужны ровно
/// так же, как осиротевшие пароли в keychain и копии .proto.
pub fn forget(root: &Path, url: &str) {
    let _ = std::fs::remove_dir_all(root.join(CACHE_DIR).join(fingerprint(url)));
    if let Ok(mut memory) = MEMORY.lock() {
        memory.retain(|((cached, _), _)| cached != url);
    }
    if let Ok(mut subjects) = SUBJECTS.lock() {
        subjects.retain(|((cached, _), _)| cached != url);
    }
    forget_known(Some(url));
}

#[cfg(test)]
mod tests {
    use super::*;

    const SCHEMA: &str = r#"{"type":"record","name":"Event","namespace":"demo","fields":[{"name":"id","type":"string"}]}"#;

    struct Dir(PathBuf);

    impl Dir {
        fn new(name: &str) -> Self {
            let path = std::env::temp_dir().join(format!("mikui-avro-cache-{name}"));
            let _ = std::fs::remove_dir_all(&path);
            std::fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for Dir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn fetched(id: u32) -> RegisteredSchema {
        RegisteredSchema {
            id,
            schema: SCHEMA.to_string(),
            references: Vec::new(),
        }
    }

    /// Ради этого дисковая половина и заведена: схема, добытая однажды, должна
    /// читаться и тогда, когда реестра рядом нет.
    #[test]
    fn a_schema_written_to_disk_is_read_back_without_the_registry() {
        let dir = Dir::new("offline");
        write_disk(&dir.0, "http://sr:8081", 7, &fetched(7));

        let cached = read_disk(&dir.0, "http://sr:8081", 7).expect("схема должна была лечь на диск");
        assert!(cached.references.is_empty());
        let linked = Linked::parse_with_refs(&cached.schema, &cached.references).unwrap();
        assert_eq!(linked.root_name().as_deref(), Some("demo.Event"));
    }

    #[test]
    fn schemas_of_different_registries_do_not_collide() {
        let dir = Dir::new("registries");
        write_disk(&dir.0, "http://dev:8081", 1, &fetched(1));

        assert!(read_disk(&dir.0, "http://dev:8081", 1).is_some());
        // Тот же id в другом реестре — другая схема, и брать её отсюда нельзя.
        assert!(read_disk(&dir.0, "http://prod:8081", 1).is_none());
    }

    #[test]
    fn references_survive_the_round_trip_to_disk() {
        let dir = Dir::new("refs");
        let money = r#"{"type":"record","name":"Money","namespace":"common","fields":[{"name":"amount","type":"long"}]}"#;
        let order = r#"{"type":"record","name":"Order","namespace":"orders","fields":[{"name":"total","type":"common.Money"}]}"#;
        let with_refs = RegisteredSchema {
            id: 3,
            schema: order.to_string(),
            references: vec![money.to_string()],
        };
        write_disk(&dir.0, "http://sr:8081", 3, &with_refs);

        let cached = read_disk(&dir.0, "http://sr:8081", 3).unwrap();
        // Без сохранённых ссылок схема бы просто не разобралась — то есть
        // офлайн ломался бы ровно там, где кэш и должен был выручить.
        let linked = Linked::parse_with_refs(&cached.schema, &cached.references).unwrap();
        assert_eq!(linked.root_name().as_deref(), Some("orders.Order"));
    }

    #[test]
    fn deleting_a_registry_takes_its_cache_along() {
        let dir = Dir::new("forget");
        write_disk(&dir.0, "http://sr:8081", 1, &fetched(1));
        forget(&dir.0, "http://sr:8081");
        assert!(read_disk(&dir.0, "http://sr:8081", 1).is_none());
    }

    #[test]
    fn a_remembered_failure_is_returned_instead_of_a_second_attempt() {
        let key = ("http://sr:8081".to_string(), 999);
        forget_failures();
        assert_eq!(failed(&key), None);

        remember_failure(key.clone(), "Subject not found");
        assert_eq!(failed(&key).as_deref(), Some("Subject not found"));

        // Новый декодер — новая попытка: иначе временно недоступный реестр
        // оставался бы недоступным до перезапуска приложения.
        forget_failures();
        assert_eq!(failed(&key), None);
    }
}
