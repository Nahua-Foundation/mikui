//! Разбор каталога .proto в descriptor-граф.
//!
//! Парсер — чисто растовый (`protobuf_parse::Parser::pure`): внешнего `protoc`
//! на машине пользователя нет и требовать его нельзя. Импорты разрешаются одним
//! include-каталогом — самим каталогом схемы, куда файлы уложены под теми
//! именами, под которыми их импортируют (см. `imports::layout`).

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use protobuf::descriptor::DescriptorProto;
use protobuf::reflect::{FileDescriptor, MessageDescriptor, RuntimeFieldType, RuntimeType};

use crate::schema::types::SchemaFile;

/// Разобранная схема: связанный граф файлов и список message на выбор.
#[derive(Debug)]
pub struct Linked {
    files: Vec<FileDescriptor>,
    /// Полные имена message, объявленных в файлах ПОЛЬЗОВАТЕЛЯ, по алфавиту.
    /// Транзитивные зависимости сюда не попадают: `google.protobuf.Timestamp`
    /// в выпадающем списке — мусор, декодировать им никто не собирается.
    pub messages: Vec<String>,
    /// Чем селектор ОТПРАВКИ позволит закодировать тело: те же message плюс
    /// ветки их `oneof` в виде `<Message>.<поле>`, по алфавиту.
    ///
    /// Отдельный список, а не `messages`: у топика выбирают тип, которым его
    /// ЧИТАЮТ, и это всегда корневой message — ветка адресует поля внутри него,
    /// а не сам тип. Сложи их вместе, и в настройках показа предлагалось бы
    /// декодировать топик полем `order_changed`.
    pub produce_choices: Vec<String>,
    /// Единственный КОРНЕВОЙ тип набора — тот, на который не ссылается ни один
    /// другой и который не объявлен внутри другого.
    ///
    /// Если такой ровно один, он и есть тип топика, и гадать по байтам незачем:
    /// .proto, написанный под один топик, выглядит именно так — конверт сверху,
    /// а всё остальное в него вложено или им переиспользовано. Это ответ по
    /// УСТРОЙСТВУ СХЕМЫ, а не по данным, и потому он доступен даже тогда, когда
    /// топик ни разу не открывали.
    ///
    /// `None` — корней несколько (в файле лежат независимые друг от друга типы)
    /// либо ни одного (типы ссылаются по кругу). Вот тогда и остаётся подбор по
    /// телам: `proto::guess`.
    pub root: Option<String>,
}

impl Linked {
    /// Ищет message по полному имени. Ведущая точка необязательна: снаружи имя
    /// приходит и из выпадающего списка (без точки), и из файла настроек.
    pub fn message(&self, full_name: &str) -> Option<MessageDescriptor> {
        message_in(&self.files, full_name)
    }

    /// Разбирает запись селектора отправки: message, которым кодировать, и
    /// выбранную ветку `oneof`, если выбрана именно она.
    ///
    /// Порядок разбора — сначала имя ЦЕЛИКОМ как message, и лишь потом как
    /// `<message>.<поле>`. Он же разрешает неоднозначность: вложенный тип
    /// (`demo.Outer.Second`) содержит точку ровно так же, как ветка, и разбирать
    /// его на «родитель плюс поле» нельзя — зато можно просто спросить граф,
    /// есть ли такое message.
    pub fn produce_target(
        &self,
        choice: &str,
    ) -> Result<(MessageDescriptor, Option<String>), String> {
        if let Some(message) = self.message(choice) {
            return Ok((message, None));
        }

        // Только последняя точка: имя message само содержит их сколько угодно.
        let (head, branch) = choice.rsplit_once('.').ok_or_else(|| unknown(choice))?;
        let message = self.message(head).ok_or_else(|| unknown(choice))?;
        // Поле обязано быть и правда вариантом `oneof` этого message: иначе
        // `produce_target("demo.Order.order_id")` вернул бы `demo.Order`, и
        // заготовка молча построилась бы по чему-то другому, чем просили.
        let known = message
            .oneofs()
            .any(|oneof| oneof.fields().any(|f| f.name() == branch));
        if !known {
            return Err(format!("{head} has no oneof field named {branch}"));
        }
        Ok((message, Some(branch.to_string())))
    }
}

fn unknown(choice: &str) -> String {
    format!("{choice} is not a message in the loaded .proto files")
}

/// Ищет message в графе файлов по полному имени.
///
/// Отдельной функцией, а не только методом `Linked`: список записей селектора
/// строится ещё до того, как `Linked` собран.
fn message_in(files: &[FileDescriptor], full_name: &str) -> Option<MessageDescriptor> {
    // `message_by_full_name` требует именно `.pkg.Name` и паникует на имени без
    // точки — дописываем её здесь, а не оставляем это вызывающим.
    let dotted = match full_name.starts_with('.') {
        true => full_name.to_string(),
        false => format!(".{full_name}"),
    };
    files.iter().find_map(|f| f.message_by_full_name(&dotted))
}

/// Разобранные схемы, ключ — каталоги включения и входные файлы.
///
/// Разбор стоит миллисекунды, а зовут его на каждое переключение топика и на
/// каждое открытие настроек. `Mutex<Vec<..>>` вместо `HashMap`: записей единицы,
/// зато `Vec::new()` — const, и статику не нужен ни `LazyLock`, ни лишний крейт.
///
/// Входные файлы в ключе обязательны, и это не предусмотрительность: наборы из
/// библиотеки ДЕЛЯТ каталог — дерево источника одно на все его контракты, а
/// набор задаётся входным файлом в нём. Ключ из одного каталога отдал бы
/// второму набору разбор первого.
static CACHE: Mutex<Vec<(String, Arc<Linked>)>> = Mutex::new(Vec::new());

/// Сколько разобранных схем держать. Пользователь ходит по нескольким топикам,
/// а не по сотне; дальше это была бы просто удерживаемая память.
const CACHE_LIMIT: usize = 8;

/// Разбирает каталог схемы топика, переиспользуя прошлый результат.
pub fn linked(dir: &Path, files: &[SchemaFile]) -> Result<Arc<Linked>, String> {
    linked_set(std::slice::from_ref(&dir.to_path_buf()), &inputs_of(files))
}

/// То же для набора с несколькими каталогами включения — то есть для набора из
/// библиотеки. См. `parse_set`.
pub fn linked_set(include: &[PathBuf], inputs: &[String]) -> Result<Arc<Linked>, String> {
    let key = cache_key(include, inputs);
    if let Some(hit) = cached(&key) {
        return Ok(hit);
    }
    let built = Arc::new(parse_set(include, inputs)?);
    remember(key, Arc::clone(&built));
    Ok(built)
}

/// Разбирает каталог, не заглядывая в кэш и не записывая в него.
///
/// Нужно ровно там, где результат ещё не имеет права стать общим: файлы лежат
/// во временном каталоге и, если разбор не удастся, будут снесены.
pub fn parse(dir: &Path, files: &[SchemaFile]) -> Result<Linked, String> {
    parse_set(std::slice::from_ref(&dir.to_path_buf()), &inputs_of(files))
}

/// Имена файлов, которые идут разбору ВХОДНЫМИ.
///
/// Только выбранные пользователем. Подтянутые по импорту лежат в том же
/// каталоге и разрешаются как зависимости, но входными не становятся: по
/// входным считается список «своих» message, а типы зависимостей в списке «чем
/// декодировать» — такой же мусор, как `google.protobuf.Any`.
fn inputs_of(files: &[SchemaFile]) -> Vec<String> {
    files
        .iter()
        .filter(|f| !f.auto)
        .map(|f| f.name.clone())
        .collect()
}

/// Разбирает набор: несколько каталогов включения, входные файлы — именами
/// относительно одного из них.
///
/// Каталогов включения несколько ради раскладок, которые иначе не разобрать.
/// Импорт в .proto — это путь ОТ КОРНЯ ВКЛЮЧЕНИЯ, и у репозитория таким корнем
/// бывает то он сам (раскладка Go), то `src/main/proto` внутри модуля
/// (Maven, Gradle, ScalaPB). Какой именно — заранее не известно, поэтому
/// передаются все подходящие; лишний корень разбору не мешает.
pub fn parse_set(include: &[PathBuf], inputs: &[String]) -> Result<Linked, String> {
    if inputs.is_empty() {
        return Err("no .proto files".to_string());
    }
    if include.is_empty() {
        return Err("no include directories".to_string());
    }

    // Путь входного файла собирается с тем корнем, под которым он и правда
    // лежит: `protobuf_parse` считает по нему относительное имя файла, и корень
    // не тот дал бы либо «no such file», либо имя, по которому потом не
    // найдутся «свои» message.
    let mut paths = Vec::with_capacity(inputs.len());
    for name in inputs {
        let found = include
            .iter()
            .map(|root| root.join(name))
            .find(|path| path.exists())
            .ok_or_else(|| format!("{name} is not in the schema directory"))?;
        paths.push(found);
    }

    let mut parser = protobuf_parse::Parser::new();
    parser.pure();
    for root in include {
        parser.include(root);
    }
    parser.inputs(paths);

    let parsed = parser
        .parse_and_typecheck()
        // anyhow-цепочка причин по умолчанию не печатается, а вся конкретика
        // («no such file», номер строки) сидит именно в ней.
        .map_err(|e| format!("{e:#}"))?;

    // Имена входных файлов — они же ключи, по которым отбираются «свои»
    // message. `relative_paths` относительны include-каталога, то есть это
    // ровно `ProtoFile::name`.
    let mine: Vec<String> = parsed
        .relative_paths
        .iter()
        .map(|p| p.to_string())
        .collect();

    let mut messages = Vec::new();
    for proto in &parsed.file_descriptors {
        if !mine.iter().any(|name| name == proto.name()) {
            continue;
        }
        for message in &proto.message_type {
            collect_messages(proto.package(), message, &mut messages);
        }
    }
    messages.sort_unstable();
    messages.dedup();

    let built = FileDescriptor::new_dynamic_fds(parsed.file_descriptors, &[])
        .map_err(|e| format!("can't link .proto files: {e}"))?;

    let produce_choices = produce_choices(&built, &messages);
    let root = single_root(&built, &messages);

    Ok(Linked {
        files: built,
        messages,
        produce_choices,
        root,
    })
}

/// Единственный корневой тип набора. См. `Linked::root`.
///
/// Корень — это message, который в этом же наборе никем не востребован: ни
/// полем другого типа, ни объявлением внутри него. Считается по СВОИМ типам:
/// `google.protobuf.Timestamp` и прочие зависимости в списке не участвуют
/// вовсе, а значит и корнями стать не могут.
///
/// Ссылка типа на самого себя корня не отменяет — дерево с полем своего же типа
/// (`message Node { Node next = 1; }`) остаётся корнем ровно так же.
fn single_root(files: &[FileDescriptor], messages: &[String]) -> Option<String> {
    let mut taken: HashSet<String> = HashSet::new();
    for name in messages {
        let Some(descriptor) = message_in(files, name) else {
            continue;
        };
        // Объявлен внутри другого — он часть его устройства, а не самостоятельный
        // тип топика. Это ровно то, что в .proto означает вложенный `message`.
        if descriptor.enclosing_message().is_some() {
            taken.insert(name.clone());
        }
        for field in descriptor.fields() {
            let referenced = match field.runtime_field_type() {
                RuntimeFieldType::Singular(RuntimeType::Message(m)) => m,
                RuntimeFieldType::Repeated(RuntimeType::Message(m)) => m,
                // Ключ map'а скаляром по определению формата — смотрим значение.
                RuntimeFieldType::Map(_, RuntimeType::Message(m)) => m,
                _ => continue,
            };
            let referenced = referenced.full_name().to_string();
            if &referenced != name {
                taken.insert(referenced);
            }
        }
    }

    let mut roots = messages.iter().filter(|name| !taken.contains(*name));
    match (roots.next(), roots.next()) {
        (Some(only), None) => Some(only.clone()),
        _ => None,
    }
}

/// Записи селектора ОТПРАВКИ: сами message плюс ветки их `oneof`.
///
/// Ветка нужна потому, что тип сообщения в топике — это конверт с `oneof`
/// внутри (`PublicEvent`), а отправляют обычно одну конкретную ветку. Без такой
/// записи выбрать в селекторе `OrderChanged` значило бы попросить приложение
/// закодировать тело ТИПОМ ВЕТКИ: в топик уехали бы поля `OrderChanged` без
/// обёртки, `oneof` в конверте остался бы пуст, а поля легли бы в корень — и
/// потребитель, читающий топик как `PublicEvent`, увидел бы пустое сообщение.
///
/// Синтетические `oneof` сюда не попадают, и это не мелочь: проставь `optional`
/// в proto3, и компилятор заводит под каждое такое поле свой `oneof`. Записи
/// вида `Msg.optional_field` тогда предлагали бы закодировать тело типом,
/// которого не существует. `MessageDescriptor::oneofs` их и отсеивает.
fn produce_choices(files: &[FileDescriptor], messages: &[String]) -> Vec<String> {
    let mut choices = Vec::with_capacity(messages.len());
    for name in messages {
        choices.push(name.clone());
        let Some(message) = message_in(files, name) else {
            continue;
        };
        for oneof in message.oneofs() {
            for field in oneof.fields() {
                choices.push(format!("{name}.{}", field.name()));
            }
        }
    }
    // По алфавиту — ровно как `messages`: в списке ищут подстрокой, и порядок,
    // в котором ветки шли бы за своим конвертом, обрывался бы на первом же
    // длинном имени. Сортировка заодно ставит ветку рядом с конвертом, потому
    // что `<Msg>.` продолжает `<Msg>`.
    choices.sort_unstable();
    choices.dedup();
    choices
}

/// Выбрасывает разобранную схему из кэша. Зовётся всякий раз, когда каталог
/// переписан: иначе после «обновить» декодировали бы прежней версией.
pub fn invalidate(dir: &Path) {
    // По префиксу, а не по точному совпадению: из одного каталога разбирается
    // сколько угодно наборов (библиотека), и знать их входные файлы тому, кто
    // просто поменял содержимое каталога, неоткуда.
    let prefix = dir.to_string_lossy().into_owned();
    if let Ok(mut cache) = CACHE.lock() {
        cache.retain(|(k, _)| !k.starts_with(&prefix));
    }
}

/// Полные имена message, включая вложенные.
///
/// Синтетические типы map-полей пропускаются: `M.FooEntry` в списке на выбор —
/// это предложение декодировать тело парой ключ-значение, чего никто не хочет.
fn collect_messages(package: &str, message: &DescriptorProto, out: &mut Vec<String>) {
    if message.name().is_empty() || message.options.map_entry() {
        return;
    }
    let full = if package.is_empty() {
        message.name().to_string()
    } else {
        format!("{package}.{}", message.name())
    };
    for nested in &message.nested_type {
        collect_messages(&full, nested, out);
    }
    out.push(full);
}

/// Ключ кэша: каталоги включения и входные файлы.
///
/// Первым идёт первый каталог включения — на этом держится `invalidate`,
/// который сбрасывает всё, что разобрано из этого каталога, не зная входных
/// файлов.
fn cache_key(include: &[PathBuf], inputs: &[String]) -> String {
    let dirs: Vec<String> = include
        .iter()
        .map(|d| d.to_string_lossy().into_owned())
        .collect();
    format!("{}|{}", dirs.join(";"), inputs.join(","))
}

fn cached(key: &str) -> Option<Arc<Linked>> {
    let cache = CACHE.lock().ok()?;
    cache
        .iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| Arc::clone(v))
}

fn remember(key: String, value: Arc<Linked>) {
    let Ok(mut cache) = CACHE.lock() else {
        return;
    };
    cache.retain(|(k, _)| *k != key);
    cache.push((key, value));
    if cache.len() > CACHE_LIMIT {
        cache.remove(0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Dir(std::path::PathBuf);

    impl Dir {
        fn new(name: &str) -> Self {
            let path = std::env::temp_dir().join(format!("mikui-proto-test-{name}"));
            let _ = std::fs::remove_dir_all(&path);
            std::fs::create_dir_all(&path).unwrap();
            Self(path)
        }

        fn write(&self, name: &str, text: &str) -> SchemaFile {
            let path = self.0.join(name);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(&path, text).unwrap();
            SchemaFile {
                name: name.to_string(),
                source: path.to_string_lossy().into_owned(),
                auto: false,
                pasted: false,
            }
        }
    }

    impl Drop for Dir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn lists_messages_sorted_including_nested() {
        let dir = Dir::new("nested");
        let file = dir.write(
            "events.proto",
            r#"
                syntax = "proto3";
                package demo;
                message Zeta { message Inner { int32 a = 1; } }
                message Alpha { map<string, int32> tags = 1; }
            "#,
        );

        let linked = parse(&dir.0, &[file]).unwrap();
        // По алфавиту, вложенные включены, синтетический `Alpha.TagsEntry` — нет.
        assert_eq!(
            linked.messages,
            vec!["demo.Alpha", "demo.Zeta", "demo.Zeta.Inner"]
        );
        assert!(linked.message("demo.Zeta.Inner").is_some());
        assert!(linked.message(".demo.Alpha").is_some());
    }

    // --- Корневой тип набора -----------------------------------------------
    //
    // Ответ по УСТРОЙСТВУ схемы, а не по данным: .proto, написанный под один
    // топик, несёт конверт сверху и всё остальное внутри него. Там, где это
    // видно, подбирать по байтам нечего — и, что важнее, не надо открывать
    // топик, чтобы получить ответ.

    /// Обёртка плюс то, что она в себе держит: корень один, и это она.
    ///
    /// Раскладка из настоящего набора — `OrderBookV2` с полями типа `Levels`.
    #[test]
    fn a_wrapper_with_its_parts_has_a_single_root() {
        let dir = Dir::new("root-wrapper");
        let file = dir.write(
            "ob.proto",
            r#"
                syntax = "proto3";
                package demo;
                message OrderBookV2 {
                    string instrument_uid = 1;
                    Levels asks = 7;
                    Levels bids = 8;
                }
                message Levels { double base_price = 1; }
            "#,
        );

        let linked = parse(&dir.0, &[file]).unwrap();
        assert_eq!(linked.root.as_deref(), Some("demo.OrderBookV2"));
    }

    /// Конверт с `oneof`: ветки — его части, корень опять один.
    #[test]
    fn a_oneof_envelope_is_the_root_of_its_branches() {
        let dir = Dir::new("root-oneof");
        let file = dir.write("events.proto", ENVELOPE);
        let linked = parse(&dir.0, &[file]).unwrap();
        assert_eq!(linked.root.as_deref(), Some("demo.PublicEvent"));
    }

    /// Вложенное объявление — тоже «внутри другого», даже если полем его никто
    /// не держит. В .proto это ровно то и значит.
    #[test]
    fn a_nested_declaration_is_not_a_root() {
        let dir = Dir::new("root-nested");
        let file = dir.write(
            "nested.proto",
            r#"
                syntax = "proto3";
                package demo;
                message Outer { message Inner { int32 a = 1; } int32 b = 1; }
            "#,
        );

        let linked = parse(&dir.0, &[file]).unwrap();
        assert_eq!(linked.root.as_deref(), Some("demo.Outer"));
    }

    /// Два независимых типа — двух корней не бывает «одним ответом». Вот тут и
    /// начинается подбор по телам.
    #[test]
    fn two_independent_types_leave_no_single_root() {
        let dir = Dir::new("root-flat");
        let file = dir.write(
            "flat.proto",
            r#"
                syntax = "proto3";
                package demo;
                message A { string id = 1; }
                message B { string id = 1; }
            "#,
        );

        let linked = parse(&dir.0, &[file]).unwrap();
        assert_eq!(linked.root, None);
    }

    /// Ссылка типа на себя корня не отменяет: дерево с полем своего же типа —
    /// такой же корень, как и всякий другой.
    #[test]
    fn a_self_referencing_type_is_still_a_root() {
        let dir = Dir::new("root-self");
        let file = dir.write(
            "tree.proto",
            r#"
                syntax = "proto3";
                package demo;
                message Node { Node next = 1; int32 value = 2; }
            "#,
        );

        let linked = parse(&dir.0, &[file]).unwrap();
        assert_eq!(linked.root.as_deref(), Some("demo.Node"));
    }

    /// Типы, ссылающиеся друг на друга по кругу, корня не дают вовсе —
    /// и выдумывать его на ровном месте нельзя.
    #[test]
    fn mutually_referencing_types_have_no_root() {
        let dir = Dir::new("root-cycle");
        let file = dir.write(
            "cycle.proto",
            r#"
                syntax = "proto3";
                package demo;
                message A { B b = 1; }
                message B { A a = 1; }
            "#,
        );

        let linked = parse(&dir.0, &[file]).unwrap();
        assert_eq!(linked.root, None);
    }

    // --- Записи селектора отправки -----------------------------------------
    //
    // Ради них здесь и заведён отдельный список: тип сообщения в топике — это
    // конверт с `oneof` внутри, а отправляют обычно одну его ветку. Выбрать её в
    // списке, где есть только имена message, нечем.

    const ENVELOPE: &str = r#"
        syntax = "proto3";
        package demo;
        message OrderChanged { string order_id = 1; }
        message WalletChanged { string wallet_id = 1; }
        message PublicEvent {
            oneof event {
                OrderChanged order_changed = 1;
                WalletChanged wallet_changed = 7;
            }
        }
    "#;

    /// Обёртка с `oneof` даёт записи и на себя, и на каждую свою ветку.
    #[test]
    fn a_oneof_wrapper_contributes_a_choice_per_branch() {
        let dir = Dir::new("produce-choices");
        let file = dir.write("events.proto", ENVELOPE);
        let linked = parse(&dir.0, &[file]).unwrap();

        assert_eq!(
            linked.produce_choices,
            vec![
                "demo.OrderChanged",
                "demo.PublicEvent",
                "demo.PublicEvent.order_changed",
                "demo.PublicEvent.wallet_changed",
                "demo.WalletChanged",
            ]
        );
        // Список «чем читать» веток не получает: ими декодируют поле, а не тип.
        assert!(!linked.messages.iter().any(|m| m.contains("order_changed")));
    }

    /// `optional` в proto3 заводит СИНТЕТИЧЕСКИЙ `oneof` на каждое такое поле.
    /// Запись вида `Msg.optional_field` предложила бы закодировать тело типом,
    /// которого не существует, — а `produce_target` её бы отверг.
    #[test]
    fn a_proto3_optional_field_does_not_become_a_choice() {
        let dir = Dir::new("produce-choices-optional");
        let file = dir.write(
            "events.proto",
            r#"
                syntax = "proto3";
                package demo;
                message Order {
                    optional string note = 1;
                    optional int32 amount = 2;
                }
            "#,
        );
        let linked = parse(&dir.0, &[file]).unwrap();

        assert_eq!(linked.produce_choices, vec!["demo.Order"]);
        assert_eq!(linked.messages, vec!["demo.Order"]);
    }

    /// Без `oneof` записи селектора не отличаются от списка message: приписывать
    /// к ним нечего.
    #[test]
    fn a_plain_schema_has_the_same_choices_as_messages() {
        let dir = Dir::new("produce-choices-plain");
        let file = dir.write(
            "events.proto",
            r#"
                syntax = "proto3";
                package demo;
                message Point { int32 x = 1; int32 y = 2; }
                message Line { Point from = 1; Point to = 2; }
            "#,
        );
        let linked = parse(&dir.0, &[file]).unwrap();

        assert_eq!(linked.produce_choices, linked.messages);
    }

    /// Ветка разбирается в пару «обёртка и её поле» — именно её и кодируем.
    #[test]
    fn a_branch_choice_resolves_to_its_wrapper_and_field() {
        let dir = Dir::new("produce-target-branch");
        let file = dir.write("events.proto", ENVELOPE);
        let linked = parse(&dir.0, &[file]).unwrap();

        let (message, branch) = linked
            .produce_target("demo.PublicEvent.order_changed")
            .unwrap();
        assert_eq!(message.full_name(), "demo.PublicEvent");
        assert_eq!(branch.as_deref(), Some("order_changed"));

        // И сама обёртка — тоже законная запись, но уже без ветки.
        let (message, branch) = linked.produce_target("demo.PublicEvent").unwrap();
        assert_eq!(message.full_name(), "demo.PublicEvent");
        assert!(branch.is_none());
    }

    /// Вложенный тип содержит точку ровно так же, как ветка, и разбирать его на
    /// «родитель плюс поле» нельзя. Спрашиваем граф, а не режем по точке.
    #[test]
    fn a_nested_message_name_is_not_mistaken_for_a_branch() {
        let dir = Dir::new("produce-target-nested");
        let file = dir.write(
            "events.proto",
            r#"
                syntax = "proto3";
                package demo;
                message Outer {
                    message Second { int32 b = 1; }
                    Second second = 1;
                }
            "#,
        );
        let linked = parse(&dir.0, &[file]).unwrap();

        let (message, branch) = linked.produce_target("demo.Outer.Second").unwrap();
        assert_eq!(message.full_name(), "demo.Outer.Second");
        assert!(branch.is_none(), "вложенный тип — не ветка: {branch:?}");
    }

    /// Поле, которое не ветка `oneof`, кодировать «через обёртку» нельзя: иначе
    /// `demo.Order.order_id` молча закодировал бы `demo.Order` целиком.
    #[test]
    fn a_field_that_is_not_a_branch_is_refused() {
        let dir = Dir::new("produce-target-not-branch");
        let file = dir.write(
            "events.proto",
            r#"
                syntax = "proto3";
                package demo;
                message Order { string order_id = 1; }
            "#,
        );
        let linked = parse(&dir.0, &[file]).unwrap();

        let error = linked.produce_target("demo.Order.order_id").unwrap_err();
        assert!(error.contains("order_id"), "{error}");
        assert!(linked.produce_target("demo.Nope").is_err());
        assert!(linked.produce_target("no dots at all").is_err());
    }

    /// Ради этого случая и заведена вся раскладка файлов: импорт по пути
    /// обязан разрешиться одним include-каталогом.
    #[test]
    fn resolves_imports_between_added_files() {
        let dir = Dir::new("imports");
        let common = dir.write(
            "common/types.proto",
            r#"
                syntax = "proto3";
                package common;
                message Money { int64 amount = 1; }
            "#,
        );
        let root = dir.write(
            "orders.proto",
            r#"
                syntax = "proto3";
                package orders;
                import "common/types.proto";
                message Order { common.Money total = 1; }
            "#,
        );

        let linked = parse(&dir.0, &[root, common]).unwrap();
        assert_eq!(linked.messages, vec!["common.Money", "orders.Order"]);
    }

    #[test]
    fn missing_import_is_an_error_with_the_missing_name_in_it() {
        let dir = Dir::new("missing-import");
        let root = dir.write(
            "orders.proto",
            r#"
                syntax = "proto3";
                import "common/types.proto";
                message Order { int32 id = 1; }
            "#,
        );

        let error = parse(&dir.0, &[root]).unwrap_err();
        assert!(
            error.contains("common/types.proto"),
            "ошибка должна называть недостающий файл, а не быть общим словом: {error}"
        );
    }

    #[test]
    fn broken_syntax_is_reported_not_swallowed() {
        let dir = Dir::new("broken");
        let file = dir.write("bad.proto", "syntax = \"proto3\"; message { oops");
        assert!(parse(&dir.0, &[file]).is_err());
    }

    /// Вызов после «обновить» обязан увидеть новый текст, а не прошлый разбор.
    #[test]
    fn invalidate_makes_the_next_parse_see_the_new_text() {
        let dir = Dir::new("invalidate");
        let file = dir.write(
            "a.proto",
            "syntax = \"proto3\"; package p; message Before {}",
        );

        assert_eq!(
            linked(&dir.0, &[file.clone()]).unwrap().messages,
            ["p.Before"]
        );

        dir.write(
            "a.proto",
            "syntax = \"proto3\"; package p; message After {}",
        );
        // Без сброса кэша разбор остался бы прежним — именно это и проверяем.
        assert_eq!(
            linked(&dir.0, &[file.clone()]).unwrap().messages,
            ["p.Before"]
        );
        invalidate(&dir.0);
        assert_eq!(linked(&dir.0, &[file]).unwrap().messages, ["p.After"]);
    }
}
