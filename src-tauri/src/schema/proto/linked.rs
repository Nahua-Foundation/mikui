//! Разбор каталога .proto в descriptor-граф.
//!
//! Парсер — чисто растовый (`protobuf_parse::Parser::pure`): внешнего `protoc`
//! на машине пользователя нет и требовать его нельзя. Импорты разрешаются одним
//! include-каталогом — самим каталогом схемы, куда файлы уложены под теми
//! именами, под которыми их импортируют (см. `imports::layout`).

use std::path::Path;
use std::sync::{Arc, Mutex};

use protobuf::descriptor::DescriptorProto;
use protobuf::reflect::{FileDescriptor, MessageDescriptor};

use crate::schema::types::SchemaFile;

/// Разобранная схема: связанный граф файлов и список message на выбор.
#[derive(Debug)]
pub struct Linked {
    files: Vec<FileDescriptor>,
    /// Полные имена message, объявленных в файлах ПОЛЬЗОВАТЕЛЯ, по алфавиту.
    /// Транзитивные зависимости сюда не попадают: `google.protobuf.Timestamp`
    /// в выпадающем списке — мусор, декодировать им никто не собирается.
    pub messages: Vec<String>,
}

impl Linked {
    /// Ищет message по полному имени. Ведущая точка необязательна: снаружи имя
    /// приходит и из выпадающего списка (без точки), и из файла настроек.
    pub fn message(&self, full_name: &str) -> Option<MessageDescriptor> {
        // `message_by_full_name` требует именно `.pkg.Name` и паникует на
        // имени без точки — дописываем её здесь, а не оставляем это вызывающим.
        let dotted = match full_name.starts_with('.') {
            true => full_name.to_string(),
            false => format!(".{full_name}"),
        };
        self.files
            .iter()
            .find_map(|f| f.message_by_full_name(&dotted))
    }
}

/// Разобранные схемы, ключ — имя каталога.
///
/// Разбор стоит миллисекунды, а зовут его на каждое переключение топика и на
/// каждое открытие настроек. `Mutex<Vec<..>>` вместо `HashMap`: записей единицы,
/// зато `Vec::new()` — const, и статику не нужен ни `LazyLock`, ни лишний крейт.
static CACHE: Mutex<Vec<(String, Arc<Linked>)>> = Mutex::new(Vec::new());

/// Сколько разобранных схем держать. Пользователь ходит по нескольким топикам,
/// а не по сотне; дальше это была бы просто удерживаемая память.
const CACHE_LIMIT: usize = 8;

/// Разбирает каталог, переиспользуя прошлый результат, если он ещё годен.
pub fn linked(dir: &Path, files: &[SchemaFile]) -> Result<Arc<Linked>, String> {
    let key = cache_key(dir);
    if let Some(hit) = cached(&key) {
        return Ok(hit);
    }
    let built = Arc::new(parse(dir, files)?);
    remember(key, Arc::clone(&built));
    Ok(built)
}

/// Разбирает каталог, не заглядывая в кэш и не записывая в него.
///
/// Нужно ровно там, где результат ещё не имеет права стать общим: файлы лежат
/// во временном каталоге и, если разбор не удастся, будут снесены.
pub fn parse(dir: &Path, files: &[SchemaFile]) -> Result<Linked, String> {
    // Входные файлы — только выбранные пользователем. Подтянутые по импорту
    // лежат в том же каталоге и разрешаются как зависимости, но входными не
    // становятся: `mine` ниже считается по входным, а типы зависимостей в
    // списке «чем декодировать» — такой же мусор, как `google.protobuf.Any`.
    let inputs: Vec<&SchemaFile> = files.iter().filter(|f| !f.auto).collect();
    if inputs.is_empty() {
        return Err("no .proto files".to_string());
    }

    let mut parser = protobuf_parse::Parser::new();
    parser
        .pure()
        .include(dir)
        .inputs(inputs.iter().map(|f| dir.join(&f.name)));

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

    Ok(Linked {
        files: built,
        messages,
    })
}

/// Выбрасывает разобранную схему из кэша. Зовётся всякий раз, когда каталог
/// переписан: иначе после «обновить» декодировали бы прежней версией.
pub fn invalidate(dir: &Path) {
    let key = cache_key(dir);
    if let Ok(mut cache) = CACHE.lock() {
        cache.retain(|(k, _)| *k != key);
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

fn cache_key(dir: &Path) -> String {
    dir.to_string_lossy().into_owned()
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
