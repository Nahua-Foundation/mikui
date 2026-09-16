//! Под каким именем .proto ложится в каталог схемы.
//!
//! Тонкость здесь ровно одна — имена. Корневому файлу достаточно его basename,
//! но `import "common/types.proto"` ищет файл по ЭТОМУ пути, а не по имени, и
//! плоская копия рядом с корневым файлом такой импорт не разрешит. Поэтому
//! имя внутри каталога берётся из самого импорта — см. `layout`.
//!
//! Сама раскладка (копирование, черновик, подмена каталога) живёт в
//! `schema::files` и про формат схемы не знает: у Avro импортов нет вовсе, и
//! ничего из этого файла ему не нужно.

use std::collections::HashSet;
use std::path::{Component, Path};

/// Собирает пути из `import "...";`.
///
/// Разбор нарочно грубый — по строкам, без лексера. Он не обязан быть точным:
/// это лишь подсказка для раскладки, а настоящую проверку синтаксиса делает
/// `protobuf-parse` сразу после, и его ошибка и уедет пользователю.
fn imports_of(text: &str) -> impl Iterator<Item = &str> {
    text.lines().filter_map(|line| {
        let line = line.trim_start();
        let rest = line.strip_prefix("import")?;
        // `import` как начало другого идентификатора — не импорт.
        if !rest.starts_with(|c: char| c.is_whitespace()) {
            return None;
        }
        let open = rest.find('"')? + 1;
        let close = rest[open..].find('"')? + open;
        Some(&rest[open..close])
    })
}

/// Является ли `suffix` хвостом `path` по компонентам пути.
///
/// Именно по компонентам, а не по символам: `types.proto` не должен считаться
/// хвостом `.../common_types.proto`.
fn ends_with_path(path: &Path, suffix: &str) -> bool {
    let wanted: Vec<&str> = suffix.split('/').filter(|s| !s.is_empty()).collect();
    if wanted.is_empty() {
        return false;
    }
    let have: Vec<&str> = path
        .components()
        .filter_map(|c| match c {
            Component::Normal(s) => s.to_str(),
            _ => None,
        })
        .collect();
    have.len() >= wanted.len() && have[have.len() - wanted.len()..] == wanted[..]
}

/// Под каким именем файл ляжет в каталог схемы.
///
/// Если кто-то из уже известных импортов указывает ровно на этот файл, берём
/// путь импорта целиком — тогда импорт разрешится по одному include-каталогу.
/// Иначе это корневой файл, и хватит его basename.
pub fn layout(source: &Path, imports: &HashSet<String>) -> String {
    // Самый длинный подходящий: `a/b/c.proto` конкретнее, чем `c.proto`.
    let best = imports
        .iter()
        .filter(|imp| ends_with_path(source, imp))
        .max_by_key(|imp| imp.len());

    match best {
        Some(imp) => imp.clone(),
        None => source
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| "schema.proto".to_string()),
    }
}

/// Все импорты, объявленные в наборе файлов.
pub fn imports_in<'a>(files: impl IntoIterator<Item = &'a [u8]>) -> HashSet<String> {
    let mut out = HashSet::new();
    for bytes in files {
        // Невалидный UTF-8 в .proto — это уже ошибка, но не наша: пусть о ней
        // скажет парсер, а раскладке просто нечего отсюда взять.
        if let Ok(text) = std::str::from_utf8(bytes) {
            out.extend(imports_of(text).map(str::to_owned));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set(items: &[&str]) -> HashSet<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn finds_imports_in_all_their_spellings() {
        let text = r#"
            syntax = "proto3";
            package a;
            import "common/types.proto";
            import public "other.proto";
            import weak  "weak.proto";
            // import "commented.proto";
            message M {}
        "#;
        let found: HashSet<&str> = imports_of(text).collect();
        assert!(found.contains("common/types.proto"));
        assert!(found.contains("other.proto"));
        assert!(found.contains("weak.proto"));
        // Закомментированный импорт начинается с `//`, а не с `import`, —
        // отсекается тем же условием, что и любая другая строка.
        assert_eq!(found.len(), 3, "нашлось лишнее: {found:?}");
    }

    #[test]
    fn a_word_starting_with_import_is_not_an_import() {
        let found: Vec<&str> = imports_of("important = \"x.proto\";").collect();
        assert!(found.is_empty());
    }

    /// Ради этого раскладка и существует: файл, который импортируют по пути,
    /// обязан лечь именно по этому пути.
    #[test]
    fn imported_file_keeps_the_path_it_is_imported_by() {
        let imports = set(&["common/types.proto"]);
        let name = layout(
            Path::new("/home/u/work/schemas/common/types.proto"),
            &imports,
        );
        assert_eq!(name, "common/types.proto");
    }

    #[test]
    fn root_file_gets_its_basename() {
        let imports = set(&["common/types.proto"]);
        let name = layout(Path::new("/home/u/work/schemas/events.proto"), &imports);
        assert_eq!(name, "events.proto");
    }

    #[test]
    fn suffix_match_respects_path_components() {
        // `types.proto` — не хвост `common_types.proto`.
        let imports = set(&["types.proto"]);
        let name = layout(Path::new("/x/common_types.proto"), &imports);
        assert_eq!(name, "common_types.proto");
    }

    #[test]
    fn the_most_specific_import_wins() {
        let imports = set(&["types.proto", "common/types.proto"]);
        let name = layout(Path::new("/x/common/types.proto"), &imports);
        assert_eq!(name, "common/types.proto");
    }
}
