//! Под каким именем .proto ложится в каталог схемы.
//!
//! Тонкость здесь ровно одна — имена. Корневому файлу достаточно его basename,
//! но `import "common/types.proto"` ищет файл по ЭТОМУ пути, а не по имени, и
//! плоская копия рядом с корневым файлом такой импорт не разрешит. Поэтому
//! имя внутри каталога берётся из самого импорта — см. `layout`.
//!
//! Имя внутри каталога схемы — НАШЕ собственное дело: оно не обязано повторять
//! раскладку на диске, оно обязано лишь совпасть с тем, что написано в
//! `import`. Раньше `layout` умел только подтвердить путь, который на диске уже
//! есть, и два файла, скопированные пользователем в одну папку, ложились
//! плоско — импорт между ними не разрешался. Теперь путь ещё и НАЗНАЧАЕТСЯ по
//! совпадению basename, см. проход 2.
//!
//! Сама раскладка (копирование, черновик, подмена каталога) живёт в
//! `schema::files` и про формат схемы не знает: у Avro импортов нет вовсе, и
//! ничего из этого файла ему не нужно.

use std::collections::HashSet;
use std::path::{Component, Path};

/// Файлы `google/protobuf/*.proto`, которые несёт в себе сам парсер
/// (`protobuf_parse`, `pure/parse_and_typecheck.rs`).
///
/// Список зафиксирован здесь затем, что резолверу надо знать, чего НЕ искать:
/// эти импорты разрешатся сами, а поиск по файловой системе нашёл бы в лучшем
/// случае их копию из чужого vendor-каталога. Именно список, а не префикс
/// `google/protobuf/`: `google/protobuf/compiler/plugin.proto` в парсер не
/// вшит, и молча считать его разрешённым — значит заменить внятную ошибку
/// «не нашёл файл» на невнятную «не нашёл тип».
const BUNDLED: [&str; 11] = [
    "google/protobuf/any.proto",
    "google/protobuf/api.proto",
    "google/protobuf/descriptor.proto",
    "google/protobuf/duration.proto",
    "google/protobuf/empty.proto",
    "google/protobuf/field_mask.proto",
    "google/protobuf/source_context.proto",
    "google/protobuf/struct.proto",
    "google/protobuf/timestamp.proto",
    "google/protobuf/type.proto",
    "google/protobuf/wrappers.proto",
];

/// Разрешается ли импорт сам, без нашего участия.
pub fn is_bundled(import: &str) -> bool {
    BUNDLED.contains(&import)
}

/// Годен ли путь импорта к тому, чтобы по нему что-то искать и куда-то класть.
///
/// Путь приходит из текста схемы, то есть из данных: `..` в нём не должен
/// превращаться ни в чтение, ни в запись куда угодно по файловой системе.
/// Запись от этого страхует ещё и `files::is_safe_name`, но отбить такое надо
/// раньше и своими словами — иначе пользователь увидит отказ записи вместо
/// внятного «в импорте недопустимый путь».
pub fn is_safe_import(import: &str) -> bool {
    !import.is_empty()
        && !import.contains('\\')
        && Path::new(import)
            .components()
            .all(|c| matches!(c, Component::Normal(_)))
}

/// Собирает пути из `import "...";`.
///
/// Сначала настоящим парсером — `protobuf_parse::pure::parse_dependencies`
/// разбирает файл по модели и отдаёт готовый список зависимостей, вместе с
/// одинарными кавычками, переносами строк и `import public`. Построчная
/// эвристика осталась запасным путём ровно для одного случая: файл не
/// разбирается вовсе. Тогда точного списка импортов не существует, а
/// раскладке всё равно надо что-то назначить — настоящую же ошибку скажет
/// `linked::parse` сразу после.
fn imports_of(text: &str) -> Vec<String> {
    if let Ok(parsed) = protobuf_parse::pure::parse_dependencies(text) {
        return parsed.dependency;
    }
    imports_roughly(text)
}

/// Запасной разбор импортов — грубый, без лексера.
///
/// По объявлениям, а не по строкам: `syntax = "proto3"; import "x.proto";`
/// одной строкой — совершенно законный .proto, и построчный разбор такой
/// импорт терял. Строчные комментарии срезаются заранее, иначе
/// `// import "x";` уехал бы в список наравне с настоящим.
fn imports_roughly(text: &str) -> Vec<String> {
    let mut cleaned = String::with_capacity(text.len());
    for line in text.lines() {
        let code = match line.find("//") {
            Some(at) => &line[..at],
            None => line,
        };
        cleaned.push_str(code);
        cleaned.push('\n');
    }

    cleaned
        .split(';')
        .filter_map(|statement| {
            let rest = statement.trim_start().strip_prefix("import")?;
            // `import` как начало другого идентификатора — не импорт.
            if !rest.starts_with(|c: char| c.is_whitespace()) {
                return None;
            }
            // Кавычки бывают обе: protobuf принимает и `"x.proto"`, и
            // `'x.proto'`.
            let quote = rest.find(['"', '\''])?;
            let mark = rest[quote..].chars().next()?;
            let open = quote + 1;
            let close = rest[open..].find(mark)? + open;
            Some(rest[open..close].to_string())
        })
        .collect()
}

/// Является ли `suffix` хвостом `path` по компонентам пути.
///
/// Именно по компонентам, а не по символам: `types.proto` не должен считаться
/// хвостом `.../common_types.proto`. Отдаёт длину совпадения в компонентах —
/// по ней `layout` выбирает между «подходит» и «подходит конкретнее».
fn tail_match(path: &Path, suffix: &str) -> Option<usize> {
    let wanted: Vec<&str> = suffix.split('/').filter(|s| !s.is_empty()).collect();
    if wanted.is_empty() {
        return None;
    }
    let have: Vec<&str> = components_of(path);
    let matches = have.len() >= wanted.len() && have[have.len() - wanted.len()..] == wanted[..];
    matches.then_some(wanted.len())
}

fn components_of(path: &Path) -> Vec<&str> {
    path.components()
        .filter_map(|c| match c {
            Component::Normal(s) => s.to_str(),
            _ => None,
        })
        .collect()
}

fn basename(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

/// Под какими именами файлы лягут в каталог схемы.
///
/// Считается для ВСЕГО набора сразу, а не для каждого файла по отдельности, и
/// это принципиально с двух сторон. Во-первых, имя зависит от импортов, а те
/// объявлены в соседях: пока `layout` звали только для новых файлов, уже
/// лежавший плоско `common_proto.proto` так и оставался плоским, даже когда
/// импорт на него наконец появлялся в наборе, — и схема не собиралась. Во-
/// вторых, один импорт не может достаться двум файлам, а перебор по одному
/// файлу за раз этого не видит.
///
/// Проходов три, от самого надёжного к самому догадливому:
///
/// 1. Хвост пути файла совпал с путём импорта. Так лежат файлы, взятые из
///    проекта как есть, и это единственное совпадение, в котором есть
///    доказательство: путь подтверждён диском.
/// 2. Совпал только basename, и совпал однозначно — ровно один неразобранный
///    файл с таким именем на ровно один незанятый импорт. Так выглядят файлы,
///    скопированные в одну папку: пути на диске больше нет, но и выбирать не
///    из чего.
/// 3. Ничего не совпало — basename. Это корневой файл, которого никто не
///    импортирует, либо файл, которому импорта и правда не нашлось; во втором
///    случае про это скажет разбор.
///
/// Возвращает по имени на каждый источник, в том же порядке.
pub fn layout(sources: &[&Path], imports: &HashSet<String>) -> Vec<String> {
    let mut names: Vec<Option<String>> = vec![None; sources.len()];
    let mut claimed: HashSet<&str> = HashSet::new();

    // Проход 1. Кандидаты сортируются по длине совпадения, чтобы `a/b/c.proto`
    // выигрывал у `c.proto`, а порядок был воспроизводимым: перебор по
    // `HashSet` сам по себе не упорядочен, и без сортировки раскладка
    // отличалась бы от запуска к запуску.
    let mut candidates: Vec<(usize, usize, &str)> = Vec::new();
    for (index, source) in sources.iter().enumerate() {
        for import in imports {
            if !is_safe_import(import) {
                continue;
            }
            if let Some(depth) = tail_match(source, import) {
                candidates.push((depth, index, import.as_str()));
            }
        }
    }
    candidates.sort_by(|a, b| b.0.cmp(&a.0).then(a.2.cmp(b.2)).then(a.1.cmp(&b.1)));
    for (_, index, import) in candidates {
        if names[index].is_some() || claimed.contains(import) {
            continue;
        }
        names[index] = Some(import.to_string());
        claimed.insert(import);
    }

    // Проход 2. Однозначность проверяется с ОБЕИХ сторон: два файла `types.proto`
    // на один импорт `a/v1/types.proto` — это не «оба туда», а «непонятно
    // который», и догадываться тут нельзя.
    let free: Vec<&str> = {
        let mut free: Vec<&str> = imports
            .iter()
            .map(String::as_str)
            .filter(|import| !claimed.contains(*import) && is_safe_import(import))
            .collect();
        free.sort_unstable();
        free
    };
    for (index, source) in sources.iter().enumerate() {
        if names[index].is_some() {
            continue;
        }
        let Some(own) = source.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        let mut matching = free
            .iter()
            .filter(|import| basename(import) == own && !claimed.contains(**import));
        let Some(import) = matching.next() else {
            continue;
        };
        if matching.next().is_some() {
            continue;
        }
        // И с другой стороны: не претендует ли на тот же импорт ещё кто-то.
        let rivals = sources
            .iter()
            .enumerate()
            .filter(|(other, path)| {
                names[*other].is_none() && path.file_name().and_then(|n| n.to_str()) == Some(own)
            })
            .count();
        if rivals > 1 {
            continue;
        }
        names[index] = Some((*import).to_string());
        claimed.insert(import);
    }

    // Проход 3.
    names
        .into_iter()
        .zip(sources)
        .map(|(name, source)| {
            name.unwrap_or_else(|| {
                source
                    .file_name()
                    .map(|n| n.to_string_lossy().into_owned())
                    .unwrap_or_else(|| "schema.proto".to_string())
            })
        })
        .collect()
}

/// Все импорты, объявленные в наборе файлов.
pub fn imports_in<'a>(files: impl IntoIterator<Item = &'a [u8]>) -> HashSet<String> {
    let mut out = HashSet::new();
    for bytes in files {
        // Невалидный UTF-8 в .proto — это уже ошибка, но не наша: пусть о ней
        // скажет парсер, а раскладке просто нечего отсюда взять.
        if let Ok(text) = std::str::from_utf8(bytes) {
            out.extend(imports_of(text));
        }
    }
    out
}

/// Импорты одного файла — в том же виде, в каком их видит `imports_in`.
pub fn imports_of_bytes(bytes: &[u8]) -> Vec<String> {
    match std::str::from_utf8(bytes) {
        Ok(text) => imports_of(text),
        Err(_) => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn set(items: &[&str]) -> HashSet<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    fn name_of(source: &str, imports: &HashSet<String>) -> String {
        let path = PathBuf::from(source);
        layout(&[path.as_path()], imports).remove(0)
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
        let found: HashSet<String> = imports_of(text).into_iter().collect();
        assert!(found.contains("common/types.proto"));
        assert!(found.contains("other.proto"));
        assert!(found.contains("weak.proto"));
        // Закомментированный импорт — не импорт.
        assert_eq!(found.len(), 3, "нашлось лишнее: {found:?}");
    }

    /// Одинарные кавычки protobuf принимает, и построчная эвристика их раньше
    /// не видела. Теперь список импортов даёт настоящий парсер.
    #[test]
    fn single_quoted_imports_are_found_too() {
        let found = imports_of("syntax = 'proto3'; import 'common/types.proto';");
        assert_eq!(found, vec!["common/types.proto"]);
    }

    /// На неразбираемом файле настоящий парсер спотыкается — и тогда работает
    /// запасной проход по строкам.
    #[test]
    fn imports_are_still_found_in_a_file_that_does_not_parse() {
        let found = imports_of("import \"common/types.proto\";\nmessage { oops");
        assert_eq!(found, vec!["common/types.proto"]);
    }

    #[test]
    fn a_word_starting_with_import_is_not_an_import() {
        assert!(imports_roughly("important = \"x.proto\";").is_empty());
    }

    /// Запасной разбор обязан видеть импорт и там, где всё объявлено одной
    /// строкой: для парсера это законный .proto, а для построчного разбора —
    /// строка, начинающаяся с `syntax`.
    #[test]
    fn the_rough_pass_reads_statements_not_lines() {
        let found = imports_roughly("syntax = \"proto3\"; import \"a/b.proto\"; message M {}");
        assert_eq!(found, vec!["a/b.proto"]);
    }

    #[test]
    fn the_rough_pass_ignores_commented_out_imports() {
        let found = imports_roughly("// import \"no.proto\";\nimport \"yes.proto\";");
        assert_eq!(found, vec!["yes.proto"]);
    }

    /// Ради этого раскладка и существует: файл, который импортируют по пути,
    /// обязан лечь именно по этому пути.
    #[test]
    fn imported_file_keeps_the_path_it_is_imported_by() {
        let imports = set(&["common/types.proto"]);
        let name = name_of("/home/u/work/schemas/common/types.proto", &imports);
        assert_eq!(name, "common/types.proto");
    }

    #[test]
    fn root_file_gets_its_basename() {
        let imports = set(&["common/types.proto"]);
        let name = name_of("/home/u/work/schemas/events.proto", &imports);
        assert_eq!(name, "events.proto");
    }

    #[test]
    fn suffix_match_respects_path_components() {
        // `types.proto` — не хвост `common_types.proto`.
        let imports = set(&["types.proto"]);
        let name = name_of("/x/common_types.proto", &imports);
        assert_eq!(name, "common_types.proto");
    }

    #[test]
    fn the_most_specific_import_wins() {
        let imports = set(&["types.proto", "common/types.proto"]);
        let name = name_of("/x/common/types.proto", &imports);
        assert_eq!(name, "common/types.proto");
    }

    /// Случай, из-за которого правка и заводилась: файлы скопированы в одну
    /// папку, пути импорта на диске больше нет — и назначить его можно только
    /// по имени файла.
    #[test]
    fn a_flat_copy_still_gets_the_path_of_its_import() {
        let imports = set(&[
            "google/protobuf/timestamp.proto",
            "internal/common_proto/common_proto.proto",
        ]);
        let ode = PathBuf::from("/scratch/ode_events.proto");
        let common = PathBuf::from("/scratch/common_proto.proto");
        let names = layout(&[ode.as_path(), common.as_path()], &imports);
        assert_eq!(
            names,
            vec![
                "ode_events.proto",
                "internal/common_proto/common_proto.proto"
            ]
        );
    }

    /// Догадка по basename работает только там, где догадываться не о чем.
    #[test]
    fn an_ambiguous_basename_is_left_alone() {
        let imports = set(&["a/v1/types.proto", "b/v1/types.proto"]);
        let one = PathBuf::from("/scratch/types.proto");
        assert_eq!(layout(&[one.as_path()], &imports), vec!["types.proto"]);

        // И с другой стороны: два одноимённых файла на один импорт.
        let imports = set(&["a/v1/types.proto"]);
        let x = PathBuf::from("/x/types.proto");
        let y = PathBuf::from("/y/types.proto");
        assert_eq!(
            layout(&[x.as_path(), y.as_path()], &imports),
            vec!["types.proto", "types.proto"]
        );
    }

    /// Один импорт — одному файлу. Подтверждённое диском совпадение сильнее
    /// догадки по имени.
    #[test]
    fn a_claimed_import_is_not_given_away_twice() {
        let imports = set(&["common/types.proto"]);
        let real = PathBuf::from("/proj/common/types.proto");
        let copy = PathBuf::from("/scratch/types.proto");
        let names = layout(&[copy.as_path(), real.as_path()], &imports);
        assert_eq!(names, vec!["types.proto", "common/types.proto"]);
    }

    #[test]
    fn imports_that_escape_the_schema_directory_are_refused() {
        assert!(!is_safe_import("../outside.proto"));
        assert!(!is_safe_import("/etc/passwd"));
        assert!(!is_safe_import(""));
        assert!(!is_safe_import("a\\b.proto"));
        assert!(is_safe_import("common/types.proto"));
    }

    /// Назначать путь из небезопасного импорта нельзя — даже если basename
    /// совпал.
    #[test]
    fn an_unsafe_import_is_never_assigned_as_a_name() {
        let imports = set(&["../../etc/types.proto"]);
        let name = name_of("/scratch/types.proto", &imports);
        assert_eq!(name, "types.proto");
    }

    #[test]
    fn bundled_well_known_types_are_recognized() {
        assert!(is_bundled("google/protobuf/timestamp.proto"));
        assert!(!is_bundled("google/protobuf/compiler/plugin.proto"));
        assert!(!is_bundled("google/api/annotations.proto"));
    }
}
