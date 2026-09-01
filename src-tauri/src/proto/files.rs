//! Раскладка .proto по каталогу схемы.
//!
//! Файлы копируются к себе, а не читаются по исходным путям: схема обязана
//! пережить и перезапуск приложения, и переезд каталога, из которого её взяли.
//! Исходный путь при этом запоминается — по нему работает «перечитать с диска».
//!
//! Тонкость здесь ровно одна — имена. Корневому файлу достаточно его basename,
//! но `import "common/types.proto"` ищет файл по ЭТОМУ пути, а не по имени, и
//! плоская копия рядом с корневым файлом такой импорт не разрешит. Поэтому
//! имя внутри каталога берётся из самого импорта — см. `layout`.

use std::collections::HashSet;
use std::path::{Component, Path, PathBuf};

/// Файл, готовый лечь в каталог схемы.
pub struct Pending {
    pub name: String,
    pub source: String,
    pub bytes: Vec<u8>,
}

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

/// Отвергает имена, которые увели бы запись за пределы каталога схемы.
///
/// Путь берётся из текста .proto, то есть из данных, а не из кода: `..` в нём
/// не должен превращаться в запись куда угодно по файловой системе.
fn is_safe_name(name: &str) -> bool {
    !name.is_empty()
        && Path::new(name).components().all(|c| matches!(c, Component::Normal(_)))
}

/// Заново раскладывает каталог схемы под указанный набор файлов.
///
/// Каталог собирается рядом и подменяется целиком, и только у вызывающего есть
/// право оставить его: `commit` вызывается ПОСЛЕ успешного разбора. Битый
/// .proto не должен ни сохраниться сам, ни испортить схему, которая работала.
pub struct Staged {
    dir: PathBuf,
    target: PathBuf,
    committed: bool,
}

impl Staged {
    /// Пишет файлы во временный каталог рядом с `target`.
    pub fn write(target: PathBuf, files: &[Pending]) -> Result<Self, String> {
        let dir = staging_path(&target);
        // Хвост от прерванной попытки: он нам не наследство, а помеха.
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir)
            .map_err(|e| format!("can't create {}: {e}", dir.display()))?;

        let staged = Self {
            dir,
            target,
            committed: false,
        };

        for file in files {
            if !is_safe_name(&file.name) {
                return Err(format!("unsafe proto file name: {}", file.name));
            }
            let path = staged.dir.join(&file.name);
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)
                    .map_err(|e| format!("can't create {}: {e}", parent.display()))?;
            }
            std::fs::write(&path, &file.bytes)
                .map_err(|e| format!("can't write {}: {e}", path.display()))?;
        }
        Ok(staged)
    }

    pub fn path(&self) -> &Path {
        &self.dir
    }

    /// Ставит собранный каталог на место рабочего.
    pub fn commit(mut self) -> Result<(), String> {
        // rename поверх непустого каталога не работает — сносим старый. Окно,
        // в котором схемы нет на диске, здесь неизбежно, но данные для новой
        // уже целиком лежат рядом и записаны.
        let _ = std::fs::remove_dir_all(&self.target);
        if let Some(parent) = self.target.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("can't create {}: {e}", parent.display()))?;
        }
        std::fs::rename(&self.dir, &self.target)
            .map_err(|e| format!("can't replace {}: {e}", self.target.display()))?;
        self.committed = true;
        Ok(())
    }
}

impl Drop for Staged {
    fn drop(&mut self) {
        if !self.committed {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }
}

fn staging_path(target: &Path) -> PathBuf {
    let mut name = target
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| "schema".to_string());
    name.push_str(".staging");
    target.with_file_name(name)
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
        let name = layout(Path::new("/home/u/work/schemas/common/types.proto"), &imports);
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

    #[test]
    fn names_escaping_the_schema_directory_are_rejected() {
        assert!(!is_safe_name("../outside.proto"));
        assert!(!is_safe_name("/etc/passwd"));
        assert!(!is_safe_name(""));
        assert!(is_safe_name("common/types.proto"));
        assert!(is_safe_name("events.proto"));
    }
}
