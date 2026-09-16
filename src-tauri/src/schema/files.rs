//! Раскладка файлов схемы по её каталогу.
//!
//! Файлы копируются к себе, а не читаются по исходным путям: схема обязана
//! пережить и перезапуск приложения, и переезд каталога, из которого её взяли.
//! Исходный путь при этом запоминается — по нему работает «перечитать с диска».
//!
//! Про формат схемы здесь не знают ничего: и `.proto`, и `.avsc` — это набор
//! именованных файлов, которые надо положить рядом и целиком заменить, если
//! новый набор разобрался. Чем именно они разбираются, решают
//! `proto::linked` и `avro::schema`, а КАК зовётся файл внутри каталога —
//! `proto::imports::layout` (у protobuf имя диктует `import`) и
//! `avro::store` (у Avro импортов нет, хватает basename).

use std::path::{Component, Path, PathBuf};

/// Файл, готовый лечь в каталог схемы.
pub struct Pending {
    pub name: String,
    pub source: String,
    pub bytes: Vec<u8>,
}

/// Отвергает имена, которые увели бы запись за пределы каталога схемы.
///
/// Путь берётся из текста схемы, то есть из данных, а не из кода: `..` в нём
/// не должен превращаться в запись куда угодно по файловой системе.
fn is_safe_name(name: &str) -> bool {
    !name.is_empty()
        && Path::new(name)
            .components()
            .all(|c| matches!(c, Component::Normal(_)))
}

/// Заново раскладывает каталог схемы под указанный набор файлов.
///
/// Каталог собирается рядом и подменяется целиком, и только у вызывающего есть
/// право оставить его: `commit` вызывается ПОСЛЕ успешного разбора. Битый
/// файл не должен ни сохраниться сам, ни испортить схему, которая работала.
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
                return Err(format!("unsafe schema file name: {}", file.name));
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

    #[test]
    fn names_escaping_the_schema_directory_are_rejected() {
        assert!(!is_safe_name("../outside.proto"));
        assert!(!is_safe_name("/etc/passwd"));
        assert!(!is_safe_name(""));
        assert!(is_safe_name("common/types.proto"));
        assert!(is_safe_name("events.proto"));
    }
}
