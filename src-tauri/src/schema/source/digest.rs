//! Слепок дерева: во что оно делится и что в этих наборах объявлено.
//!
//! Зачем он есть. Чтобы показать библиотеку, надо назвать наборы, их корневой
//! тип и число объявленных типов, — а это ответ не про файлы, а про их
//! СОДЕРЖИМОЕ: входные файлы ищутся по импортам (`tree::entries` читает каждый
//! .proto области), корень считается по разобранному графу. На библиотеке
//! владельца это 232 файла и 96 наборов, то есть полторы секунды в отладочной
//! сборке, и платились они на КАЖДОЕ открытие настроек топика.
//!
//! Кэш разбора (`linked::linked_set`) тут не помогал и помочь не мог: он держит
//! восемь последних графов — столько, сколько стоит держать разобранными, —
//! а наборов девяносто шесть, и обход вымывал его подчистую. Заодно и у
//! открытого топика: кэш общий, и схема, которой таблица прямо сейчас читает
//! тела, вылетала из него при каждом взгляде на библиотеку.
//!
//! Поэтому хранится не промежуточный разбор, а ГОТОВЫЙ ОТВЕТ — четыре коротких
//! поля на набор вместо descriptor-графа. Девяносто шесть таких записей ничего
//! не весят и переживают перезапуск, так что первое открытие после запуска
//! стоит столько же, сколько десятое.
//!
//! Когда слепок врёт. Содержимое дерева меняется ровно одним способом —
//! `store::ingest` подменяет каталог целиком, — и каждая такая подмена
//! переписывает у записи источника и `files`, и `fetched_at`. Третье, от чего
//! зависит деление на наборы, — `subdir`: он сужает поиск контрактов, и его
//! меняют без похода за деревом. Эти три поля и составляют метку (`Stamp`);
//! несовпадение означает пересчёт. Обходить дерево ради проверки не нужно
//! вовсе — ни одного `stat` по файлам.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use super::types::{SchemaSource, SetView};

const DIGEST_FILE: &str = "proto-sets.json";

/// Чем помечено дерево, по которому слепок посчитан. См. шапку модуля.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) struct Stamp {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    fetched_at: Option<String>,
    #[serde(default)]
    files: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    subdir: Option<String>,
}

impl Stamp {
    pub(super) fn of(source: &SchemaSource) -> Self {
        Self {
            fetched_at: source.fetched_at.clone(),
            files: source.files,
            subdir: source.subdir.clone(),
        }
    }
}

/// Слепок одного источника.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) struct Entry {
    pub(super) stamp: Stamp,
    pub(super) sets: Vec<SetView>,
    /// Беда источника целиком — пустое дерево, пустая область, круг импортов.
    /// Хранится вместе с наборами: она тоже посчитана по содержимому.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) error: Option<String>,
}

/// Слепки всех источников, ключ — `SchemaSource::id`.
///
/// `BTreeMap` ради самого файла: порядок ключей постоянный, и подряд идущие
/// записи отличаются только тем, что и правда изменилось.
pub(super) type Digest = BTreeMap<String, Entry>;

fn path(root: &Path) -> PathBuf {
    root.join(DIGEST_FILE)
}

/// Читает слепки. Нет файла, не читается, не разбирается — пусто.
///
/// Намеренно НЕ через `config::read_json`, хотя рядом весь остальной JSON
/// читается им: тот отодвигает непрочитанное в `<имя>.bad` и возвращает ошибку,
/// чтобы можно было посмотреть глазами. Здесь смотреть не на что — это
/// производные данные, восстановимые пересчётом, и ни оставлять от них обломки,
/// ни тем более ронять из-за них показ библиотеки незачем.
pub(super) fn read(root: &Path) -> Digest {
    let Ok(bytes) = std::fs::read(path(root)) else {
        return Digest::new();
    };
    serde_json::from_slice(&bytes).unwrap_or_default()
}

/// Записывает слепки. Неудача ничего не ломает: следующий показ библиотеки
/// просто пересчитает их заново, — но молчать о ней не стоит, иначе
/// неписуемый каталог настроек выглядел бы как «почему-то медленно».
pub(super) fn write(root: &Path, digest: &Digest) {
    let bytes = match serde_json::to_vec_pretty(digest) {
        Ok(bytes) => bytes,
        Err(e) => {
            eprintln!("can't serialize the .proto set digest: {e}");
            return;
        }
    };
    if let Err(e) = crate::config::write_atomic(&path(root), &bytes) {
        eprintln!("can't save the .proto set digest: {e}");
    }
}
