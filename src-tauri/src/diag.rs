//! Куда девается паника.
//!
//! Без этого модуля паника уходила в stderr, которого у пользователя нет: в
//! собранном бандле поток не подключён ни к какому терминалу. На экран при
//! этом приезжало обезличенное «kafka worker is gone», и разобраться по нему
//! было нельзя ни в одном из случаев — ровно так неделю искали порчу арены в
//! `MessageStore::truncate`.
//!
//! Поэтому паника ещё и записывается в файл: его можно прислать целиком, и в
//! нём будет всё, чего не хватало, — где именно упало и как туда пришли.
//!
//! Сам журнал пишется по-английски, в отличие от комментариев здесь. Его
//! читает не только тот, кто его завёл: файл уезжает в переписку, в тикет и в
//! чужие руки, а сообщения самих паник приходят из стандартной библиотеки и
//! переводу всё равно не поддаются.

use std::backtrace::Backtrace;
use std::fmt::Write as _;
use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

/// Подкаталог настроек, в котором живут падения.
///
/// Отдельной папкой, а не вперемешку с `clusters.json`: файлов тут по одному
/// на панику, и без папки они за пару неудачных дней похоронили бы настройки,
/// за которыми в этот каталог и ходят. Заодно её удобно прислать целиком.
const CRASH_DIR: &str = "crashes";
/// Начало имени файла. По нему же они и подчищаются — чтобы не тронуть ничего
/// чужого, что вдруг окажется в той же папке.
const FILE_PREFIX: &str = "panic-";
const FILE_SUFFIX: &str = ".log";
/// Сколько последних падений хранить. Трейс — единицы килобайт, так что предел
/// здесь не про место, а про то, чтобы папка оставалась обозримой.
const KEEP_FILES: usize = 20;

static DIR: OnceLock<PathBuf> = OnceLock::new();

/// Папка с падениями. `None` — обработчик ещё не поставлен.
pub fn crash_dir() -> Option<&'static Path> {
    DIR.get().map(PathBuf::as_path)
}

/// Самое свежее падение.
///
/// Имена начинаются с времени в сортируемом виде, поэтому «самое свежее» — это
/// просто наибольшее имя, без обращения к временам файловой системы (те ещё и
/// врут после копирования папки).
pub fn latest_crash() -> Option<PathBuf> {
    files().pop()
}

/// Все файлы падений, от старых к новым.
fn files() -> Vec<PathBuf> {
    crash_dir().map(files_in).unwrap_or_default()
}

/// То же по заданному каталогу — чтобы это можно было проверить тестом, не
/// занимая глобальный `DIR` (он `OnceLock`, а тесты идут в одном процессе).
fn files_in(dir: &Path) -> Vec<PathBuf> {
    let Ok(entries) = fs::read_dir(dir) else {
        return Vec::new();
    };

    let mut found: Vec<PathBuf> = entries
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|path| {
            path.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with(FILE_PREFIX) && n.ends_with(FILE_SUFFIX))
        })
        .collect();
    found.sort();
    found
}

/// Ставит обработчик паники, пишущий в `<dir>/crashes/panic-<время>.log`.
///
/// Прежний обработчик не заменяется, а вызывается тоже: в отладочной сборке
/// паника обязана по-прежнему оказаться в терминале, и терять её там ради
/// файла было бы обменом в худшую сторону.
///
/// Вызывать один раз. Повторный вызов ничего не делает — иначе обработчики
/// сложились бы в цепочку и одна паника записалась бы несколько раз.
pub fn install(dir: &Path) {
    let crashes = dir.join(CRASH_DIR);
    if DIR.set(crashes).is_err() {
        return;
    }

    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        // Трейс снимается ПЕРВЫМ делом: любой вызов между паникой и этим
        // местом добавляет в него свои кадры.
        let trace = Backtrace::force_capture();
        // Своё поведение — после стандартного. Если в записи что-то пойдёт не
        // так, stderr к этому моменту уже получил своё.
        previous(info);

        let Some(dir) = crash_dir() else { return };
        let now = chrono::Local::now();
        write_crash(dir, &now, &record(info, &trace, &now));
        prune_in(dir);
    }));
}

/// Одна запись журнала.
///
/// Время в заголовке продублировано с именем файла нарочно: файл переименуют
/// или пришлют без имени (в мессенджерах это обычное дело), а знать, когда это
/// случилось, всё равно нужно.
fn record(
    info: &std::panic::PanicHookInfo<'_>,
    trace: &Backtrace,
    now: &chrono::DateTime<chrono::Local>,
) -> String {
    // Сообщение паники лежит в `Any`, и достать его можно только перебором
    // двух форм, которыми его кладёт `panic!`: литерал и форматированная
    // строка.
    let payload = info.payload();
    let message = payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("<unknown reason>");

    let thread = std::thread::current();
    let mut out = String::with_capacity(2048);
    let _ = writeln!(
        out,
        "=== {} | mikui {} | {} | thread {} ===",
        now.format("%Y-%m-%d %H:%M:%S%.3f %:z"),
        env!("CARGO_PKG_VERSION"),
        std::env::consts::OS,
        thread.name().unwrap_or("<unnamed>"),
    );
    match info.location() {
        Some(at) => {
            let _ = writeln!(
                out,
                "panicked at {}:{}:{}",
                at.file(),
                at.line(),
                at.column()
            );
        }
        None => {
            let _ = writeln!(out, "panicked at an unknown location");
        }
    }
    let _ = writeln!(out, "{message}\n");
    let _ = writeln!(out, "{trace}");
    out
}

/// Имя файла под падение, случившееся в этот момент.
///
/// Двоеточий в нём нет намеренно: в имени файла Windows их не допускает, а
/// журнал должен получаться одинаковым на всех трёх системах.
fn file_name(now: &chrono::DateTime<chrono::Local>) -> String {
    format!(
        "{FILE_PREFIX}{}{FILE_SUFFIX}",
        now.format("%Y-%m-%d_%H-%M-%S%.3f")
    )
}

/// Пишет запись в свой файл.
///
/// Ни одна ошибка здесь не всплывает наружу, и это не небрежность: обработчик
/// паники, сам впавший в панику, обрывает процесс (`panic in a panic`). Не
/// записалось — значит не записалось, приложение от этого страдать не должно.
fn write_crash(dir: &Path, now: &chrono::DateTime<chrono::Local>, text: &str) {
    // Каталог создаётся здесь, а не один раз в `install`: тогда неудача при
    // старте выключила бы журнал навсегда, а так следующая паника попробует
    // снова. Стоит это один системный вызов на падение.
    let _ = fs::create_dir_all(dir);

    let path = dir.join(file_name(now));
    // `append`, а не `create_new`: две паники в одну миллисекунду попадут в
    // один файл, и это лучше, чем потерять вторую из-за занятого имени.
    if let Ok(mut file) = fs::OpenOptions::new().create(true).append(true).open(path) {
        // Одним вызовом, а не по строкам: паники в разных потоках попадают
        // сюда одновременно, и построчная запись перемешала бы два трейса.
        let _ = file.write_all(text.as_bytes());
    }
}

/// Оставляет только последние `KEEP_FILES` падений.
fn prune_in(dir: &Path) {
    let found = files_in(dir);
    let Some(extra) = found.len().checked_sub(KEEP_FILES) else {
        return;
    };
    for path in found.iter().take(extra) {
        let _ = fs::remove_file(path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone as _;

    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("mikui-diag-{}-{name}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).expect("временный каталог");
        dir
    }

    fn at(day: u32, hour: u32, sec: u32) -> chrono::DateTime<chrono::Local> {
        chrono::Local
            .with_ymd_and_hms(2026, 9, day, hour, 0, sec)
            .single()
            .expect("однозначное местное время")
    }

    #[test]
    fn the_name_carries_a_sortable_time_and_no_colons() {
        let name = file_name(&at(16, 15, 22));

        assert!(name.starts_with("panic-2026-09-16_15-00-22"), "{name}");
        assert!(name.ends_with(".log"), "{name}");
        assert!(
            !name.contains(':'),
            "двоеточие ломает имя под Windows: {name}"
        );
    }

    /// Порядок по имени обязан совпадать с порядком по времени — на этом
    /// держатся и `latest_crash`, и подчистка.
    #[test]
    fn names_sort_the_same_way_as_time() {
        assert!(
            file_name(&at(9, 23, 59)) < file_name(&at(10, 0, 0)),
            "имена должны сортироваться как время"
        );
    }

    #[test]
    fn each_crash_gets_its_own_file() {
        let dir = temp_dir("separate");

        write_crash(&dir, &at(16, 10, 0), "first crash\n");
        write_crash(&dir, &at(16, 10, 1), "second crash\n");

        let found = files_in(&dir);
        assert_eq!(found.len(), 2, "каждая паника — свой файл: {found:?}");
        assert_eq!(
            fs::read_to_string(found.last().expect("есть свежий")).unwrap(),
            "second crash\n",
            "последним по порядку должен идти последний по времени"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    /// Две паники в одну миллисекунду попадают в один файл — это лучше, чем
    /// потерять вторую из-за занятого имени.
    #[test]
    fn a_collision_appends_instead_of_losing_the_record() {
        let dir = temp_dir("collision");
        let same = at(16, 10, 0);

        write_crash(&dir, &same, "first\n");
        write_crash(&dir, &same, "second\n");

        let found = files_in(&dir);
        assert_eq!(found.len(), 1);
        assert_eq!(
            fs::read_to_string(&found[0]).unwrap(),
            "first\nsecond\n",
            "вторая запись потеряна"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn pruning_keeps_the_newest_and_touches_nothing_else() {
        let dir = temp_dir("prune");
        // Чужой файл в той же папке — подчистка не должна его замечать.
        fs::write(dir.join("notes.txt"), "not mine").unwrap();

        for i in 0..(KEEP_FILES as u32 + 5) {
            write_crash(&dir, &at(16, 10, i), &format!("crash {i}\n"));
        }
        prune_in(&dir);

        let found = files_in(&dir);
        assert_eq!(found.len(), KEEP_FILES, "предел не соблюдён");
        assert_eq!(
            fs::read_to_string(found.last().unwrap()).unwrap(),
            format!("crash {}\n", KEEP_FILES + 4),
            "самое свежее падение обязано уцелеть"
        );
        assert!(dir.join("notes.txt").exists(), "тронут чужой файл");

        let _ = fs::remove_dir_all(&dir);
    }
}
