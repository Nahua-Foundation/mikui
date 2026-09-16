//! Проверка журнала паник целиком — на настоящей панике в отдельном потоке.
//!
//! Отдельным тестовым бинарником, а не юнит-тестом рядом с модулем: обработчик
//! паники глобальный и ставится один раз на процесс. В общем тестовом процессе
//! он мешал бы остальным тестам, а `OnceLock` внутри `install` не дал бы
//! поставить его дважды.
//!
//! Проверяется ровно то, на что рассчитан весь модуль: что после падения
//! ПОТОКА (а не всего процесса) на диске остаётся запись, по которой можно
//! разобраться, — причина, место и трейс.

use std::fs;

#[test]
fn a_panicking_thread_leaves_a_readable_record() {
    let dir = std::env::temp_dir().join(format!("mikui-panic-log-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).expect("временный каталог");

    mikui_lib::diag::install(&dir);
    assert_eq!(
        mikui_lib::diag::crash_dir(),
        Some(dir.join("crashes").as_path()),
        "падения складываются в свой подкаталог"
    );
    assert!(
        mikui_lib::diag::latest_crash().is_none(),
        "до падения журналу взяться неоткуда"
    );

    // Именно в потоке с именем: воркер Kafka так и падал, и имя потока — первое,
    // по чему в присланном журнале видно, где это случилось.
    let worker = std::thread::Builder::new()
        .name("kafka-worker".into())
        .spawn(|| panic!("arena corruption: {}", 42))
        .expect("поток создан");
    assert!(worker.join().is_err(), "поток обязан был упасть");

    let path = mikui_lib::diag::latest_crash().expect("падение записано");
    let name = path
        .file_name()
        .and_then(|n| n.to_str())
        .expect("имя файла")
        .to_string();
    assert!(name.starts_with("panic-"), "имя без времени: {name}");
    assert!(name.ends_with(".log"), "имя без расширения: {name}");

    let text = fs::read_to_string(&path).expect("журнал читается");

    assert!(
        text.contains("arena corruption: 42"),
        "нет причины:\n{text}"
    );
    assert!(text.contains("kafka-worker"), "нет имени потока:\n{text}");
    assert!(text.contains("tests/panic_log.rs"), "нет места:\n{text}");
    assert!(
        text.contains(env!("CARGO_PKG_VERSION")),
        "нет версии:\n{text}"
    );
    // Журнал уезжает в чужие руки, поэтому он на английском — русского в нём
    // быть не должно ни в заголовке, ни в подписях.
    assert!(
        !text.chars().any(|c| matches!(c, 'а'..='я' | 'А'..='Я')),
        "в журнале русский текст:\n{text}"
    );
    // Трейс снимается через `force_capture`, то есть не зависит от
    // `RUST_BACKTRACE` в окружении — иначе у пользователя он был бы пустым.
    assert!(
        text.contains("panic_log") || text.contains("backtrace"),
        "трейс не попал в запись:\n{text}"
    );

    let _ = fs::remove_dir_all(&dir);
}
