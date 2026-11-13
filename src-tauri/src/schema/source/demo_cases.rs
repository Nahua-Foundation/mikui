//! Стенд `demo/proto-cases`: контракты и тела пятнадцати случаев, прогнанные
//! настоящим кодом приложения — без кластера и без интерфейса.
//!
//! Зачем он тут, а не в заметках. Правило корня — главное, чем приложение
//! определяет тип топика, и проверено оно было на пяти топиках руками. Здесь
//! сверка корней держится обычным тестом: пятнадцать контрактов разной формы,
//! тридцать миллисекунд, и ни одной внешней зависимости.
//!
//! Самого каталога `demo/` в репозитории НЕТ — он лежит только в рабочем дереве.
//! Поэтому оба теста начинают с проверки, на месте ли он, и без него молча
//! проходят: тест, который падает у всех, кроме одного человека, хуже, чем
//! пропущенный.
//!
//! Дерево читается ровно так, как его прочтёт библиотека (`tree::proto_files` +
//! `tree::entries` + `tree::include_roots`), наборы разбирает
//! `linked::parse_set`. Второй тест — подбор по телам; тела для него кладёт на
//! диск сам сеятель, теми же байтами, которые уезжают в Kafka:
//!
//! ```sh
//! python3 demo/seed-proto-cases.py --dump "$TMPDIR/proto-cases-dump"
//! MIKUI_CASES_BODIES="$TMPDIR/proto-cases-dump" \
//!   cargo test --lib demo_cases -- --nocapture
//! ```
//!
//! Числа, которые он печатает, записаны в `demo/proto-cases.md` — там же сказано,
//! что в каждом случае должен увидеть человек.

use std::path::{Path, PathBuf};

use super::tree;
use crate::schema::proto::guess::{self, Candidate, Guess};
use crate::schema::proto::linked;

/// Случай: входной файл набора, ожидаемый корень и тип, которым топик записан.
///
/// `root = None` — корня у набора нет, и это правильный ответ, а не неудача.
/// `written` отличается от корня там, где топик записан не корнем (случай 15)
/// или где корня нет вовсе.
struct Case {
    number: u32,
    topic: &'static str,
    entry: Option<&'static str>,
    root: Option<&'static str>,
    written: Option<&'static str>,
}

const CASES: [Case; 15] = [
    Case {
        number: 1,
        topic: "proto.cases.01.single-message",
        entry: Some("case01_heartbeat.proto"),
        root: Some("mikui.cases.case01.Heartbeat"),
        written: Some("mikui.cases.case01.Heartbeat"),
    },
    Case {
        number: 2,
        topic: "proto.cases.02.envelope-and-nested",
        entry: Some("case02_orderbook.proto"),
        root: Some("mikui.cases.case02.OrderBookV2"),
        written: Some("mikui.cases.case02.OrderBookV2"),
    },
    Case {
        number: 3,
        topic: "proto.cases.03.oneof-envelope",
        entry: Some("case03_public_event.proto"),
        root: Some("mikui.cases.case03.PublicEvent"),
        written: Some("mikui.cases.case03.PublicEvent"),
    },
    Case {
        number: 4,
        topic: "proto.cases.04.three-level-chain",
        entry: Some("case04_portfolio.proto"),
        root: Some("mikui.cases.case04.Asset"),
        written: Some("mikui.cases.case04.Asset"),
    },
    Case {
        number: 5,
        topic: "proto.cases.05.two-roots",
        entry: Some("case05_device.proto"),
        root: None,
        written: Some("mikui.cases.case05.DeviceTelemetry"),
    },
    Case {
        number: 6,
        topic: "proto.cases.06.cycle",
        entry: Some("case06_catalog.proto"),
        root: None,
        written: Some("mikui.cases.case06.Folder"),
    },
    Case {
        number: 7,
        topic: "proto.cases.07.multi-file-set",
        entry: Some("case07_fills.proto"),
        root: Some("mikui.cases.case07.Fill"),
        written: Some("mikui.cases.case07.Fill"),
    },
    Case {
        number: 8,
        topic: "proto.cases.08.structural-tie",
        entry: Some("case08_events.proto"),
        root: None,
        written: Some("mikui.cases.case08.ShipmentEvent"),
    },
    Case {
        number: 9,
        topic: "proto.cases.09.trimmed-contract",
        entry: Some("case09_candle.proto"),
        root: Some("mikui.cases.case09.Candle"),
        written: Some("mikui.cases.case09.Candle"),
    },
    Case {
        number: 10,
        topic: "proto.cases.10.wire-type-clash",
        entry: Some("case10_quote.proto"),
        root: Some("mikui.cases.case10.Quote"),
        written: None,
    },
    Case {
        number: 11,
        topic: "proto.cases.11.mixed-types",
        entry: Some("case11_payment.proto"),
        root: Some("mikui.cases.case11.PaymentCaptured"),
        written: Some("mikui.cases.case11.PaymentCaptured"),
    },
    Case {
        number: 12,
        topic: "proto.cases.12.empty-and-tombstones",
        entry: Some("case12_sensor.proto"),
        root: Some("mikui.cases.case12.SensorReading"),
        written: Some("mikui.cases.case12.SensorReading"),
    },
    Case {
        number: 13,
        topic: "proto.cases.13.no-contract",
        entry: None,
        root: None,
        written: None,
    },
    Case {
        number: 14,
        topic: "proto.cases.14.bare-protobuf",
        entry: Some("case14_shipment.proto"),
        root: Some("mikui.cases.case14.ShipmentUpdate"),
        written: Some("mikui.cases.case14.ShipmentUpdate"),
    },
    Case {
        number: 15,
        topic: "proto.cases.15.confluent-indexes",
        entry: Some("case15_audit.proto"),
        root: Some("mikui.cases.case15.AuditBatch"),
        written: Some("mikui.cases.case15.AuditEnvelope.Entry"),
    },
];

fn contracts() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("demo/proto-cases")
}

/// Тела одного топика из дампа: `u32` длины и тело.
fn read_bodies(dir: &Path, topic: &str) -> Vec<Vec<u8>> {
    let bytes = std::fs::read(dir.join(format!("{topic}.bin"))).expect(topic);
    let mut out = Vec::new();
    let mut at = 0;
    while at + 4 <= bytes.len() {
        let len = u32::from_be_bytes(bytes[at..at + 4].try_into().unwrap()) as usize;
        at += 4;
        out.push(bytes[at..at + len].to_vec());
        at += len;
    }
    out
}

/// Деление дерева на наборы и корень каждого — то, на чём держится весь стенд.
#[test]
fn every_case_set_has_the_root_its_contract_promises() {
    let root = contracts();
    if !root.is_dir() {
        println!("{} рядом нет — пропускаем", root.display());
        return;
    }
    let files = tree::proto_files(&root);
    let entries = tree::entries(&root, &files, None);
    let include = tree::include_roots(&root);

    println!("файлов в дереве: {}, наборов: {}", files.len(), entries.len());
    for name in &files {
        println!("  файл  {name}");
    }

    let expected: Vec<&str> = CASES.iter().filter_map(|case| case.entry).collect();
    assert_eq!(entries, expected, "состав наборов разошёлся с таблицей");

    for case in &CASES {
        let Some(entry) = case.entry else {
            continue;
        };
        let linked = linked::parse_set(&include, &[entry.to_string()])
            .unwrap_or_else(|error| panic!("случай {}: набор {entry} не разобрался: {error}", case.number));
        println!(
            "  набор {entry}: корень {:?}, типов {}",
            linked.root,
            linked.messages.len()
        );
        assert_eq!(
            linked.root.as_deref(),
            case.root,
            "случай {}: корень набора {entry}",
            case.number
        );
        if let Some(written) = case.written {
            assert!(
                linked.messages.iter().any(|name| name == written),
                "случай {}: типа {written} в наборе нет",
                case.number
            );
        }
    }
}

/// Подбор по телам — так же, как его делает `store::guess_message`: кандидаты
/// от ВСЕХ наборов разом, свои и чужие, с теми же флагами `own` и `root`.
#[test]
fn guessing_each_case_topic_gives_the_promised_numbers() {
    let Ok(dump) = std::env::var("MIKUI_CASES_BODIES") else {
        println!("MIKUI_CASES_BODIES не задан — пропускаем (тела кладёт `--dump` сеятеля)");
        return;
    };
    let dump = PathBuf::from(dump);
    let root = contracts();
    if !root.is_dir() {
        println!("{} рядом нет — пропускаем", root.display());
        return;
    }
    let include = tree::include_roots(&root);

    // Разбор всех наборов один раз: топиков пятнадцать, наборов четырнадцать, и
    // разбирать их заново на каждый топик значило бы ждать минуту ни за что.
    let sets: Vec<(&Case, linked::Linked)> = CASES
        .iter()
        .filter_map(|case| {
            let entry = case.entry?;
            Some((case, linked::parse_set(&include, &[entry.to_string()]).ok()?))
        })
        .collect();

    for case in &CASES {
        let samples = read_bodies(&dump, case.topic);
        let mut candidates = Vec::new();
        for (owner, linked) in &sets {
            for name in &linked.messages {
                let Some(descriptor) = linked.message(name) else {
                    continue;
                };
                candidates.push(Candidate {
                    message: name.clone(),
                    from_topic: owner.topic.to_string(),
                    own: owner.number == case.number,
                    root: linked.root.as_deref() == Some(name.as_str()),
                    fit: guess::fit(&descriptor, &samples),
                });
            }
        }
        let result = Guess::new(samples.len(), candidates);

        println!(
            "\n=== случай {} · {} · тел {} · ничья наверху {} · полных {} · скрыто {}",
            case.number,
            case.topic,
            result.samples,
            result.tied_at_top,
            result.fitting,
            result.dropped
        );
        for candidate in result.candidates.iter().take(4) {
            let fit = &candidate.fit;
            println!(
                "  {:<46} own={} root={} parsed={}/{} clean={} exact={} unknown={}/{}",
                candidate.message,
                u8::from(candidate.own),
                u8::from(candidate.root),
                fit.parsed,
                fit.samples,
                fit.clean,
                fit.exact,
                fit.unknown_bytes,
                fit.body_bytes
            );
        }
        // Свои кандидаты целиком: именно на них смотрит человек, и место
        // верного типа в списке — весь ответ подбора.
        for (place, candidate) in result.candidates.iter().enumerate() {
            if !candidate.own {
                continue;
            }
            let fit = &candidate.fit;
            println!(
                "  свой #{place}: {:<40} root={} parsed={}/{} clean={} unknown={}/{}",
                candidate.message,
                u8::from(candidate.root),
                fit.parsed,
                fit.samples,
                fit.clean,
                fit.unknown_bytes,
                fit.body_bytes
            );
        }
        if let Some(written) = case.written {
            let place = result
                .candidates
                .iter()
                .position(|candidate| candidate.message == written);
            println!("  записан {written} → место в списке {place:?}");
        }
    }
}
