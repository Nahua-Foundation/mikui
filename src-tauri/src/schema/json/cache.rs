//! Скомпилированные JSON-схемы, добытые из реестра.
//!
//! Кэш здесь нужен по другой причине, чем аврошный. На пути показа таблицы
//! схемы JSON Schema нет вовсе — тело читается и без неё (см. `decoder`).
//! Зато она стоит на пути ПРОВЕРКИ ОТПРАВКИ, а `check_produce_payload` зовётся,
//! пока тело набирают: без памяти каждое нажатие клавиши оборачивалось бы
//! HTTP-запросом в реестр и повторной компиляцией схемы.
//!
//! Дисковой половины нет намеренно, и это не упущение. Дисковый кэш аврошных
//! схем существует ради сохранённых сообщений: без схемы их не прочитать вовсе.
//! Тело JSON Schema читается без схемы всегда, поэтому офлайн ей нечего
//! спасать.

use std::sync::{Arc, Mutex};

use super::compiled::Compiled;
use crate::schema::registry::{Registry, SchemaKind};

/// Сколько схем держать. Отправляют в один-два топика за сессию; десяток —
/// запас, а не расчёт.
const LIMIT: usize = 8;

/// Ключ — реестр и `subject@версия`: схемы разных реестров нумеруются
/// независимо, и смешать их значило бы проверять по чужому контракту.
type Key = (String, String);

static COMPILED: Mutex<Vec<(Key, Registered)>> = Mutex::new(Vec::new());

/// Схема вместе с её id в реестре.
///
/// id хранится РЯДОМ со схемой, а не запрашивается отдельно, ровно затем, чтобы
/// его не пришлось спрашивать вторым запросом: он нужен confluent-заголовку
/// отправляемого тела, а зовётся всё это из `check_produce_payload`, то есть
/// пока тело набирают.
#[derive(Clone)]
pub struct Registered {
    pub compiled: Arc<Compiled>,
    pub id: u32,
}

/// Схема subject'а: из памяти или из реестра.
pub fn by_subject(
    registry: &Registry,
    url: &str,
    subject: &str,
    version: Option<i32>,
) -> Result<Registered, String> {
    let key = (
        url.to_string(),
        format!(
            "{subject}@{}",
            version.map_or_else(|| "latest".to_string(), |v| v.to_string())
        ),
    );
    if let Some(hit) = remembered(&key) {
        return Ok(hit);
    }

    // Тип проверяется здесь, а не в клиенте: реестр общий на три формата, и
    // компилировать avro-схему как JSON Schema значило бы объяснять
    // пользователю несоответствие контракту, которого он не нарушал.
    let fetched = registry
        .by_subject(subject, version)?
        .expect(SchemaKind::Json)?;
    let registered = Registered {
        compiled: Arc::new(Compiled::parse(&fetched.schema, &fetched.references)?),
        id: fetched.id,
    };
    memorize(key, registered.clone());
    Ok(registered)
}

/// Забывает добытое. Зовётся оттуда же, откуда аврошный `forget_subjects`:
/// пользователь, тронувший настройки схемы, ждёт свежего взгляда на реестр.
pub fn forget() {
    if let Ok(mut compiled) = COMPILED.lock() {
        compiled.clear();
    }
}

fn remembered(key: &Key) -> Option<Registered> {
    let compiled = COMPILED.lock().ok()?;
    compiled
        .iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.clone())
}

fn memorize(key: Key, value: Registered) {
    let Ok(mut compiled) = COMPILED.lock() else {
        return;
    };
    compiled.retain(|(k, _)| *k != key);
    compiled.push((key, value));
    if compiled.len() > LIMIT {
        compiled.remove(0);
    }
}
