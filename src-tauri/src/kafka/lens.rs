//! Линзы: как показывать сообщение, которое приехало в конверте.
//!
//! Два самых массовых применения Kafka — CDC через Debezium и перекладывание
//! данных через Kafka Connect — кладут полезную нагрузку внутрь конверта.
//! Читаются такие тела и без всякой линзы (это обычный JSON или Avro), но
//! показываются конвертом: у Connect-топика первые двести символов КАЖДОЙ
//! строки — это кусок описания типов, а у Debezium — `{"before":null,"after":
//! {…` , где не видно ни операции, ни того, что изменилось.
//!
//! # Линза ортогональна формату
//!
//! `BodyFormat` отвечает на вопрос «как превратить байты в JSON», линза — «что
//! из этого JSON показать». Поэтому она работает поверх `Decoder`, а не вместо
//! него: Debezium с Avro-сериализатором — обычное дело, и линза обязана
//! работать и там.
//!
//! # Что здесь есть и чего здесь нет
//!
//! Здесь только ПРЕВЬЮ СТРОКИ и метка операции. Тело в окне просмотра линза не
//! подменяет: диф `before`/`after` полезен ровно вместе с `source` и `ts_ms`,
//! и обрезать конверт по дороге в модалку значило бы потерять то, ради чего
//! туда и заходят. Модалка разбирает конверт сама, уже во фронте, по полному
//! телу.
//!
//! # Цена
//!
//! На топике со схемой тело и так разбирается на каждую видимую строку. А вот
//! на топике с обычным JSON — а Debezium и Connect чаще всего именно такие —
//! разбора сегодня нет вовсе: `text::preview` просто режет байты. Линза вводит
//! его с нуля, и на окне в двести строк это уже десятки миллисекунд на том же
//! потоке, который крутит раунды чтения.
//!
//! Отсюда два ограничителя. Первый — `sniff`: прежде чем разбирать тело, по
//! сырым байтам ищутся ключи конверта, SIMD-поиском без единой аллокации (тем
//! же `memchr::memmem`, которым идёт фильтрация). Тело, в котором их нет,
//! разбирать не за чем. Второй — `MAX_BYTES`: тело крупнее не разбирается
//! вовсе, превью у него остаётся прежним.

use memchr::memmem;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::schema::LensSetting;

/// Потолок размера тела, к которому применяется линза.
///
/// Одно сообщение на десять мегабайт иначе съело бы всё окно: разбор
/// пропорционален размеру, а показать из него надо двести символов. Реальные
/// конверты CDC на порядки меньше.
pub const MAX_BYTES: usize = 256 * 1024;

/// Какой конверт распознан.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Lens {
    /// `{"schema": {...}, "payload": {...}}` — дефолт `JsonConverter` при
    /// `schemas.enable=true`.
    Connect,
    /// `{"before": …, "after": …, "source": …, "op": "u", "ts_ms": …}`.
    Debezium,
}

/// Что делать с телами открытого топика.
///
/// `Auto` — распознавать конверт по каждому телу. Отдельно от `Fixed`, а не
/// «распознали один раз и закрепили», потому что топик, переживший смену
/// конвейера, вполне может нести и то, и другое; а стоит распознавание одного
/// прохода `memmem` по сырым байтам.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum LensMode {
    #[default]
    Auto,
    /// Показывать конверт как есть. Не то же самое, что `Auto`: это ответ
    /// пользователя, и распознавание его отменять не должно.
    Off,
    Fixed(Lens),
}

/// Выбор из настроек топика — в режим работы воркера.
///
/// Одним местом на всё приложение: `LensSetting` хранится на диске и переживает
/// перезапуск, `LensMode` живёт в воркере, и второе правило соответствия рано
/// или поздно разошлось бы с первым.
impl From<Option<LensSetting>> for LensMode {
    fn from(setting: Option<LensSetting>) -> Self {
        match setting {
            // Не выбирали — решает распознавание.
            None => Self::Auto,
            Some(LensSetting::Off) => Self::Off,
            Some(LensSetting::Connect) => Self::Fixed(Lens::Connect),
            Some(LensSetting::Debezium) => Self::Fixed(Lens::Debezium),
        }
    }
}

/// Результат применения линзы к одному телу.
#[derive(Default)]
pub struct Lensed {
    /// Метка операции для строки таблицы: `c`, `u`, `d`, `r`, `t`, `m`. Пусто у
    /// Connect — там операции нет, есть только конверт.
    pub tag: Option<String>,
    /// Чем заменить превью строки. `None` — линза не сработала, показывать как
    /// показывали.
    pub preview: Option<String>,
}

/// Применяет линзу к телу.
///
/// `body` — уже разобранный текст (выход `Decoder`) либо сырые байты у топика
/// без схемы. Разбирать его здесь, а не выше, намеренно: на топике без линзы
/// разбора не происходит вовсе.
pub fn apply(mode: LensMode, body: &[u8]) -> Lensed {
    let wanted = match mode {
        LensMode::Off => return Lensed::default(),
        LensMode::Fixed(lens) => Some(lens),
        LensMode::Auto => {
            // Просмотр байтов только ОТСЕИВАЕТ: тело без ключей конверта
            // разбирать не за чем. Какая именно линза — решает точная проверка
            // по разобранному телу, поэтому его догадка здесь и отбрасывается.
            if sniff(body).is_none() {
                return Lensed::default();
            }
            None
        }
    };
    if body.len() > MAX_BYTES {
        return Lensed::default();
    }

    let Ok(value) = serde_json::from_slice::<Value>(body) else {
        return Lensed::default();
    };
    render(wanted, &value)
}

/// Что показать по уже разобранному телу.
///
/// `wanted` — линза, назначенная руками; `None` — определить по самому телу.
/// Назначенная сильнее: пользователь мог выбрать её на топике, где распознавание
/// молчит, и переспрашивать его вопреки выбору значило бы не слушать.
fn render(wanted: Option<Lens>, value: &Value) -> Lensed {
    // Debezium ЖИВЁТ ВНУТРИ Connect-конверта, когда у коннектора включены
    // схемы. Поэтому сначала снимаем внешний, а потом смотрим, что под ним, —
    // иначе на таком топике была бы видна не операция, а конверт с ней внутри.
    let (outer, inner) = match unwrap_connect(value) {
        Some(payload) => (Some(Lens::Connect), payload),
        None => (None, value),
    };

    let debezium = is_debezium(inner);
    let lens = match wanted {
        Some(lens) => lens,
        None if debezium => Lens::Debezium,
        None if outer.is_some() => Lens::Connect,
        None => return Lensed::default(),
    };

    match lens {
        Lens::Debezium => {
            if !debezium {
                return Lensed::default();
            }
            let op = inner.get("op").and_then(Value::as_str);
            Lensed {
                tag: op.map(str::to_string),
                // У удаления содержательна прежняя строка: `after` там null, и
                // показывать пустоту в таблице удалений бессмысленно.
                preview: match op {
                    Some("d") => pick(inner, &["before", "after"]),
                    _ => pick(inner, &["after", "before"]),
                },
            }
        }
        Lens::Connect => Lensed {
            tag: None,
            // Внешнего конверта могло и не быть — тогда показывать нечего, и
            // менять превью не на что.
            preview: outer.map(|_| compact(inner)),
        },
    }
}

/// Первое из полей, которое есть и не `null`.
fn pick(value: &Value, fields: &[&str]) -> Option<String> {
    fields
        .iter()
        .find_map(|name| value.get(name).filter(|v| !v.is_null()))
        .map(compact)
}

fn compact(value: &Value) -> String {
    value.to_string()
}

/// Снимает конверт Kafka Connect.
///
/// Признак — `schema` и `payload` на верхнем уровне, причём `schema` описывает
/// структуру. Проверять `schema.type` обязательно: пара полей с такими именами
/// встречается и в обычных прикладных сообщениях, а `"type": "struct"` рядом с
/// ними — уже почерк конвертера.
fn unwrap_connect(value: &Value) -> Option<&Value> {
    let object = value.as_object()?;
    let schema = object.get("schema")?.as_object()?;
    if schema.get("type").and_then(Value::as_str) != Some("struct") {
        return None;
    }
    object.get("payload")
}

/// Похоже ли тело на конверт Debezium.
///
/// `op` из известного набора ПЛЮС хотя бы одно из полей конверта. Одного `op`
/// мало: поле с таким именем есть в куче прикладных сообщений, и объявлять
/// каждое из них CDC-событием значило бы показывать вместо тела его кусок.
fn is_debezium(value: &Value) -> bool {
    let Some(object) = value.as_object() else {
        return false;
    };
    let known_op = object
        .get("op")
        .and_then(Value::as_str)
        .is_some_and(|op| matches!(op, "c" | "u" | "d" | "r" | "t" | "m"));

    known_op
        && ["before", "after", "source"]
            .iter()
            .any(|name| object.contains_key(*name))
}

/// Есть ли в сырых байтах ключи, без которых конверта быть не может.
///
/// Дешёвый отсев ПЕРЕД разбором: SIMD-поиск подстроки без единой аллокации.
/// Ложное срабатывание стоит одного разбора и отсеивается уже точной проверкой;
/// пропустить настоящий конверт эта проверка не может — ключи в нём есть по
/// определению.
fn sniff(body: &[u8]) -> Option<Lens> {
    if contains(body, b"\"payload\"") && contains(body, b"\"schema\"") {
        return Some(Lens::Connect);
    }
    if contains(body, b"\"op\"") && (contains(body, b"\"before\"") || contains(body, b"\"after\""))
    {
        return Some(Lens::Debezium);
    }
    None
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    memmem::find(haystack, needle).is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    const DEBEZIUM: &str = r#"{
        "before": null,
        "after": {"id": 1, "name": "first"},
        "source": {"table": "orders"},
        "op": "c",
        "ts_ms": 1700000000000
    }"#;

    const CONNECT: &str = r#"{
        "schema": {"type": "struct", "fields": [{"type": "int64", "field": "id"}]},
        "payload": {"id": 7}
    }"#;

    fn auto(body: &str) -> Lensed {
        apply(LensMode::Auto, body.as_bytes())
    }

    #[test]
    fn a_debezium_body_shows_the_row_and_the_operation() {
        let out = auto(DEBEZIUM);
        assert_eq!(out.tag.as_deref(), Some("c"));
        assert_eq!(out.preview.as_deref(), Some(r#"{"id":1,"name":"first"}"#));
    }

    /// У удаления содержательна прежняя строка: `after` там null, и превью из
    /// него было бы пустым ровно там, где интереснее всего.
    #[test]
    fn a_delete_shows_the_row_that_was_removed() {
        let body = r#"{"before":{"id":9},"after":null,"op":"d","source":{}}"#;
        let out = auto(body);
        assert_eq!(out.tag.as_deref(), Some("d"));
        assert_eq!(out.preview.as_deref(), Some(r#"{"id":9}"#));
    }

    #[test]
    fn a_connect_envelope_shows_the_payload_instead_of_the_schema() {
        let out = auto(CONNECT);
        assert_eq!(out.tag, None);
        assert_eq!(out.preview.as_deref(), Some(r#"{"id":7}"#));
    }

    /// Ровно тот случай, ради которого распознавание идёт в два слоя: с
    /// включёнными схемами Debezium едет ВНУТРИ Connect-конверта, и операция
    /// видна только под ним.
    #[test]
    fn debezium_inside_a_connect_envelope_is_still_debezium() {
        let body =
            format!(r#"{{"schema": {{"type": "struct", "fields": []}}, "payload": {DEBEZIUM}}}"#);
        let out = auto(&body);
        assert_eq!(out.tag.as_deref(), Some("c"));
        assert_eq!(out.preview.as_deref(), Some(r#"{"id":1,"name":"first"}"#));
    }

    #[test]
    fn an_ordinary_body_is_left_alone() {
        let out = auto(r#"{"id": 1, "name": "first"}"#);
        assert!(out.tag.is_none());
        assert!(out.preview.is_none());
    }

    /// Одного `op` мало: поле с таким именем есть в куче прикладных сообщений.
    #[test]
    fn a_plain_message_with_an_op_field_is_not_cdc() {
        let out = auto(r#"{"op": "c", "value": 1}"#);
        assert!(out.preview.is_none(), "{:?}", out.preview);
    }

    /// И пары `schema`/`payload` мало: решает `"type": "struct"` — почерк
    /// конвертера, а не совпадение имён.
    #[test]
    fn a_plain_message_with_schema_and_payload_is_not_a_connect_envelope() {
        let out = auto(r#"{"schema": "v1", "payload": {"id": 1}}"#);
        assert!(out.preview.is_none());

        let out = auto(r#"{"schema": {"type": "object"}, "payload": {"id": 1}}"#);
        assert!(out.preview.is_none());
    }

    /// Выбор пользователя сильнее распознавания — но не сильнее фактов: линза,
    /// которой в теле не за что зацепиться, ничего не меняет.
    #[test]
    fn a_chosen_lens_that_does_not_fit_the_body_changes_nothing() {
        let out = apply(LensMode::Fixed(Lens::Debezium), br#"{"id": 1}"#);
        assert!(out.preview.is_none());
        assert!(out.tag.is_none());
    }

    /// Назначенная руками линза работает и там, где распознавание молчало бы:
    /// `Fixed` не проходит через `sniff`.
    #[test]
    fn a_chosen_lens_applies_where_detection_would_have_stayed_silent() {
        // Ни `before`, ни `after` — `sniff` такое тело пропустил бы.
        let body = br#"{"op":"u","source":{"table":"t"},"after":{"id":3}}"#;
        assert_eq!(
            apply(LensMode::Fixed(Lens::Debezium), body).tag.as_deref(),
            Some("u")
        );
    }

    #[test]
    fn off_means_off_even_on_an_obvious_envelope() {
        let out = apply(LensMode::Off, CONNECT.as_bytes());
        assert!(out.preview.is_none());
        assert!(out.tag.is_none());
    }

    /// Тело за потолком не разбирается вовсе: разбор пропорционален размеру, а
    /// показать из него надо двести символов.
    #[test]
    fn a_body_over_the_cap_is_left_alone() {
        let filler = "x".repeat(MAX_BYTES);
        let body = format!(r#"{{"op":"c","source":{{}},"after":{{"v":"{filler}"}}}}"#);
        assert!(body.len() > MAX_BYTES);
        assert!(auto(&body).preview.is_none());
    }

    /// Битое тело — не повод уронить окно выдачи: показываем как показывали.
    #[test]
    fn a_body_that_is_not_json_is_left_alone() {
        let out = auto(r#"{"op":"c","after":{"#);
        assert!(out.preview.is_none());
    }

    /// Просмотр байтов обязан пропускать настоящие конверты и отсеивать всё
    /// остальное — на этом держится вся экономия.
    #[test]
    fn the_byte_sniff_passes_envelopes_and_stops_ordinary_bodies() {
        assert_eq!(sniff(DEBEZIUM.as_bytes()), Some(Lens::Debezium));
        assert_eq!(sniff(CONNECT.as_bytes()), Some(Lens::Connect));
        assert_eq!(sniff(br#"{"id":1,"name":"first"}"#), None);
    }

    /// «Не выбирали» и «выбрали off» — разные вещи, и путать их нельзя:
    /// распознавание отменяет только второе.
    #[test]
    fn the_stored_choice_maps_onto_the_working_mode() {
        assert_eq!(LensMode::from(None), LensMode::Auto);
        assert_eq!(LensMode::from(Some(LensSetting::Off)), LensMode::Off);
        assert_eq!(
            LensMode::from(Some(LensSetting::Connect)),
            LensMode::Fixed(Lens::Connect)
        );
        assert_eq!(
            LensMode::from(Some(LensSetting::Debezium)),
            LensMode::Fixed(Lens::Debezium)
        );
    }
}
