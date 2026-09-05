//! Заготовка сообщения: JSON, в котором уже перечислены все поля схемы, а
//! значения — нулевые.
//!
//! Та же надобность, что у заготовок protobuf и Avro: отправить сообщение,
//! глядя на пустое поле ввода, нельзя — контракт живёт в схеме, а не в голове у
//! того, кто отлаживает инцидент.
//!
//! В отличие от аврошной, печатается через `serde_json`, а не вручную. Причина
//! в том, что порядок полей здесь сохраняется сам: `serde_json` собран с фичей
//! `preserve_order` (см. `Cargo.toml`), и `properties` остаются в том порядке, в
//! каком записаны в схеме. `to_string_pretty` даёт те же два пробела отступа,
//! которыми модалка чтения печатает тела.
//!
//! # Чем заполнены значения
//!
//! По убыванию доказательности, и порядок тут важнее самих правил:
//!
//! 1. `default` — автор схемы прямо сказал, чем заполнять;
//! 2. `const` — другого значения быть и не может;
//! 3. `enum` — первое из перечисленных: какое-то выбрать надо, а произвольная
//!    строка тут заведомо не пройдёт проверку;
//! 4. `type` — ноль своего типа.
//!
//! Заготовка обязана проходить собственную проверку отправки (`Compiled::
//! validate`) — иначе она бесполезна. Это и проверяется тестами.

use std::collections::HashSet;

use serde_json::{Map, Value};

/// Сколько уровней `$ref` разворачивать.
///
/// Схема, ссылающаяся сама на себя (дерево, связный список), — обычное дело, и
/// раскрыть её до конца невозможно: любая заготовка такого типа где-то
/// обрывается. Стек имён здесь не спасает, потому что одна и та же ссылка
/// законно встречается в разных ветках; поэтому ограничение по глубине.
const MAX_DEPTH: usize = 16;

/// JSON-заготовка по схеме.
pub fn skeleton(schema: &Value) -> String {
    let mut seen = HashSet::new();
    let value = zero(schema, schema, 0, &mut seen);
    // `to_string_pretty` не может отказать на значении, которое мы сами и
    // построили, но паниковать в отладочном инструменте всё равно не за чем.
    serde_json::to_string_pretty(&value).unwrap_or_else(|_| "{}".to_string())
}

fn zero(schema: &Value, root: &Value, depth: usize, seen: &mut HashSet<String>) -> Value {
    // Схема-булев: `true` разрешает что угодно, `false` — ничего. Заполнять
    // нечем, и объект тут был бы такой же догадкой, как null.
    let Some(object) = schema.as_object() else {
        return Value::Null;
    };
    if depth > MAX_DEPTH {
        return Value::Null;
    }

    // Порядок проверок — это и есть правило заполнения, см. заголовок модуля.
    if let Some(default) = object.get("default") {
        return default.clone();
    }
    if let Some(constant) = object.get("const") {
        return constant.clone();
    }
    if let Some(first) = object.get("enum").and_then(|e| e.as_array()?.first()) {
        return first.clone();
    }
    if let Some(pointer) = object.get("$ref").and_then(Value::as_str) {
        return follow(pointer, root, depth, seen);
    }
    // Композиция: разворачиваем первую ветку. Строго правильным для `allOf`
    // было бы слияние всех, но заготовка — это подсказка, а не генератор
    // валидных тел; первая ветка почти всегда несёт основную форму.
    for keyword in ["allOf", "oneOf", "anyOf"] {
        if let Some(first) = object.get(keyword).and_then(|c| c.as_array()?.first()) {
            return zero(first, root, depth + 1, seen);
        }
    }

    match type_of(object) {
        Some("object") => object_zero(object, root, depth, seen),
        Some("array") => Value::Array(Vec::new()),
        Some("string") => Value::String(String::new()),
        Some("integer") => Value::from(0),
        Some("number") => Value::from(0.0),
        Some("boolean") => Value::Bool(false),
        Some("null") => Value::Null,
        // Тип не назван. У схемы с `properties` он всё равно объектный — так
        // пишут сплошь и рядом, и показать поля полезнее, чем null.
        _ if object.contains_key("properties") => object_zero(object, root, depth, seen),
        _ => Value::Null,
    }
}

/// Имя типа. `type` бывает и массивом (`["string", "null"]`) — тогда `null`
/// означает «поля может не быть», и ноль для такого поля именно он; это то же
/// правило, по которому аврошная заготовка выбирает null в nullable-union.
fn type_of(object: &Map<String, Value>) -> Option<&str> {
    match object.get("type")? {
        Value::String(name) => Some(name),
        Value::Array(names) => {
            if names.iter().any(|n| n.as_str() == Some("null")) {
                return Some("null");
            }
            names.first()?.as_str()
        }
        _ => None,
    }
}

fn object_zero(
    object: &Map<String, Value>,
    root: &Value,
    depth: usize,
    seen: &mut HashSet<String>,
) -> Value {
    let Some(properties) = object.get("properties").and_then(Value::as_object) else {
        return Value::Object(Map::new());
    };
    let mut out = Map::new();
    for (name, property) in properties {
        out.insert(name.clone(), zero(property, root, depth + 1, seen));
    }
    Value::Object(out)
}

/// Разворачивает `$ref`.
///
/// Ищется только ВНУТРИ схемы: по сети мы за ссылками не ходим намеренно (см.
/// `Cargo.toml`), а всё, на что схема ссылается, реестр присылает сам, и
/// `Compiled::parse` кладёт это в `$defs`. Поддержаны две формы — JSON Pointer
/// (`#/$defs/Money`) и `$id` соседней схемы, которым и адресует реестр.
fn follow(pointer: &str, root: &Value, depth: usize, seen: &mut HashSet<String>) -> Value {
    // Ссылка, уже раскрытая выше по ветке, — это цикл. `seen` очищается на
    // выходе, иначе один и тот же тип, законно встреченный в двух СОСЕДНИХ
    // полях, второй раз пришёл бы пустым.
    if !seen.insert(pointer.to_string()) {
        return Value::Null;
    }
    let target = resolve(pointer, root);
    let value = match target {
        Some(target) => zero(target, root, depth + 1, seen),
        None => Value::Null,
    };
    seen.remove(pointer);
    value
}

fn resolve<'a>(pointer: &str, root: &'a Value) -> Option<&'a Value> {
    if let Some(path) = pointer.strip_prefix("#/") {
        // `serde_json::Value::pointer` ждёт ведущий слэш и понимает экранирование
        // `~0`/`~1` — то самое, что нужно именам с `/` внутри.
        return root.pointer(&format!("/{path}"));
    }
    if pointer == "#" {
        return Some(root);
    }
    // Ссылка по `$id`: `Compiled::parse` сложила такие схемы в `$defs` под этим
    // же ключом.
    root.get("$defs")?.get(pointer)
}

#[cfg(test)]
mod tests {
    use super::super::compiled::Compiled;
    use super::*;

    fn skeleton_of(text: &str) -> String {
        skeleton(&serde_json::from_str(text).unwrap())
    }

    #[test]
    fn every_type_gets_its_zero_and_fields_keep_declaration_order() {
        let text = skeleton_of(
            r#"{
                "type": "object",
                "properties": {
                    "title":  {"type": "string"},
                    "count":  {"type": "integer"},
                    "ratio":  {"type": "number"},
                    "ok":     {"type": "boolean"},
                    "tags":   {"type": "array", "items": {"type": "string"}},
                    "meta":   {"type": "object"},
                    "note":   {"type": ["string", "null"]}
                }
            }"#,
        );
        let expected = "{\n  \"title\": \"\",\n  \"count\": 0,\n  \"ratio\": 0.0,\n  \"ok\": false,\n  \"tags\": [],\n  \"meta\": {},\n  \"note\": null\n}";
        assert_eq!(text, expected);
    }

    /// Главное требование к заготовке: её обязана принять собственная проверка
    /// отправки. Без этого теста легко разойтись с тем, что понимает `Compiled`.
    #[test]
    fn the_skeleton_passes_our_own_validation() {
        const ORDER: &str = r#"{
            "type": "object",
            "properties": {
                "id":     {"type": "string"},
                "total":  {"type": "integer"},
                "state":  {"enum": ["NEW", "PAID"]},
                "lines":  {"type": "array", "items": {"type": "string"}},
                "inner":  {"type": "object", "properties": {"x": {"type": "integer"}}}
            },
            "required": ["id", "total", "state", "lines", "inner"]
        }"#;

        let compiled = Compiled::parse(ORDER, &[]).unwrap();
        let value: Value = serde_json::from_str(&skeleton_of(ORDER))
            .expect("заготовка обязана быть валидным JSON");
        assert_eq!(compiled.validate(&value), Ok(()));
    }

    #[test]
    fn a_declared_default_wins_over_the_type_zero() {
        let text = skeleton_of(
            r#"{"type": "object", "properties": {
                "kind": {"type": "string", "default": "order"},
                "n":    {"type": "integer", "default": 7}
            }}"#,
        );
        assert!(text.contains("\"kind\": \"order\""), "{text}");
        assert!(text.contains("\"n\": 7"), "{text}");
    }

    #[test]
    fn an_enum_takes_its_first_value_and_const_takes_the_only_one() {
        let text = skeleton_of(
            r#"{"type": "object", "properties": {
                "state":   {"enum": ["NEW", "PAID"]},
                "version": {"const": 3}
            }}"#,
        );
        assert!(text.contains("\"state\": \"NEW\""), "{text}");
        assert!(text.contains("\"version\": 3"), "{text}");
    }

    /// Тип массивом — это способ сказать «поля может не быть», и ноль для
    /// такого поля именно null.
    #[test]
    fn a_nullable_type_gets_null() {
        let text = skeleton_of(r#"{"type": "object", "properties": {"a": {"type": ["null", "integer"]}}}"#);
        assert!(text.contains("\"a\": null"), "{text}");
    }

    // Ограничители `r##"…"##`, а не `r#"…"#`: в JSON Schema ссылка на себя —
    // это `"#"`, и такой строкой сырой литерал закрылся бы прямо посередине.
    #[test]
    fn a_local_ref_is_expanded_into_its_fields() {
        let text = skeleton_of(
            r##"{
                "type": "object",
                "properties": {"total": {"$ref": "#/$defs/Money"}},
                "$defs": {"Money": {"type": "object", "properties": {"amount": {"type": "integer"}}}}
            }"##,
        );
        // Не null и не имя типа: пользователю нужны поля, которые предстоит
        // заполнить.
        assert!(text.contains("\"amount\": 0"), "{text}");
    }

    /// Тот же тип в двух СОСЕДНИХ полях — не цикл, и второе поле обязано
    /// раскрыться так же, как первое.
    #[test]
    fn the_same_ref_twice_side_by_side_expands_both_times() {
        let text = skeleton_of(
            r##"{
                "type": "object",
                "properties": {
                    "from": {"$ref": "#/$defs/M"},
                    "to":   {"$ref": "#/$defs/M"}
                },
                "$defs": {"M": {"type": "object", "properties": {"amount": {"type": "integer"}}}}
            }"##,
        );
        assert_eq!(text.matches("\"amount\": 0").count(), 2, "{text}");
    }

    /// Ради этого случая и стоит ограничение: раскрыть такую схему до конца
    /// невозможно, а падать по переполнению стека — тем более.
    #[test]
    fn a_self_referencing_schema_stops_instead_of_recursing_forever() {
        let text = skeleton_of(
            r##"{
                "$id": "node",
                "type": "object",
                "properties": {
                    "value": {"type": "integer"},
                    "next":  {"$ref": "#"}
                }
            }"##,
        );
        assert!(text.contains("\"next\": null"), "{text}");
    }

    /// Схема без `type`, но с `properties` — распространённая запись, и поля
    /// показать полезнее, чем null.
    #[test]
    fn properties_without_a_type_still_make_an_object() {
        let text = skeleton_of(r#"{"properties": {"a": {"type": "string"}}}"#);
        assert!(text.contains("\"a\": \"\""), "{text}");
    }

    #[test]
    fn a_boolean_schema_does_not_panic() {
        assert_eq!(skeleton_of("true"), "null");
    }
}
