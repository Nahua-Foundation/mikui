//! Заготовка сообщения: JSON, в котором уже перечислены все поля схемы, а
//! значения — нулевые.
//!
//! Та же надобность, что и у protobuf-заготовки: отправить Avro, глядя на
//! пустое поле ввода, нельзя — контракт живёт в .avsc, а не в голове у того,
//! кто отлаживает инцидент. Причём для Avro это жёстче: protobuf простит
//! пропущенное поле, а Avro — нет, у него в записи обязаны быть все.
//!
//! Печатается вручную, а не через `serde_json`: порядок полей здесь — порядок
//! объявления, тот же, в котором тело приедет обратно при чтении.
//!
//! # Чем заполнены значения
//!
//! Не «чем попало», а тем, что приложение потом само же и примет: заготовка,
//! которую отвергает собственная проверка отправки, бесполезна. Отсюда
//! неочевидные места — union, bytes и fixed:
//!
//! * `["null", "T"]` — это `null`. Nullable-union в Avro и означает «поля может
//!   не быть», и ноль для него — именно null, а не нулевое значение `T`.
//! * `bytes` — пустой массив, а не строка: ровно в таком виде байты приезжают
//!   при чтении (`Value::Bytes` печатается массивом чисел), и заготовка обязана
//!   показывать ту же форму.
//! * `fixed` — строка нужной длины. Единственное место, где заготовка
//!   расходится с показом: прочитанный `fixed` печатается массивом, а обратно
//!   массив не принимается — `resolve` знает для него только строку и байты.
//!   Скопировать прочитанное тело в форму отправки на таком поле не выйдет;
//!   `fixed` в контрактах встречается редко (decimal и uuid — отдельные типы),
//!   и городить ради него перевод форм не стоит.

use apache_avro::schema::{Name, Schema};

use super::linked::Linked;

const INDENT: &str = "  ";

/// JSON-заготовка, с отступами в два пробела — ровно как печатает тела модалка
/// чтения (`JSON.stringify(x, null, 2)`).
pub fn skeleton(linked: &Linked) -> String {
    let mut out = String::new();
    let mut stack = Vec::new();
    write_value(linked.root(), linked.all(), 0, &mut stack, &mut out);
    out
}

fn write_value(
    schema: &Schema,
    all: &[Schema],
    depth: usize,
    stack: &mut Vec<String>,
    out: &mut String,
) {
    match schema {
        Schema::Null => out.push_str("null"),
        Schema::Boolean => out.push_str("false"),
        Schema::Int | Schema::Long => out.push('0'),
        Schema::Float | Schema::Double => out.push_str("0.0"),
        Schema::String => out.push_str("\"\""),
        Schema::Bytes | Schema::Decimal(_) | Schema::BigDecimal => out.push_str("[]"),
        Schema::Array(_) => out.push_str("[]"),
        Schema::Map(_) => out.push_str("{}"),
        Schema::Enum(e) => {
            out.push('"');
            // Значение по умолчанию, если оно объявлено, иначе первый символ:
            // пустой enum спецификация запрещает, так что запасной вариант
            // нужен только чтобы не паниковать.
            let symbol = e
                .default
                .clone()
                .or_else(|| e.symbols.first().cloned())
                .unwrap_or_default();
            out.push_str(&symbol);
            out.push('"');
        }
        Schema::Fixed(f) => {
            out.push('"');
            for _ in 0..f.size {
                out.push('0');
            }
            out.push('"');
        }
        Schema::Duration(_) => out.push_str("[0,0,0,0,0,0,0,0,0,0,0,0]"),
        Schema::Uuid(_) => out.push_str("\"00000000-0000-0000-0000-000000000000\""),
        Schema::Date
        | Schema::TimeMillis
        | Schema::TimeMicros
        | Schema::TimestampMillis
        | Schema::TimestampMicros
        | Schema::TimestampNanos
        | Schema::LocalTimestampMillis
        | Schema::LocalTimestampMicros
        | Schema::LocalTimestampNanos => out.push('0'),
        Schema::Union(u) => {
            let variants = u.variants();
            match variants.iter().find(|v| matches!(v, Schema::Null)) {
                // Nullable-union: ноль для него — именно `null`.
                Some(_) => out.push_str("null"),
                None => match variants.first() {
                    Some(first) => write_value(first, all, depth, stack, out),
                    None => out.push_str("null"),
                },
            }
        }
        Schema::Record(r) => {
            let name = r.name.fullname(None);
            // Запись, которая (прямо или через цепочку полей) содержит саму
            // себя, — обычное дело для деревьев и списков. Раскрыть её до конца
            // невозможно, и любая заготовка такого типа где-то обрывается.
            if stack.contains(&name) {
                out.push_str("{}");
                return;
            }
            stack.push(name);

            if r.fields.is_empty() {
                out.push_str("{}");
                stack.pop();
                return;
            }

            out.push_str("{\n");
            for (index, field) in r.fields.iter().enumerate() {
                for _ in 0..=depth {
                    out.push_str(INDENT);
                }
                out.push('"');
                out.push_str(&field.name);
                out.push_str("\": ");
                write_value(&field.schema, all, depth + 1, stack, out);
                if index + 1 < r.fields.len() {
                    out.push(',');
                }
                out.push('\n');
            }
            for _ in 0..depth {
                out.push_str(INDENT);
            }
            out.push('}');
            stack.pop();
        }
        // Ссылка несёт только имя — сам тип лежит в общем наборе.
        Schema::Ref { name } => match resolve(name, all) {
            Some(target) => write_value(target, all, depth, stack, out),
            None => out.push_str("null"),
        },
    }
}

fn resolve<'a>(name: &Name, all: &'a [Schema]) -> Option<&'a Schema> {
    let wanted = name.fullname(None);
    all.iter().find(|s| {
        let found = match s {
            Schema::Record(r) => Some(&r.name),
            Schema::Enum(e) => Some(&e.name),
            Schema::Fixed(f) => Some(&f.name),
            _ => None,
        };
        found.is_some_and(|n| n.fullname(None) == wanted)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn linked_of(text: &str) -> Linked {
        Linked::parse_files(&[text.to_string()], None).unwrap()
    }

    #[test]
    fn every_type_gets_its_zero_and_fields_keep_declaration_order() {
        let linked = linked_of(
            r#"{
                "type": "record", "name": "All", "namespace": "demo",
                "fields": [
                    {"name": "title", "type": "string"},
                    {"name": "count", "type": "int"},
                    {"name": "total", "type": "long"},
                    {"name": "ratio", "type": "double"},
                    {"name": "ok", "type": "boolean"},
                    {"name": "blob", "type": "bytes"},
                    {"name": "tags", "type": {"type": "array", "items": "string"}},
                    {"name": "meta", "type": {"type": "map", "values": "string"}},
                    {"name": "kind", "type": {"type": "enum", "name": "Kind", "symbols": ["UNKNOWN", "CLICK"]}},
                    {"name": "note", "type": ["null", "string"]},
                    {"name": "at", "type": {"type": "long", "logicalType": "timestamp-millis"}}
                ]
            }"#,
        );

        let expected = "{\n  \"title\": \"\",\n  \"count\": 0,\n  \"total\": 0,\n  \"ratio\": 0.0,\n  \"ok\": false,\n  \"blob\": [],\n  \"tags\": [],\n  \"meta\": {},\n  \"kind\": \"UNKNOWN\",\n  \"note\": null,\n  \"at\": 0\n}";
        assert_eq!(skeleton(&linked), expected);
    }

    /// Главное требование к заготовке: приложение обязано принять её обратно.
    /// Без этого теста легко разойтись с тем, что понимает `resolve`.
    #[test]
    fn the_skeleton_survives_a_full_round_trip() {
        let linked = linked_of(
            r#"{
                "type": "record", "name": "Event", "namespace": "demo",
                "fields": [
                    {"name": "id", "type": "string"},
                    {"name": "count", "type": "int"},
                    {"name": "blob", "type": "bytes"},
                    {"name": "kind", "type": {"type": "enum", "name": "Kind", "symbols": ["UNKNOWN", "CLICK"]}},
                    {"name": "note", "type": ["null", "string"]},
                    {"name": "tags", "type": {"type": "array", "items": "string"}},
                    {"name": "inner", "type": {
                        "type": "record", "name": "Inner",
                        "fields": [{"name": "x", "type": "int"}]
                    }}
                ]
            }"#,
        );

        let json: serde_json::Value = serde_json::from_str(&skeleton(&linked))
            .expect("заготовка обязана быть валидным JSON");
        let bytes = linked
            .encode(json.clone())
            .expect("заготовку обязана принимать собственная отправка");
        // И прочитаться обратно тем же самым, чем была.
        assert_eq!(linked.decode(&bytes).unwrap(), json);
    }

    #[test]
    fn a_nullable_union_gets_null_not_the_inner_zero() {
        let linked = linked_of(
            r#"{
                "type": "record", "name": "R", "fields": [
                    {"name": "maybe", "type": ["null", "string"]},
                    {"name": "either", "type": ["int", "string"]}
                ]
            }"#,
        );
        let text = skeleton(&linked);
        assert!(text.contains("\"maybe\": null"), "{text}");
        // Union без null: заготовка берёт первый вариант — какой-то выбрать
        // надо, а первый в Avro и есть основной.
        assert!(text.contains("\"either\": 0"), "{text}");
    }

    /// Ради этого случая и нужен стек имён: раскрыть такую запись до конца
    /// невозможно, а падать по переполнению стека — тем более.
    #[test]
    fn a_self_referencing_record_stops_instead_of_recursing_forever() {
        let linked = linked_of(
            r#"{
                "type": "record", "name": "Node", "namespace": "demo",
                "fields": [
                    {"name": "value", "type": "int"},
                    {"name": "next", "type": ["null", "demo.Node"]}
                ]
            }"#,
        );
        let text = skeleton(&linked);
        assert!(text.contains("\"next\": null"), "{text}");
    }

    #[test]
    fn a_reference_to_another_schema_is_expanded() {
        let money = r#"{"type":"record","name":"Money","namespace":"common","fields":[{"name":"amount","type":"long"}]}"#;
        let order = r#"{"type":"record","name":"Order","namespace":"orders","fields":[{"name":"total","type":"common.Money"}]}"#;
        let linked =
            Linked::parse_files(&[order.to_string(), money.to_string()], Some("orders.Order"))
                .unwrap();

        let text = skeleton(&linked);
        // Не `null` и не имя типа: пользователю нужны поля, которые предстоит
        // заполнить, а они лежат в соседней схеме.
        assert!(text.contains("\"amount\": 0"), "{text}");
    }
}
