//! Заготовка сообщения: JSON, в котором уже перечислены все поля выбранного
//! message, а значения — нулевые.
//!
//! Нужна затем, что отправить protobuf, глядя только на пустое поле ввода,
//! нельзя: контракт живёт в .proto, а не в голове у того, кто отлаживает
//! инцидент. Заполнить готовые ключи — работа на минуту, вспомнить их все —
//! возврат в IDE за файлом схемы.
//!
//! Печатается вручную, а не через `serde_json`: карта serde_json без фичи
//! `preserve_order` — это `BTreeMap`, и поля в заготовке шли бы по алфавиту.
//! Человек сверяет её со своим .proto сверху вниз, поэтому порядок здесь —
//! порядок объявления.

//! # Почему здесь нет особых правил для well-known types
//!
//! Спецификация protobuf-JSON печатает `google.protobuf.Timestamp` строкой
//! RFC 3339, `Duration` — строкой вроде `"1.5s"`, и так далее. В этом
//! приложении — не печатает. Особые правила в protobuf-json-mapping включаются
//! через `downcast_ref` к СГЕНЕРИРОВАННОМУ типу, а схемы топиков связываются
//! динамически (`FileDescriptor::new_dynamic_fds` в `linked::parse`) — внешнего
//! `protoc` и кодогенерации у нас нет. Значит и при чтении, и при отправке
//! `Timestamp` здесь — обычное сообщение с полями `seconds` и `nanos`.
//!
//! Заготовка обязана совпадать с этим, а не со спецификацией: строка RFC 3339 в
//! ней была бы текстом, который приложение само же и отвергнет при отправке.
//! Проверяется тестом `the_skeleton_survives_a_full_round_trip`.

use protobuf::reflect::{
    EnumDescriptor, FieldDescriptor, MessageDescriptor, RuntimeFieldType, RuntimeType,
};

const INDENT: &str = "  ";

/// JSON-заготовка сообщения, с отступами в два пробела — ровно как печатает
/// тела модалка чтения (`JSON.stringify(x, null, 2)`).
pub fn skeleton(message: &MessageDescriptor) -> String {
    let mut out = String::new();
    let mut stack = Vec::new();
    write_message(message, 0, &mut stack, &mut out);
    out
}

fn write_message(
    message: &MessageDescriptor,
    depth: usize,
    stack: &mut Vec<String>,
    out: &mut String,
) {
    stack.push(message.full_name().to_string());
    let fields: Vec<FieldDescriptor> = message.fields().filter(included).collect();

    if fields.is_empty() {
        out.push_str("{}");
        stack.pop();
        return;
    }

    out.push_str("{\n");
    for (index, field) in fields.iter().enumerate() {
        for _ in 0..=depth {
            out.push_str(INDENT);
        }
        // Имя из .proto, а не lowerCamelCase: ровно так же печатает тела
        // `ProtoDecoder` (`proto_field_name: true`), и заготовка не должна
        // выглядеть иначе, чем то, что пользователь видит при чтении. Парсер
        // понимает оба написания.
        out.push('"');
        out.push_str(field.name());
        out.push_str("\": ");
        write_value(field, depth + 1, stack, out);
        if index + 1 < fields.len() {
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

/// Попадает ли поле в заготовку.
///
/// Из `oneof` берётся только первый вариант: активен там ровно один, и
/// перечислить все значило бы предложить заполнить взаимоисключающее. Что
/// именно выбрано, видно по имени поля, а соседние варианты — в .proto.
fn included(field: &FieldDescriptor) -> bool {
    let Some(oneof) = field.containing_oneof() else {
        return true;
    };
    // Через `let`, а не одним выражением: итератор заимствует `oneof`, а тот
    // в хвостовой позиции успел бы умереть раньше временного значения.
    let first = oneof.fields().next().map(|f| f.number());
    first == Some(field.number())
}

fn write_value(
    field: &FieldDescriptor,
    depth: usize,
    stack: &mut Vec<String>,
    out: &mut String,
) {
    match field.runtime_field_type() {
        // Пустые список и карта, а не образец элемента: сколько их нужно,
        // знает только отправляющий, а лишний элемент пришлось бы удалять.
        RuntimeFieldType::Repeated(_) => out.push_str("[]"),
        RuntimeFieldType::Map(_, _) => out.push_str("{}"),
        RuntimeFieldType::Singular(t) => write_zero(&t, depth, stack, out),
    }
}

fn write_zero(t: &RuntimeType, depth: usize, stack: &mut Vec<String>, out: &mut String) {
    match t {
        RuntimeType::I32 | RuntimeType::U32 => out.push('0'),
        // 64-битные целые канонический protobuf-JSON печатает СТРОКОЙ: в
        // double, которым JSON представляет числа, они не помещаются без
        // потерь. Парсер принимает и число, но заготовка обязана показывать
        // ту форму, в которой тело приедет обратно при чтении.
        RuntimeType::I64 | RuntimeType::U64 => out.push_str("\"0\""),
        RuntimeType::F32 | RuntimeType::F64 => out.push_str("0.0"),
        RuntimeType::Bool => out.push_str("false"),
        // bytes в JSON — base64, и пустым байтам соответствует пустая строка.
        RuntimeType::String | RuntimeType::VecU8 => out.push_str("\"\""),
        RuntimeType::Enum(e) => {
            out.push('"');
            out.push_str(&zero_value_name(e));
            out.push('"');
        }
        RuntimeType::Message(m) => {
            // Message, который (прямо или через цепочку полей) содержит сам
            // себя, — обычное дело для деревьев и связных списков. Раскрывать
            // его до конца невозможно, и любая заготовка такого типа где-то
            // обрывается; обрываемся пустым объектом. `null` был бы точнее по
            // смыслу («поля нет»), но парсер protobuf-json-mapping на месте
            // сообщения его не принимает — а заготовка, которую приложение
            // само же отвергает, бесполезна.
            if stack.iter().any(|name| name == m.full_name()) {
                out.push_str("{}");
            } else {
                write_message(m, depth, stack, out);
            }
        }
    }
}

/// Имя значения, которое enum принимает по умолчанию.
///
/// В proto3 это всегда значение с номером 0, но в proto2 нумерация вольная —
/// поэтому если нуля нет, берём первое объявленное. Пустой enum синтаксис
/// запрещает, так что запасной вариант нужен только чтобы не паниковать.
fn zero_value_name(e: &EnumDescriptor) -> String {
    e.value_by_number(0)
        .or_else(|| e.values().next())
        .map(|v| v.name().to_string())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::proto::linked;
    use crate::schema::types::SchemaFile;

    /// Разбирает текст .proto во временном каталоге и достаёт из него message.
    fn message_of(name: &str, text: &str, message: &str) -> (std::path::PathBuf, MessageDescriptor) {
        let dir = std::env::temp_dir().join(format!("mikui-template-test-{name}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("t.proto"), text).unwrap();

        let file = SchemaFile {
            name: "t.proto".to_string(),
            source: dir.join("t.proto").to_string_lossy().into_owned(),
        };
        let linked = linked::parse(&dir, &[file]).unwrap();
        let md = linked.message(message).unwrap();
        (dir, md)
    }

    #[test]
    fn every_scalar_gets_its_zero_and_fields_keep_declaration_order() {
        let (dir, md) = message_of(
            "scalars",
            r#"
                syntax = "proto3";
                package demo;
                enum Kind { UNKNOWN = 0; CLICK = 1; }
                message All {
                    string title = 1;
                    int32 count = 2;
                    int64 total = 3;
                    double ratio = 4;
                    bool active = 5;
                    bytes blob = 6;
                    Kind kind = 7;
                    repeated string tags = 8;
                    map<string, int32> labels = 9;
                }
            "#,
            "demo.All",
        );

        assert_eq!(
            skeleton(&md),
            r#"{
  "title": "",
  "count": 0,
  "total": "0",
  "ratio": 0.0,
  "active": false,
  "blob": "",
  "kind": "UNKNOWN",
  "tags": [],
  "labels": {}
}"#
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    /// Главная проверка модуля.
    ///
    /// Заготовка обязана пройти весь путь отправляемого сообщения: разобраться
    /// парсером (`proto::encode`), закодироваться в байты и разобраться обратно
    /// декодером, которым приложение показывает прочитанное (`ProtoDecoder`).
    /// Пока это так, пользователю не предлагается текст, который приложение
    /// само же и отвергнет, — а это ровно то, ради чего заготовка и нужна.
    #[test]
    fn the_skeleton_survives_a_full_round_trip() {
        let (dir, md) = message_of(
            "roundtrip",
            r#"
                syntax = "proto3";
                package demo;
                import "google/protobuf/timestamp.proto";
                enum Tier { BASIC = 0; GOLD = 1; }
                message Point { int32 x = 1; int32 y = 2; }
                message Event {
                    string event_id = 1;
                    Tier tier = 2;
                    Point point = 3;
                    google.protobuf.Timestamp at = 4;
                    repeated Point trail = 5;
                    int64 sequence = 6;
                }
            "#,
            "demo.Event",
        );

        let text = skeleton(&md);
        // Заполняем одно поле, как это сделал бы пользователь: заготовка со
        // всеми нулями доехала бы обратно ПУСТОЙ (proto3 не печатает значения
        // по умолчанию), и такой круг ничего бы не доказал.
        let filled = text.replace(r#""event_id": """#, r#""event_id": "a1""#);
        assert_ne!(filled, text, "поле для заполнения не найдено:\n{text}");

        let parsed = protobuf_json_mapping::parse_dyn_from_str(&md, &filled)
            .unwrap_or_else(|e| panic!("заготовка не разбирается: {e}\n{filled}"));
        let bytes = parsed.write_to_bytes_dyn().unwrap();
        let decoded = crate::schema::proto::ProtoDecoder::new(md.clone())
            .decode(&bytes)
            .unwrap();
        // Круг замкнулся: то, что уедет в топик, вернётся оттуда тем же.
        let back: serde_json::Value = serde_json::from_str(&decoded).unwrap();
        assert_eq!(back["event_id"], "a1", "{decoded}");

        // Вложенное сообщение раскрыто, повторяющееся — нет.
        assert!(text.contains("\"point\": {"), "{text}");
        assert!(text.contains("\"trail\": []"), "{text}");
        // Well-known type раскрыт ПО ПОЛЯМ, а не строкой RFC 3339 из
        // спецификации: схемы здесь связываются динамически — см. шапку модуля.
        assert!(text.contains("\"seconds\""), "{text}");
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn a_self_referencing_message_stops_instead_of_recursing_forever() {
        let (dir, md) = message_of(
            "cycle",
            r#"
                syntax = "proto3";
                package demo;
                message Node { string name = 1; Node parent = 2; }
            "#,
            "demo.Node",
        );

        let text = skeleton(&md);
        assert!(text.contains("\"parent\": {}"), "{text}");
        protobuf_json_mapping::parse_dyn_from_str(&md, &text).unwrap();
        let _ = std::fs::remove_dir_all(dir);
    }

    /// Активен ровно один вариант oneof, поэтому в заготовке он и один.
    #[test]
    fn oneof_contributes_only_its_first_variant() {
        let (dir, md) = message_of(
            "oneof",
            r#"
                syntax = "proto3";
                package demo;
                message Payload {
                    string id = 1;
                    oneof body { string text = 2; bytes blob = 3; int32 code = 4; }
                }
            "#,
            "demo.Payload",
        );

        let text = skeleton(&md);
        assert!(text.contains("\"text\""), "{text}");
        assert!(!text.contains("\"blob\""), "{text}");
        assert!(!text.contains("\"code\""), "{text}");
        protobuf_json_mapping::parse_dyn_from_str(&md, &text).unwrap();
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn a_message_without_fields_is_an_empty_object() {
        let (dir, md) = message_of(
            "empty",
            "syntax = \"proto3\"; package demo; message Ping {}",
            "demo.Ping",
        );
        assert_eq!(skeleton(&md), "{}");
        let _ = std::fs::remove_dir_all(dir);
    }
}
