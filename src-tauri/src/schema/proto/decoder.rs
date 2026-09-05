//! Декодирование тела сообщения по выбранному message.

use std::collections::HashSet;

use protobuf::reflect::{MessageDescriptor, RuntimeFieldType, RuntimeType};
use protobuf_json_mapping::PrintOptions;

/// Декодер одного топика: descriptor выбранного message плюс настройки печати.
///
/// Живёт в Kafka-воркере под `Arc` и переживает сколько угодно окон выдачи:
/// descriptor стоит дорого один раз, при разборе схемы, и ничего не стоит потом.
pub struct ProtoDecoder {
    message: MessageDescriptor,
    print: PrintOptions,
    /// Имена всех enum-значений, до которых можно дотянуться из полей этого
    /// message (включая вложенные сообщения). Разобранный protobuf печатает
    /// enum тем же JSON-string, что и обычную строку (см. `decode`), и снаружи
    /// их иначе не отличить — а модалке для подсветки нужно отличать по-настоящему,
    /// а не гадать по регистру букв. Список — не значения ИЗ конкретного тела,
    /// а вообще все, что этот тип может принять: считается один раз при выборе
    /// message, а не на каждое декодирование.
    enum_values: Vec<String>,
}

impl ProtoDecoder {
    pub fn new(message: MessageDescriptor) -> Self {
        let mut seen = HashSet::new();
        let mut values = HashSet::new();
        collect_enum_values(&message, &mut seen, &mut values);
        let mut enum_values: Vec<String> = values.into_iter().collect();
        enum_values.sort_unstable();

        Self {
            message,
            print: PrintOptions {
                // Имена полей — как в .proto, а не lowerCamelCase из канона
                // protobuf-JSON. Пользователь смотрит в свой контракт и ищет
                // в выдаче `event_id`, а не `eventId`.
                proto_field_name: true,
                ..PrintOptions::default()
            },
            enum_values,
        }
    }

    /// Разбирает тело и печатает его компактным JSON — без переносов строк.
    ///
    /// Компактным намеренно: таблице нужна ровно одна строка, а модалке —
    /// отступы, которые она и так расставит сама тем же кодом, каким давно
    /// печатает JSON-топики. Печатать здесь дважды, в двух видах, значило бы
    /// платить за форматирование на каждой строке таблицы.
    pub fn decode(&self, payload: &[u8]) -> Result<String, String> {
        let message = self
            .message
            .parse_from_bytes(payload)
            .map_err(|e| format!("can't decode as {}: {e}", self.message.full_name()))?;
        protobuf_json_mapping::print_to_string_with_options(&*message, &self.print)
            .map_err(|e| format!("can't render {} as JSON: {e}", self.message.full_name()))
    }

    /// Имена enum-значений этого message — модалке, чтобы подсветить их
    /// отдельным цветом, а не гадать по формату строки.
    pub fn enum_values(&self) -> &[String] {
        &self.enum_values
    }
}

/// Обходит граф полей message, собирая имена значений всех встреченных enum.
///
/// `seen` защищает от бесконечной рекурсии на message, который (прямо или
/// через цепочку полей) ссылается сам на себя.
fn collect_enum_values(
    message: &MessageDescriptor,
    seen: &mut HashSet<String>,
    out: &mut HashSet<String>,
) {
    if !seen.insert(message.full_name().to_string()) {
        return;
    }
    for field in message.fields() {
        let (a, b) = match field.runtime_field_type() {
            RuntimeFieldType::Singular(t) => (Some(t), None),
            RuntimeFieldType::Repeated(t) => (Some(t), None),
            RuntimeFieldType::Map(k, v) => (Some(k), Some(v)),
        };
        for t in [a, b].into_iter().flatten() {
            match t {
                RuntimeType::Enum(e) => out.extend(e.values().map(|v| v.name().to_string())),
                RuntimeType::Message(m) => collect_enum_values(&m, seen, out),
                _ => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::proto::linked;
    use crate::schema::types::SchemaFile;

    /// Строит декодер по тексту .proto, разложенному во временный каталог.
    fn decoder_for(name: &str, text: &str, message: &str) -> (std::path::PathBuf, ProtoDecoder) {
        let dir = std::env::temp_dir().join(format!("mikui-decoder-test-{name}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("t.proto"), text).unwrap();

        let file = SchemaFile {
            name: "t.proto".to_string(),
            source: dir.join("t.proto").to_string_lossy().into_owned(),
        };
        let linked = linked::parse(&dir, &[file]).unwrap();
        let md = linked.message(message).unwrap();
        (dir, ProtoDecoder::new(md))
    }

    const SCHEMA: &str = r#"
        syntax = "proto3";
        package demo;
        enum Kind { UNKNOWN = 0; CLICK = 1; }
        message Point { int32 x = 1; int32 y = 2; }
        message Event {
            string event_id = 1;
            Kind kind = 2;
            Point point = 3;
            repeated string tags = 4;
        }
    "#;

    /// Тело собираем руками по wire-формату: генератор кода сюда не подключён,
    /// а формат достаточно прост, чтобы это было честнее, чем мокать декодер.
    fn encoded_event() -> Vec<u8> {
        let mut out = Vec::new();
        // field 1 (event_id), wire type 2: "a1"
        out.extend_from_slice(&[0x0a, 0x02, b'a', b'1']);
        // field 2 (kind), wire type 0: CLICK = 1
        out.extend_from_slice(&[0x10, 0x01]);
        // field 3 (point), wire type 2: { x = 10, y = 20 }
        out.extend_from_slice(&[0x1a, 0x04, 0x08, 0x0a, 0x10, 0x14]);
        // field 4 (tags), wire type 2, дважды: "a", "b"
        out.extend_from_slice(&[0x22, 0x01, b'a']);
        out.extend_from_slice(&[0x22, 0x01, b'b']);
        out
    }

    #[test]
    fn decodes_to_compact_json_with_proto_field_names() {
        let (dir, decoder) = decoder_for("ok", SCHEMA, "demo.Event");
        let json = decoder.decode(&encoded_event()).unwrap();

        // Ни одного переноса строки: строка таблицы обязана остаться одной строкой.
        assert!(!json.contains('\n'), "ожидался компактный JSON, получено: {json}");
        // Имена полей — из .proto, а не lowerCamelCase.
        assert!(json.contains("\"event_id\""), "{json}");
        assert!(!json.contains("eventId"), "{json}");
        // Enum — именем, вложенное сообщение и повторяющееся поле на месте.
        assert!(json.contains("\"CLICK\""), "{json}");
        assert!(json.contains("\"point\""), "{json}");
        assert!(json.contains("\"tags\""), "{json}");

        // И это действительно JSON: модалка разберёт его тем же путём, что и
        // тело JSON-топика.
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["event_id"], "a1");
        assert_eq!(parsed["point"]["x"], 10);

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn enum_values_come_from_the_field_type_not_the_payload() {
        let (dir, decoder) = decoder_for("enum-values", SCHEMA, "demo.Event");
        let mut values = decoder.enum_values().to_vec();
        values.sort_unstable();
        // Обе константы `Kind`, хотя тело несёт только `CLICK` — модалке нужен
        // полный список, чтобы отличить enum от строки в ЛЮБОМ сообщении этого
        // типа, а не только в разобранном сейчас.
        assert_eq!(values, ["CLICK", "UNKNOWN"]);
        let _ = std::fs::remove_dir_all(dir);
    }

    /// Ради этого случая enum и собирают обходом графа полей, а не только
    /// верхнего уровня: enum за вложенным message иначе остался бы неизвестен.
    #[test]
    fn enum_values_are_collected_through_nested_messages() {
        let dir = std::env::temp_dir().join("mikui-decoder-test-nested-enum");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("t.proto"),
            r#"
                syntax = "proto3";
                package demo;
                enum Tier { BASIC = 0; GOLD = 1; }
                message Customer { Tier tier = 1; }
                message Order { Customer customer = 1; }
            "#,
        )
        .unwrap();
        let file = SchemaFile {
            name: "t.proto".to_string(),
            source: dir.join("t.proto").to_string_lossy().into_owned(),
        };
        let linked = linked::parse(&dir, &[file]).unwrap();
        let decoder = ProtoDecoder::new(linked.message("demo.Order").unwrap());

        let mut values = decoder.enum_values().to_vec();
        values.sort_unstable();
        assert_eq!(values, ["BASIC", "GOLD"]);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn undecodable_payload_reports_the_message_it_tried() {
        let (dir, decoder) = decoder_for("bad", SCHEMA, "demo.Event");
        // Тег поля 1 обещает длину 200 байт, а их нет.
        let error = decoder.decode(&[0x0a, 0xc8, 0x01, b'x']).unwrap_err();
        assert!(
            error.contains("demo.Event"),
            "ошибка должна называть тип, которым пробовали: {error}"
        );
        let _ = std::fs::remove_dir_all(dir);
    }
}
