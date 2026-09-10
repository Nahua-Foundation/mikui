//! Декодирование тела сообщения по выбранному message.

use std::collections::HashSet;

use protobuf::reflect::{MessageDescriptor, RuntimeFieldType, RuntimeType};
use protobuf_json_mapping::PrintOptions;

use crate::schema::wire::{proto_framing, ProtoFraming};

/// Декодер одного топика: descriptor выбранного message плюс настройки печати.
///
/// Живёт в Kafka-воркере под `Arc` и переживает сколько угодно окон выдачи:
/// descriptor стоит дорого один раз, при разборе схемы, и ничего не стоит потом.
pub struct ProtoDecoder {
    message: MessageDescriptor,
    /// Путь до `message` внутри его файла: индекс типа верхнего уровня, затем
    /// индексы вложенных. Ровно то, что `KafkaProtobufSerializer` пишет в
    /// заголовок каждого сообщения, — и сравнить их можно только тут, потому
    /// что тело своего имени не несёт.
    ///
    /// Считается один раз, при выборе message: на строке таблицы это сравнение
    /// двух коротких массивов чисел, а не обход дерева типов.
    indexes: Option<Vec<i64>>,
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
            indexes: index_path(&message),
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
    ///
    /// # Confluent-обёртка
    ///
    /// Тело от `KafkaProtobufSerializer` начинается с маркера, id схемы и
    /// массива message-indexes. Пока их не снять, разобрать его нечем: первый
    /// же varint индексов притворяется тегом поля, и топик не читался вовсе —
    /// даже с правильным .proto на руках.
    pub fn decode(&self, payload: &[u8]) -> Result<String, String> {
        let datum = match proto_framing(payload) {
            ProtoFraming::Confluent { indexes, datum, .. } => {
                self.check_indexes(&indexes)?;
                datum
            }
            ProtoFraming::Bare(body) => body,
        };

        let message = self
            .message
            .parse_from_bytes(datum)
            .map_err(|e| format!("can't decode as {}: {e}", self.message.full_name()))?;
        protobuf_json_mapping::print_to_string_with_options(&*message, &self.print)
            .map_err(|e| format!("can't render {} as JSON: {e}", self.message.full_name()))
    }

    /// Сверяет тип, названный отправителем, с тем, который выбран у топика.
    ///
    /// Отказ, а не молчаливый разбор, — по тому же правилу, по которому мы не
    /// разбираем ключ схемой значения: protobuf разберёт чужое тело охотно и
    /// без единой жалобы, выдав правдоподобный мусор. Отправитель здесь прямо
    /// сказал, чем он это писал, и игнорировать его слова хуже, чем показать
    /// одну строку ошибки с обоими именами.
    ///
    /// Индексы, которые в нашем файле не разрешаются, пропускаются молча: схема
    /// зарегистрирована из ДРУГОГО файла, сопоставлять не с чем, и объявлять
    /// это ошибкой значило бы сломать ровно тот случай, ради которого всё и
    /// затевалось, — «.proto у меня свой, а топик confluent-овский».
    fn check_indexes(&self, indexes: &[i64]) -> Result<(), String> {
        let (Some(ours), Some(theirs)) = (
            self.indexes.as_deref(),
            resolve_indexes(&self.message, indexes),
        ) else {
            return Ok(());
        };
        if ours == indexes {
            return Ok(());
        }
        Err(format!(
            "the message says it is {theirs}, but this topic is set to decode {}",
            self.message.full_name()
        ))
    }

    /// Имена enum-значений этого message — модалке, чтобы подсветить их
    /// отдельным цветом, а не гадать по формату строки.
    pub fn enum_values(&self) -> &[String] {
        &self.enum_values
    }
}

/// Путь до message внутри его файла — в том же виде, в каком его пишет
/// confluent-сериализатор.
///
/// `None` — типа не нашлось в собственном файле. Случиться этого не должно, но
/// строить на этом отказ было бы неправильно: тогда сверка просто не делается.
fn index_path(message: &MessageDescriptor) -> Option<Vec<i64>> {
    fn walk(candidates: Vec<MessageDescriptor>, wanted: &str, path: &mut Vec<i64>) -> bool {
        for (index, candidate) in candidates.into_iter().enumerate() {
            path.push(index as i64);
            if candidate.full_name() == wanted {
                return true;
            }
            if walk(candidate.nested_messages().collect(), wanted, path) {
                return true;
            }
            path.pop();
        }
        false
    }

    let mut path = Vec::new();
    let file = message.file_descriptor();
    walk(file.messages().collect(), message.full_name(), &mut path).then_some(path)
}

/// Куда указывают индексы из заголовка — в том файле, который загружен у нас.
///
/// `None` — не указывают никуда: файл у отправителя другой. Это не ошибка, а
/// отсутствие сведений, см. `ProtoDecoder::check_indexes`.
fn resolve_indexes(message: &MessageDescriptor, indexes: &[i64]) -> Option<String> {
    let file = message.file_descriptor();
    let (first, rest) = indexes.split_first()?;
    let mut current = file.messages().nth(usize::try_from(*first).ok()?)?;
    for step in rest {
        // Через промежуточный `Vec`, а не прямым присваиванием: итератор
        // `nested_messages` заимствует `current`, и переприсвоить его на месте
        // не даёт заимствование.
        let nested: Vec<_> = current.nested_messages().collect();
        current = nested.into_iter().nth(usize::try_from(*step).ok()?)?;
    }
    Some(current.full_name().to_string())
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

    // --- Confluent-обёртка -------------------------------------------------
    //
    // Ровно тот случай, ради которого рамка и заводилась: тело от
    // `KafkaProtobufSerializer` не разбиралось вовсе, даже когда .proto был
    // правильный.

    /// Заголовок с сокращённым массивом индексов (`0x00` = `[0]`).
    /// `demo.Point` — как раз первый message файла.
    #[test]
    fn a_confluent_framed_body_is_decoded_after_the_header_is_stripped() {
        let (dir, decoder) = decoder_for("framed", SCHEMA, "demo.Point");
        // { x = 10, y = 20 }
        let bare = [0x08, 0x0a, 0x10, 0x14];

        let mut framed = vec![0x00, 0x00, 0x00, 0x00, 0x07, 0x00];
        framed.extend_from_slice(&bare);

        let json = decoder.decode(&framed).unwrap();
        assert_eq!(json, decoder.decode(&bare).unwrap());
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&json).unwrap()["x"],
            10
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    /// Тело без обёртки обязано читаться как читалось: рамка не должна ломать
    /// топики, которые работали.
    #[test]
    fn a_bare_body_still_decodes_exactly_as_before() {
        let (dir, decoder) = decoder_for("still-bare", SCHEMA, "demo.Event");
        let json = decoder.decode(&encoded_event()).unwrap();
        assert!(json.contains("\"event_id\""), "{json}");
        let _ = std::fs::remove_dir_all(dir);
    }

    /// Отправитель прямо назвал тип, и он не тот, который выбран у топика.
    /// Разобрать чужим типом protobuf согласится молча, выдав правдоподобный
    /// мусор, — поэтому отказ, а не показ.
    #[test]
    fn a_body_that_names_another_type_is_refused_by_name() {
        // demo.Point — индекс 0, demo.Event — индекс 1 (enum в счёт не идёт).
        let (dir, decoder) = decoder_for("mismatch", SCHEMA, "demo.Point");
        // Индексы `[1]`: длина 1 (зигзаг 0x02), индекс 1 (зигзаг 0x02).
        let mut framed = vec![0x00, 0x00, 0x00, 0x00, 0x07, 0x02, 0x02];
        framed.extend_from_slice(&encoded_event());

        let error = decoder.decode(&framed).unwrap_err();
        assert!(error.contains("demo.Event"), "{error}");
        assert!(error.contains("demo.Point"), "{error}");
        let _ = std::fs::remove_dir_all(dir);
    }

    /// Схема зарегистрирована из другого файла, и сопоставлять индексы не с
    /// чем. Это НЕ повод отказать — ровно так выглядит случай «.proto у меня
    /// свой, а топик confluent-овский», ради которого всё и делалось.
    #[test]
    fn indexes_that_do_not_resolve_here_are_ignored_rather_than_refused() {
        let (dir, decoder) = decoder_for("unresolvable", SCHEMA, "demo.Point");
        // Индексы `[9]` — столько типов верхнего уровня в нашем файле нет.
        let mut framed = vec![0x00, 0x00, 0x00, 0x00, 0x07, 0x02, 0x12];
        framed.extend_from_slice(&[0x08, 0x0a, 0x10, 0x14]);

        let json = decoder.decode(&framed).unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&json).unwrap()["y"],
            20
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn the_index_path_of_a_top_level_message_is_its_position_in_the_file() {
        let (dir, decoder) = decoder_for("path", SCHEMA, "demo.Event");
        // Point объявлен первым, Event вторым; enum в нумерации message не
        // участвует.
        assert_eq!(decoder.indexes.as_deref(), Some([1].as_slice()));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn a_nested_message_gets_the_whole_path() {
        let dir = std::env::temp_dir().join("mikui-decoder-test-nested-path");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("t.proto"),
            r#"
                syntax = "proto3";
                package demo;
                message Outer {
                    message First { int32 a = 1; }
                    message Second { int32 b = 1; }
                }
            "#,
        )
        .unwrap();
        let file = SchemaFile {
            name: "t.proto".to_string(),
            source: dir.join("t.proto").to_string_lossy().into_owned(),
        };
        let linked = linked::parse(&dir, &[file]).unwrap();
        let decoder = ProtoDecoder::new(linked.message("demo.Outer.Second").unwrap());

        assert_eq!(decoder.indexes.as_deref(), Some([0, 1].as_slice()));
        let _ = std::fs::remove_dir_all(dir);
    }
}
