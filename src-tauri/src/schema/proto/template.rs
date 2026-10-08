//! Заготовка сообщения: JSON, в котором уже перечислены все поля выбранного
//! message, а значения — нулевые. Два исключения — поля, у которых нулевое
//! значение заведомо бесполезно: идентификаторы (`*_id`) получают случайный
//! UUID, enum — имя своего нулевого значения, а не пустую строку.
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
///
/// `branch` — имя ветки `oneof`, если выбирали именно её (см.
/// `Linked::produce_target`). Ветка нужна затем, что тип сообщения в топике —
/// часто конверт с `oneof` внутри (`PublicEvent`), а кладут в него ОДНУ
/// конкретную ветку. Без имени ветки заготовка раскрыла бы первую по порядку
/// объявления, и пользователь заполнял бы соседнюю с той, которую просил: без
/// имени поля внутри конверта `oneof` остаётся пустым, а потребитель читает
/// пустое сообщение.
///
/// Действует ветка только на САМ message: у вложенных сообщений `oneof` свой, и
/// выбирать её там не из чего.
pub fn skeleton(message: &MessageDescriptor, branch: Option<&str>) -> String {
    let mut out = String::new();
    let mut stack = Vec::new();
    write_message(message, branch, 0, &mut stack, &mut out);
    out
}

fn write_message(
    message: &MessageDescriptor,
    branch: Option<&str>,
    depth: usize,
    stack: &mut Vec<String>,
    out: &mut String,
) {
    stack.push(message.full_name().to_string());
    // Ветка — только у корня: у вложенных сообщений `oneof` свой, и выбранной
    // ветки для них никто не называл.
    let branch = (depth == 0).then_some(branch).flatten();
    let fields: Vec<FieldDescriptor> = message.fields().filter(|f| included(f, branch)).collect();

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
/// Из каждого `oneof` берётся только ОДИН вариант: активен там ровно один, и
/// перечислить все значило бы предложить заполнить взаимоисключающее. Какой
/// именно — решает `branch`, когда он назван, а иначе первый: заготовка не
/// должна зависеть от того, что пользователь ещё ничего не выбрал, и первый
/// вариант даёт ей законченный вид. Соседние варианты при этом остаются видны в
/// .proto, а их имена — в селекторе отправки.
fn included(field: &FieldDescriptor, branch: Option<&str>) -> bool {
    let Some(oneof) = field.containing_oneof() else {
        return true;
    };
    // Ветка выбирает вариант в СВОЁМ `oneof`, а не отменяет остальные: у message
    // их бывает несколько, и выбор в одном не должен глушить соседние группы —
    // иначе выбранная ветка молча выкинула бы из заготовки поля, которые к ней
    // отношения не имеют.
    if let Some(branch) = branch {
        if oneof.fields().any(|f| f.name() == branch) {
            return field.name() == branch;
        }
    }
    // Через `let`, а не одним выражением: итератор заимствует `oneof`, а тот
    // в хвостовой позиции успел бы умереть раньше временного значения.
    let first = oneof.fields().next().map(|f| f.number());
    first == Some(field.number())
}

fn write_value(field: &FieldDescriptor, depth: usize, stack: &mut Vec<String>, out: &mut String) {
    match field.runtime_field_type() {
        // Пустые список и карта, а не образец элемента: сколько их нужно,
        // знает только отправляющий, а лишний элемент пришлось бы удалять.
        RuntimeFieldType::Repeated(_) => out.push_str("[]"),
        RuntimeFieldType::Map(_, _) => out.push_str("{}"),
        // Имя поля идёт вниз вместе с типом: у строк от него зависит значение
        // (см. `looks_like_id`), а по типу идентификатор от прочих строк не
        // отличается никак.
        RuntimeFieldType::Singular(t) => write_zero(&t, field.name(), depth, stack, out),
    }
}

/// Похоже ли имя поля на идентификатор, которому нулевое значение не подходит.
///
/// По имени, а не по типу: в схеме идентификатор — обычная строка, и ничем
/// другим он от строки не отличается. Кроме суффиксов сюда попадают и сами
/// `id`/`uid`/`uuid`: поле, названное так целиком, — тот же случай.
///
/// Регистр не учитывается: имена полей в .proto по стилю строчные, но
/// `SCREAMING_CASE` в схемах встречается, и заготовка не должна зависеть от
/// того, чей стиль победил в конкретном файле.
fn looks_like_id(name: &str) -> bool {
    let name = name.to_ascii_lowercase();
    matches!(name.as_str(), "id" | "uid" | "uuid")
        || ["_id", "_uid", "_uuid"].iter().any(|s| name.ends_with(s))
}

fn write_zero(
    t: &RuntimeType,
    name: &str,
    depth: usize,
    stack: &mut Vec<String>,
    out: &mut String,
) {
    match t {
        RuntimeType::I32 | RuntimeType::U32 => out.push('0'),
        // 64-битные целые канонический protobuf-JSON печатает СТРОКОЙ: в
        // double, которым JSON представляет числа, они не помещаются без
        // потерь. Парсер принимает и число, но заготовка обязана показывать
        // ту форму, в которой тело приедет обратно при чтении.
        RuntimeType::I64 | RuntimeType::U64 => out.push_str("\"0\""),
        RuntimeType::F32 | RuntimeType::F64 => out.push_str("0.0"),
        RuntimeType::Bool => out.push_str("false"),
        // Идентификатор — единственное поле, для которого «значение по
        // умолчанию» заведомо не годится: пустой `order_id` не отправляют
        // никогда, а придумывать его руками — работа, которой видно, что она
        // машинная. Свежий UUID здесь ещё и полезнее готового: два сообщения,
        // отправленных подряд, не склеятся по ключу идемпотентности.
        RuntimeType::String if looks_like_id(name) => {
            out.push('"');
            out.push_str(&uuid::Uuid::new_v4().to_string());
            out.push('"');
        }
        // bytes в JSON — base64, и пустым байтам соответствует пустая строка.
        // Правило про идентификаторы сюда не распространяется намеренно: UUID
        // в текстовом виде — не base64, и такую заготовку приложение отвергло
        // бы само.
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
            if stack.iter().any(|seen| seen == m.full_name()) {
                out.push_str("{}");
            } else {
                // Без ветки: `oneof` вложенного сообщения свой, и выбирать её
                // здесь не из чего — назвать её пользователь мог только у корня.
                write_message(m, None, depth, stack, out);
            }
        }
    }
}

/// Имя значения, которое enum принимает по умолчанию.
///
/// В proto3 это всегда значение с номером 0, но в proto2 нумерация вольная —
/// поэтому если нуля нет, берём НАИМЕНЬШИЙ номер, а не первый объявленный:
/// порядок объявления в .proto ни к чему не обязывает, а младший номер — это
/// то, что в таком enum ближе всего по смыслу к «не указано». Пустой enum
/// синтаксис запрещает, так что запасной вариант нужен только чтобы не
/// паниковать.
fn zero_value_name(e: &EnumDescriptor) -> String {
    e.value_by_number(0)
        .or_else(|| e.values().min_by_key(|v| v.value()))
        .map(|v| v.name().to_string())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::proto::linked;
    use crate::schema::types::SchemaFile;

    /// Разбирает текст .proto во временном каталоге и достаёт из него message.
    fn message_of(
        name: &str,
        text: &str,
        message: &str,
    ) -> (std::path::PathBuf, MessageDescriptor) {
        let dir = std::env::temp_dir().join(format!("mikui-template-test-{name}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("t.proto"), text).unwrap();

        let file = SchemaFile {
            name: "t.proto".to_string(),
            source: dir.join("t.proto").to_string_lossy().into_owned(),
            auto: false,
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
            skeleton(&md, None),
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

        let text = skeleton(&md, None);
        // Круг проверяется по `event_id`: остальные поля заготовки нулевые, а
        // proto3 значения по умолчанию не печатает — они вернулись бы пустыми,
        // и такой круг ничего бы не доказал. Идентификатор же заполнен сам, и
        // ровно поэтому заготовку больше не нужно править руками.
        let ahead: serde_json::Value = serde_json::from_str(&text).unwrap();
        let sent = ahead["event_id"].as_str().unwrap().to_string();
        assert!(!sent.is_empty(), "идентификатор не заполнен:\n{text}");

        let parsed = protobuf_json_mapping::parse_dyn_from_str(&md, &text)
            .unwrap_or_else(|e| panic!("заготовка не разбирается: {e}\n{text}"));
        let bytes = parsed.write_to_bytes_dyn().unwrap();
        let decoded = crate::schema::proto::ProtoDecoder::new(md.clone())
            .decode(&bytes)
            .unwrap();
        // Круг замкнулся: то, что уедет в топик, вернётся оттуда тем же.
        let back: serde_json::Value = serde_json::from_str(&decoded).unwrap();
        assert_eq!(back["event_id"], sent.as_str(), "{decoded}");

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

        let text = skeleton(&md, None);
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

        let text = skeleton(&md, None);
        assert!(text.contains("\"text\""), "{text}");
        assert!(!text.contains("\"blob\""), "{text}");
        assert!(!text.contains("\"code\""), "{text}");
        protobuf_json_mapping::parse_dyn_from_str(&md, &text).unwrap();
        let _ = std::fs::remove_dir_all(dir);
    }

    /// Названная ветка раскрывается вместо первой по порядку объявления.
    ///
    /// Ради этого случая ветка и заведена: тип сообщения в топике — конверт с
    /// `oneof` внутри, а кладут в него конкретную ветку. Без выбора заготовка
    /// показала бы соседнюю, и заполнять пришлось бы то, чего не просили.
    #[test]
    fn a_named_branch_replaces_the_first_variant() {
        let (dir, md) = message_of(
            "oneof-branch",
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

        let text = skeleton(&md, Some("blob"));
        assert!(text.contains("\"blob\""), "{text}");
        assert!(!text.contains("\"text\""), "{text}");
        assert!(!text.contains("\"code\""), "{text}");
        protobuf_json_mapping::parse_dyn_from_str(&md, &text).unwrap();
        let _ = std::fs::remove_dir_all(dir);
    }

    /// Главная проверка отправки ветки.
    ///
    /// Выбранная ветка обязана уехать ПОЛЕМ ОБЁРТКИ, а не собственным типом: у
    /// потребителя, читающего топик как `PublicEvent`, иначе не будет ни одного
    /// поля, и он покажет пустое сообщение. Круг замыкается тем же декодером,
    /// которым приложение показывает прочитанное.
    #[test]
    fn a_branch_skeleton_round_trips_as_the_wrapper_it_belongs_to() {
        let (dir, md) = message_of(
            "oneof-roundtrip",
            r#"
                syntax = "proto3";
                package demo;
                message OrderChanged { string order_id = 1; }
                message WalletChanged { string wallet_id = 1; }
                message PublicEvent {
                    oneof event {
                        OrderChanged order_changed = 1;
                        WalletChanged wallet_changed = 7;
                    }
                }
            "#,
            "demo.PublicEvent",
        );

        let text = skeleton(&md, Some("wallet_changed"));
        // Заготовка — конверт целиком, с выбранной веткой внутри.
        assert!(text.contains("\"wallet_changed\": {"), "{text}");
        assert!(!text.contains("\"order_changed\""), "{text}");

        // Ветка для проверки заполняется своим значением: нулевое у неё не
        // печатается, и круг на пустом теле ничего бы не доказал. Через разбор
        // JSON, а не подстановкой текста: `wallet_id` — идентификатор, и
        // заготовка подставила туда UUID, а не пустую строку.
        let mut filled: serde_json::Value = serde_json::from_str(&text).unwrap();
        filled["wallet_changed"]["wallet_id"] = "w-1".into();
        let filled = filled.to_string();
        let parsed = protobuf_json_mapping::parse_dyn_from_str(&md, &filled)
            .unwrap_or_else(|e| panic!("заготовка не разбирается: {e}\n{filled}"));
        let bytes = parsed.write_to_bytes_dyn().unwrap();

        // Кодировали обёрткой: читаем её же — и находим поле ВНУТРИ конверта.
        let decoded = crate::schema::proto::ProtoDecoder::new(md.clone())
            .decode(&bytes)
            .unwrap();
        let back: serde_json::Value = serde_json::from_str(&decoded).unwrap();
        assert_eq!(back["wallet_changed"]["wallet_id"], "w-1", "{decoded}");
        assert!(back.get("order_changed").is_none(), "{decoded}");

        // А теперь тот самый баг: то же тело, закодированное СОБСТВЕННЫМ типом
        // ветки, — как оно уезжало бы без обёртки. Номера полей у веток свои, и
        // конверт читает такие байты как чужое поле: отправителем заявлен
        // `wallet_changed`, а приезжает либо отказ разбора, либо другая ветка.
        // Ветка, которую положили, не приходит НИКОГДА — потребитель не ошибётся
        // заметно, он просто не увидит отправленного сообщения.
        let branch = md
            .file_descriptor()
            .message_by_full_name(".demo.WalletChanged")
            .unwrap();
        let unwrapped = protobuf_json_mapping::parse_dyn_from_str(&branch, r#"{"wallet_id": "w-1"}"#)
            .unwrap()
            .write_to_bytes_dyn()
            .unwrap();
        let as_wrapper = crate::schema::proto::ProtoDecoder::new(md)
            .decode(&unwrapped)
            .ok()
            .and_then(|json| serde_json::from_str::<serde_json::Value>(&json).ok());
        assert!(
            as_wrapper
                .as_ref()
                .is_none_or(|v| v["wallet_changed"].is_null()),
            "тело без обёртки не должно дойти как отправленная ветка: {as_wrapper:?}"
        );

        let _ = std::fs::remove_dir_all(dir);
    }

    /// Ветка — только у корня. Вложенное сообщение со своим `oneof` обязано
    /// получить первый вариант, а не ветку, названную для конверта: одинаковые
    /// имена полей у двух типов — обычное дело, и совпадение по имени молча
    /// раскрыло бы не то поле.
    #[test]
    fn a_branch_does_not_reach_into_nested_messages() {
        let (dir, md) = message_of(
            "oneof-branch-nested",
            r#"
                syntax = "proto3";
                package demo;
                message Inner {
                    oneof kind { string text = 1; string blob = 2; }
                }
                message Outer {
                    Inner inner = 1;
                    oneof body { string text = 2; string blob = 3; }
                }
            "#,
            "demo.Outer",
        );

        let text = skeleton(&md, Some("blob"));
        assert!(text.contains("\"inner\": {"), "{text}");
        // У корня раскрыта названная ветка...
        let parsed: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(parsed["blob"], "", "{text}");
        assert!(parsed.get("text").is_none(), "{text}");
        // ...а у вложенного — по-прежнему первый вариант по объявлению. Поле с
        // тем же именем `blob` внутри `Inner` осталось закрытым.
        assert_eq!(parsed["inner"]["text"], "", "{text}");
        assert!(parsed["inner"].get("blob").is_none(), "{text}");

        let _ = std::fs::remove_dir_all(dir);
    }

    /// У message бывает несколько `oneof`, и выбор ветки в одном не должен
    /// глушить соседние группы: ветка выбирает вариант в СВОЁМ `oneof`, а не
    /// отменяет остальные.
    #[test]
    fn a_branch_leaves_other_oneofs_alone() {
        let (dir, md) = message_of(
            "oneof-several",
            r#"
                syntax = "proto3";
                package demo;
                message Event {
                    oneof what { string click = 1; string view = 2; }
                    oneof how { string device = 3; string referrer = 4; }
                }
            "#,
            "demo.Event",
        );

        let text = skeleton(&md, Some("view"));
        let parsed: serde_json::Value = serde_json::from_str(&text).unwrap();
        // В своём `oneof` — названная ветка...
        assert_eq!(parsed["view"], "", "{text}");
        assert!(parsed.get("click").is_none(), "{text}");
        // ...а соседняя группа по-прежнему даёт первый свой вариант, а не
        // пропадает целиком.
        assert_eq!(parsed["device"], "", "{text}");
        assert!(parsed.get("referrer").is_none(), "{text}");

        protobuf_json_mapping::parse_dyn_from_str(&md, &text).unwrap();
        let _ = std::fs::remove_dir_all(dir);
    }

    /// Идентификаторы узнаются по имени и заполняются каждый своим UUID.
    #[test]
    fn identifier_fields_get_a_fresh_uuid_each() {
        let (dir, md) = message_of(
            "ids",
            r#"
                syntax = "proto3";
                package demo;
                message Order {
                    string id = 1;
                    string order_id = 2;
                    string client_uid = 3;
                    string request_uuid = 4;
                    string ID = 5;
                    // Не идентификаторы: имя не подходит, тип не подходит,
                    // поле повторяющееся.
                    string identity = 6;
                    string valid = 7;
                    int64 sequence_id = 8;
                    bytes trace_id = 9;
                    repeated string tag_id = 10;
                }
            "#,
            "demo.Order",
        );

        let text = skeleton(&md, None);
        let value: serde_json::Value = serde_json::from_str(&text).unwrap();

        let ids = ["id", "order_id", "client_uid", "request_uuid", "ID"];
        let mut seen = std::collections::HashSet::new();
        for field in ids {
            let got = value[field].as_str().unwrap();
            uuid::Uuid::parse_str(got).unwrap_or_else(|e| panic!("{field} — не UUID: {got} ({e})"));
            // Один UUID на все поля значил бы, что сообщение ссылается само на
            // себя пятью разными полями, — а это почти наверняка не то, что
            // отправляют.
            assert!(seen.insert(got.to_string()), "{field} повторяет чужой UUID");
        }

        assert_eq!(value["identity"], "", "{text}");
        assert_eq!(value["valid"], "", "{text}");
        // Тип важнее имени: у 64-битного целого нулевое значение — строка
        // `"0"`, у bytes — пустой base64, и UUID не лёг бы ни туда, ни туда.
        assert_eq!(value["sequence_id"], "0", "{text}");
        assert_eq!(value["trace_id"], "", "{text}");
        assert_eq!(value["tag_id"], serde_json::json!([]), "{text}");

        // Заготовка обязана оставаться отправляемой.
        protobuf_json_mapping::parse_dyn_from_str(&md, &text).unwrap();
        let _ = std::fs::remove_dir_all(dir);
    }

    /// Заготовка одного и того же типа каждый раз новая — иначе «свежий UUID»
    /// был бы просто константой, зашитой в первый вызов.
    #[test]
    fn two_skeletons_of_one_type_differ_by_their_identifiers() {
        let (dir, md) = message_of(
            "ids-vary",
            r#"
                syntax = "proto3";
                package demo;
                message Ref { string ref_id = 1; }
            "#,
            "demo.Ref",
        );
        assert_ne!(skeleton(&md, None), skeleton(&md, None));
        let _ = std::fs::remove_dir_all(dir);
    }

    /// Порядок объявления в .proto ни к чему не обязывает — «по умолчанию» у
    /// enum задаёт номер.
    #[test]
    fn an_enum_without_a_zero_falls_back_to_its_smallest_number() {
        let (dir, md) = message_of(
            "enum-min",
            r#"
                syntax = "proto2";
                package demo;
                enum Direction {
                    DIRECTION_SELL = 7;
                    DIRECTION_UNSPECIFIED = 2;
                    DIRECTION_BUY = 4;
                }
                message Trade { optional Direction direction = 1; }
            "#,
            "demo.Trade",
        );

        let text = skeleton(&md, None);
        assert!(
            text.contains("\"direction\": \"DIRECTION_UNSPECIFIED\""),
            "{text}"
        );
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
        assert_eq!(skeleton(&md, None), "{}");
        let _ = std::fs::remove_dir_all(dir);
    }
}
