//! Разобранная Avro-схема вместе со всем, на что она ссылается.
//!
//! У Avro нет `import`: схема — это один JSON, а ссылка на чужой именованный
//! тип выглядит просто строкой с его полным именем. Разрешать такие ссылки
//! обязан тот, кто собирает схему, — и делает он это, подавая парсеру ВЕСЬ
//! набор сразу. Отсюда и этот тип: держать разобранную схему в отрыве от её
//! зависимостей бессмысленно, разобрать по ней тело всё равно не выйдет.
//!
//! Оба источника схемы сводятся сюда же:
//!   * локальные .avsc — `parse_files`, корень выбирается по имени;
//!   * реестр — `parse_with_refs`, корень известен, ссылки догружены по
//!     `references` из ответа.

use std::collections::HashSet;

use apache_avro::reader::datum::GenericDatumReader;
use apache_avro::schema::{Name, Schema};
use apache_avro::types::Value;
use apache_avro::writer::datum::GenericDatumWriter;

/// Схема, которой разбирается тело, плюс всё, на что она ссылается.
///
/// Корень хранится индексом в общем списке, а не отдельным полем: в списке он и
/// так лежит, а `from_avro_datum_schemata` требует и его, и остальных, причём
/// ссылками на элементы одного набора.
#[derive(Debug)]
pub struct Linked {
    schemas: Vec<Schema>,
    root: usize,
    /// Символы всех enum, до которых можно дотянуться из корня. Считаются один
    /// раз при сборке: список не зависит от конкретного тела, а модалке он
    /// нужен на каждое открытие сообщения.
    enum_values: Vec<String>,
}

impl Linked {
    /// Разбирает набор .avsc и берёт корнем тип с указанным именем.
    ///
    /// Разбираются ВСЕ файлы разом, даже если нужен один: ссылки между ними
    /// иначе не разрешатся, а файл, валидный сам по себе, но конфликтующий с
    /// соседом по имени, — такая же поломка набора, как синтаксическая.
    pub fn parse_files(texts: &[String], root: Option<&str>) -> Result<Self, String> {
        if texts.is_empty() {
            return Err("no .avsc files".to_string());
        }
        let schemas = Schema::parse_list(texts).map_err(|e| format!("{e}"))?;
        let root = match root {
            Some(name) => index_of(&schemas, name)
                .ok_or_else(|| format!("record {name} is not in the loaded .avsc files"))?,
            // Единственная схема подставляется сама — выбирать не из чего, а
            // требовать выбор значило бы требовать лишний клик. Та же логика,
            // что у `settle_message` для protobuf.
            None if schemas.len() == 1 => 0,
            None => return Err("pick the record this topic carries".to_string()),
        };
        Ok(Self::new(schemas, root))
    }

    /// Собирает схему из реестра: корень плюс тексты схем, на которые он
    /// ссылается через `references`.
    pub fn parse_with_refs(text: &str, refs: &[String]) -> Result<Self, String> {
        let (root, rest) =
            Schema::parse_str_with_list(text, refs).map_err(|e| format!("{e}"))?;
        let mut schemas = Vec::with_capacity(rest.len() + 1);
        schemas.push(root);
        schemas.extend(rest);
        Ok(Self::new(schemas, 0))
    }

    fn new(schemas: Vec<Schema>, root: usize) -> Self {
        let mut values = HashSet::new();
        let mut seen = HashSet::new();
        collect_enum_values(&schemas[root], &schemas, &mut seen, &mut values);
        let mut enum_values: Vec<String> = values.into_iter().collect();
        enum_values.sort_unstable();
        Self {
            schemas,
            root,
            enum_values,
        }
    }

    /// Полные имена всех именованных типов набора, по алфавиту — это и есть
    /// содержимое выпадающего списка «чем декодировать».
    pub fn names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.schemas.iter().filter_map(full_name).collect();
        names.sort_unstable();
        names.dedup();
        names
    }

    pub fn root_name(&self) -> Option<String> {
        full_name(&self.schemas[self.root])
    }

    pub fn enum_values(&self) -> &[String] {
        &self.enum_values
    }

    /// Разбирает одиночный datum — то, что лежит в сообщении Kafka.
    ///
    /// Именно datum, а не контейнерный файл: заголовка со схемой в теле нет,
    /// схему мы приносим свои. Читательская схема не задаётся (`None`) —
    /// показываем ровно то, что записал писатель, без разрешения версий: любое
    /// приведение здесь молча меняло бы данные, которые пришли смотреть.
    pub fn decode(&self, bytes: &[u8]) -> Result<serde_json::Value, String> {
        let mut cursor = std::io::Cursor::new(bytes);
        let value = GenericDatumReader::builder(&self.schemas[self.root])
            .writer_schemata(self.schemata())
            .and_then(|b| b.build())
            .and_then(|reader| reader.read_value(&mut cursor))
            .map_err(|e| format!("can't decode as {}: {e}", self.describe_root()))?;

        // Хвост после разобранного значения — верный признак, что схема не та:
        // Avro не самоописателен, и чужая схема почти всегда «разбирает» тело
        // до конца по-своему, оставляя лишние байты. Молча их проглотить
        // значило бы показать правдоподобный мусор.
        let read = cursor.position() as usize;
        if read < bytes.len() {
            return Err(format!(
                "decoded as {} but {} of {} bytes are left over — most likely a different schema",
                self.describe_root(),
                bytes.len() - read,
                bytes.len()
            ));
        }

        // Вторая половина той же защиты, и без неё первая дырява.
        //
        // apache-avro на КОНЧИВШИХСЯ байтах не ошибается: он отдаёт `Null`,
        // сдвинув курсор до конца входа. То есть тело, которое схеме не
        // подошло, доезжало бы до UI словом «null» — тем самым правдоподобным
        // мусором, ради недопущения которого сделана проверка хвоста, только
        // хвоста после такого разбора не остаётся и она молчит.
        //
        // Признак надёжный: схема, которой null не выразить, значения null дать
        // не может. Union с null-веткой и сам null — могут, и их мы не трогаем:
        // там null законен.
        if matches!(value, Value::Null) && !self.root_holds_null() {
            return Err(format!(
                "can't decode as {}: the body ran out mid-value — most likely a different \
                 schema, or not Avro at all",
                self.describe_root()
            ));
        }

        serde_json::Value::try_from(value)
            .map_err(|e| format!("can't render {} as JSON: {e}", self.describe_root()))
    }

    /// Может ли корневая схема законно дать значение null.
    ///
    /// Только про сам корень: null внутри записи приезжает полем, а не всем
    /// значением, и на корневой `Value::Null` не похож.
    fn root_holds_null(&self) -> bool {
        match &self.schemas[self.root] {
            Schema::Null => true,
            Schema::Union(union) => union.variants().iter().any(|v| matches!(v, Schema::Null)),
            _ => false,
        }
    }

    /// Кодирует JSON в avro-datum по этой схеме.
    ///
    /// `resolve` и есть проверка: он приводит свободный JSON к схеме и на
    /// несоответствии объясняет, чем именно оно вызвано, — этот текст и уезжает
    /// в подсказку под полем ввода.
    pub fn encode(&self, json: serde_json::Value) -> Result<Vec<u8>, String> {
        let value = Value::try_from(json).map_err(|e| format!("{e}"))?;
        let resolved = value
            .resolve_schemata(&self.schemas[self.root], self.schemata())
            .map_err(|e| format!("can't encode as {}: {e}", self.describe_root()))?;
        GenericDatumWriter::builder(&self.schemas[self.root])
            .schemata(self.schemata())
            .and_then(|b| b.build())
            .and_then(|writer| writer.write_value_to_vec(resolved))
            .map_err(|e| format!("can't serialize {}: {e}", self.describe_root()))
    }

    pub fn root(&self) -> &Schema {
        &self.schemas[self.root]
    }

    /// Весь набор — нужен там, где приходится разрешать `Schema::Ref` руками:
    /// ссылка несёт только имя, а сам тип лежит рядом (см. `template`).
    pub fn all(&self) -> &[Schema] {
        &self.schemas
    }

    /// Все схемы набора ссылками — в таком виде их принимают `*_schemata`.
    fn schemata(&self) -> Vec<&Schema> {
        self.schemas.iter().collect()
    }

    /// Как назвать корень в сообщении об ошибке. У безымянных схем (топик с
    /// телом из одной строки — вполне законный Avro) имени нет вовсе.
    fn describe_root(&self) -> String {
        self.root_name().unwrap_or_else(|| "the schema".to_string())
    }
}

/// Полное имя именованной схемы. `None` у примитивов и контейнеров: у них
/// имени нет, и в списке «чем декодировать» им делать нечего.
fn full_name(schema: &Schema) -> Option<String> {
    named(schema).map(|n| n.fullname(None))
}

fn named(schema: &Schema) -> Option<&Name> {
    match schema {
        Schema::Record(r) => Some(&r.name),
        Schema::Enum(e) => Some(&e.name),
        Schema::Fixed(f) => Some(&f.name),
        _ => None,
    }
}

fn index_of(schemas: &[Schema], name: &str) -> Option<usize> {
    schemas
        .iter()
        .position(|s| full_name(s).is_some_and(|n| n == name))
}

/// Обходит схему, собирая символы всех встреченных enum.
///
/// `seen` защищает от бесконечной рекурсии на записи, которая (прямо или через
/// цепочку полей) ссылается сама на себя, — для Avro это обычное дело: дерево
/// или связный список иначе не выразить.
fn collect_enum_values(
    schema: &Schema,
    all: &[Schema],
    seen: &mut HashSet<String>,
    out: &mut HashSet<String>,
) {
    if let Some(name) = full_name(schema) {
        if !seen.insert(name) {
            return;
        }
    }
    match schema {
        Schema::Enum(e) => out.extend(e.symbols.iter().cloned()),
        Schema::Record(r) => {
            for field in &r.fields {
                collect_enum_values(&field.schema, all, seen, out);
            }
        }
        Schema::Array(a) => collect_enum_values(&a.items, all, seen, out),
        Schema::Map(m) => collect_enum_values(&m.types, all, seen, out),
        Schema::Union(u) => {
            for variant in u.variants() {
                collect_enum_values(variant, all, seen, out);
            }
        }
        // Ссылка на именованный тип: сам тип лежит в общем наборе, и без этой
        // ветки enum за ссылкой остался бы неизвестен.
        Schema::Ref { name } => {
            let wanted = name.fullname(None);
            if let Some(target) = all.iter().find(|s| full_name(s).is_some_and(|n| n == wanted)) {
                collect_enum_values(target, all, seen, out);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const EVENT: &str = r#"{
        "type": "record", "name": "Event", "namespace": "demo",
        "fields": [
            {"name": "id", "type": "string"},
            {"name": "kind", "type": {"type": "enum", "name": "Kind", "symbols": ["UNKNOWN", "CLICK"]}},
            {"name": "tags", "type": {"type": "array", "items": "string"}},
            {"name": "note", "type": ["null", "string"]}
        ]
    }"#;

    fn event() -> Linked {
        Linked::parse_files(&[EVENT.to_string()], None).unwrap()
    }

    /// Кончившиеся байты apache-avro считает не ошибкой, а значением `Null`, и
    /// курсор при этом доезжает до конца — проверка хвоста такое пропускает.
    /// Без отдельной защиты тело чужого формата доезжало бы до UI словом
    /// «null», то есть ровно тем правдоподобным мусором, которого мы избегаем.
    #[test]
    fn a_body_that_ran_out_mid_value_is_an_error_and_not_the_word_null() {
        let string = Linked::parse_with_refs("\"string\"", &[]).unwrap();
        // 'T' = 0x54, как зигзаг-длина это 42 — а байт после него всего три.
        let error = string.decode(b"TCBR").unwrap_err();
        assert!(error.contains("ran out"), "{error}");

        // А то, что схеме подошло, по-прежнему разбирается.
        assert_eq!(string.decode(b"\x08TCBR").unwrap(), "TCBR");
    }

    /// Схема, которой null выразим, вправе дать null — и запрещать ей это
    /// нельзя: у union'а с null-веткой это законное значение.
    #[test]
    fn a_nullable_schema_may_legitimately_decode_to_null() {
        let nullable = Linked::parse_with_refs(r#"["null","string"]"#, &[]).unwrap();
        // Нулевой байт — индекс первой ветки union'а, то есть null.
        assert_eq!(nullable.decode(b"\x00").unwrap(), serde_json::Value::Null);
    }

    #[test]
    fn a_single_schema_becomes_the_root_without_asking() {
        let linked = event();
        assert_eq!(linked.root_name().as_deref(), Some("demo.Event"));
    }

    /// Ради этого случая и заведён весь `Linked`: ссылка на чужой тип
    /// разрешается только тогда, когда парсеру подали весь набор сразу.
    #[test]
    fn a_reference_between_files_resolves() {
        let money = r#"{
            "type": "record", "name": "Money", "namespace": "common",
            "fields": [{"name": "amount", "type": "long"}]
        }"#;
        let order = r#"{
            "type": "record", "name": "Order", "namespace": "orders",
            "fields": [{"name": "total", "type": "common.Money"}]
        }"#;

        let linked = Linked::parse_files(
            &[order.to_string(), money.to_string()],
            Some("orders.Order"),
        )
        .unwrap();
        assert_eq!(linked.names(), ["common.Money", "orders.Order"]);
        assert_eq!(linked.root_name().as_deref(), Some("orders.Order"));
    }

    #[test]
    fn a_missing_reference_is_reported_not_swallowed() {
        let order = r#"{
            "type": "record", "name": "Order", "namespace": "orders",
            "fields": [{"name": "total", "type": "common.Money"}]
        }"#;
        let error = Linked::parse_files(&[order.to_string()], None).unwrap_err();
        assert!(error.contains("Money"), "ошибка должна называть недостающий тип: {error}");
    }

    #[test]
    fn several_schemas_without_a_choice_are_refused() {
        let a = r#"{"type": "record", "name": "A", "fields": []}"#;
        let b = r#"{"type": "record", "name": "B", "fields": []}"#;
        // Подсовывать первый попавшийся нельзя: разобралось бы молча и не тем.
        assert!(Linked::parse_files(&[a.to_string(), b.to_string()], None).is_err());
    }

    #[test]
    fn round_trip_through_json_keeps_every_field() {
        let linked = event();
        let json: serde_json::Value = serde_json::from_str(
            r#"{"id": "a-1", "kind": "CLICK", "tags": ["x", "y"], "note": null}"#,
        )
        .unwrap();

        let bytes = linked.encode(json.clone()).unwrap();
        let back = linked.decode(&bytes).unwrap();
        assert_eq!(back, json);
    }

    /// Порядок полей в разобранном теле — порядок объявления в .avsc, а не
    /// алфавит: ради этого в `serde_json` и включена фича `preserve_order`.
    #[test]
    fn decoded_fields_keep_declaration_order() {
        let linked = event();
        let json: serde_json::Value =
            serde_json::from_str(r#"{"id": "a", "kind": "CLICK", "tags": [], "note": null}"#)
                .unwrap();
        let bytes = linked.encode(json).unwrap();
        let text = linked.decode(&bytes).unwrap().to_string();
        assert!(text.starts_with(r#"{"id":"a","kind":"CLICK","tags":[]"#), "{text}");
    }

    /// Enum печатается обычной строкой в кавычках, и отличить его от значения
    /// поля `string` можно только по схеме — за этим список и собирается.
    #[test]
    fn enum_symbols_come_from_the_schema_not_the_payload() {
        assert_eq!(event().enum_values(), ["CLICK", "UNKNOWN"]);
    }

    #[test]
    fn enum_symbols_are_collected_through_references() {
        let tier = r#"{"type": "enum", "name": "Tier", "namespace": "demo", "symbols": ["BASIC", "GOLD"]}"#;
        let customer = r#"{
            "type": "record", "name": "Customer", "namespace": "demo",
            "fields": [{"name": "tier", "type": "demo.Tier"}]
        }"#;
        let linked =
            Linked::parse_files(&[customer.to_string(), tier.to_string()], Some("demo.Customer"))
                .unwrap();
        assert_eq!(linked.enum_values(), ["BASIC", "GOLD"]);
    }

    /// Главная защита от «схема не та»: чужая схема разбирает чужие байты как
    /// придётся, и без проверки хвоста наружу уехал бы правдоподобный мусор.
    #[test]
    fn leftover_bytes_are_treated_as_a_wrong_schema() {
        let linked = event();
        let json: serde_json::Value =
            serde_json::from_str(r#"{"id": "a", "kind": "CLICK", "tags": [], "note": null}"#)
                .unwrap();
        let mut bytes = linked.encode(json).unwrap();
        bytes.extend_from_slice(&[0x01, 0x02, 0x03]);

        let error = linked.decode(&bytes).unwrap_err();
        assert!(error.contains("left over"), "{error}");
    }

    #[test]
    fn a_body_that_does_not_fit_the_schema_names_the_schema_it_tried() {
        let linked = event();
        // Первое поле — строка; длина обещает больше байт, чем есть.
        let error = linked.decode(&[0x7f, 0x61]).unwrap_err();
        assert!(error.contains("demo.Event"), "{error}");
    }

    #[test]
    fn json_that_does_not_fit_the_schema_is_refused_with_a_reason() {
        let linked = event();
        // `kind` не из символов enum.
        let json: serde_json::Value =
            serde_json::from_str(r#"{"id": "a", "kind": "NOPE", "tags": [], "note": null}"#)
                .unwrap();
        assert!(linked.encode(json).is_err());
    }
}
