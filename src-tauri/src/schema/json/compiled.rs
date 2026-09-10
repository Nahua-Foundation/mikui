//! Разобранная JSON Schema: то, чем проверяют отправляемое и по чему строят
//! заготовку.
//!
//! Аналог `avro::Linked` и `proto::linked::Linked`, только заметно проще, и
//! разница тут принципиальная. Тело Avro и protobuf без схемы не прочитать
//! вовсе — там нет ни имён полей, ни границ значений. Тело JSON Schema читается
//! И БЕЗ НЕЁ: за confluent-заголовком лежит обычный JSON. Поэтому схема здесь
//! нужна ровно двум вещам — проверке при отправке и заготовке тела, — а на пути
//! показа таблицы её нет совсем.
//!
//! Из этого следует и то, чего здесь НЕТ: списка enum-значений. В JSON Schema
//! `enum` — это перечень допустимых литералов (`["RUB", 1, null]`), а не
//! именованные символы, как в Avro и protobuf. Красить их как enum значило бы
//! красить обычные строки, которые просто оказались в белом списке.

use serde_json::Value;

pub struct Compiled {
    /// Сама схема. Хранится рядом с валидатором, потому что заготовку тела
    /// строят обходом схемы, а из скомпилированного валидатора её не достать.
    schema: Value,
    validator: jsonschema::Validator,
}

impl Compiled {
    /// Разбирает и компилирует схему.
    ///
    /// `references` — тексты схем, на которые ссылается эта; реестр отдаёт их
    /// отдельным полем. Они складываются в `$defs` под своими именами: именно
    /// этим именем (`name` в ответе реестра — как правило, URI из `$id`) на них
    /// и ссылается `$ref` внутри основной схемы.
    pub fn parse(text: &str, references: &[String]) -> Result<Self, String> {
        let mut schema: Value = serde_json::from_str(text)
            .map_err(|e| format!("the JSON schema is not valid JSON: {e}"))?;
        if !references.is_empty() {
            embed_references(&mut schema, references)?;
        }

        let validator = jsonschema::Validator::new(&schema)
            .map_err(|e| format!("can't compile the JSON schema: {e}"))?;
        Ok(Self { schema, validator })
    }

    pub fn schema(&self) -> &Value {
        &self.schema
    }

    /// Проверяет тело по схеме.
    ///
    /// Сообщается ПЕРВАЯ ошибка, а не все: форма отправки показывает одну строку
    /// под полем ввода, и вываливать в неё десяток претензий к одному телу
    /// значит не сказать ничего. Путь до места вписан в текст — без него
    /// «строка не подходит» на схеме в сотню полей бесполезно.
    pub fn validate(&self, value: &Value) -> Result<(), String> {
        match self.validator.validate(value) {
            Ok(()) => Ok(()),
            Err(error) => {
                let path = error.instance_path().to_string();
                Err(match path.is_empty() {
                    true => error.to_string(),
                    false => format!("{path}: {error}"),
                })
            }
        }
    }
}

/// Складывает тексты ссылок в `$defs` основной схемы.
///
/// Своего механизма разрешения `$ref` по сети мы не включаем намеренно (см.
/// `Cargo.toml`): схема из реестра не должна заставлять приложение ходить по
/// произвольным URL. Всё, на что она ссылается, реестр уже прислал сам — надо
/// лишь положить это туда, где `$ref` их найдёт.
fn embed_references(schema: &mut Value, references: &[String]) -> Result<(), String> {
    let parsed: Vec<Value> = references
        .iter()
        .map(|text| {
            serde_json::from_str(text)
                .map_err(|e| format!("a referenced JSON schema is not valid JSON: {e}"))
        })
        .collect::<Result<_, String>>()?;

    let Some(object) = schema.as_object_mut() else {
        // Схема-булев (`true`/`false`) — законная JSON Schema, но ссылаться
        // ей не на что: класть `$defs` некуда, и это не ошибка.
        return Ok(());
    };
    let defs = object
        .entry("$defs")
        .or_insert_with(|| Value::Object(serde_json::Map::new()));
    let Some(defs) = defs.as_object_mut() else {
        return Err("the schema has a $defs that is not an object".to_string());
    };

    for (index, reference) in parsed.into_iter().enumerate() {
        // Ключ — `$id` схемы, если он есть: именно им её и адресует `$ref`.
        // Без него имя роли не играет, но занять уникальное место всё равно
        // надо, иначе вторая безымянная ссылка затрёт первую.
        let key = reference
            .get("$id")
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| format!("reference-{index}"));
        defs.entry(key).or_insert(reference);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const ORDER: &str = r#"{
        "type": "object",
        "properties": {
            "id": {"type": "string"},
            "total": {"type": "integer"}
        },
        "required": ["id"]
    }"#;

    #[test]
    fn a_body_that_fits_the_schema_passes() {
        let compiled = Compiled::parse(ORDER, &[]).unwrap();
        let value = serde_json::json!({"id": "a-1", "total": 10});
        assert_eq!(compiled.validate(&value), Ok(()));
    }

    /// Ошибка обязана называть МЕСТО: на схеме в сотню полей «строка не
    /// подходит» без пути бесполезна.
    #[test]
    fn a_wrong_type_is_reported_with_its_path() {
        let compiled = Compiled::parse(ORDER, &[]).unwrap();
        let value = serde_json::json!({"id": "a-1", "total": "ten"});
        let error = compiled.validate(&value).unwrap_err();
        assert!(error.contains("/total"), "{error}");
    }

    #[test]
    fn a_missing_required_field_is_refused() {
        let compiled = Compiled::parse(ORDER, &[]).unwrap();
        let value = serde_json::json!({"total": 1});
        assert!(compiled.validate(&value).is_err());
    }

    #[test]
    fn a_schema_that_is_not_json_says_so() {
        // `unwrap_err` тут не годится: он требует `Debug` от значения в `Ok`, а
        // скомпилированный валидатор его не имеет и иметь не должен.
        let Err(error) = Compiled::parse("{not json", &[]) else {
            panic!("схема, которая не разбирается, обязана быть ошибкой");
        };
        assert!(error.contains("not valid JSON"), "{error}");
    }

    /// Ссылки приезжают отдельным полем ответа реестра и обязаны разрешаться
    /// БЕЗ выхода в сеть.
    #[test]
    fn a_reference_is_resolved_from_the_texts_the_registry_sent() {
        let money = r#"{
            "$id": "https://example.test/money.json",
            "type": "object",
            "properties": {"amount": {"type": "integer"}},
            "required": ["amount"]
        }"#;
        let order = r#"{
            "type": "object",
            "properties": {"total": {"$ref": "https://example.test/money.json"}},
            "required": ["total"]
        }"#;

        let compiled = Compiled::parse(order, &[money.to_string()]).unwrap();
        assert_eq!(
            compiled.validate(&serde_json::json!({"total": {"amount": 5}})),
            Ok(())
        );
        // И ссылка действительно РАБОТАЕТ, а не просто не мешает: тело, не
        // подходящее под неё, обязано быть отвергнуто.
        assert!(compiled
            .validate(&serde_json::json!({"total": {"amount": "five"}}))
            .is_err());
    }

    /// Схема-булев — законная JSON Schema. Не падать на ней важнее, чем
    /// что-либо в ней разрешать.
    #[test]
    fn a_boolean_schema_compiles_and_accepts_anything() {
        let compiled = Compiled::parse("true", &["{}".to_string()]).unwrap();
        assert_eq!(compiled.validate(&serde_json::json!({"any": 1})), Ok(()));
    }
}
