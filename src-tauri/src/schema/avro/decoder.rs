//! Декодер Avro: что делать с телом, которое пришло из Kafka.
//!
//! Два источника схемы, и они не исключают друг друга:
//!   * закреплённая — локальные .avsc или выбранный subject реестра. Ею
//!     разбирается голый datum, у которого никаких подсказок в теле нет;
//!   * реестр по id из заголовка confluent-формата. Она сильнее закреплённой:
//!     id назвал тот, кто это сообщение писал, а закреплённая — наша догадка о
//!     топике в целом.
//!
//! Если не разобралось — ошибка касается ОДНОГО сообщения, а не топика. Тело
//! чужого формата посреди топика, пережившего смену контракта, — обычное дело,
//! и переставать показывать остальное из-за него нельзя.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::cache;
use super::linked::Linked;
use super::registry::Registry;
use super::wire::{framing, Framing};

pub struct AvroDecoder {
    /// Каталог настроек — под дисковый кэш схем. `None` там, где кэшу негде
    /// жить: в тестах и у подключения, заведённого прямо в форме.
    root: Option<PathBuf>,
    /// Схема, назначенная топику. `None` — топик читается только по id.
    pinned: Option<Arc<Linked>>,
    /// Адрес реестра и клиент к нему. Адрес отдельно, потому что он же ключ
    /// кэша: схемы разных реестров нумеруются независимо.
    registry: Option<(String, Arc<Registry>)>,
}

impl AvroDecoder {
    pub fn new(
        root: Option<&Path>,
        pinned: Option<Arc<Linked>>,
        registry: Option<(String, Arc<Registry>)>,
    ) -> Self {
        // Новый декодер — новая попытка достучаться до реестра: пользователь,
        // переоткрывший топик после того, как реестр подняли, вправе увидеть
        // тела, а не вчерашнюю ошибку из отрицательного кэша.
        cache::forget_failures();
        Self {
            root: root.map(Path::to_path_buf),
            pinned,
            registry,
        }
    }

    pub fn decode(&self, payload: &[u8]) -> Result<String, String> {
        self.render(payload).map(|(json, _)| json)
    }

    /// Тело вместе с символами enum ИМЕННО ТОЙ схемы, которой оно разобрано.
    ///
    /// Отдельным методом, а не всегда: в confluent-формате схема своя на каждое
    /// сообщение, поэтому список приходится отдавать вместе с телом — а список
    /// это `Vec<String>`, и платить за него на каждой строке таблицы не за что.
    /// Зовётся только для сообщения, которое действительно открыли.
    pub fn decode_with_enums(&self, payload: &[u8]) -> Result<(String, Vec<String>), String> {
        self.render(payload)
    }

    fn render(&self, payload: &[u8]) -> Result<(String, Vec<String>), String> {
        match framing(payload) {
            Framing::Confluent { id, datum } => match &self.registry {
                Some((url, registry)) => {
                    let linked = cache::by_id(self.root.as_deref(), registry, url, id)
                        .map_err(|e| format!("schema {id}: {e}"))?;
                    let json = linked.decode(datum)?;
                    Ok((compact(&json), linked.enum_values().to_vec()))
                }
                // Нулевой первый байт — ещё не доказательство: им начинается и
                // вполне законный голый datum (пустая строка, ноль, false).
                // Поэтому без реестра пробуем закреплённой схемой ЦЕЛИКОМ, а
                // не с пятого байта.
                None => self.decode_pinned(payload).map_err(|e| {
                    format!(
                        "{e} (the body looks like it carries schema id {id}, \
                         but no schema registry is configured for this cluster)"
                    )
                }),
            },
            // Контейнерный файл несёт схему в себе — ни реестр, ни настройки
            // топика для него не нужны.
            Framing::Container => container(payload).map(|json| (json, Vec::new())),
            Framing::SingleObject => Err(
                "the body uses Avro single-object encoding, which names its schema by a \
                 CRC-64 fingerprint; the registry cannot be searched by fingerprint, so \
                 there is nothing to decode it with"
                    .to_string(),
            ),
            Framing::Bare(datum) => self.decode_pinned(datum),
        }
    }

    fn decode_pinned(&self, datum: &[u8]) -> Result<(String, Vec<String>), String> {
        let linked = self.pinned.as_ref().ok_or(
            "no Avro schema for this topic — pick a subject or load an .avsc file in the \
             topic settings",
        )?;
        let json = linked.decode(datum)?;
        Ok((compact(&json), linked.enum_values().to_vec()))
    }
}

/// Разбирает контейнерный файл (OCF).
///
/// В топиках он редкость — контейнер придуман для файлов, а не для сообщений,
/// — но встречается там, где в Kafka перекладывают готовые выгрузки, и стоит
/// одной ветки. Записей в нём может быть несколько; тогда наружу уезжает
/// массив, а не первая попавшаяся.
fn container(payload: &[u8]) -> Result<String, String> {
    let reader = apache_avro::Reader::new(std::io::Cursor::new(payload))
        .map_err(|e| format!("can't read the Avro container: {e}"))?;

    let mut values = Vec::new();
    for value in reader {
        let value = value.map_err(|e| format!("can't read a record from the container: {e}"))?;
        values.push(
            serde_json::Value::try_from(value)
                .map_err(|e| format!("can't render a container record as JSON: {e}"))?,
        );
    }

    let json = match values.len() {
        1 => values.remove(0),
        _ => serde_json::Value::Array(values),
    };
    Ok(json.to_string())
}

/// Компактный JSON — строка таблицы обязана остаться одной строкой. Отступы
/// расставит модалка, тем же кодом, каким она давно печатает JSON-топики.
fn compact(json: &serde_json::Value) -> String {
    json.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    const EVENT: &str = r#"{
        "type": "record", "name": "Event", "namespace": "demo",
        "fields": [
            {"name": "id", "type": "string"},
            {"name": "kind", "type": {"type": "enum", "name": "Kind", "symbols": ["UNKNOWN", "CLICK"]}}
        ]
    }"#;

    fn pinned() -> Arc<Linked> {
        Arc::new(Linked::parse_files(&[EVENT.to_string()], None).unwrap())
    }

    fn body() -> Vec<u8> {
        let json: serde_json::Value =
            serde_json::from_str(r#"{"id": "a-1", "kind": "CLICK"}"#).unwrap();
        pinned().encode(json).unwrap()
    }

    #[test]
    fn a_bare_datum_is_decoded_by_the_pinned_schema() {
        let decoder = AvroDecoder::new(None, Some(pinned()), None);
        let json = decoder.decode(&body()).unwrap();
        assert_eq!(json, r#"{"id":"a-1","kind":"CLICK"}"#);
        // Компактный: строка таблицы обязана остаться одной строкой.
        assert!(!json.contains('\n'));
    }

    #[test]
    fn enum_symbols_travel_with_the_body() {
        let decoder = AvroDecoder::new(None, Some(pinned()), None);
        let (_, symbols) = decoder.decode_with_enums(&body()).unwrap();
        assert_eq!(symbols, ["CLICK", "UNKNOWN"]);
    }

    /// Без схемы и без реестра декодировать нечем — и сказать об этом надо так,
    /// чтобы было понятно, что делать дальше.
    #[test]
    fn without_a_schema_the_message_says_where_to_get_one() {
        let decoder = AvroDecoder::new(None, None, None);
        let error = decoder.decode(&body()).unwrap_err();
        assert!(error.contains("topic settings"), "{error}");
    }

    /// Confluent-заголовок без настроенного реестра: пробуем закреплённой
    /// схемой, но объясняем, что тело похоже на адресованное реестру.
    #[test]
    fn a_confluent_body_without_a_registry_explains_itself() {
        let decoder = AvroDecoder::new(None, Some(pinned()), None);
        let mut framed = vec![0x00, 0x00, 0x00, 0x00, 0x2a];
        framed.extend_from_slice(&body());

        let error = decoder.decode(&framed).unwrap_err();
        assert!(error.contains("schema id 42"), "{error}");
        assert!(error.contains("no schema registry is configured"), "{error}");
    }

    #[test]
    fn single_object_encoding_is_refused_with_a_reason() {
        let decoder = AvroDecoder::new(None, Some(pinned()), None);
        let mut framed = vec![0xc3, 0x01, 1, 2, 3, 4, 5, 6, 7, 8];
        framed.extend_from_slice(&body());

        let error = decoder.decode(&framed).unwrap_err();
        assert!(error.contains("fingerprint"), "{error}");
    }

    /// Контейнер несёт схему в себе, и декодировать его можно вообще без
    /// настроек топика.
    #[test]
    fn a_container_file_is_decoded_by_its_own_header() {
        let schema = apache_avro::Schema::parse_str(EVENT).unwrap();
        let mut writer = apache_avro::Writer::new(&schema, Vec::new()).unwrap();
        let json: serde_json::Value =
            serde_json::from_str(r#"{"id": "a-1", "kind": "CLICK"}"#).unwrap();
        let value = apache_avro::types::Value::try_from(json)
            .unwrap()
            .resolve(&schema)
            .unwrap();
        writer.append_value(value).unwrap();
        let bytes = writer.into_inner().unwrap();

        let decoder = AvroDecoder::new(None, None, None);
        assert_eq!(decoder.decode(&bytes).unwrap(), r#"{"id":"a-1","kind":"CLICK"}"#);
    }
}
