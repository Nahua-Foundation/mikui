//! Декодер JSON Schema: что делать с телом, которое пришло из Kafka.
//!
//! Самый простой из трёх, и по существу, а не по недоделанности. За
//! confluent-заголовком у этого формата лежит ОБЫЧНЫЙ JSON — тот же текст, что
//! и в топике без всякой схемы. Значит, чтобы его показать, схема не нужна
//! вовсе: достаточно снять пять байт заголовка. Ровно из-за них такой топик и
//! выглядел сломанным — `0x00` и четыре байта id ехали в UI управляющими
//! символами перед `{`.
//!
//! Поэтому декодер не хранит ничего и в реестр не ходит. Схема этому формату
//! нужна только на отправке (проверить тело и построить заготовку) — там она и
//! берётся, в `store::json_for_produce`.
//!
//! Тело здесь НЕ разбирается в `serde_json::Value` и не печатается заново.
//! Разбор на каждой видимой строке таблицы — это работа, которой у топика с
//! обычным JSON сегодня нет, и вводить её ради переставленных пробелов не за
//! чем: модалка всё равно раскладывает тело сама, а таблице нужны первые
//! двести символов.

use crate::schema::wire::{framing, Framing};

pub struct JsonDecoder;

impl JsonDecoder {
    pub fn new() -> Self {
        Self
    }

    pub fn decode(&self, payload: &[u8]) -> Result<String, String> {
        let body = match framing(payload) {
            // Заголовок снимаем, а id не проверяем: тело за ним самодостаточно.
            // Ходить в реестр ради «схема с таким id и правда есть» значило бы
            // поставить сеть на путь отрисовки таблицы ради ответа, которым мы
            // всё равно не воспользуемся.
            Framing::Confluent { datum, .. } => datum,
            // Ни контейнера, ни single-object у этого формата не бывает — это
            // аврошные способы приложить схему к телу. Что бы `framing` о них
            // ни решил, для нас всё остальное — тело как есть.
            _ => payload,
        };

        // Не UTF-8 — значит, и не JSON. Сказать об этом честнее, чем показать
        // строку из символов замены: у топика, объявленного JSON Schema, такое
        // тело означает, что формат назначен не тот.
        std::str::from_utf8(body)
            .map(str::to_string)
            .map_err(|e| format!("the body is not valid UTF-8, so it can't be JSON: {e}"))
    }
}

impl Default for JsonDecoder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Ровно тот баг, ради которого формат и заводился: пять байт заголовка
    /// ехали в UI перед телом.
    #[test]
    fn a_confluent_header_is_stripped() {
        let mut body = vec![0x00, 0x00, 0x00, 0x00, 0x2a];
        body.extend_from_slice(br#"{"id":"a-1"}"#);
        assert_eq!(JsonDecoder::new().decode(&body).unwrap(), r#"{"id":"a-1"}"#);
    }

    /// Топик от простого `JsonSerializer` заголовка не несёт, и тело у него
    /// начинается сразу.
    #[test]
    fn a_bare_body_passes_through_untouched() {
        let body = br#"{"id":"a-1"}"#;
        assert_eq!(JsonDecoder::new().decode(body).unwrap(), r#"{"id":"a-1"}"#);
    }

    /// Пробелы и переносы не трогаем: раскладывает тело модалка, а таблица
    /// схлопывает управляющие символы сама (`text::preview`).
    #[test]
    fn formatting_is_left_exactly_as_it_arrived() {
        let body = b"{\n  \"id\": \"a-1\"\n}";
        assert_eq!(
            JsonDecoder::new().decode(body).unwrap(),
            "{\n  \"id\": \"a-1\"\n}"
        );
    }

    #[test]
    fn a_body_that_is_not_utf8_says_so_instead_of_showing_replacement_characters() {
        let error = JsonDecoder::new().decode(&[0xff, 0xfe]).unwrap_err();
        assert!(error.contains("UTF-8"), "{error}");
    }

    /// Заголовок без тела — законно: схема, у которой всё по умолчанию,
    /// сериализуется в пустое тело.
    #[test]
    fn a_header_with_no_body_is_an_empty_string() {
        let body = [0x00, 0x00, 0x00, 0x00, 0x01];
        assert_eq!(JsonDecoder::new().decode(&body).unwrap(), "");
    }

    /// Битый JSON декодер НЕ отвергает: разбирать тело он не берётся вовсе, а
    /// показать текст как есть полезнее, чем спрятать его за ошибкой. Судит о
    /// валидности тот, кто его отправляет, — там для этого есть схема.
    #[test]
    fn a_malformed_body_is_still_shown() {
        let body = b"{not json";
        assert_eq!(JsonDecoder::new().decode(body).unwrap(), "{not json");
    }
}
