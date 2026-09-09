//! Превращение сырых байтов Kafka в то, что можно показать в UI.
//!
//! Kafka отдаёт байты, а не строки. Тело может быть Avro, Protobuf или чем
//! угодно ещё, поэтому декодирование обязано быть безопасным и обязано
//! сообщать вызывающему, что данные бинарные.

use crate::schema::Decoder;

/// Максимум байт тела, уезжающих в строку таблицы. Полное тело отдаётся
/// отдельной командой, когда пользователь открывает сообщение.
pub const PREVIEW_BYTES: usize = 256;

/// Валиден ли срез как UTF-8.
pub fn is_text(bytes: &[u8]) -> bool {
    std::str::from_utf8(bytes).is_ok()
}

/// Полное тело для окна просмотра. Невалидный UTF-8 не роняет и не режется —
/// заменяется символами замены, а вызывающий узнаёт о бинарности по `is_text`.
pub fn decode(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

/// Короткое однострочное превью для таблицы.
///
/// Обрезает по границе UTF-8 символа (иначе получили бы «кракозябру» в конце)
/// и схлопывает управляющие символы в пробелы, чтобы перевод строки внутри
/// JSON не ломал вёрстку строки таблицы.
pub fn preview(bytes: &[u8], max_bytes: usize) -> String {
    let truncated = bytes.len() > max_bytes;
    let head = if truncated {
        let mut end = max_bytes;
        // Отходим назад до валидной границы символа.
        while end > 0 && std::str::from_utf8(&bytes[..end]).is_err() {
            end -= 1;
        }
        &bytes[..end]
    } else {
        bytes
    };

    let mut out: String = String::from_utf8_lossy(head)
        .chars()
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect();

    if truncated {
        out.push('…');
    }
    out
}

/// Ключ в том виде, в каком его показывают.
///
/// У ключа своя схема — в Kafka он сериализуется отдельно от тела, и в реестре
/// у него отдельный subject (`<topic>-key`). Поэтому и декодер сюда приходит
/// свой, а не тот, которым разбирается тело: разобрать ключ схемой значения
/// значило бы показать правдоподобный мусор.
///
/// Три отличия от `body`, и все три — про то, что ключ не тело:
///   * неудача разбора молчит. У avro-топика ключ бывает и avro-шным, и обычной
///     строкой от `StringSerializer` — второе встречается чаще, чем первое, и
///     объявлять его ошибкой было бы неверно. Не разобралось — показываем
///     байты, как показывали всегда;
///   * enum не нужны: ключ не подсвечивается;
///   * строка разворачивается из кавычек. Схема ключа сплошь и рядом — это
///     просто `"string"`, и `"TCBR"` вместо `TCBR` было бы ровно тем же
///     мусором перед ключом, только в кавычках.
pub fn key(decoder: Option<&Decoder>, raw: &[u8]) -> String {
    match decoder.map(|d| d.decode(raw)) {
        Some(Ok(json)) => unquote(&json),
        _ => decode(raw),
    }
}

/// Разворачивает JSON-строку в её содержимое. Всё остальное — как есть.
fn unquote(json: &str) -> String {
    serde_json::from_str::<String>(json).unwrap_or_else(|_| json.to_string())
}

// --- Показ тела по схеме -----------------------------------------------------

/// Результат попытки декодировать тело.
///
/// Оба поля пустые — декодера нет, тело показывается как есть. Именно поэтому
/// это не `Result`: «декодера не задали» и «декодер не справился» ведут к разным
/// строкам таблицы, и слить их в одну ветку значило бы врать про первый случай.
#[derive(Default)]
pub struct Rendered {
    pub decoded: Option<String>,
    pub error: Option<String>,
}

impl Rendered {
    /// Показывать ли тело как двоичное. Успешно разобранный protobuf двоичным
    /// не считается: наружу уехал JSON, и метка `[binary]` на нём была бы ложью.
    pub fn binary(&self, raw: &[u8]) -> bool {
        self.decoded.is_none() && !is_text(raw)
    }
}

/// Прогоняет тело через декодер, если он задан.
///
/// Декодирование ленивое — только для строк, которые действительно уезжают на
/// экран, и для сообщения, которое действительно открыли. Ни кэша, ни второй
/// копии тела в памяти: буфер и так держит десятки тысяч сообщений, и класть
/// рядом их разобранные представления значило бы удвоить его цену ради данных,
/// которые переживут один экран прокрутки.
pub fn render(decoder: Option<&Decoder>, value: &[u8]) -> Rendered {
    let Some(decoder) = decoder else {
        return Rendered::default();
    };
    match decoder.decode(value) {
        Ok(json) => Rendered {
            decoded: Some(json),
            error: None,
        },
        // Схему не трогаем и топик не закрываем: одно сообщение чужого формата
        // — обычное дело в топике, который переживал смену контракта. Показываем
        // его текстом и говорим, что случилось.
        Err(e) => Rendered {
            decoded: None,
            error: Some(e),
        },
    }
}

/// Тело целиком в том виде, в каком его показывает окно просмотра.
///
/// Живёт здесь, а не в воркере, потому что показывать сообщение приходится с
/// двух сторон: из буфера открытого топика и из архива сохранённых, где вместо
/// буфера файл на диске. Правило «enum только у разобранного» и признак
/// двоичности обязаны быть в этих двух местах одинаковыми — значит, местом им
/// быть одним.
pub struct Body {
    pub value: String,
    pub binary: bool,
    pub decode_error: Option<String>,
    /// Имена enum-значений схемы. Пусто у неразобранного тела: на самостоятельном
    /// JSON или тексте это был бы список из чужой схемы.
    pub enum_values: Vec<String>,
}

pub fn body(decoder: Option<&Decoder>, raw: &[u8]) -> Body {
    // Не `render`: здесь нужны ещё и enum, а у Avro они принадлежат схеме
    // КОНКРЕТНОГО сообщения — она приезжает с ним в заголовке. Спросить их
    // отдельно, после разбора, значило бы разбирать тело второй раз.
    let (decoded, error) = match decoder.map(|d| d.decode_with_enums(raw)) {
        Some(Ok((json, enums))) => (Some((json, enums)), None),
        // Схему не трогаем и топик не закрываем: одно сообщение чужого формата
        // — обычное дело в топике, который переживал смену контракта.
        Some(Err(e)) => (None, Some(e)),
        None => (None, None),
    };

    let binary = decoded.is_none() && !is_text(raw);
    match decoded {
        Some((value, enum_values)) => Body {
            value,
            binary,
            decode_error: error,
            enum_values,
        },
        None => Body {
            value: decode(raw),
            binary,
            decode_error: error,
            // Пусто у неразобранного тела: на самостоятельном JSON или тексте
            // это был бы список из чужой схемы.
            enum_values: Vec::new(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_ascii_passes_through() {
        assert_eq!(preview(b"hello", PREVIEW_BYTES), "hello");
        assert!(is_text(b"hello"));
    }

    #[test]
    fn control_characters_become_spaces() {
        assert_eq!(preview(b"a\nb\tc\rd", PREVIEW_BYTES), "a b c d");
    }

    #[test]
    fn truncation_appends_ellipsis() {
        let out = preview(b"abcdefghij", 4);
        assert_eq!(out, "abcd…");
    }

    #[test]
    fn truncation_respects_utf8_boundaries() {
        // Каждая кириллическая буква занимает 2 байта: обрезка по 5 байтам
        // попадает в середину третьей и обязана отступить назад.
        let text = "привет".as_bytes();
        let out = preview(text, 5);
        assert_eq!(out, "пр…");
        // Главное: результат — валидная строка без символов замены.
        assert!(!out.contains('\u{fffd}'));
    }

    #[test]
    fn truncation_at_exact_boundary_keeps_everything_it_can() {
        let text = "привет".as_bytes();
        assert_eq!(preview(text, 6), "при…");
    }

    #[test]
    fn no_ellipsis_when_nothing_was_cut() {
        let text = "привет".as_bytes();
        assert_eq!(preview(text, text.len()), "привет");
    }

    #[test]
    fn binary_payload_is_detected_and_does_not_panic() {
        let bytes = [0xff, 0xfe, 0x00, 0x01];
        assert!(!is_text(&bytes));
        let out = preview(&bytes, PREVIEW_BYTES);
        // Не паникуем и не роняем; содержимое — символы замены.
        assert!(!out.is_empty());
        assert_eq!(decode(&bytes).chars().count(), 4);
    }

    #[test]
    fn empty_input_is_empty_output() {
        assert_eq!(preview(b"", PREVIEW_BYTES), "");
        assert_eq!(decode(b""), "");
        assert!(is_text(b""));
    }

    /// Hex — это уже текст, и метка `[binary]` на нём была бы ложью: показано
    /// ровно то, что лежит в теле, и показано целиком. Ровно за этим формат и
    /// заводился — то же тело без него уезжает символами замены.
    #[test]
    fn a_hex_body_is_shown_in_full_and_is_not_called_binary() {
        let raw = [0xff, 0x00, 0x1a];
        assert!(!is_text(&raw));

        let shown = body(Some(&Decoder::Hex), &raw);
        assert_eq!(shown.value, "FF 00 1A");
        assert!(!shown.binary);
        assert!(shown.decode_error.is_none());
        // Enum здесь взяться неоткуда: схемы за форматом нет никакой.
        assert!(shown.enum_values.is_empty());
    }

    // --- Ключ --------------------------------------------------------------
    //
    // Ровно тот случай, ради которого у ключа завёлся свой декодер: на
    // avro-топике ключ тоже закодирован, и байтами перед ним ехала длина
    // строки — в окне просмотра это выглядело парой нечитаемых знаков.

    fn string_key_decoder() -> crate::schema::Decoder {
        // Через `parse_with_refs`, а не `parse_files`: схема ключа — примитив,
        // а `Schema::parse_list` требует именованных типов. Реестр отдаёт её
        // ровно этим путём, так что проверяется тот же код, что и в жизни.
        let linked = std::sync::Arc::new(
            crate::schema::avro::Linked::parse_with_refs("\"string\"", &[]).unwrap(),
        );
        crate::schema::Decoder::Avro(crate::schema::avro::AvroDecoder::new(
            None,
            Some(linked),
            None,
        ))
    }

    #[test]
    fn an_avro_string_key_loses_both_its_length_byte_and_its_quotes() {
        // Голый datum: зигзаг-длина 4, затем сами байты.
        let raw = b"\x08TCBR";
        assert_eq!(
            decode(raw),
            "\u{8}TCBR",
            "без схемы длина едет в UI управляющим символом — это и был баг"
        );
        assert_eq!(key(Some(&string_key_decoder()), raw), "TCBR");
    }

    /// Ключ-строка от `StringSerializer` на том же топике: схемы у него нет, и
    /// разбор обязан молча уступить, а не превратить ключ в ошибку.
    #[test]
    fn a_plain_key_survives_a_decoder_that_cannot_read_it() {
        // Первый байт 'T' = 0x54, как длина это 42 — до конца не хватит байт.
        assert_eq!(key(Some(&string_key_decoder()), b"TCBR"), "TCBR");
    }

    #[test]
    fn without_a_decoder_the_key_is_the_bytes_it_always_was() {
        assert_eq!(key(None, b"TCBR"), "TCBR");
        assert_eq!(key(None, b""), "");
    }

    /// Разворачивается только строка целиком: ключ-запись обязан остаться
    /// JSON'ом, а не растерять кавычки внутри себя.
    #[test]
    fn a_record_key_stays_json() {
        assert_eq!(unquote(r#"{"id":"a-1"}"#), r#"{"id":"a-1"}"#);
        assert_eq!(unquote(r#""a-1""#), "a-1");
    }
}
