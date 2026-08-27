//! Превращение сырых байтов Kafka в то, что можно показать в UI.
//!
//! Kafka отдаёт байты, а не строки. Тело может быть Avro, Protobuf или чем
//! угодно ещё, поэтому декодирование обязано быть безопасным и обязано
//! сообщать вызывающему, что данные бинарные.

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
}
