//! Шестнадцатеричное тело: разбор введённого руками и печать прочитанного.
//!
//! При ОТПРАВКЕ это единственный способ положить в топик байты, которых не
//! выражает ни текст, ни загруженная схема. Поэтому и разделители допускаются
//! щедро: hex попадает сюда копипастой из дампа, а дампы печатают его кто во
//! что горазд — `1a2b3c`, `1a 2b 3c`, `1a:2b:3c`, `1a-2b-3c`, `0x1a2b3c`.
//!
//! При ЧТЕНИИ (`BodyFormat::Hex`) — способ забрать тело как есть, побайтно:
//! ни текст, ни схема не годятся, когда сообщение надо переслать дальше или
//! воспроизвести где-то ещё. Печатаем поэтому в том виде, который `decode`
//! принимает обратно без единой правки, — круг замыкается тестом.

/// Что считается разделителем между байтами. Символы, а не «всё, что не hex»:
/// иначе опечатка вроде `1g2b` молча превратилась бы в `12b`.
const SEPARATORS: [char; 6] = [' ', '\t', '\n', '\r', ',', ':'];

/// Разбирает hex-строку в байты.
///
/// Ошибка называет позицию в ИСХОДНОЙ строке (1-based, по символам), а не в
/// очищенной от разделителей: пользователь ищет её глазами в том, что ввёл.
pub fn decode(text: &str) -> Result<Vec<u8>, String> {
    let text = text.trim();
    // Префикс снимается только у всей строки целиком. `0x` перед каждым
    // байтом — это уже другой формат записи, и притворяться, что мы его
    // понимаем, значило бы принимать `0x1a0x2b` за что-то осмысленное.
    let body = text.strip_prefix("0x").or_else(|| text.strip_prefix("0X"));
    let (body, shift) = match body {
        Some(rest) => (rest, 2),
        None => (text, 0),
    };

    let mut digits: Vec<(usize, u8)> = Vec::with_capacity(body.len());
    for (index, ch) in body.chars().enumerate() {
        if SEPARATORS.contains(&ch) || ch == '-' {
            continue;
        }
        let value = ch
            .to_digit(16)
            .ok_or_else(|| format!("{ch:?} is not a hex digit (position {})", index + shift + 1))?;
        digits.push((index + shift + 1, value as u8));
    }

    if digits.len() % 2 != 0 {
        // Называем последнюю цифру: именно у неё не хватает пары, и именно там
        // человек чаще всего и промахнулся.
        let (position, _) = digits[digits.len() - 1];
        return Err(format!(
            "odd number of hex digits ({}) — one is missing a pair at position {position}",
            digits.len()
        ));
    }

    Ok(digits
        .chunks(2)
        .map(|pair| (pair[0].1 << 4) | pair[1].1)
        .collect())
}

const DIGITS: &[u8; 16] = b"0123456789ABCDEF";

/// Печатает байты парами заглавных цифр через пробел: `0A 15 08 C8`.
///
/// Вид дампа, а не сплошной строки, — ради того, чтобы hex читался как байты с
/// первого взгляда и не путался с обычным текстом, который тоже бывает из одних
/// цифр и букв. Пробел и заглавные буквы — единственное, чем это достигается
/// бесплатно: `decode` разделители пропускает, а регистр ему безразличен, так
/// что скопированное отсюда вставляется в форму отправки без единой правки
/// (закреплено тестом ниже).
///
/// Цена — ширина: в строке таблицы под превью отведено `PREVIEW_BYTES`
/// символов, и с разделителями туда влезает не 128 байт, а 85. Для строки
/// таблицы, которая и так обрывается многоточием, это дешевле, чем нечитаемая
/// лента цифр.
pub fn encode(bytes: &[u8]) -> String {
    // На байт — две цифры и разделитель; у последнего разделителя нет.
    let mut out = String::with_capacity(bytes.len() * 3);
    for &byte in bytes {
        if !out.is_empty() {
            out.push(' ');
        }
        out.push(DIGITS[(byte >> 4) as usize] as char);
        out.push(DIGITS[(byte & 0x0f) as usize] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_the_shapes_dumps_actually_print() {
        let expected = vec![0x1a, 0x2b, 0x3c];
        for text in [
            "1a2b3c", "1a 2b 3c", "1a:2b:3c", "1a-2b-3c", "0x1a2b3c", " 1A2B3C ",
        ] {
            assert_eq!(decode(text).unwrap(), expected, "не разобрано: {text:?}");
        }
    }

    #[test]
    fn empty_input_is_an_empty_body_not_an_error() {
        // Сообщение без тела — законная запись в Kafka (tombstone), и
        // отказывать в ней просмотрщику незачем.
        assert_eq!(decode("").unwrap(), Vec::<u8>::new());
        assert_eq!(decode("   ").unwrap(), Vec::<u8>::new());
    }

    /// Ради этого случая разделители и перечислены поимённо: «пропускать всё
    /// нераспознанное» проглотило бы опечатку вместе с ошибкой.
    #[test]
    fn a_typo_is_reported_with_its_position_not_skipped() {
        let error = decode("1a2g3c").unwrap_err();
        assert!(error.contains("'g'"), "{error}");
        assert!(error.contains("position 4"), "{error}");
    }

    #[test]
    fn odd_digit_count_is_reported() {
        let error = decode("1a2").unwrap_err();
        assert!(error.contains("odd"), "{error}");
    }

    /// Позиция считается по исходной строке — вместе с префиксом и
    /// разделителями, а не по очищенной от них.
    #[test]
    fn position_points_into_the_original_text() {
        let error = decode("0x1a 2z").unwrap_err();
        assert!(error.contains("position 7"), "{error}");
    }

    // --- Печать ---------------------------------------------------------------

    #[test]
    fn prints_upper_case_pairs_separated_by_spaces() {
        assert_eq!(encode(&[0x1a, 0x2b, 0x3c]), "1A 2B 3C");
        // Ведущий ноль обязателен и с разделителями: пара всегда из двух цифр,
        // иначе выравнивание колонки поехало бы на каждом байте меньше 0x10.
        assert_eq!(encode(&[0x0f, 0x00, 0xff]), "0F 00 FF");
        // Ни ведущего, ни хвостового пробела: строку копируют целиком.
        assert_eq!(encode(&[0x01]), "01");
        assert_eq!(encode(&[]), "");
    }

    /// Главное свойство: показанное можно скопировать в форму отправки и
    /// получить те же байты. Разделители и регистр печати выбираются свободно
    /// ровно потому, что разбор к ним равнодушен, — и проверяется это здесь, а
    /// не на глаз.
    #[test]
    fn what_is_printed_reads_back_as_the_same_bytes() {
        let bytes: Vec<u8> = (0..=255u8).collect();
        assert_eq!(decode(&encode(&bytes)).unwrap(), bytes);
    }
}
