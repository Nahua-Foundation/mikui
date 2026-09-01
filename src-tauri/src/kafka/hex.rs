//! Разбор шестнадцатеричного тела, введённого руками.
//!
//! Единственный способ положить в топик байты, которых не выражает ни текст, ни
//! загруженная схема. Поэтому и разделители допускаются щедро: hex попадает
//! сюда копипастой из дампа, а дампы печатают его кто во что горазд —
//! `1a2b3c`, `1a 2b 3c`, `1a:2b:3c`, `1a-2b-3c`, `0x1a2b3c`.

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_the_shapes_dumps_actually_print() {
        let expected = vec![0x1a, 0x2b, 0x3c];
        for text in ["1a2b3c", "1a 2b 3c", "1a:2b:3c", "1a-2b-3c", "0x1a2b3c", " 1A2B3C "] {
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
}
