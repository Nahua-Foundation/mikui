//! Поиск подстроки по сырым байтам сообщения.
//!
//! Смысл в том, чтобы не декодировать тело в UTF-8 и ничего не аллоцировать:
//! фильтр прогоняется по всему буферу на каждое изменение запроса, и любая
//! аллокация на сообщение здесь превращается в десятки тысяч аллокаций.
//!
//! Поэтому запрос готовится один раз — в [`Needle`], — а дальше по сообщениям
//! ходит уже разобранная игла, которая знает свой режим и не выбирает его
//! заново на каждом стоге.

use memchr::memmem;

/// Подготовленный поисковый запрос.
///
/// Режим выбирается один раз при разборе запроса, а не на каждом сообщении:
/// от него зависит, каким алгоритмом идти, и решать это внутри цикла по
/// десяткам тысяч тел значило бы платить за один и тот же вывод многократно.
pub enum Needle {
    /// Пустой запрос совпадает всегда — «не фильтровать».
    Empty,
    Sensitive(Vec<u8>),
    /// Игла целиком ASCII: работает быстрый байтовый путь.
    ///
    /// Он корректен и на UTF-8-стогах, а не только на ASCII: все байты
    /// многобайтовой последовательности `>= 0x80`, поэтому ASCII-байт никогда
    /// не встречается ВНУТРИ символа и байтовое совпадение не может оказаться
    /// обрывком чужой буквы.
    AsciiInsensitive(Vec<u8>),
    /// В игле есть не-ASCII: складывать регистр придётся по символам.
    ///
    /// Храним символами, а не байтами, потому что фолдинг посимвольный, и
    /// раскладывать UTF-8 обратно на каждом сравнении было бы лишней работой.
    UnicodeInsensitive(Vec<char>),
}

impl Needle {
    pub fn new(text: &str, case_sensitive: bool) -> Self {
        if text.is_empty() {
            Needle::Empty
        } else if case_sensitive {
            Needle::Sensitive(text.as_bytes().to_vec())
        } else if text.is_ascii() {
            Needle::AsciiInsensitive(text.as_bytes().to_vec())
        } else {
            Needle::UnicodeInsensitive(text.chars().collect())
        }
    }

    pub fn is_empty(&self) -> bool {
        matches!(self, Needle::Empty)
    }

    pub fn matches(&self, haystack: &[u8]) -> bool {
        match self {
            Needle::Empty => true,
            // SIMD-ускоренный путь.
            Needle::Sensitive(n) => {
                n.len() <= haystack.len() && memmem::find(haystack, n).is_some()
            }
            Needle::AsciiInsensitive(n) => ascii_contains(haystack, n),
            Needle::UnicodeInsensitive(n) => unicode_contains(haystack, n),
        }
    }
}

/// Регистронезависимо по ASCII. Аллоцировать lowercase-копию haystack на
/// каждое сообщение слишком дорого, поэтому идём по кандидатам первого байта в
/// обоих регистрах и досравниваем окно.
fn ascii_contains(haystack: &[u8], needle: &[u8]) -> bool {
    if needle.len() > haystack.len() {
        return false;
    }

    let first = needle[0];
    let lower = first.to_ascii_lowercase();
    let upper = first.to_ascii_uppercase();
    let last_start = haystack.len() - needle.len();

    let mut offset = 0usize;
    while offset <= last_start {
        let window = &haystack[offset..=last_start];
        let found = if lower == upper {
            memchr::memchr(lower, window)
        } else {
            memchr::memchr2(lower, upper, window)
        };

        let Some(pos) = found else { return false };
        let start = offset + pos;
        if haystack[start..start + needle.len()].eq_ignore_ascii_case(needle) {
            return true;
        }
        offset = start + 1;
    }
    false
}

/// Регистронезависимо по символам — для игл с кириллицей и прочим не-ASCII.
///
/// Тело сообщения валидным UTF-8 быть не обязано (protobuf на проводе — не
/// текст), поэтому идём по его валидным кускам. Ничего при этом не теряем:
/// игла — валидный UTF-8, значит её вхождение не может пересекать невалидный
/// байт, и искать через границу куска нечего.
fn unicode_contains(haystack: &[u8], needle: &[char]) -> bool {
    // Длину сверяем в символах, а не в байтах: фолдинг меняет длину UTF-8
    // (`İ` занимает два байта, `i` — один), и байтовая отсечка дала бы ложный
    // промах. Символ — минимум один байт, так что это по-прежнему верхняя
    // граница и по-прежнему бесплатно.
    if haystack.len() < needle.len() {
        return false;
    }
    haystack
        .utf8_chunks()
        .any(|chunk| str_contains_fold(chunk.valid(), needle))
}

fn str_contains_fold(haystack: &str, needle: &[char]) -> bool {
    let first = needle[0];
    // Отсев по первому символу вместо `memchr` по его ведущим байтам:
    // множество символов, складывающихся в данный, шире, чем {сам, нижний,
    // верхний} — есть U+212A KELVIN SIGN, U+017F ſ и титульные диграфы вроде
    // U+01C5. Байтовый отсев по трём вариантам молча терял бы такие совпадения.
    haystack
        .char_indices()
        .any(|(i, c)| eq_fold(c, first) && starts_with_fold(&haystack[i..], needle))
}

fn starts_with_fold(haystack: &str, needle: &[char]) -> bool {
    let mut chars = haystack.chars();
    needle
        .iter()
        .all(|&n| chars.next().is_some_and(|c| eq_fold(c, n)))
}

/// Равны ли символы с точностью до регистра.
///
/// Сравнение посимвольное, поэтому раскрывающиеся отображения (`ß` → `ss`)
/// друг с другом не сойдутся. Для задачи — поиск по телам Kafka, где не-ASCII
/// это прежде всего кириллица, — этого достаточно, а полный fold потребовал бы
/// нормализации и аллокаций на каждом сообщении.
fn eq_fold(a: char, b: char) -> bool {
    a == b || a.to_lowercase().eq(b.to_lowercase())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Старый фасад функции: тесты ниже — регрессионная сетка на байтовые пути,
    /// и переписывать их под подготовленную иглу значило бы переписать сетку
    /// вместе с тем, что она стережёт.
    fn contains(haystack: &[u8], needle: &str, case_sensitive: bool) -> bool {
        Needle::new(needle, case_sensitive).matches(haystack)
    }

    #[test]
    fn empty_needle_always_matches() {
        assert!(contains(b"anything", "", true));
        assert!(contains(b"", "", false));
        assert!(Needle::new("", false).is_empty());
        assert!(!Needle::new("a", false).is_empty());
    }

    #[test]
    fn needle_longer_than_haystack_never_matches() {
        assert!(!contains(b"ab", "abc", true));
        assert!(!contains(b"ab", "abc", false));
    }

    #[test]
    fn case_sensitive_search() {
        assert!(contains(b"hello world", "world", true));
        assert!(!contains(b"hello world", "World", true));
    }

    #[test]
    fn case_insensitive_search() {
        assert!(contains(b"hello world", "World", false));
        assert!(contains(b"HELLO WORLD", "world", false));
        assert!(contains(b"MiXeD", "mIxEd", false));
        assert!(!contains(b"hello", "xyz", false));
    }

    #[test]
    fn matches_at_the_very_end() {
        assert!(contains(b"abcdef", "def", true));
        assert!(contains(b"abcdef", "DEF", false));
    }

    #[test]
    fn matches_at_the_very_start() {
        assert!(contains(b"abcdef", "abc", true));
        assert!(contains(b"abcdef", "ABC", false));
    }

    #[test]
    fn retries_after_a_false_candidate() {
        // Первая 'a' не начинает совпадение — поиск обязан пойти дальше,
        // а не сдаться на первом кандидате.
        assert!(contains(b"axxabc", "ABC", false));
        assert!(contains(b"aaaaab", "AB", false));
    }

    #[test]
    fn works_on_non_utf8_bytes() {
        let haystack = [0xff, 0x00, b'k', b'e', b'y', 0xfe];
        assert!(contains(&haystack, "key", true));
        assert!(contains(&haystack, "KEY", false));
    }

    #[test]
    fn digits_and_symbols_have_no_case() {
        assert!(contains(b"id-12345", "12345", false));
        assert!(contains(b"a-b-c", "-B-", false));
    }

    // --- Не-ASCII ------------------------------------------------------------

    #[test]
    fn cyrillic_folds_both_ways() {
        assert!(contains("Привет".as_bytes(), "привет", false));
        assert!(contains("привет".as_bytes(), "ПРИВЕТ", false));
        assert!(contains("ПРИВЕТ".as_bytes(), "привет", false));
        assert!(contains("ПрИвЕт".as_bytes(), "пРиВеТ", false));
    }

    #[test]
    fn cyrillic_stays_exact_when_case_sensitive() {
        assert!(contains("Привет".as_bytes(), "Привет", true));
        assert!(!contains("Привет".as_bytes(), "привет", true));
    }

    #[test]
    fn cyrillic_inside_a_longer_body() {
        let body = "{\"name\":\"Сбербанк\",\"id\":42}".as_bytes();
        assert!(contains(body, "сбербанк", false));
        assert!(contains(body, "СБЕРБАНК", false));
        assert!(!contains(body, "газпром", false));
    }

    #[test]
    fn cyrillic_found_inside_non_utf8_payload() {
        // Ровно случай protobuf: строковое поле лежит на проводе непрерывным
        // UTF-8, а вокруг — служебные байты, которые UTF-8 не образуют.
        let mut haystack = vec![0x0a, 0x0c, 0xff];
        haystack.extend_from_slice("Тинькофф".as_bytes());
        haystack.extend_from_slice(&[0xfe, 0x10]);
        assert!(contains(&haystack, "тинькофф", false));
        assert!(contains(&haystack, "ТИНЬКОФФ", false));
    }

    #[test]
    fn match_may_not_straddle_an_invalid_byte() {
        // «При» и «вет» лежат по разные стороны мусорного байта — это не
        // «Привет», и склеивать куски мы не имеем права.
        let mut haystack = Vec::from("При".as_bytes());
        haystack.push(0xff);
        haystack.extend_from_slice("вет".as_bytes());
        assert!(!contains(&haystack, "привет", false));
        // А по отдельности половинки обязаны находиться.
        assert!(contains(&haystack, "при", false));
        assert!(contains(&haystack, "ВЕТ", false));
    }

    #[test]
    fn ascii_needle_does_not_false_positive_on_cyrillic() {
        // У «р» (U+0440) второй байт 0x80, у «А» (U+0410) — 0x90; ни один
        // ASCII-байт внутрь многобайтового символа попасть не может.
        let haystack = "рАЯЪёЭ".as_bytes();
        for needle in ["a", "p", "e", "A", "P", "E"] {
            assert!(!contains(haystack, needle, false), "ложное на {needle}");
        }
    }

    #[test]
    fn needle_longer_in_chars_than_haystack() {
        // Стог короче иглы В СИМВОЛАХ, но не в байтах: «аб» — четыре байта,
        // и байтовая отсечка здесь бы не сработала.
        assert!(!contains("аб".as_bytes(), "абвг", false));
    }

    #[test]
    fn matches_at_chunk_boundaries() {
        // В начале валидного куска.
        let mut head = vec![0xff];
        head.extend_from_slice("Юг".as_bytes());
        assert!(contains(&head, "юг", false));

        // И в самом его конце.
        let mut tail = Vec::from("Юг".as_bytes());
        tail.push(0xff);
        assert!(contains(&tail, "юг", false));
    }

    #[test]
    fn retries_after_a_false_candidate_in_unicode() {
        // Первая «а» кандидат, но продолжение не то — обязаны идти дальше.
        assert!(contains("аха-абв".as_bytes(), "АБВ", false));
    }

    #[test]
    fn mixed_ascii_and_cyrillic_needle() {
        assert!(contains("id-Москва-01".as_bytes(), "id-москва-01", false));
        assert!(contains("ID-МОСКВА-01".as_bytes(), "id-Москва-01", false));
    }
}
