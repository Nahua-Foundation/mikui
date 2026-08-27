//! Поиск подстроки по сырым байтам сообщения.
//!
//! Смысл в том, чтобы не декодировать тело в UTF-8 и ничего не аллоцировать:
//! фильтр прогоняется по всему буферу на каждое изменение запроса, и любая
//! аллокация на сообщение здесь превращается в десятки тысяч аллокаций.

use memchr::memmem;

/// Ищет `needle` в `haystack`. Пустая иголка совпадает всегда.
pub fn contains(haystack: &[u8], needle: &[u8], case_sensitive: bool) -> bool {
    if needle.is_empty() {
        return true;
    }
    if needle.len() > haystack.len() {
        return false;
    }

    if case_sensitive {
        // SIMD-ускоренный путь.
        return memmem::find(haystack, needle).is_some();
    }

    // Регистронезависимо (ASCII). Аллоцировать lowercase-копию haystack на
    // каждое сообщение слишком дорого, поэтому идём по кандидатам первого
    // байта в обоих регистрах и досравниваем окно.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_needle_always_matches() {
        assert!(contains(b"anything", b"", true));
        assert!(contains(b"", b"", false));
    }

    #[test]
    fn needle_longer_than_haystack_never_matches() {
        assert!(!contains(b"ab", b"abc", true));
        assert!(!contains(b"ab", b"abc", false));
    }

    #[test]
    fn case_sensitive_search() {
        assert!(contains(b"hello world", b"world", true));
        assert!(!contains(b"hello world", b"World", true));
    }

    #[test]
    fn case_insensitive_search() {
        assert!(contains(b"hello world", b"World", false));
        assert!(contains(b"HELLO WORLD", b"world", false));
        assert!(contains(b"MiXeD", b"mIxEd", false));
        assert!(!contains(b"hello", b"xyz", false));
    }

    #[test]
    fn matches_at_the_very_end() {
        assert!(contains(b"abcdef", b"def", true));
        assert!(contains(b"abcdef", b"DEF", false));
    }

    #[test]
    fn matches_at_the_very_start() {
        assert!(contains(b"abcdef", b"abc", true));
        assert!(contains(b"abcdef", b"ABC", false));
    }

    #[test]
    fn retries_after_a_false_candidate() {
        // Первая 'a' не начинает совпадение — поиск обязан пойти дальше,
        // а не сдаться на первом кандидате.
        assert!(contains(b"axxabc", b"ABC", false));
        assert!(contains(b"aaaaab", b"AB", false));
    }

    #[test]
    fn works_on_non_utf8_bytes() {
        let haystack = [0xff, 0x00, b'k', b'e', b'y', 0xfe];
        assert!(contains(&haystack, b"key", true));
        assert!(contains(&haystack, b"KEY", false));
    }

    #[test]
    fn digits_and_symbols_have_no_case() {
        assert!(contains(b"id-12345", b"12345", false));
        assert!(contains(b"a-b-c", b"-B-", false));
    }
}
