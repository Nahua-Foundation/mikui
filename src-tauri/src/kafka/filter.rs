//! Поиск подстроки по сырым байтам сообщения.
//!
//! Смысл в том, чтобы не декодировать тело в UTF-8 и ничего не аллоцировать:
//! фильтр прогоняется по всему буферу на каждое изменение запроса, и любая
//! аллокация на сообщение здесь превращается в десятки тысяч аллокаций.
//!
//! Поэтому запрос готовится один раз — в [`Needle`], — а дальше по сообщениям
//! ходит уже разобранная игла, которая знает свой режим и не выбирает его
//! заново на каждом стоге.
//!
//! Вторая забота иглы — ПРОБЕЛЫ. Тело показывается отформатированным, с
//! отступами и переносами, а лежит оно сжатым: скопированный из модалки
//! `"key": "value",` в сыром теле выглядит как `"key":"value",` и буквально не
//! находится. Расхождение неочевидное и целиком наше: пользователь искал ровно
//! то, что видел. Поэтому у иглы есть второй, «нежёсткий» вид — см. [`Loose`].

use memchr::memmem;

/// Подготовленный поисковый запрос.
///
/// Режим выбирается один раз при разборе запроса, а не на каждом сообщении:
/// от него зависит, каким алгоритмом идти, и решать это внутри цикла по
/// десяткам тысяч тел значило бы платить за один и тот же вывод многократно.
pub struct Needle {
    exact: Exact,
    /// Тот же запрос, но с пробелами JSON на правах необязательных. `None` —
    /// пробелам в этом запросе взяться неоткуда, и второй проход был бы
    /// повторением первого.
    loose: Option<Loose>,
}

/// Буквальный поиск — тот, что был здесь всегда.
enum Exact {
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
        let exact = if text.is_empty() {
            Exact::Empty
        } else if case_sensitive {
            Exact::Sensitive(text.as_bytes().to_vec())
        } else if text.is_ascii() {
            Exact::AsciiInsensitive(text.as_bytes().to_vec())
        } else {
            Exact::UnicodeInsensitive(text.chars().collect())
        };
        Self {
            loose: Loose::compile(text, case_sensitive),
            exact,
        }
    }

    pub fn is_empty(&self) -> bool {
        matches!(self.exact, Exact::Empty)
    }

    pub fn matches(&self, haystack: &[u8]) -> bool {
        if self.exact.matches(haystack) {
            return true;
        }
        // Нежёсткий проход — ТОЛЬКО на промахе буквального. Так запрос, который
        // и так находится, не платит за него ничего, и ни одно совпадение,
        // работавшее раньше, не может пропасть: буквальный путь остался первым
        // и нетронутым.
        self.loose
            .as_ref()
            .is_some_and(|loose| loose.matches(haystack))
    }
}

impl Exact {
    fn matches(&self, haystack: &[u8]) -> bool {
        match self {
            Exact::Empty => true,
            // SIMD-ускоренный путь.
            Exact::Sensitive(n) => n.len() <= haystack.len() && memmem::find(haystack, n).is_some(),
            Exact::AsciiInsensitive(n) => ascii_contains(haystack, n),
            Exact::UnicodeInsensitive(n) => unicode_contains(haystack, n),
        }
    }
}

/// Знаки, вокруг которых JSON-форматирование ставит пробелы по своему
/// усмотрению. Всё расхождение между показанным телом и лежащим в топике — в
/// них: `"key": "value",` против `"key":"value",`.
const STRUCTURAL: [char; 6] = ['{', '}', '[', ']', ':', ','];

/// Запрос, в котором пробелы вокруг знаков JSON объявлены необязательными.
///
/// Работает в обе стороны, и это главное. Скопированный из модалки
/// `"key": "value"` находится в сжатом теле, потому что пробел после `:` стал
/// необязательным; набранный руками `"key":"value"` находится в отформатированном
/// теле, потому что необязательный пробел разрешён и там, где в запросе его не
/// было. Одно правило вместо двух: пробел между кусками запроса и пробел в
/// стоге равноправны.
///
/// Чего это правило НЕ касается — содержимого строковых литералов. `"hello
/// world"` ищется как есть, вместе со своим пробелом: пробел внутри строки
/// JSON значащий, и склеивать по нему `"helloworld"` было бы уже не поиском, а
/// догадкой. Поэтому запрос разбирается с оглядкой на кавычки.
struct Loose {
    /// Первый кусок — всегда текст. По нему идёт быстрый отсев кандидатов,
    /// прежде чем проверять остальное.
    head: String,
    pieces: Vec<Piece>,
    sensitive: bool,
}

enum Piece {
    Text(String),
    /// Пробелы. `required` — был ли пробел в самом запросе ВНЕ знаков JSON;
    /// тогда он обязателен, иначе `hello world` находило бы `helloworld`.
    Gap {
        required: bool,
    },
}

impl Loose {
    /// `None` — пробелам в этом запросе взяться неоткуда, и второй проход был
    /// бы точным повторением первого.
    fn compile(text: &str, sensitive: bool) -> Option<Self> {
        let mut pieces: Vec<Piece> = Vec::new();
        let mut current = String::new();
        let mut in_string = false;
        let mut escaped = false;

        for c in text.chars() {
            // Внутри строкового литерала всё буквально, включая пробелы и
            // запятые. Закрывающая кавычка — только неэкранированная.
            if in_string {
                current.push(c);
                if escaped {
                    escaped = false;
                } else if c == '\\' {
                    escaped = true;
                } else if c == '"' {
                    in_string = false;
                }
                continue;
            }
            if c == '"' {
                in_string = true;
                current.push(c);
            } else if c.is_whitespace() {
                flush(&mut current, &mut pieces);
                push_gap(&mut pieces, true);
            } else if STRUCTURAL.contains(&c) {
                flush(&mut current, &mut pieces);
                // Пробел разрешён с обеих сторон знака — и когда его в запросе
                // не было тоже. Ровно это и делает правило двусторонним.
                push_gap(&mut pieces, false);
                pieces.push(Piece::Text(c.to_string()));
                push_gap(&mut pieces, false);
            } else {
                current.push(c);
            }
        }
        flush(&mut current, &mut pieces);

        // Пробел на краю запроса не с чем соотнести: он не разделяет куски, а
        // требует пробела там, где кончается совпадение.
        while matches!(pieces.first(), Some(Piece::Gap { .. })) {
            pieces.remove(0);
        }
        while matches!(pieces.last(), Some(Piece::Gap { .. })) {
            pieces.pop();
        }

        if !pieces.iter().any(|p| matches!(p, Piece::Gap { .. })) {
            return None;
        }
        let Some(Piece::Text(head)) = pieces.first() else {
            return None;
        };
        Some(Self {
            head: head.clone(),
            pieces,
            sensitive,
        })
    }

    fn matches(&self, haystack: &[u8]) -> bool {
        // По валидным кускам, как и `unicode_contains`: запрос — валидный
        // UTF-8, значит его вхождение не может пересекать негодный байт.
        haystack
            .utf8_chunks()
            .any(|chunk| self.matches_in(chunk.valid()))
    }

    fn matches_in(&self, haystack: &str) -> bool {
        let mut from = 0usize;
        while from <= haystack.len() {
            let tail = &haystack[from..];
            let Some(at) = self.find_head(tail) else {
                return false;
            };
            let start = from + at;
            if self.matches_at(&haystack[start..]) {
                return true;
            }
            // Следующий кандидат — с следующего символа, а не байта: резать
            // строку посреди символа нельзя.
            let step = haystack[start..].chars().next().map_or(1, char::len_utf8);
            from = start + step;
        }
        false
    }

    /// Где в `haystack` может начинаться совпадение. Только отсев: полную
    /// проверку делает `matches_at`.
    fn find_head(&self, haystack: &str) -> Option<usize> {
        if self.sensitive {
            return memmem::find(haystack.as_bytes(), self.head.as_bytes());
        }
        let first = self.head.chars().next()?;
        if first.is_ascii() {
            // Тот же отсев, что в `ascii_contains`: два байта-кандидата.
            let byte = first as u8;
            let (lower, upper) = (byte.to_ascii_lowercase(), byte.to_ascii_uppercase());
            return match lower == upper {
                true => memchr::memchr(lower, haystack.as_bytes()),
                false => memchr::memchr2(lower, upper, haystack.as_bytes()),
            };
        }
        haystack
            .char_indices()
            .find(|(_, c)| eq_fold(*c, first))
            .map(|(i, _)| i)
    }

    fn matches_at(&self, haystack: &str) -> bool {
        let mut rest = haystack;
        for piece in &self.pieces {
            match piece {
                Piece::Text(text) => match self.consume(rest, text) {
                    Some(len) => rest = &rest[len..],
                    None => return false,
                },
                Piece::Gap { required } => {
                    let skipped = rest.len() - rest.trim_start().len();
                    if *required && skipped == 0 {
                        return false;
                    }
                    rest = &rest[skipped..];
                }
            }
        }
        true
    }

    /// Сколько байт стога съел кусок запроса. `None` — не съел вовсе.
    fn consume(&self, haystack: &str, text: &str) -> Option<usize> {
        if self.sensitive {
            return haystack.starts_with(text).then_some(text.len());
        }
        let mut chars = haystack.char_indices();
        let mut eaten = 0usize;
        for wanted in text.chars() {
            let (at, c) = chars.next()?;
            if !eq_fold(c, wanted) {
                return None;
            }
            eaten = at + c.len_utf8();
        }
        Some(eaten)
    }
}

fn flush(current: &mut String, pieces: &mut Vec<Piece>) {
    if !current.is_empty() {
        pieces.push(Piece::Text(std::mem::take(current)));
    }
}

/// Добавляет пробельный кусок, сливая его с предыдущим таким же.
///
/// При слиянии побеждает необязательность: если пробел разрешено опустить хотя
/// бы по одной причине, значит его можно опустить.
fn push_gap(pieces: &mut Vec<Piece>, required: bool) {
    match pieces.last_mut() {
        Some(Piece::Gap { required: had }) => *had &= required,
        _ => pieces.push(Piece::Gap { required }),
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

    // --- Пробелы JSON --------------------------------------------------------
    //
    // Тело показывается отформатированным, а лежит сжатым. Ни та ни другая
    // сторона расхождения пользователю не видна, поэтому запрос обязан
    // находиться в обоих видах.

    const COMPACT: &[u8] = br#"{"id":42,"name":"Alice","tags":["a","b"]}"#;
    const PRETTY: &[u8] = br#"{
  "id": 42,
  "name": "Alice",
  "tags": [
    "a",
    "b"
  ]
}"#;

    /// Ровно тот случай, из-за которого всё это и заведено: пару ключ-значение
    /// скопировали из модалки, где тело отформатировано.
    #[test]
    fn a_pair_copied_from_the_formatted_body_is_found_in_the_compact_one() {
        assert!(contains(COMPACT, r#""name": "Alice""#, true));
        assert!(contains(COMPACT, r#""id": 42,"#, true));
        assert!(contains(COMPACT, r#""name": "alice""#, false));
    }

    /// И наоборот: набранный руками сжатый запрос обязан находиться в
    /// отформатированном теле. Правило одно и работает в обе стороны.
    #[test]
    fn a_compact_query_is_found_in_a_formatted_body() {
        assert!(contains(PRETTY, r#""name":"Alice""#, true));
        assert!(contains(PRETTY, r#""id":42,"#, true));
        assert!(contains(PRETTY, r#"["a","b"]"#, true));
    }

    /// Перенос строки с отступом — такой же пробел, как один пробел.
    #[test]
    fn indentation_counts_as_whitespace() {
        assert!(contains(PRETTY, "{\"id\": 42", true));
        assert!(contains(PRETTY, r#""tags": ["a""#, true));
    }

    /// Пробел ВНУТРИ строки JSON значащий, и склеивать по нему нельзя.
    #[test]
    fn whitespace_inside_a_string_literal_stays_exact() {
        let body = br#"{"name":"Alice Smith"}"#;
        assert!(contains(body, r#""name": "Alice Smith""#, true));
        // Внутри литерала пробел обязателен — иначе это другое значение.
        assert!(!contains(body, r#""name": "AliceSmith""#, true));
        assert!(!contains(body, r#""name":"Alice  Smith""#, true));
    }

    /// Обычный текстовый запрос от послаблений не меняется: пробел вне знаков
    /// JSON по-прежнему обязателен.
    #[test]
    fn plain_text_still_requires_its_spaces() {
        assert!(!contains(b"helloworld", "hello world", false));
        assert!(contains(b"hello world", "hello world", false));
        // А вот через перенос строки — то же самое слово через пробел.
        assert!(contains(b"hello\n  world", "hello world", false));
    }

    #[test]
    fn a_needle_without_json_punctuation_takes_the_exact_path_only() {
        // Нечего послаблять — второй проход не строится вовсе.
        assert!(Loose::compile("hello", false).is_none());
        assert!(Loose::compile(r#""hello world""#, false).is_none());
        // А здесь есть.
        assert!(Loose::compile(r#""a": 1"#, false).is_some());
        assert!(Loose::compile("a,b", false).is_some());
    }

    /// Пробел на краю запроса не с чем соотнести, и требовать его нельзя:
    /// иначе скопированная с отступом строка не нашлась бы в сжатом теле.
    #[test]
    fn leading_and_trailing_whitespace_is_dropped() {
        assert!(contains(COMPACT, "  \"name\": \"Alice\",\n", true));
    }

    #[test]
    fn the_loose_pass_folds_case_in_cyrillic_too() {
        let body = r#"{"город":"Москва"}"#.as_bytes();
        assert!(contains(body, r#""город": "москва""#, false));
        assert!(contains(body, r#""ГОРОД": "МОСКВА""#, false));
        assert!(!contains(body, r#""город": "москва""#, true));
        assert!(contains(body, r#""город": "Москва""#, true));
    }

    /// Послабление касается пробелов, а не текста: сами куски сравниваются
    /// буквально, и чужое значение не подойдёт.
    #[test]
    fn loosening_whitespace_does_not_loosen_anything_else() {
        assert!(!contains(COMPACT, r#""name": "Bob""#, true));
        assert!(!contains(COMPACT, r#""nam": "Alice""#, true));
        assert!(!contains(COMPACT, r#""id": 43,"#, true));
    }

    /// Незакрытая кавычка — обычное дело для скопированного куска. Разбор
    /// обязан это пережить, а не потерять запрос.
    #[test]
    fn an_unbalanced_quote_does_not_break_the_query() {
        assert!(contains(COMPACT, r#""name": "Alice"#, true));
        assert!(contains(COMPACT, r#""tags": ["#, true));
    }

    /// Совпадение ищется дальше первого неудачного кандидата и на нежёстком
    /// пути тоже.
    #[test]
    fn the_loose_pass_retries_after_a_false_candidate() {
        let body = br#"{"a":1,"aa":{"b": 2}}"#;
        assert!(contains(body, r#""aa": {"b":2}"#, true));
    }
}
