//! Компактное хранилище сообщений.
//!
//! Наивный `Vec<Struct { String, String, HashMap }>` даёт 3-4 аллокации на
//! сообщение и раскидывает данные по куче. На десятках тысяч сообщений это
//! становится основным потребителем памяти и убийцей кэша процессора.
//!
//! Здесь всё иначе: байты всех сообщений лежат подряд в одном растущем буфере
//! (`blob`), а рядом идёт плотный массив записей фиксированного размера
//! (`index`) со смещениями в него. Байты копируются ровно один раз — из буфера
//! rdkafka в арену. Сортировка и фильтрация работают по `index`, который на
//! 50k сообщений занимает около 2 МБ и отлично ложится в кэш.

/// Кусок байтов в `blob`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Span {
    start: u32,
    len: u32,
}

impl Span {
    fn range(&self) -> std::ops::Range<usize> {
        self.start as usize..(self.start as usize + self.len as usize)
    }
}

/// Запись фиксированного размера. Никаких `String` и `HashMap`.
#[derive(Clone, Copy, Debug)]
pub struct MessageMeta {
    pub partition: i32,
    pub offset: i64,
    pub timestamp: i64,
    key: Span,
    value: Span,
    /// Заголовки уложены в blob как последовательность
    /// `u32 key_len | key | u32 val_len | val`.
    headers: Span,
    header_count: u32,
}

pub struct MessageStore {
    blob: Vec<u8>,
    index: Vec<MessageMeta>,
    max_bytes: usize,
}

/// Потолок буфера. Упёрлись — считаем выдачу усечённой и прекращаем чтение,
/// вместо того чтобы расти до OOM на топике с гигабайтными сообщениями.
pub const DEFAULT_MAX_BYTES: usize = 256 * 1024 * 1024;

impl Default for MessageStore {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_BYTES)
    }
}

impl MessageStore {
    pub fn new(max_bytes: usize) -> Self {
        Self {
            blob: Vec::new(),
            index: Vec::new(),
            max_bytes,
        }
    }

    pub fn len(&self) -> usize {
        self.index.len()
    }

    pub fn byte_size(&self) -> usize {
        self.blob.len()
    }

    pub fn is_full(&self) -> bool {
        self.blob.len() >= self.max_bytes
    }

    pub fn clear(&mut self) {
        // Ёмкость сохраняем: следующее открытие топика переиспользует буфер
        // вместо новой крупной аллокации.
        self.blob.clear();
        self.index.clear();
    }

    /// Освобождает память под буфером. Вызывается при закрытии топика —
    /// нет смысла держать сотни мегабайт, пока пользователь ничего не смотрит.
    pub fn release(&mut self) {
        self.blob = Vec::new();
        self.index = Vec::new();
    }

    pub fn get(&self, i: usize) -> Option<&MessageMeta> {
        self.index.get(i)
    }

    pub fn key(&self, i: usize) -> &[u8] {
        &self.blob[self.index[i].key.range()]
    }

    pub fn value(&self, i: usize) -> &[u8] {
        &self.blob[self.index[i].value.range()]
    }

    /// Разбирает уложенные заголовки. Ленивая операция: вызывается только для
    /// сообщения, которое пользователь реально открыл.
    pub fn headers(&self, i: usize) -> Vec<(&str, &[u8])> {
        let meta = &self.index[i];
        let mut out = Vec::with_capacity(meta.header_count as usize);
        let bytes = &self.blob[meta.headers.range()];
        let mut pos = 0usize;

        for _ in 0..meta.header_count {
            let Some((key, next)) = read_chunk(bytes, pos) else {
                break;
            };
            let Some((value, next)) = read_chunk(bytes, next) else {
                break;
            };
            pos = next;
            out.push((std::str::from_utf8(key).unwrap_or("<invalid utf-8>"), value));
        }
        out
    }

    /// Добавляет сообщение. Возвращает false, если буфер исчерпан.
    pub fn push(
        &mut self,
        partition: i32,
        offset: i64,
        timestamp: i64,
        key: &[u8],
        value: &[u8],
        headers: &[(&str, &[u8])],
    ) -> bool {
        if self.is_full() {
            return false;
        }

        let key_span = self.append(key);
        let value_span = self.append(value);

        let headers_start = self.blob.len();
        for (name, val) in headers {
            self.append_chunk(name.as_bytes());
            self.append_chunk(val);
        }
        let headers_span = Span {
            start: headers_start as u32,
            len: (self.blob.len() - headers_start) as u32,
        };

        self.index.push(MessageMeta {
            partition,
            offset,
            timestamp,
            key: key_span,
            value: value_span,
            headers: headers_span,
            header_count: headers.len() as u32,
        });
        true
    }

    /// Новые сверху / старые сверху. Сортируется только `index` — байты в blob
    /// не двигаются, поэтому перестановка стоит копейки даже на 50k записей.
    pub fn sort_by_time(&mut self, newest_first: bool) {
        if newest_first {
            self.index
                .sort_unstable_by_key(|m| (std::cmp::Reverse(m.timestamp), m.partition, m.offset));
        } else {
            self.index
                .sort_unstable_by_key(|m| (m.timestamp, m.partition, m.offset));
        }
    }

    fn append(&mut self, bytes: &[u8]) -> Span {
        let start = self.blob.len() as u32;
        self.blob.extend_from_slice(bytes);
        Span {
            start,
            len: bytes.len() as u32,
        }
    }

    fn append_chunk(&mut self, bytes: &[u8]) {
        self.blob
            .extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        self.blob.extend_from_slice(bytes);
    }
}

fn read_chunk(bytes: &[u8], pos: usize) -> Option<(&[u8], usize)> {
    let header_end = pos.checked_add(4)?;
    let len_bytes: [u8; 4] = bytes.get(pos..header_end)?.try_into().ok()?;
    let len = u32::from_le_bytes(len_bytes) as usize;
    let end = header_end.checked_add(len)?;
    Some((bytes.get(header_end..end)?, end))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn push_simple(store: &mut MessageStore, partition: i32, offset: i64, ts: i64) -> bool {
        store.push(
            partition,
            offset,
            ts,
            format!("key-{offset}").as_bytes(),
            format!("value-{offset}").as_bytes(),
            &[],
        )
    }

    #[test]
    fn stores_and_reads_back_payloads() {
        let mut store = MessageStore::new(DEFAULT_MAX_BYTES);
        assert!(push_simple(&mut store, 0, 1, 100));
        assert!(push_simple(&mut store, 1, 2, 200));

        assert_eq!(store.len(), 2);
        assert_eq!(store.key(0), b"key-1");
        assert_eq!(store.value(0), b"value-1");
        assert_eq!(store.key(1), b"key-2");
        assert_eq!(store.value(1), b"value-2");
        assert_eq!(store.get(1).unwrap().partition, 1);
    }

    #[test]
    fn handles_empty_key_and_value() {
        let mut store = MessageStore::new(DEFAULT_MAX_BYTES);
        assert!(store.push(0, 0, 0, b"", b"", &[]));
        assert_eq!(store.key(0), b"");
        assert_eq!(store.value(0), b"");
        assert!(store.headers(0).is_empty());
    }

    #[test]
    fn roundtrips_headers_including_empty_values() {
        let mut store = MessageStore::new(DEFAULT_MAX_BYTES);
        let headers: Vec<(&str, &[u8])> = vec![
            ("content-type", b"application/json".as_slice()),
            ("empty", b"".as_slice()),
            ("binary", &[0xff, 0x00, 0xfe]),
        ];
        assert!(store.push(0, 0, 0, b"k", b"v", &headers));

        let read = store.headers(0);
        assert_eq!(read.len(), 3);
        assert_eq!(read[0], ("content-type", b"application/json".as_slice()));
        assert_eq!(read[1], ("empty", b"".as_slice()));
        assert_eq!(read[2], ("binary", [0xff, 0x00, 0xfe].as_slice()));
    }

    #[test]
    fn spans_stay_correct_across_many_messages() {
        let mut store = MessageStore::new(DEFAULT_MAX_BYTES);
        for i in 0..1000 {
            assert!(push_simple(&mut store, (i % 4) as i32, i, i));
        }
        // Границы не поехали ни на первом, ни на последнем, ни в середине.
        assert_eq!(store.key(0), b"key-0");
        assert_eq!(store.value(500), b"value-500");
        assert_eq!(store.key(999), b"key-999");
    }

    #[test]
    fn sorts_newest_first_without_touching_payloads() {
        let mut store = MessageStore::new(DEFAULT_MAX_BYTES);
        push_simple(&mut store, 0, 1, 100);
        push_simple(&mut store, 0, 2, 300);
        push_simple(&mut store, 0, 3, 200);

        store.sort_by_time(true);

        // Порядок по времени убывающий, и payload по-прежнему тот, что нужно.
        assert_eq!(store.get(0).unwrap().timestamp, 300);
        assert_eq!(store.value(0), b"value-2");
        assert_eq!(store.get(2).unwrap().timestamp, 100);
        assert_eq!(store.value(2), b"value-1");
    }

    #[test]
    fn stops_accepting_when_budget_is_exhausted() {
        // Потолка хватит на пару сообщений, не больше.
        let mut store = MessageStore::new(32);
        let mut accepted = 0;
        for i in 0..100 {
            if push_simple(&mut store, 0, i, i) {
                accepted += 1;
            }
        }
        assert!(accepted > 0, "хоть что-то должно поместиться");
        assert!(accepted < 100, "переполнение должно остановить приём");
        assert!(store.is_full());
    }

    #[test]
    fn clear_keeps_capacity_release_frees_it() {
        let mut store = MessageStore::new(DEFAULT_MAX_BYTES);
        for i in 0..100 {
            push_simple(&mut store, 0, i, i);
        }
        let capacity_before = store.blob.capacity();
        store.clear();
        assert_eq!(store.len(), 0);
        assert_eq!(store.byte_size(), 0);
        assert_eq!(store.blob.capacity(), capacity_before);

        store.release();
        assert_eq!(store.blob.capacity(), 0);
    }

    #[test]
    fn truncate_drops_tail_only() {
        let mut store = MessageStore::new(DEFAULT_MAX_BYTES);
        for i in 0..10 {
            push_simple(&mut store, 0, i, i);
        }
        store.truncate(3);
        assert_eq!(store.len(), 3);
        assert_eq!(store.value(2), b"value-2");
    }
}
