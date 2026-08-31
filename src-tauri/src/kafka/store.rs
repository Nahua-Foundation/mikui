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
//!
//! `index` разрезан границей `committed` на две части:
//!
//! * `[0, committed)` — **опубликованное**. Это и только это видит UI. Порядок
//!   здесь заморожен навсегда: строка, однажды показанная под номером 42, под
//!   этим номером и останется.
//! * `[committed, len)` — **отстойник**. Сюда падает всё, что вычитано, но ещё
//!   не заняло окончательного места в глобальном порядке: пока хоть одна
//!   партиция может выдать сообщение новее (или, в режиме oldest, старее),
//!   публиковать нельзя — иначе следующая порция вклинилась бы ВЫШЕ уже
//!   показанных строк, и таблица поехала бы под курсором.
//!
//! Раньше границы не было, и `sort_by_time` перетряхивал весь буфер на каждом
//! шаге чтения. Строки прыгали, а фронту приходилось сбрасывать кэш окон —
//! отсюда и бесконечно мерцающие плейсхолдеры.

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
    /// Граница опубликованного префикса — см. модульный комментарий.
    committed: usize,
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
            committed: 0,
        }
    }

    /// Сколько всего вычитано, включая ещё не опубликованный отстойник.
    pub fn len(&self) -> usize {
        self.index.len()
    }

    /// Сколько строк видит UI. Именно это число фигурирует в статистике —
    /// показывать «загружено 5000», когда в таблице 4200 строк, значило бы
    /// врать пользователю.
    pub fn committed_len(&self) -> usize {
        self.committed
    }

    pub fn staged_len(&self) -> usize {
        self.index.len() - self.committed
    }

    /// ПОЛЕЗНЫЕ байты: ровно то, что уложено в арену. Не путать с занятой
    /// памятью — см. `memory_bytes`.
    pub fn byte_size(&self) -> usize {
        self.blob.len()
    }

    /// Сколько памяти хранилище занимает НА САМОМ ДЕЛЕ.
    ///
    /// К полезным байтам добавляются две вещи, о которых нельзя умалчивать в
    /// шкале потребления: плотный индекс (по записи фиксированного размера на
    /// сообщение — на 50k это ещё пара мегабайт) и незанятый запас ёмкости,
    /// который `Vec` держит после роста и который тоже занят по-настоящему.
    ///
    /// Считается по `capacity`, а не по `len`: удвоение ёмкости — это реально
    /// выделенная память, и в момент, когда буфер вырос вдвое «на будущее»,
    /// шкала обязана это показать.
    pub fn memory_bytes(&self) -> usize {
        self.blob.capacity() + self.index.capacity() * std::mem::size_of::<MessageMeta>()
    }

    /// Потолок, по которому чтение считается усечённым. Отдаётся наружу, чтобы
    /// шкала в футере не повторяла константу у себя.
    pub fn max_bytes(&self) -> usize {
        self.max_bytes
    }

    pub fn is_full(&self) -> bool {
        self.blob.len() >= self.max_bytes
    }

    pub fn clear(&mut self) {
        // Ёмкость сохраняем: следующее открытие топика переиспользует буфер
        // вместо новой крупной аллокации.
        self.blob.clear();
        self.index.clear();
        self.committed = 0;
    }

    /// Освобождает память под буфером. Вызывается при закрытии топика —
    /// нет смысла держать сотни мегабайт, пока пользователь ничего не смотрит.
    pub fn release(&mut self) {
        self.blob = Vec::new();
        self.index = Vec::new();
        self.committed = 0;
    }

    /// Отрезает хвост индекса вместе с его байтами. Нужна ровно для одного
    /// случая: раунд чтения оборвался на середине (дедлайн, переполнение
    /// буфера), и его добыча — дырявое окно, которое нельзя ни опубликовать,
    /// ни докатить. Дешевле выбросить и перечитать окно целиком.
    pub fn truncate(&mut self, len: usize) {
        if len >= self.index.len() {
            return;
        }
        // Сообщения кладутся в blob последовательно, а заголовки — последними
        // в записи, поэтому конец записи `len - 1` и есть новая длина blob.
        let blob_len = match len.checked_sub(1) {
            Some(last) => {
                let meta = &self.index[last];
                meta.headers.start as usize + meta.headers.len as usize
            }
            None => 0,
        };
        self.index.truncate(len);
        self.blob.truncate(blob_len);
        self.committed = self.committed.min(len);
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

    /// Новые сверху / старые сверху — но **только внутри отстойника**.
    /// Опубликованный префикс не трогается: он уже занял своё место.
    /// Сортируется только `index`, байты в blob не двигаются, поэтому
    /// перестановка стоит копейки.
    pub fn sort_staged(&mut self, newest_first: bool) {
        let staged = &mut self.index[self.committed..];
        if newest_first {
            staged
                .sort_unstable_by_key(|m| (std::cmp::Reverse(m.timestamp), m.partition, m.offset));
        } else {
            staged.sort_unstable_by_key(|m| (m.timestamp, m.partition, m.offset));
        }
    }

    /// Публикует префикс отсортированного отстойника, пока `keep` возвращает
    /// true, и отдаёт число опубликованных записей.
    ///
    /// Предполагает, что `keep` монотонен по уже применённому порядку (у нас
    /// это всегда сравнение timestamp с порогом) — отсюда `partition_point`
    /// вместо линейного прохода.
    pub fn commit_staged_while(&mut self, keep: impl Fn(&MessageMeta) -> bool) -> usize {
        let n = self.index[self.committed..].partition_point(keep);
        self.committed += n;
        n
    }

    /// Публикует отстойник целиком. Законно только когда читать больше нечего:
    /// иначе следующая порция могла бы встать выше только что показанного.
    pub fn commit_all(&mut self) -> usize {
        let n = self.staged_len();
        self.committed = self.index.len();
        n
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

        store.sort_staged(true);

        // Порядок по времени убывающий, и payload по-прежнему тот, что нужно.
        assert_eq!(store.get(0).unwrap().timestamp, 300);
        assert_eq!(store.value(0), b"value-2");
        assert_eq!(store.get(2).unwrap().timestamp, 100);
        assert_eq!(store.value(2), b"value-1");
    }

    #[test]
    fn published_prefix_never_moves_when_older_data_arrives() {
        let mut store = MessageStore::new(DEFAULT_MAX_BYTES);
        // Первый раунд: две свежие записи, публикуем обе.
        push_simple(&mut store, 0, 1, 300);
        push_simple(&mut store, 0, 2, 200);
        store.sort_staged(true);
        store.commit_all();

        // Второй раунд приносит запись, которая ПО ВРЕМЕНИ должна была бы
        // встать между ними. Права на это у неё уже нет.
        push_simple(&mut store, 0, 3, 250);
        store.sort_staged(true);
        store.commit_all();

        assert_eq!(store.get(0).unwrap().timestamp, 300);
        assert_eq!(store.get(1).unwrap().timestamp, 200);
        assert_eq!(store.get(2).unwrap().timestamp, 250);
    }

    #[test]
    fn commit_staged_while_holds_back_the_unsafe_tail() {
        let mut store = MessageStore::new(DEFAULT_MAX_BYTES);
        for ts in [500, 400, 300, 200] {
            push_simple(&mut store, 0, ts, ts);
        }
        store.sort_staged(true);

        // Отстающая партиция вычитана только до 300 — всё, что не новее,
        // придерживаем: она ещё может выдать что-то между 300 и 200.
        let published = store.commit_staged_while(|m| m.timestamp > 300);
        assert_eq!(published, 2);
        assert_eq!(store.committed_len(), 2);
        assert_eq!(store.staged_len(), 2);

        // Партиция догналась — остаток встал следом, префикс не шелохнулся.
        store.sort_staged(true);
        assert_eq!(store.commit_all(), 2);
        assert_eq!(store.get(0).unwrap().timestamp, 500);
        assert_eq!(store.get(3).unwrap().timestamp, 200);
    }

    #[test]
    fn truncate_reclaims_blob_and_pulls_committed_back() {
        let mut store = MessageStore::new(DEFAULT_MAX_BYTES);
        for i in 0..10 {
            push_simple(&mut store, 0, i, i);
        }
        store.commit_all();
        let bytes_before = store.byte_size();

        store.truncate(4);
        assert_eq!(store.len(), 4);
        assert_eq!(store.committed_len(), 4);
        assert!(
            store.byte_size() < bytes_before,
            "байты хвоста должны освободиться"
        );
        // Уцелевшие записи по-прежнему читаются корректно.
        assert_eq!(store.value(3), b"value-3");
        // И буфер снова растёт с правильного места.
        assert!(push_simple(&mut store, 0, 99, 99));
        assert_eq!(store.value(4), b"value-99");
        assert_eq!(store.value(0), b"value-0");
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

    #[test]
    fn clear_and_release_reset_the_published_boundary() {
        let mut store = MessageStore::new(DEFAULT_MAX_BYTES);
        push_simple(&mut store, 0, 1, 100);
        store.commit_all();
        assert_eq!(store.committed_len(), 1);

        store.clear();
        assert_eq!(store.committed_len(), 0);

        push_simple(&mut store, 0, 1, 100);
        store.commit_all();
        store.release();
        assert_eq!(store.committed_len(), 0);
    }
}
