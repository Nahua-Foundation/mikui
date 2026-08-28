//! Kafka-воркер на выделенном потоке.
//!
//! Раньше состояние жило в `Mutex<App>`, и блокирующий сетевой вызов
//! (`fetch_metadata`, poll) выполнялся прямо в async-команде Tauri, удерживая
//! и мьютекс, и поток исполнителя. Долгая вычитка вставала поперёк всего
//! остального UI.
//!
//! Теперь весь блокирующий Kafka-код заперт на одном обычном потоке. Команды
//! приезжают к нему по каналу, ответ уходит через oneshot, который
//! Tauri-команда спокойно `.await`-ит, никого не блокируя.

use std::collections::HashSet;
use std::sync::mpsc::{self, Receiver, Sender};
use std::time::{Duration, Instant};

use rdkafka::admin::AdminClient;
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::types::RDKafkaRespErr;
use tokio::sync::oneshot;

use super::filter::contains;
use super::raw_consumer::{self, RawMessage, RawQueue, RawTopic};
use super::store::MessageStore;
use super::text::{self, PREVIEW_BYTES};
use super::types::*;
use crate::helpers::get_cluster_config;

/// Потолок времени на одно открытие топика. Без него пустой или медленный
/// топик подвесил бы воркер навсегда.
const READ_DEADLINE: Duration = Duration::from_secs(30);
/// Шаг опроса очереди, в миллисекундах — для `rd_kafka_consume_queue`.
/// Достаточно мал, чтобы дедлайн срабатывал вовремя.
const POLL_TICK_MS: i32 = 200;
const METADATA_TIMEOUT: Duration = Duration::from_secs(10);

type Reply<T> = oneshot::Sender<T>;

pub enum Command {
    Connect(ClusterConnectPayload, Reply<Result<(), String>>),
    Test(ClusterConnectPayload, Reply<Result<(), String>>),
    Disconnect(Reply<()>),
    ListTopics(Reply<Result<Vec<TopicInfo>, String>>),
    OpenTopic(OpenTopicParams, Reply<Result<OpenTopicResult, String>>),
    SetFilter(MessageFilter, Reply<Result<usize, String>>),
    GetWindow {
        start: usize,
        count: usize,
        reply: Reply<Result<Vec<RowPreview>, String>>,
    },
    GetBody(usize, Reply<Result<FullMessage, String>>),
    CloseTopic(Reply<()>),
}

/// Ручка воркера. Кладётся в Tauri state; `Sender` начиная с Rust 1.72
/// является `Sync`, поэтому обёртка в мьютекс не нужна.
pub struct WorkerHandle {
    tx: Sender<Command>,
}

impl WorkerHandle {
    pub fn spawn() -> Self {
        let (tx, rx) = mpsc::channel();
        std::thread::Builder::new()
            .name("kafka-worker".into())
            .spawn(move || Worker::default().run(rx))
            .expect("failed to spawn kafka worker thread");
        Self { tx }
    }

    /// Отправляет команду и ждёт ответ, не занимая поток исполнителя.
    pub async fn call<T, F>(&self, make: F) -> Result<T, String>
    where
        F: FnOnce(Reply<T>) -> Command,
    {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(make(reply_tx))
            .map_err(|_| "kafka worker is gone".to_string())?;
        reply_rx
            .await
            .map_err(|_| "kafka worker dropped the reply".to_string())
    }
}

/// Перекладывает сообщение из буфера rdkafka в арену. Свободная функция, а не
/// метод: во время чтения `store` заимствуется отдельно от остального `Worker`.
/// Возвращает false, когда бюджет буфера исчерпан.
fn absorb(store: &mut MessageStore, msg: &RawMessage) -> bool {
    let headers = msg.headers();
    store.push(
        msg.partition(),
        msg.offset(),
        msg.timestamp_millis(),
        msg.key().unwrap_or(&[]),
        msg.payload().unwrap_or(&[]),
        &headers,
    )
}

/// Читает из очереди, пока не наберётся `limit` сообщений или все партиции
/// не дойдут до EOF. Возвращает `true`, если упёрлись в лимит буфера или
/// дедлайн раньше, чем реально кончились данные.
fn read_into_store(
    queue: &RawQueue,
    store: &mut MessageStore,
    limit: usize,
    partition_count: usize,
) -> Result<bool, String> {
    let mut eof: HashSet<i32> = HashSet::with_capacity(partition_count);
    let deadline = Instant::now() + READ_DEADLINE;
    let started = Instant::now();
    let mut polls: u64 = 0;
    let mut absorbed: u64 = 0;

    eprintln!("[read_into_store] start limit={limit} partition_count={partition_count}");

    while store.len() < limit && eof.len() < partition_count {
        if Instant::now() >= deadline {
            eprintln!(
                "[read_into_store] DEADLINE after {:?}: polls={polls} absorbed={absorbed} eof={}/{partition_count} store.len={}",
                started.elapsed(),
                eof.len(),
                store.len()
            );
            return Ok(true);
        }
        polls += 1;
        match raw_consumer::consume(queue, POLL_TICK_MS) {
            None => continue,
            Some(msg) => match msg.err() {
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR => {
                    absorbed += 1;
                    if !absorb(store, &msg) {
                        eprintln!(
                            "[read_into_store] buffer budget hit after {:?}: polls={polls} absorbed={absorbed} eof={}/{partition_count}",
                            started.elapsed(),
                            eof.len()
                        );
                        return Ok(true);
                    }
                }
                RDKafkaRespErr::RD_KAFKA_RESP_ERR__PARTITION_EOF => {
                    eof.insert(msg.partition());
                    eprintln!(
                        "[read_into_store] partition {} EOF ({}/{partition_count}) after {:?}, absorbed so far={absorbed}",
                        msg.partition(),
                        eof.len(),
                        started.elapsed()
                    );
                }
                other => {
                    let e = raw_consumer::err_str(other);
                    eprintln!(
                        "[read_into_store] READ ERROR after {:?}: {e} (polls={polls} absorbed={absorbed})",
                        started.elapsed()
                    );
                    return Err(format!("read failed: {e}"));
                }
            },
        }
    }

    eprintln!(
        "[read_into_store] done after {:?}: polls={polls} absorbed={absorbed} eof={}/{partition_count} store.len={}",
        started.elapsed(),
        eof.len(),
        store.len()
    );
    Ok(false)
}

#[derive(Default)]
struct Worker {
    consumer: Option<BaseConsumer>,
    admin: Option<AdminClient<DefaultClientContext>>,
    /// Количество партиций по топикам — приезжает с метаданными и переиспользуется,
    /// чтобы не ходить за ними повторно на каждое открытие топика.
    partition_counts: std::collections::HashMap<String, usize>,
    store: MessageStore,
    /// Индексы в `store` после применения фильтра. Это и есть то, что видит UI.
    view: Vec<u32>,
    filter: MessageFilter,
    open_topic: Option<String>,
}

impl Worker {
    fn run(mut self, rx: Receiver<Command>) {
        // Канал закрылся — приложение завершается, поток выходит.
        while let Ok(cmd) = rx.recv() {
            match cmd {
                Command::Connect(payload, reply) => {
                    let _ = reply.send(self.connect(payload));
                }
                Command::Test(payload, reply) => {
                    let _ = reply.send(Self::test(payload));
                }
                Command::Disconnect(reply) => {
                    self.disconnect();
                    let _ = reply.send(());
                }
                Command::ListTopics(reply) => {
                    let _ = reply.send(self.list_topics());
                }
                Command::OpenTopic(params, reply) => {
                    let _ = reply.send(self.open_topic(params));
                }
                Command::SetFilter(filter, reply) => {
                    self.filter = filter;
                    self.rebuild_view();
                    let _ = reply.send(Ok(self.view.len()));
                }
                Command::GetWindow {
                    start,
                    count,
                    reply,
                } => {
                    let _ = reply.send(Ok(self.window(start, count)));
                }
                Command::GetBody(index, reply) => {
                    let _ = reply.send(self.body(index));
                }
                Command::CloseTopic(reply) => {
                    self.close_topic();
                    let _ = reply.send(());
                }
            }
        }
    }

    fn connect(&mut self, payload: ClusterConnectPayload) -> Result<(), String> {
        let conf = get_cluster_config(&payload);
        let consumer: BaseConsumer = conf
            .create()
            .map_err(|e| format!("can't create consumer: {e}"))?;
        let admin: AdminClient<_> = conf
            .create()
            .map_err(|e| format!("can't create admin client: {e}"))?;

        self.consumer = Some(consumer);
        self.admin = Some(admin);
        self.partition_counts.clear();
        self.close_topic();
        Ok(())
    }

    fn test(payload: ClusterConnectPayload) -> Result<(), String> {
        let conf = get_cluster_config(&payload);
        let consumer: BaseConsumer = conf
            .create()
            .map_err(|e| format!("can't create consumer: {e}"))?;
        // Создание клиента ещё ничего не доказывает — librdkafka соединяется
        // лениво. Дёргаем метаданные, чтобы реально сходить в кластер.
        consumer
            .fetch_metadata(None, METADATA_TIMEOUT)
            .map_err(|e| format!("can't reach cluster: {e}"))?;
        Ok(())
    }

    fn disconnect(&mut self) {
        self.close_topic();
        drop(self.consumer.take());
        drop(self.admin.take());
        self.partition_counts.clear();
    }

    fn list_topics(&mut self) -> Result<Vec<TopicInfo>, String> {
        let consumer = self.consumer.as_ref().ok_or("not connected to a cluster")?;
        let md = consumer
            .fetch_metadata(None, METADATA_TIMEOUT)
            .map_err(|e| format!("can't load cluster metadata: {e}"))?;

        let mut topics = Vec::with_capacity(md.topics().len());
        self.partition_counts.clear();
        for t in md.topics() {
            let name = t.name().to_string();
            let partitions = t.partitions().len();
            self.partition_counts.insert(name.clone(), partitions);
            topics.push(TopicInfo { name, partitions });
        }
        Ok(topics)
    }

    /// Вычитывает окно сообщений в арену через group-less legacy consumer API
    /// (`kafka::raw_consumer`) — партиции читаются напрямую, без объекта
    /// консьюмер-группы и без связанных с ним ACL.
    ///
    /// Ключевой момент — `raw_consumer::offset_tail(n)`: librdkafka сама
    /// вычисляет `high_watermark - n` на своей стороне, поэтому «последние N»
    /// не требуют ни обхода `fetch_watermarks` по каждой партиции, ни чтения
    /// топика с начала.
    fn open_topic(&mut self, params: OpenTopicParams) -> Result<OpenTopicResult, String> {
        let limit = params.limit.clamp(1, 500_000);
        let partition_count = *self
            .partition_counts
            .get(&params.topic)
            .ok_or("unknown topic; refresh the topic list first")?;

        if partition_count == 0 {
            return Err(format!("topic '{}' has no partitions", params.topic));
        }

        let partitions: Vec<i32> = match params.partition {
            Some(p) if p >= 0 && (p as usize) < partition_count => vec![p],
            Some(p) => return Err(format!("partition {p} is out of range")),
            None => (0..partition_count as i32).collect(),
        };

        // Делим бюджет между партициями. Перебор не больше числа партиций —
        // лишнее срежется после сортировки.
        let per_partition = limit.div_ceil(partitions.len()).max(1) as i64;
        let start_offset = match params.start_from {
            StartFrom::Newest => raw_consumer::offset_tail(per_partition),
            StartFrom::Oldest => raw_consumer::OFFSET_BEGINNING,
        };

        eprintln!(
            "[open_topic] {} partitions={partitions:?} start_from={:?} limit={limit} per_partition={per_partition} start_offset={start_offset}",
            params.topic, params.start_from
        );

        let consumer = self.consumer.as_ref().ok_or("not connected to a cluster")?;
        for &p in &partitions {
            match consumer.fetch_watermarks(&params.topic, p, METADATA_TIMEOUT) {
                Ok((low, high)) => eprintln!(
                    "[open_topic] {} p{p} watermarks low={low} high={high} available={}",
                    params.topic,
                    high - low
                ),
                Err(e) => eprintln!(
                    "[open_topic] {} p{p} fetch_watermarks FAILED: {e}",
                    params.topic
                ),
            }
        }
        let client_ptr = consumer.client().native_ptr();

        let topic = RawTopic::new(client_ptr, &params.topic)?;
        let queue = RawQueue::new(client_ptr)?;

        let mut started: Vec<i32> = Vec::with_capacity(partitions.len());
        for &p in &partitions {
            match topic.consume_start_queue(p, start_offset, &queue) {
                Ok(()) => {
                    eprintln!("[open_topic] consume_start_queue p{p} offset={start_offset} ok");
                    started.push(p);
                }
                Err(e) => {
                    eprintln!(
                        "[open_topic] consume_start_queue p{p} offset={start_offset} FAILED: {e}"
                    );
                    for started_p in started {
                        topic.consume_stop(started_p);
                    }
                    return Err(format!("can't start reading partition {p}: {e}"));
                }
            }
        }

        self.store.clear();
        let read_result = read_into_store(&queue, &mut self.store, limit, partitions.len());

        for &p in &partitions {
            topic.consume_stop(p);
        }
        let hit_budget = read_result?;

        let truncated = hit_budget || self.store.len() >= limit;
        self.open_topic = Some(params.topic.clone());
        self.store
            .sort_by_time(params.start_from == StartFrom::Newest);
        self.store.truncate(limit);

        self.filter = params.filter;
        self.rebuild_view();

        eprintln!(
            "[open_topic] {} result: loaded={} view={} bytes={} truncated={truncated}",
            params.topic,
            self.store.len(),
            self.view.len(),
            self.store.byte_size()
        );

        Ok(OpenTopicResult {
            total: self.view.len(),
            loaded: self.store.len(),
            buffer_bytes: self.store.byte_size(),
            truncated,
        })
    }

    #[allow(dead_code)]
    fn currently_open(&self) -> Option<&str> {
        self.open_topic.as_deref()
    }

    fn close_topic(&mut self) {
        // Раньше здесь снималось назначение консьюмер-группы (`unassign()`);
        // legacy consumer API ничего не оставляет "подвешенным" между
        // вызовами `open_topic` — `RawTopic`/`RawQueue` уже остановлены и
        // уничтожены внутри него, снимать здесь нечего.
        self.open_topic = None;
        self.filter = MessageFilter::default();
        self.view.clear();
        // Освобождаем буфер целиком: держать сотни мегабайт, пока пользователь
        // ничего не смотрит, незачем.
        self.store.release();
    }

    /// Пересобирает отфильтрованное представление. Работает по буферу в памяти,
    /// без единого сетевого запроса — поэтому смена фильтра мгновенна.
    fn rebuild_view(&mut self) {
        // Забираем вектор себе: иначе `self.view.push` конфликтует с
        // одновременным заимствованием `self.filter` и `self.store`.
        // Ёмкость при этом сохраняется, повторных аллокаций нет.
        let mut view = std::mem::take(&mut self.view);
        view.clear();
        let total = self.store.len();

        if self.filter.is_empty() {
            view.extend(0..total as u32);
        } else {
            let key_needle = self.filter.key.as_bytes();
            let value_needle = self.filter.value.as_bytes();
            let cs = self.filter.case_sensitive;

            for i in 0..total {
                if !key_needle.is_empty() && !contains(self.store.key(i), key_needle, cs) {
                    continue;
                }
                if !value_needle.is_empty() && !contains(self.store.value(i), value_needle, cs) {
                    continue;
                }
                view.push(i as u32);
            }
        }

        self.view = view;
    }

    /// Отдаёт ровно то, что видно на экране, а не весь буфер.
    fn window(&self, start: usize, count: usize) -> Vec<RowPreview> {
        let end = start.saturating_add(count).min(self.view.len());
        if start >= end {
            return Vec::new();
        }

        self.view[start..end]
            .iter()
            .enumerate()
            .map(|(offset_in_window, &store_index)| {
                let i = store_index as usize;
                let meta = self.store.get(i).expect("view index out of sync with store");
                let value = self.store.value(i);
                RowPreview {
                    index: start + offset_in_window,
                    partition: meta.partition,
                    offset: meta.offset,
                    timestamp: meta.timestamp,
                    key: text::preview(self.store.key(i), PREVIEW_BYTES),
                    preview: text::preview(value, PREVIEW_BYTES),
                    value_size: value.len(),
                    binary: !text::is_text(value),
                }
            })
            .collect()
    }

    /// Полное тело — только когда пользователь открыл конкретное сообщение.
    fn body(&self, view_index: usize) -> Result<FullMessage, String> {
        let store_index = *self
            .view
            .get(view_index)
            .ok_or("message index out of range")? as usize;
        let meta = self
            .store
            .get(store_index)
            .ok_or("message index out of range")?;
        let value = self.store.value(store_index);

        Ok(FullMessage {
            partition: meta.partition,
            offset: meta.offset,
            timestamp: meta.timestamp,
            key: text::decode(self.store.key(store_index)),
            value: text::decode(value),
            value_size: value.len(),
            binary: !text::is_text(value),
            headers: self
                .store
                .headers(store_index)
                .into_iter()
                .map(|(k, v)| MessageHeader {
                    key: k.to_string(),
                    value: text::decode(v),
                })
                .collect(),
        })
    }
}
