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
//!
//! Долгая вычитка топика (`open_topic`/`load_more`) при этом не блокирует сам
//! поток целиком: она разбита на шаги (`Command::ContinueRead`), между
//! которыми воркер обслуживает остальные команды (`GetWindow`,
//! `GetOpenTopicProgress`, новый `OpenTopic` и т.д.) — без единого мьютекса
//! или второго потока, просто кооперативно уступая цикл `run()` после каждого
//! шага. Это и даёт прогрессивную отрисовку: фронт опрашивает прогресс, пока
//! `open_topic`/`load_more` ещё не ответили финальным результатом.

use std::collections::HashMap;
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

/// Потолок времени на одно чтение (открытие топика или "загрузить ещё").
/// Без него пустой или медленный топик подвесил бы чтение навсегда — партиции,
/// не успевшие закончиться, просто принудительно закрываются.
const READ_DEADLINE: Duration = Duration::from_secs(30);
/// Таймаут одного вызова `consume_batch` в шаге чтения, в миллисекундах.
const POLL_TICK_MS: i32 = 200;
/// Сколько сообщений забирать за один шаг кооперативного чтения. Между шагами
/// воркер обслуживает остальные команды — чем меньше шаг, тем отзывчивее
/// `GetWindow`/`GetOpenTopicProgress` во время долгой вычитки, но тем больше
/// накладных расходов на переключение.
const BATCH_SIZE: usize = 500;
const METADATA_TIMEOUT: Duration = Duration::from_secs(10);

type Reply<T> = oneshot::Sender<T>;

pub enum Command {
    Connect(ClusterConnectPayload, Reply<Result<(), String>>),
    Test(ClusterConnectPayload, Reply<Result<(), String>>),
    Disconnect(Reply<()>),
    ListTopics(Reply<Result<Vec<TopicInfo>, String>>),
    OpenTopic(OpenTopicParams, Reply<Result<OpenTopicResult, String>>),
    /// Самоадресованная команда: следующий шаг уже идущего чтения. Никогда не
    /// шлётся снаружи воркера.
    ContinueRead,
    LoadMore(LoadMoreParams, Reply<Result<OpenTopicResult, String>>),
    GetOpenTopicProgress(Reply<Result<OpenTopicProgress, String>>),
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
        let worker_tx = tx.clone();
        std::thread::Builder::new()
            .name("kafka-worker".into())
            .spawn(move || Worker::new(worker_tx).run(rx))
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

/// Состояние одной партиции в рамках текущего чтения (`open_topic` или
/// `load_more`).
struct PartitionCursor {
    /// Сколько сообщений разрешено набрать в ЭТОМ чтении (не суммарно).
    per_partition_limit: i64,
    absorbed: i64,
    oldest_offset_seen: Option<i64>,
    newest_offset_seen: Option<i64>,
    /// Только для "загрузить ещё" в режиме Newest: офсет, дойдя до которого
    /// (`>=`), партиция уже пересекается с ранее загруженным диапазоном —
    /// дальше читать незачем.
    boundary_offset: Option<i64>,
    /// Партиция больше не читается (не обязательно означает конец данных —
    /// см. `eof`).
    done: bool,
    /// true — партиция закрылась по-настоящему (`PARTITION_EOF`), то есть
    /// вычитан весь доступный диапазон и грузить дальше нечего. false —
    /// остановились из-за собственного лимита или общего дедлайна, то есть
    /// данные ещё могут быть.
    eof: bool,
}

/// Ещё не завершённое чтение — раскладывается по шагам через
/// `Command::ContinueRead`.
struct PendingRead {
    topic: RawTopic,
    queue: RawQueue,
    cursors: HashMap<i32, PartitionCursor>,
    started: Instant,
    reply: Reply<Result<OpenTopicResult, String>>,
    topic_name: String,
}

struct Worker {
    /// Клон собственного отправителя — нужен, чтобы слать себе `ContinueRead`.
    tx: Sender<Command>,
    consumer: Option<BaseConsumer>,
    admin: Option<AdminClient<DefaultClientContext>>,
    /// Количество партиций по топикам — приезжает с метаданными и переиспользуется,
    /// чтобы не ходить за ними повторно на каждое открытие топика.
    partition_counts: HashMap<String, usize>,
    store: MessageStore,
    /// Индексы в `store` после применения фильтра. Это и есть то, что видит UI.
    view: Vec<u32>,
    filter: MessageFilter,
    open_topic: Option<String>,
    /// Сортировать ли по убыванию времени. Ставится при `open_topic`,
    /// переиспользуется без изменений в `load_more` того же топика.
    newest_first: bool,
    pending_read: Option<PendingRead>,
    /// Курсоры последнего ЗАВЕРШЁННОГО чтения по каждой партиции — точка
    /// отсчёта для следующего `load_more`.
    partition_cursors: HashMap<i32, PartitionCursor>,
}

impl Worker {
    fn new(tx: Sender<Command>) -> Self {
        Self {
            tx,
            consumer: None,
            admin: None,
            partition_counts: HashMap::new(),
            store: MessageStore::default(),
            view: Vec::new(),
            filter: MessageFilter::default(),
            open_topic: None,
            newest_first: false,
            pending_read: None,
            partition_cursors: HashMap::new(),
        }
    }

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
                Command::OpenTopic(params, reply) => self.start_open_topic(params, reply),
                Command::ContinueRead => self.continue_read(),
                Command::LoadMore(params, reply) => self.start_load_more(params, reply),
                Command::GetOpenTopicProgress(reply) => {
                    let _ = reply.send(Ok(self.progress()));
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

    /// Останавливает уже идущее чтение (пользователь переключил топик или
    /// закрыл его, пока предыдущее чтение ещё не закончилось). Партии, уже
    /// закрытые (`done`), второй раз не трогаем. `reply` просто дропается —
    /// это отклоняет старый `invoke()` на фронте, что уже сегодня спокойно
    /// переживается флагом `cancelled` в эффекте открытия топика.
    fn cancel_pending_read(&mut self) {
        if let Some(pending) = self.pending_read.take() {
            for (&p, cursor) in &pending.cursors {
                if !cursor.done {
                    pending.topic.consume_stop(p);
                }
            }
            eprintln!(
                "[worker] {} read cancelled (superseded) after {:?}",
                pending.topic_name,
                pending.started.elapsed()
            );
        }
    }

    /// Вычитывает окно сообщений через group-less legacy consumer API
    /// (`kafka::raw_consumer`) — партиции читаются напрямую, без объекта
    /// консьюмер-группы и без связанных с ним ACL.
    ///
    /// Не блокирует воркер целиком: делает подготовку (валидация, офсеты,
    /// `consume_start_queue`) и один первый шаг чтения, дальше эстафету
    /// подхватывает `Command::ContinueRead`.
    fn start_open_topic(
        &mut self,
        params: OpenTopicParams,
        reply: Reply<Result<OpenTopicResult, String>>,
    ) {
        self.cancel_pending_read();

        let limit = params.limit.clamp(1, 100_000) as i64;
        let partition_count = match self.partition_counts.get(&params.topic).copied() {
            Some(c) => c,
            None => {
                let _ = reply.send(Err("unknown topic; refresh the topic list first".into()));
                return;
            }
        };
        if partition_count == 0 {
            let _ = reply.send(Err(format!("topic '{}' has no partitions", params.topic)));
            return;
        }

        let partitions: Vec<i32> = match params.partition {
            Some(p) if p >= 0 && (p as usize) < partition_count => vec![p],
            Some(p) => {
                let _ = reply.send(Err(format!("partition {p} is out of range")));
                return;
            }
            None => (0..partition_count as i32).collect(),
        };

        let start_offset = match params.start_from {
            StartFrom::Newest => raw_consumer::offset_tail(limit),
            StartFrom::Oldest => raw_consumer::OFFSET_BEGINNING,
        };

        eprintln!(
            "[open_topic] {} partitions={partitions:?} start_from={:?} per_partition_limit={limit} start_offset={start_offset}",
            params.topic, params.start_from
        );

        let consumer = match self.consumer.as_ref() {
            Some(c) => c,
            None => {
                let _ = reply.send(Err("not connected to a cluster".into()));
                return;
            }
        };
        let client_ptr = consumer.client().native_ptr();

        let topic = match RawTopic::new(client_ptr, &params.topic) {
            Ok(t) => t,
            Err(e) => {
                let _ = reply.send(Err(e));
                return;
            }
        };
        let queue = match RawQueue::new(client_ptr) {
            Ok(q) => q,
            Err(e) => {
                let _ = reply.send(Err(e));
                return;
            }
        };

        let mut cursors = HashMap::with_capacity(partitions.len());
        let mut started: Vec<i32> = Vec::with_capacity(partitions.len());
        for &p in &partitions {
            match topic.consume_start_queue(p, start_offset, &queue) {
                Ok(()) => {
                    started.push(p);
                    cursors.insert(
                        p,
                        PartitionCursor {
                            per_partition_limit: limit,
                            absorbed: 0,
                            oldest_offset_seen: None,
                            newest_offset_seen: None,
                            boundary_offset: None,
                            done: false,
                            eof: false,
                        },
                    );
                }
                Err(e) => {
                    eprintln!(
                        "[open_topic] consume_start_queue p{p} offset={start_offset} FAILED: {e}"
                    );
                    for started_p in started {
                        topic.consume_stop(started_p);
                    }
                    let _ = reply.send(Err(format!("can't start reading partition {p}: {e}")));
                    return;
                }
            }
        }

        self.store.clear();
        self.filter = params.filter;
        self.open_topic = Some(params.topic.clone());
        self.newest_first = params.start_from == StartFrom::Newest;
        self.partition_cursors.clear();
        self.rebuild_view();

        self.pending_read = Some(PendingRead {
            topic,
            queue,
            cursors,
            started: Instant::now(),
            reply,
            topic_name: params.topic,
        });

        self.continue_read();
    }

    /// "Загрузить ещё": продолжает каждую ещё не исчерпанную (`!eof`)
    /// партицию с того места, где остановилось предыдущее чтение — без
    /// повторного похода за watermarks (см. `PartitionCursor`).
    fn start_load_more(
        &mut self,
        params: LoadMoreParams,
        reply: Reply<Result<OpenTopicResult, String>>,
    ) {
        if self.pending_read.is_some() {
            let _ = reply.send(Err("a read is already in progress".into()));
            return;
        }
        let Some(topic_name) = self.open_topic.clone() else {
            let _ = reply.send(Err("no topic is open".into()));
            return;
        };
        if self.partition_cursors.is_empty() {
            let _ = reply.send(Err("nothing loaded yet".into()));
            return;
        }

        let additional = params.additional.clamp(1, 100_000) as i64;
        let consumer = match self.consumer.as_ref() {
            Some(c) => c,
            None => {
                let _ = reply.send(Err("not connected to a cluster".into()));
                return;
            }
        };
        let client_ptr = consumer.client().native_ptr();

        let topic = match RawTopic::new(client_ptr, &topic_name) {
            Ok(t) => t,
            Err(e) => {
                let _ = reply.send(Err(e));
                return;
            }
        };
        let queue = match RawQueue::new(client_ptr) {
            Ok(q) => q,
            Err(e) => {
                let _ = reply.send(Err(e));
                return;
            }
        };

        let mut cursors = HashMap::new();
        let mut started: Vec<i32> = Vec::new();
        for (&p, prev) in &self.partition_cursors {
            // По-настоящему кончилась (реальный EOF) — грузить нечего.
            if prev.eof {
                continue;
            }

            let (start_offset, boundary_offset) = if self.newest_first {
                (
                    raw_consumer::offset_tail(prev.absorbed.max(0) + additional),
                    prev.oldest_offset_seen,
                )
            } else {
                let resume = prev
                    .newest_offset_seen
                    .map(|o| o + 1)
                    .unwrap_or(raw_consumer::OFFSET_BEGINNING);
                (resume, None)
            };

            match topic.consume_start_queue(p, start_offset, &queue) {
                Ok(()) => {
                    started.push(p);
                    cursors.insert(
                        p,
                        PartitionCursor {
                            per_partition_limit: additional,
                            absorbed: 0,
                            oldest_offset_seen: prev.oldest_offset_seen,
                            newest_offset_seen: prev.newest_offset_seen,
                            boundary_offset,
                            done: false,
                            eof: false,
                        },
                    );
                }
                Err(e) => {
                    eprintln!(
                        "[load_more] consume_start_queue p{p} offset={start_offset} FAILED: {e}"
                    );
                    for started_p in started {
                        topic.consume_stop(started_p);
                    }
                    let _ = reply.send(Err(format!("can't resume partition {p}: {e}")));
                    return;
                }
            }
        }

        if cursors.is_empty() {
            // Все партиции уже вычитаны до конца — отвечаем текущим срезом.
            let _ = reply.send(Ok(OpenTopicResult {
                total: self.view.len(),
                loaded: self.store.len(),
                buffer_bytes: self.store.byte_size(),
                truncated: false,
            }));
            return;
        }

        eprintln!("[load_more] {topic_name} resuming partitions={started:?} additional={additional}");

        self.pending_read = Some(PendingRead {
            topic,
            queue,
            cursors,
            started: Instant::now(),
            reply,
            topic_name,
        });

        self.continue_read();
    }

    /// Один шаг уже идущего чтения: один `consume_batch`, обновление курсоров,
    /// пересборка `store`/`view` до консистентного состояния. Если не всё
    /// готово — шлёт себе `Command::ContinueRead` и возвращает управление
    /// циклу `run()`, чтобы между шагами обслуживались остальные команды.
    fn continue_read(&mut self) {
        let Some(mut pending) = self.pending_read.take() else {
            return;
        };

        let batch = raw_consumer::consume_batch(&pending.queue, POLL_TICK_MS, BATCH_SIZE);
        let mut budget_hit = false;

        for msg in batch {
            let p = msg.partition();
            match msg.err() {
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR => {
                    let already_done = pending.cursors.get(&p).is_none_or(|c| c.done);
                    if already_done {
                        // Партиция уже закрыта, но librdkafka успела доставить
                        // ещё пару "в полёте" сообщений после consume_stop.
                        continue;
                    }
                    if let Some(boundary) =
                        pending.cursors.get(&p).and_then(|c| c.boundary_offset)
                    {
                        if msg.offset() >= boundary {
                            if let Some(c) = pending.cursors.get_mut(&p) {
                                c.done = true;
                            }
                            pending.topic.consume_stop(p);
                            eprintln!(
                                "[continue_read] {} p{p} reached load-more boundary offset={boundary}",
                                pending.topic_name
                            );
                            continue;
                        }
                    }
                    if !absorb(&mut self.store, &msg) {
                        eprintln!(
                            "[continue_read] {} buffer budget hit, absorbed so far in store={}",
                            pending.topic_name,
                            self.store.len()
                        );
                        budget_hit = true;
                        break;
                    }
                    if let Some(c) = pending.cursors.get_mut(&p) {
                        c.absorbed += 1;
                        let offset = msg.offset();
                        c.oldest_offset_seen =
                            Some(c.oldest_offset_seen.map_or(offset, |o| o.min(offset)));
                        c.newest_offset_seen =
                            Some(c.newest_offset_seen.map_or(offset, |o| o.max(offset)));
                        if c.absorbed >= c.per_partition_limit {
                            c.done = true;
                            pending.topic.consume_stop(p);
                        }
                    }
                }
                RDKafkaRespErr::RD_KAFKA_RESP_ERR__PARTITION_EOF => {
                    if let Some(c) = pending.cursors.get_mut(&p) {
                        if !c.done {
                            c.done = true;
                            c.eof = true;
                            eprintln!(
                                "[continue_read] {} p{p} EOF, absorbed={}",
                                pending.topic_name, c.absorbed
                            );
                        }
                    }
                }
                other => {
                    let e = raw_consumer::err_str(other);
                    eprintln!("[continue_read] {} READ ERROR: {e}", pending.topic_name);
                    for (&pp, c) in &pending.cursors {
                        if !c.done {
                            pending.topic.consume_stop(pp);
                        }
                    }
                    let _ = pending.reply.send(Err(format!("read failed: {e}")));
                    return;
                }
            }
        }

        let deadline_hit = pending.started.elapsed() >= READ_DEADLINE;
        if deadline_hit {
            eprintln!(
                "[continue_read] {} DEADLINE after {:?}",
                pending.topic_name,
                pending.started.elapsed()
            );
            for (&p, c) in pending.cursors.iter_mut() {
                if !c.done {
                    c.done = true;
                    pending.topic.consume_stop(p);
                }
            }
        }

        // Пересобираем ВСЕГДА, даже на промежуточном шаге: конкурентные
        // GetWindow/GetOpenTopicProgress должны видеть консистентный,
        // отсортированный и отфильтрованный срез того, что уже вычитано.
        self.store.sort_by_time(self.newest_first);
        self.rebuild_view();

        let all_done = pending.cursors.values().all(|c| c.done);
        if all_done || budget_hit {
            for (&p, c) in &pending.cursors {
                if !c.done {
                    pending.topic.consume_stop(p);
                }
            }
            let truncated = budget_hit || pending.cursors.values().any(|c| !c.eof);
            eprintln!(
                "[continue_read] {} finished: loaded={} view={} bytes={} truncated={truncated}",
                pending.topic_name,
                self.store.len(),
                self.view.len(),
                self.store.byte_size()
            );
            for (p, c) in pending.cursors {
                self.partition_cursors.insert(p, c);
            }
            let _ = pending.reply.send(Ok(OpenTopicResult {
                total: self.view.len(),
                loaded: self.store.len(),
                buffer_bytes: self.store.byte_size(),
                truncated,
            }));
        } else {
            self.pending_read = Some(pending);
            let _ = self.tx.send(Command::ContinueRead);
        }
    }

    /// Снимок хода ещё не завершённого чтения — для опроса с фронта, пока
    /// `open_topic`/`load_more` не ответили финальным результатом.
    fn progress(&self) -> OpenTopicProgress {
        OpenTopicProgress {
            loaded: self.store.len(),
            total: self.view.len(),
            truncated: self.pending_read.is_some(),
            done: self.pending_read.is_none(),
        }
    }

    #[allow(dead_code)]
    fn currently_open(&self) -> Option<&str> {
        self.open_topic.as_deref()
    }

    fn close_topic(&mut self) {
        self.cancel_pending_read();
        self.open_topic = None;
        self.filter = MessageFilter::default();
        self.view.clear();
        self.partition_cursors.clear();
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
