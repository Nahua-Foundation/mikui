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
//! # Чтение раундами
//!
//! Kafka умеет читать только ВПЕРЁД от заданного офсета. Наивная реализация
//! «последние N сообщений» — сесть на `high_watermark - N` и читать до конца —
//! выдаёт их в порядке от старых к новым, а показать надо наоборот. Прошлая
//! версия решала это глобальной пересортировкой буфера после каждого шага:
//! каждая новая порция втыкалась в НАЧАЛО таблицы и сдвигала всё вниз. Строки
//! уезжали из-под курсора, фронту приходилось сбрасывать кэш окон, и вместо
//! данных мигали плейсхолдеры.
//!
//! Здесь чтение разбито на раунды. Один раз, при открытии топика, снимаются
//! watermarks — точные границы `[low, high)` каждой партиции. Дальше каждый
//! раунд читает по одному ОКНУ на партицию:
//!
//! * `newest` — окна идут назад: `[high-C, high)`, `[high-2C, high-C)`, …
//! * `oldest` — окна идут вперёд: `[low, low+C)`, `[low+C, low+2C)`, …
//!
//! Внутри окна librdkafka по-прежнему читает вперёд, но само окно уже стоит на
//! своём месте в глобальном порядке. Порядок приезда данных совпадает с
//! порядком показа, поэтому опубликованный префикс таблицы больше никогда не
//! перестраивается (см. `MessageStore::committed`).
//!
//! Между окнами партиция НЕ останавливается: чтение открывается один раз на
//! всё чтение, а окна меняются неблокирующим `RawTopic::seek`. В режиме
//! `oldest` окна идут встык, так что там не делается даже seek. Причина
//! принципиальная — `rd_kafka_consume_stop` ждёт брокерский поток бесконечно,
//! и на кластере с квотой восемь таких остановок за раунд давали пол в 8–12
//! секунд, не зависевший от размера окна вообще.
//!
//! Границу публикации задаёт `safe_frontier`: пока хоть одна партиция вычитана
//! не так глубоко, как остальные, её «догоняющие» сообщения могли бы встать
//! выше — такой хвост придерживается в отстойнике до следующего раунда. Это
//! обычное k-way слияние отсортированных потоков, просто растянутое во времени.
//!
//! Размер окна не задан константой, а СЧИТАЕТСЯ (`next_window`) из
//! измеренной пропускной способности кластера (`kafka::quota`) так, чтобы раунд
//! занимал примерно `TARGET_ROUND_SECS`. Задать его числом нельзя: он зависит
//! от размера сообщений (в соседних топиках одного кластера встречались и
//! 400 байт, и 31 КБ), от плотности офсетов и от квоты брокера на чтение.
//! Фиксированное окно на квотированном кластере давало шаг ровно в десять
//! секунд — таблица замирала, дёргалась и снова замирала.
//!
//! Ни watermarks, ни раунды не блокируют поток целиком: и то, и другое разбито
//! на шаги (`Command::ContinueRead`), между которыми воркер обслуживает
//! остальные команды (`GetWindow`, `GetOpenTopicProgress`, новый `OpenTopic`) —
//! без единого мьютекса и без второго потока, просто кооперативно уступая цикл
//! `run()`.

use std::collections::{HashMap, HashSet};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rdkafka::admin::AdminClient;
use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext};
use rdkafka::message::{Header, Message, OwnedHeaders};
use rdkafka::producer::{BaseProducer, BaseRecord, DeliveryResult, ProducerContext};
use rdkafka::topic_partition_list::TopicPartitionList;
use rdkafka::Offset;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::statistics::Statistics;
use rdkafka::types::RDKafkaRespErr;
use tokio::sync::oneshot;

use super::filter::contains;
use super::quota::{QuotaEstimate, QuotaMeter};
use super::raw_consumer::{self, RawMessage, RawQueue, RawTopic};
use super::store::MessageStore;
use super::text::{self, PREVIEW_BYTES};
use super::types::*;
use crate::helpers::{base_config, get_cluster_config, producer_config, PRODUCE_TIMEOUT};
use crate::proto::ProtoDecoder;

/// Потолок времени на одно чтение (открытие топика или "загрузить ещё").
/// Проверяется НА ГРАНИЦЕ РАУНДА: чтение останавливается, не потеряв ни одного
/// уже вычитанного сообщения. Остаток добирается кнопкой "Load more".
const READ_DEADLINE: Duration = Duration::from_secs(30);
/// Страховка от партиции, которая перестала отдавать данные и не даёт раунду
/// закрыться. Раунд, не уложившийся в этот срок, выбрасывается целиком (см.
/// `abort_round`), поэтому запас здесь щедрый: лучше подождать лишнее, чем
/// выкинуть честно вычитанные мегабайты на медленном канале.
const ROUND_DEADLINE: Duration = Duration::from_secs(20);
/// Таймаут одного вызова `consume_batch` в шаге чтения, в миллисекундах.
const POLL_TICK_MS: i32 = 200;
/// Сколько сообщений забирать за один шаг кооперативного чтения. Между шагами
/// воркер обслуживает остальные команды — чем меньше шаг, тем отзывчивее
/// `GetWindow`/`GetOpenTopicProgress` во время долгой вычитки, но тем больше
/// накладных расходов на переключение.
const BATCH_SIZE: usize = 500;
/// Сколько ДОЛЖЕН длиться один раунд. Раунд — это шаг, которым растёт таблица;
/// постоянной надо держать именно его длительность.
///
/// Размер окна для этого не выводится аналитически и не подбирается вслепую, а
/// считается из измеренной пропускной способности (`kafka::quota`):
///
/// ```text
/// офсетов в раунде = (байт/с × TARGET_ROUND_SECS) / байт на офсет
/// ```
///
/// Обе величины берутся из одного измерения, поэтому в цену офсета сами собой
/// входят и размер сообщений (в соседних топиках одного кластера встречались и
/// 400 байт, и 31 КБ), и плотность офсетов, и то, что часть присланного мы
/// выбрасываем при переходе к следующему окну — за неё квота списывается
/// наравне с полезным.
const TARGET_ROUND_SECS: f64 = 2.0;
/// Окно первого раунда: про кластер ещё не известно ничего, поэтому пробуем
/// заведомо мало — лишь бы первые строки появились быстро даже там, где одно
/// сообщение весит десятки килобайт.
const FIRST_ROUND_CHUNK: i64 = 10;
/// Ниже этого окно не опускается даже на самом медленном канале: каждый раунд
/// заново открывает чтение партиции, и дробить его до единиц сообщений — уже
/// одни накладные расходы.
///
/// Это АБСОЛЮТНЫЙ пол, страховка от деления на мусор. Настоящий пол считает
/// `window_floor` — он заметно выше и зависит от размера сообщений.
const MIN_ROUND_CHUNK: i64 = 5;
/// Копия `fetch.message.max.bytes` из `helpers::get_cluster_config`. Держать её
/// здесь приходится потому, что от неё зависит минимальный ОСМЫСЛЕННЫЙ размер
/// окна: столько байт брокер пришлёт по партиции в одном фетче в любом случае,
/// сколько бы офсетов мы ни попросили. Менять только вместе с оригиналом.
const FETCH_MESSAGE_MAX_BYTES: f64 = 262_144.0;
/// Потолок вычисленного пола: на топике с крошечными сообщениями один фетч
/// покрывает тысячи офсетов, и делать это МИНИМАЛЬНЫМ окном значило бы тянуть
/// мегабайты там, где хватило бы экрана строк.
const MAX_WINDOW_FLOOR: i64 = 500;
const MAX_ROUND_CHUNK: i64 = 5000;
/// Во сколько раз окно может вырасти за один раунд.
///
/// Ограничение именно на РОСТ, и оно не косметическое. Kafka разрешает
/// перебрать квоту, а потом расплатиться одной длинной паузой, поэтому первые
/// секунды чтения идут на скорости, которой на самом деле нет. Пока измеритель
/// не накопит окно шире квотного, оценка завышена — и без этого предела одна
/// такая оценка превратилась бы в раунд, за который брокер заставит стоять
/// десяток секунд. Вниз ограничения нет: ужиматься надо сразу.
const CHUNK_GROWTH_LIMIT: i64 = 2;
/// Во сколько раз расширять окно партиции, в котором не нашлось НИ ОДНОГО
/// сообщения. Такое бывает на compacted-топиках: диапазон офсетов есть, а
/// сообщений в нём не осталось. Ужимать окно там бессмысленно — за фетч мы
/// платим в любом случае, — поэтому наоборот проскакиваем пустоту быстрее.
const BARREN_GROWTH: u32 = 2;
/// Потолок такого разгона, чтобы не улететь в окно на миллион офсетов.
const BARREN_MAX_DOUBLINGS: u32 = 6;
/// Сколько байт вычитывает ОДНО чтение, прежде чем остановиться и отдать
/// управление пользователю. Без этого потолка `limit` в 1000 сообщений на
/// партицию на "жирном" топике означает сотни мегабайт и минуты ожидания.
const READ_BYTE_BUDGET: usize = 32 * 1024 * 1024;
/// Сколько партиций опрашивается на watermarks за один шаг подготовки. Каждый
/// вызов — отдельный поход в сеть (на реальном кластере наблюдалось до 600 мс
/// на партицию), а шаг блокирует воркер целиком — поэтому по чуть-чуть.
const WATERMARK_BATCH: usize = 2;
const WATERMARK_TIMEOUT: Duration = Duration::from_secs(10);
const METADATA_TIMEOUT: Duration = Duration::from_secs(10);

/// Сколько ждать первого ответа от кластера при подключении.
///
/// Меньше `METADATA_TIMEOUT` намеренно: на подключении человек СМОТРИТ на
/// приложение и ждёт реакции, а на живом кластере рукопожатие укладывается в
/// доли секунды (в замере с боевого стенда — 137 мс до `AUTH_REQ`). Десять
/// секунд тишины здесь неотличимы от зависшего приложения.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Шаг проверки связи. Он же — задержка, с которой замечается ошибка
/// аутентификации: она приезжает колбэком между попытками.
const CONNECT_PROBE_STEP: Duration = Duration::from_millis(300);

/// Как часто librdkafka отдаёт статистику. Из неё измеритель квоты берёт
/// принятые байты и наложенные брокером задержки — см. `kafka::quota`.
const STATS_INTERVAL_MS: &str = "1000";

/// Ошибка, после которой ждать ответа от кластера бессмысленно.
///
/// Нужна потому, что `fetch_metadata` о таком не сообщает. Неверный пароль
/// librdkafka распознаёт за ~150 мс и кричит об этом в колбэк ошибок, но для
/// самого запроса метаданных это обычный неответивший брокер: она молча
/// ретраится до конца таймаута. Без этой отметки неверный пароль неотличим от
/// медленного кластера — и стоит секунд ожидания там, где ответ уже известен.
#[derive(Default)]
struct FatalError(Mutex<Option<String>>);

impl FatalError {
    fn record(&self, reason: &str) {
        let mut slot = self.0.lock().unwrap_or_else(|e| e.into_inner());
        // Первая ошибка информативнее: дальше пойдут её следствия вроде
        // «1/1 brokers are down».
        slot.get_or_insert_with(|| reason.to_string());
    }

    fn take(&self) -> Option<String> {
        self.0.lock().unwrap_or_else(|e| e.into_inner()).take()
    }
}

/// Ошибки, которые повтором не лечатся. Всё остальное (брокер не ответил,
/// разорвалось соединение) librdkafka чинит сама, и вмешиваться незачем.
fn is_fatal(error: &KafkaError) -> bool {
    matches!(
        error,
        KafkaError::Global(RDKafkaErrorCode::Authentication)
            | KafkaError::Global(RDKafkaErrorCode::SaslAuthenticationFailed)
    )
}

/// Пересечение границ партиции `[low, high)` с запрошенным диапазоном.
/// `to` — exclusive, как и `high`.
///
/// Пустой результат сам по себе не ошибка: в соседней партиции такие офсеты
/// вполне могут быть, и решает это вызывающий, посмотрев сразу на все.
fn intersect(low: i64, high: i64, from: Option<i64>, to: Option<i64>) -> (i64, i64) {
    let start = from.map_or(low, |f| f.max(low));
    let end = to.map_or(high, |t| t.min(high)).max(start);
    (start, end)
}

/// Верхняя граница, названная человеком, — inclusive; внутри всё считается
/// полуинтервалами. `saturating_add` здесь не педантизм: `to` приходит из поля
/// ввода и вполне может оказаться `i64::MAX`, а переполнение превратило бы
/// границу в отрицательную и молча отдало бы пустоту вместо хвоста партиции.
fn exclusive(inclusive: Option<i64>) -> Option<i64> {
    inclusive.map(|v| v.saturating_add(1))
}

/// Офсет, найденный по времени, либо `fallback`, если такого момента в
/// партиции нет.
fn resolved(found: &HashMap<i32, i64>, partition: i32, fallback: i64) -> i64 {
    found.get(&partition).copied().unwrap_or(fallback)
}

/// Границы, названные человеком, должны быть согласованы между собой — иначе
/// чтение молча вернёт пустоту вместо внятного «начало позже конца».
fn validate_range(range: &ReadRange) -> Result<(), String> {
    if let (Some(from), Some(to)) = (range.from_offset, range.to_offset) {
        if from > to {
            return Err(format!("offset {from} is after {to}"));
        }
    }
    if let (Some(from), Some(to)) = (range.from_timestamp, range.to_timestamp) {
        if from > to {
            return Err("the start of the time range is after its end".into());
        }
    }
    if range.from_offset.is_some_and(|o| o < 0) || range.to_offset.is_some_and(|o| o < 0) {
        return Err("offsets cannot be negative".into());
    }
    Ok(())
}

/// Человекочитаемые границы партиций — для сообщения о том, что запрошенный
/// диапазон в топик не попал.
fn describe_bounds(cursors: &HashMap<i32, PartitionCursor>) -> String {
    let mut parts: Vec<(i32, String)> = cursors
        .iter()
        .map(|(&p, c)| {
            let what = if c.low >= c.high {
                "is empty".to_string()
            } else {
                format!("holds {}..{}", c.low, c.high - 1)
            };
            (p, format!("partition {p} {what}"))
        })
        .collect();
    parts.sort_by_key(|(p, _)| *p);
    parts
        .into_iter()
        .map(|(_, text)| text)
        .collect::<Vec<_>>()
        .join(", ")
}

/// Ждёт, пока кластер либо ответит метаданными, либо откажет так, что ждать
/// дальше бессмысленно.
///
/// Опрос здесь обязателен: колбэк ошибок висит на главной очереди клиента, и
/// сам по себе `fetch_metadata` её не выгребает — без `poll` отказ в
/// аутентификации так и остался бы незамеченным до конца таймаута.
fn wait_until_ready(
    consumer: &BaseConsumer<MeteredContext>,
    fatal: &FatalError,
) -> Result<(), String> {
    let started = Instant::now();
    loop {
        let _ = consumer.poll(Duration::ZERO);
        if let Some(reason) = fatal.take() {
            return Err(reason.to_string());
        }
        match consumer.fetch_metadata(None, CONNECT_PROBE_STEP) {
            Ok(_) => return Ok(()),
            Err(e) if started.elapsed() >= CONNECT_TIMEOUT => {
                // Ошибка могла приехать колбэком ровно на этом шаге — тогда
                // она точнее, чем «истекло время ожидания».
                let _ = consumer.poll(Duration::ZERO);
                return Err(match fatal.take() {
                    Some(reason) => reason,
                    None => format!("can't reach cluster: {e}"),
                });
            }
            Err(_) => {}
        }
    }
}

/// Контекст клиента: сливает статистику librdkafka в измеритель квоты и
/// перехватывает ошибки, после которых подключение можно не ждать.
#[derive(Clone)]
struct MeteredContext {
    meter: Arc<QuotaMeter>,
    fatal: Arc<FatalError>,
}

impl ClientContext for MeteredContext {
    fn stats(&self, statistics: Statistics) {
        self.meter.observe(&statistics);
    }

    /// Своя диагностика librdkafka по умолчанию уходит в крейт `log`, который
    /// в приложении никем не инициализирован, — то есть в никуда. Ровно из-за
    /// этой слепоты таймауты фетча пришлось вычислять по косвенным признакам:
    /// библиотека всё это время писала о них, но её никто не слушал.
    ///
    /// Уровень `Notice` и ниже отбрасываем — иначе на каждое переподключение
    /// придёт стена рутины.
    fn log(&self, level: RDKafkaLogLevel, fac: &str, log_message: &str) {
        if level as i32 <= RDKafkaLogLevel::Warning as i32 {
            eprintln!("[rdkafka:{fac}] {log_message}");
        }
    }

    fn error(&self, error: KafkaError, reason: &str) {
        eprintln!("[rdkafka] {error}: {reason}");
        if is_fatal(&error) {
            self.fatal.record(reason);
        }
    }
}

impl ConsumerContext for MeteredContext {}

/// Куда легло последнее отправленное сообщение.
///
/// `Mutex<Option<..>>`, а не канал: отчёт о доставке приезжает колбэком из
/// `poll`, который зовёт сам воркер, на своём же потоке, и разбирается ровно
/// один вызов `produce` за раз. Городить вокруг этого канал не за чем.
type Delivery = Arc<Mutex<Option<Result<ProduceResult, String>>>>;

/// Контекст продьюсера.
///
/// Отдельно от `MeteredContext` по двум причинам. Во-первых, `ProducerContext`
/// и `ConsumerContext` — разные трейты с разными требованиями. Во-вторых,
/// статистику отправки нельзя сливать в измеритель квоты: он меряет, с какой
/// скоростью кластер ОТДАЁТ данные, и подмешанные туда байты записи испортили
/// бы расчёт размера окна чтения.
///
/// Отметки фатальных ошибок, как у консьюмера (`FatalError`), здесь нет и не
/// нужно. Она заведена ради `wait_until_ready`, где отказ в аутентификации
/// иначе неотличим от медленного кластера. У отправки такой слепоты нет: и
/// отказ по ACL, и любая другая неустранимая ошибка приезжают отчётом о
/// доставке немедленно — librdkafka не ретраит permanent-ошибки.
#[derive(Clone)]
struct ProducerCtx {
    delivery: Delivery,
}

impl ClientContext for ProducerCtx {
    fn log(&self, level: RDKafkaLogLevel, fac: &str, log_message: &str) {
        if level as i32 <= RDKafkaLogLevel::Warning as i32 {
            eprintln!("[rdkafka:{fac}] {log_message}");
        }
    }

    fn error(&self, error: KafkaError, reason: &str) {
        eprintln!("[rdkafka:producer] {error}: {reason}");
    }
}

impl ProducerContext for ProducerCtx {
    type DeliveryOpaque = ();

    fn delivery(&self, result: &DeliveryResult<'_>, _: ()) {
        let outcome = match result {
            Ok(message) => Ok(ProduceResult {
                partition: message.partition(),
                offset: message.offset(),
            }),
            Err((error, _)) => Err(error.to_string()),
        };
        if let Ok(mut slot) = self.delivery.lock() {
            *slot = Some(outcome);
        }
    }
}

/// Шаг ожидания отчёта о доставке. Он же — сколько воркер стоит, не обслуживая
/// остальные команды: отправка одного сообщения занимает миллисекунды, дробить
/// её на кооперативные шаги было бы сложностью без выигрыша.
const DELIVERY_POLL_STEP: Duration = Duration::from_millis(100);

/// Ответ на чтение, отменённое из-за того, что пользователь ушёл с топика.
/// Фронт узнаёт его по строке и молчит: это не сбой, а нормальный ход событий.
pub const READ_SUPERSEDED: &str = "read superseded";

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
    /// Клик по заголовку колонки. `None` возвращает обычный порядок чтения.
    /// Как и `SetFilter`, считается по уже загруженному буферу — без единого
    /// сетевого запроса.
    SetSort(Option<SortSpec>, Reply<Result<usize, String>>),
    GetWindow {
        start: usize,
        count: usize,
        reply: Reply<Result<Vec<RowPreview>, String>>,
    },
    GetBody(usize, Reply<Result<FullMessage, String>>),
    /// Чем декодировать тела открытого топика. `None` — отдавать как есть.
    ///
    /// Отдельная команда, а не поле в `OpenTopic`: выбор message в настройках
    /// топика обязан примениться сразу, а перечитывать ради этого весь топик из
    /// Kafka — значит платить квотой на чтение за смену способа показа.
    SetDecoder(Option<Arc<ProtoDecoder>>, Reply<()>),
    /// Положить сообщение в топик. Тело приезжает уже байтами: кодированием по
    /// схеме заведует `lib.rs`, воркер про схемы не знает.
    Produce(ProduceRecord, Reply<Result<ProduceResult, String>>),
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

/// Результат попытки декодировать тело.
///
/// Оба поля пустые — декодера нет, тело показывается как раньше. Именно поэтому
/// это не `Result`: «декодера не задали» и «декодер не справился» ведут к разным
/// строкам таблицы, и слить их в одну ветку значило бы врать про первый случай.
#[derive(Default)]
struct Rendered {
    decoded: Option<String>,
    error: Option<String>,
}

impl Rendered {
    /// Показывать ли тело как двоичное. Успешно разобранный protobuf двоичным
    /// не считается: наружу уехал JSON, и метка `[binary]` на нём была бы ложью.
    fn binary(&self, raw: &[u8]) -> bool {
        self.decoded.is_none() && !text::is_text(raw)
    }
}

/// Что известно про партицию между раундами. Переживает завершение чтения:
/// именно отсюда `load_more` узнаёт, куда возвращаться.
struct PartitionCursor {
    /// Границы партиции, снятые один раз при открытии топика.
    low: i64,
    /// Exclusive.
    high: i64,
    /// Куда сядет следующее окно: в режиме `newest` это его правая граница
    /// (exclusive, окна ползут вниз), в `oldest` — левая (inclusive).
    next: i64,
    /// Сколько сообщений уже взято из этой партиции за все раунды.
    absorbed: i64,
    /// Потолок `absorbed` для текущего чтения. `load_more` его поднимает.
    budget: i64,
    /// Где стоит читатель прямо сейчас. Если следующее окно начинается ровно
    /// здесь, переставлять его не нужно — а в режиме `oldest` окна как раз
    /// идут встык, так что там seek не делается ни разу.
    read_position: Option<i64>,
    /// Дочитано до конца — до `low` в `newest`, до `high` в `oldest`.
    exhausted: bool,
    /// Самый старый (в `newest`) либо самый новый (в `oldest`) уже вычитанный
    /// timestamp. Дальше эта партиция не выдаст ничего «за» этой отметкой,
    /// поэтому именно она определяет, что безопасно публиковать.
    frontier_ts: Option<i64>,
    /// Сколько окон подряд не дали ни одного сообщения — см. `BARREN_GROWTH`.
    barren_rounds: u32,
}

impl PartitionCursor {
    /// Окно этой партиции для очередного раунда — полуинтервал `[start, end)`.
    /// `None` — раунд ей не положен: дочитана до конца либо выбрала бюджет.
    ///
    /// Окна не пересекаются и всегда двигаются в одну сторону, поэтому раунды
    /// гарантированно сходятся: в `newest` `next` строго убывает до `low`,
    /// в `oldest` строго растёт до `high`.
    fn next_window(&self, newest_first: bool, chunk: i64) -> Option<(i64, i64)> {
        if self.exhausted || self.absorbed >= self.budget {
            return None;
        }
        // Бюджет считается в СООБЩЕНИЯХ, а окно — в офсетах. На плотной
        // партиции это одно и то же, на дырявой офсетов нужно больше, поэтому
        // окно ограничивается остатком бюджета только до разгона по пустотам.
        let chunk = chunk.min(self.budget - self.absorbed).max(MIN_ROUND_CHUNK)
            * BARREN_GROWTH.pow(self.barren_rounds.min(BARREN_MAX_DOUBLINGS)) as i64;
        let (start, end) = if newest_first {
            ((self.next - chunk).max(self.low), self.next)
        } else {
            (self.next, (self.next + chunk).min(self.high))
        };
        if start >= end {
            return None;
        }
        Some((start, end))
    }

    /// Двигает курсор на окно, вычитанное ЦЕЛИКОМ. Частично вычитанное окно
    /// сюда попадать не должно — см. `Worker::abort_round`.
    fn advance(&mut self, newest_first: bool, window: &RoundPartition) {
        self.absorbed += window.absorbed;
        // Окно вычитано целиком, значит читатель доехал до его правой границы.
        self.read_position = Some(window.end);
        self.barren_rounds = if window.absorbed == 0 {
            self.barren_rounds + 1
        } else {
            0
        };
        if newest_first {
            self.next = window.start;
            self.exhausted |= window.start <= self.low;
        } else {
            self.next = window.end;
            self.exhausted |= window.end >= self.high;
        }
    }

    /// Обновляет отметку, за которую эта партиция уже точно ничего не выдаст.
    fn observe(&mut self, newest_first: bool, timestamp: i64) {
        self.frontier_ts = Some(match self.frontier_ts {
            None => timestamp,
            Some(prev) if newest_first => prev.min(timestamp),
            Some(prev) => prev.max(timestamp),
        });
    }
}

/// Окно одной партиции в текущем раунде: полуинтервал `[start, end)`.
struct RoundPartition {
    start: i64,
    end: i64,
    absorbed: i64,
    /// Окно вычитано целиком (дошли до `end` или до конца лога).
    done: bool,
}

enum ReadPhase {
    /// Границы партиций ещё не сняты; в списке — те, что остались.
    Watermarks(Vec<i32>),
    /// Идёт раунд.
    Reading(HashMap<i32, RoundPartition>),
}

/// Размер окна следующего раунда, в офсетах на партицию.
///
/// Тормозов два, и они независимы — это не перестраховка, а вывод из ошибки.
///
/// 1. **Измеренная квота** (`estimate`) — основной, принципиальный: прямой
///    расчёт «сколько офсетов кластер успеет отдать за `TARGET_ROUND_SECS`».
/// 2. **Длительность прошлого раунда** (`last_round`) — запасной, но всегда
///    доступный. Не уложились в цель — расти нельзя вовсе, а ужаться надо
///    пропорционально переработке.
///
/// Второй нужен именно потому, что первый может молча отсутствовать: колбэк
/// статистики висел на неопрошенной очереди, `estimate` был вечным `None`, и
/// окно, у которого не осталось ни одной обратной связи, удваивалось до упора —
/// 20, 40, 80, 160, 320, 640, пока раунды не растянулись до десяти секунд.
/// Наблюдаемая длительность раунда есть всегда и не зависит ни от какой
/// внешней подсистемы.
fn next_window(
    previous: i64,
    estimate: Option<QuotaEstimate>,
    active: usize,
    last_round: Option<Duration>,
    floor: i64,
) -> i64 {
    let ceiling = match last_round {
        Some(elapsed) if elapsed.as_secs_f64() > TARGET_ROUND_SECS => {
            let overrun = elapsed.as_secs_f64() / TARGET_ROUND_SECS;
            (previous as f64 / overrun).round() as i64
        }
        _ => previous.saturating_mul(CHUNK_GROWTH_LIMIT),
    };

    let target = match estimate {
        Some(est) => {
            let bytes = est.bytes_per_sec * TARGET_ROUND_SECS;
            let offsets = bytes / est.bytes_per_offset.max(1.0) / active.max(1) as f64;
            offsets.round() as i64
        }
        None => ceiling,
    };
    target
        .min(ceiling)
        .clamp(floor.clamp(MIN_ROUND_CHUNK, MAX_WINDOW_FLOOR), MAX_ROUND_CHUNK)
}

/// Ниже какого окна ужиматься не просто бесполезно, а ВРЕДНО.
///
/// Брокер отдаёт до `fetch.message.max.bytes` НА ПАРТИЦИЮ в каждом фетче
/// независимо от того, сколько офсетов мы попросили, и квота списывается за
/// всё присланное. Значит окно мельче одного фетча не экономит ни байта
/// квоты — оно только уменьшает то, что мы из этого фетча оставляем себе.
///
/// Ровно на этом чтение и разваливалось. Замер с боевого кластера
/// (`fireg.securities`, 8 партиций, ~8 КБ полезных на офсет): окно ужалось до
/// 5 офсетов, а брокер продолжал слать свои 256 КБ на партицию — и
/// «цена офсета» в измерителе поехала с 37 КБ до 101 КБ, хотя полезных байт
/// на офсет всё это время было те же 8 КБ. Измеритель видел дорожающий офсет,
/// ужимал окно ещё сильнее, и цена росла дальше. Раунд, отдававший 386
/// сообщений в секунду, к седьмому кругу отдавал 6.
///
/// `kept_per_offset` — ПОЛЕЗНЫЕ байты на офсет (то, что осело в арене), а не
/// сетевые: именно они говорят, сколько офсетов покрывает один фетч.
fn window_floor(kept_per_offset: Option<f64>) -> i64 {
    let Some(kept) = kept_per_offset.filter(|k| *k > 0.0) else {
        return MIN_ROUND_CHUNK;
    };
    let offsets_per_fetch = (FETCH_MESSAGE_MAX_BYTES / kept).round() as i64;
    offsets_per_fetch.clamp(MIN_ROUND_CHUNK, MAX_WINDOW_FLOOR)
}

/// Какие партиции читать по выбору из UI.
///
/// `None` и пустой список означают одно и то же — «все». Пустой список сюда
/// приезжать не должен (селектор в шапке возвращается к «all», когда снят
/// последний чек), но трактовать его как ошибку значило бы завести состояние,
/// из которого таблица не может выйти сама.
///
/// Дубликаты убираются: одна и та же партиция, прицепленная к очереди дважды,
/// — это `consume_start` поверх уже открытой, то есть ошибка librdkafka на
/// ровном месте.
fn selected_partitions(selected: Option<&[i32]>, partition_count: usize) -> Result<Vec<i32>, String> {
    let all = || (0..partition_count as i32).collect::<Vec<i32>>();
    let Some(selected) = selected.filter(|s| !s.is_empty()) else {
        return Ok(all());
    };

    if let Some(&p) = selected
        .iter()
        .find(|&&p| p < 0 || p as usize >= partition_count)
    {
        return Err(format!(
            "partition {p} is out of range; the topic has {partition_count}"
        ));
    }

    let mut partitions = selected.to_vec();
    partitions.sort_unstable();
    partitions.dedup();
    Ok(partitions)
}

// Активного pacing (намеренной паузы между раундами, чтобы не «занимать» из
// брокерского токен-бакета) здесь больше нет — его пробовали и убрали.
//
// Идея была в том, что раунд, закончившийся быстрее измеренной устойчивой
// скорости, взял в долг, и расплата за это придёт одной длинной паузой. На
// боевом кластере это не подтвердилось дважды. В первой редакции цена раунда
// считалась в байтах арены, а скорость — по `rxbytes`, и пауза не назначилась
// ни разу. Во второй, уже в одних единицах, вышло хуже: пауза упиралась в свой
// потолок после КАЖДОГО раунда и съела 18 секунд из 34.
//
// Причина в том, что оценка, на которую опиралась пауза, во время самой паузы
// и застревала: часы измерителя на ней останавливаются, накопленные офсеты
// сбрасываются, а раунды короче интервала статистики (1 с) не дают ни одной
// засчитанной выборки. В логе это видно прямо — `1385 KB/s` и `39302 B/offset`
// побайтово повторялись во всех раундах после первой же паузы. Пауза кормилась
// одним замером с раунда, поймавшего throttle, и по нему укладывала спать
// раунды, только что отработавшие за 0.4 секунды.
//
// Держать темп ниже квоты, если это когда-нибудь понадобится, нужно из
// измерения, которое во время удержания продолжает обновляться, — иначе
// регулятор управляет по собственному следу.

/// Что публикация может себе позволить, см. `Worker::safe_frontier`.
enum Frontier {
    /// Читать больше не из чего — отстойник можно опубликовать целиком.
    Everything,
    /// Публикуем всё, что строго «за» этой отметкой времени.
    Beyond(i64),
    /// Есть партиция, о которой не известно ничего. Публиковать нельзя ничего.
    Nothing,
}

/// Ещё не завершённое чтение — раскладывается по шагам через
/// `Command::ContinueRead`.
struct PendingRead {
    topic: RawTopic,
    /// Одна на всё чтение. Партиции цепляются к ней один раз и живут до конца:
    /// между окнами они не останавливаются, а переставляются через
    /// `RawTopic::seek`, и хвосты «в полёте» отбрасывает сама librdkafka по
    /// барьеру версии.
    queue: RawQueue,
    /// Партиции, у которых чтение уже открыто. Открывать повторно нельзя, а
    /// закрывать надо ровно один раз — и только в самом конце.
    open_partitions: HashSet<i32>,
    phase: ReadPhase,
    started: Instant,
    /// Начало ТЕКУЩЕГО раунда — под собственный, короткий дедлайн.
    round_started: Instant,
    /// Размер арены на начало чтения: от него считается `READ_BYTE_BUDGET`.
    bytes_at_start: usize,
    reply: Reply<Result<OpenTopicResult, String>>,
    topic_name: String,
    /// Длина `store` на начало текущего раунда. Оборванный раунд — дырявое
    /// окно, его добыча отматывается ровно сюда.
    round_mark: usize,
    /// Размер арены на начало раунда — только для диагностики.
    round_mark_bytes: usize,
    /// Стартовый бюджет партиции. Нужен только в фазе `Watermarks`: курсоров,
    /// которым его можно проставить, до неё ещё не существует.
    budget: i64,
    /// Запрошенные границы. Живут в чтении, а не в воркере: применяются они
    /// ровно один раз, сразу после watermarks, и дальше уже вшиты в курсоры —
    /// поэтому `load_more` за диапазон не выходит, ничего о нём не зная.
    range: ReadRange,
}

struct Worker {
    /// Клон собственного отправителя — нужен, чтобы слать себе `ContinueRead`.
    tx: Sender<Command>,
    consumer: Option<BaseConsumer<MeteredContext>>,
    admin: Option<AdminClient<MeteredContext>>,
    /// Адреса, сеть и безопасность текущего подключения — то, из чего
    /// достраивается продьюсер. Держим конфиг, а не `ClusterConnectPayload`:
    /// пароль сохранённой учётки подставляется из keychain один раз, при
    /// подключении, и ходить за ним второй раз ради отправки незачем.
    connection: Option<ClientConfig>,
    /// Продьюсер поднимается ЛЕНИВО, на первой отправке.
    ///
    /// Приложение прежде всего просмотрщик: за сеанс, в котором никто ничего не
    /// отправлял, платить ещё одним соединением с кластером (а на закрытых
    /// кластерах — ещё и рукопожатием, которое может не пройти по ACL) не за
    /// что. Живёт до конца подключения: отправляют обычно не по одному разу.
    producer: Option<BaseProducer<ProducerCtx>>,
    /// Куда легло последнее отправленное сообщение — общая ячейка с колбэком
    /// доставки `ProducerCtx`.
    delivery: Delivery,
    /// Сколько байт в секунду кластер РЕАЛЬНО отдаёт. Живёт на уровне
    /// подключения, а не чтения: квота — свойство пары «пользователь-кластер»,
    /// и нащупывать её заново на каждое открытие топика значило бы каждый раз
    /// платить теми же несколькими медленными раундами.
    quota: Arc<QuotaMeter>,
    /// Количество партиций по топикам — приезжает с метаданными и переиспользуется,
    /// чтобы не ходить за ними повторно на каждое открытие топика.
    partition_counts: HashMap<String, usize>,
    store: MessageStore,
    /// Индексы в опубликованной части `store` после применения фильтра.
    /// Это и есть то, что видит UI.
    view: Vec<u32>,
    filter: MessageFilter,
    /// Сортировка по столбцу поверх обычного порядка чтения. `None` — порядок
    /// как есть в `store` (уже отфильтрованный, см. `rebuild_view`).
    sort: Option<SortSpec>,
    open_topic: Option<String>,
    /// Сортировать ли по убыванию времени. Ставится при `open_topic`,
    /// переиспользуется без изменений в `load_more` того же топика.
    newest_first: bool,
    pending_read: Option<PendingRead>,
    /// Состояние партиций открытого топика. Живёт между чтениями.
    cursors: HashMap<i32, PartitionCursor>,
    /// Размер окна последнего раунда, в офсетах на партицию. Отправная точка
    /// для следующего — см. `next_window`.
    window: i64,
    /// Сколько занял последний раунд. Запасная обратная связь на случай, когда
    /// измерителю квоты нечего сказать.
    last_round: Option<Duration>,
    /// Хоть у одного вычитанного сообщения был непустой timestamp. Если нет
    /// (совсем старый топик, брокер не отдаёт время), сортировать и придерживать
    /// нечего по чему — публикуем в порядке приезда, иначе таблица осталась бы
    /// пустой навсегда.
    has_timestamps: bool,
    /// ПОЛЕЗНЫХ байт на офсет по последнему раунду — то, что осело в арене,
    /// без выброшенной предвыборки. Не путать с `bytes_per_offset` измерителя:
    /// та считает сетевую цену офсета и потому зависит от размера окна, а эта
    /// — свойство самих данных. Отсюда берётся пол окна (`window_floor`).
    kept_per_offset: Option<f64>,
    /// Чем декодировать тела. Ставится командой `SetDecoder` перед открытием
    /// топика и переживает `load_more`; `close_topic` его снимает.
    decoder: Option<Arc<ProtoDecoder>>,
}

impl Worker {
    fn new(tx: Sender<Command>) -> Self {
        Self {
            tx,
            consumer: None,
            admin: None,
            connection: None,
            producer: None,
            delivery: Arc::new(Mutex::new(None)),
            partition_counts: HashMap::new(),
            store: MessageStore::default(),
            view: Vec::new(),
            filter: MessageFilter::default(),
            sort: None,
            open_topic: None,
            newest_first: false,
            pending_read: None,
            cursors: HashMap::new(),
            quota: Arc::new(QuotaMeter::new()),
            window: FIRST_ROUND_CHUNK,
            last_round: None,
            has_timestamps: false,
            kept_per_offset: None,
            decoder: None,
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
                Command::SetSort(sort, reply) => {
                    self.sort = sort;
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
                Command::SetDecoder(decoder, reply) => {
                    self.decoder = decoder;
                    let _ = reply.send(());
                }
                Command::Produce(record, reply) => {
                    let _ = reply.send(self.produce(record));
                }
                Command::CloseTopic(reply) => {
                    self.close_topic();
                    let _ = reply.send(());
                }
            }
            // Без этого измеритель квоты не получает вообще ничего: колбэк
            // статистики висит на главной очереди клиента, а её обслуживает
            // только `rd_kafka_poll` — см. `raw_consumer::poll_main`.
            self.drain_events();
            // Часы измерителя идут только пока идёт чтение. Ставится здесь, а
            // не в каждой ветке завершения: путей выхода из чтения много
            // (штатный конец, дедлайн, ошибка, смена топика), и забыть один из
            // них означало бы засчитать простой пользователя как нулевую
            // скорость кластера.
            self.quota.set_reading(self.pending_read.is_some());
        }
    }

    fn connect(&mut self, payload: ClusterConnectPayload) -> Result<(), String> {
        let mut conf = get_cluster_config(&payload);
        // Статистика нужна только ради измерителя квоты, поэтому включается
        // здесь, а не в общем конфиге: `cluster_test` поднимает клиента на
        // одну проверку связи, и собирать для него JSON раз в секунду незачем.
        conf.set("statistics.interval.ms", STATS_INTERVAL_MS);

        let fatal = Arc::new(FatalError::default());
        let context = MeteredContext {
            meter: Arc::clone(&self.quota),
            fatal: Arc::clone(&fatal),
        };

        let consumer: BaseConsumer<MeteredContext> = conf
            .create_with_context(context.clone())
            .map_err(|e| format!("can't create consumer: {e}"))?;
        let admin: AdminClient<MeteredContext> = conf
            .create_with_context(context)
            .map_err(|e| format!("can't create admin client: {e}"))?;

        // Связь проверяется ДО того, как расстаться с прежним подключением.
        //
        // `create_with_context` не доказывает ровно ничего: librdkafka
        // соединяется лениво, и клиент с заведомо неверным паролем создаётся
        // так же успешно, как рабочий. Раньше на этом всё и заканчивалось —
        // пользователь, переключившийся на учётку с опечаткой в пароле, терял
        // рабочее подключение и открытый топик, а узнавал об этом секунд через
        // десять, когда истекал таймаут следующего же запроса метаданных.
        wait_until_ready(&consumer, &fatal)?;

        // Другой кластер (или другая учётка) — другая квота. Сбрасываем только
        // здесь: провалившееся подключение не должно стирать измерение
        // работающего, которое после ошибки остаётся в силе.
        self.quota.reset();

        // Идущее чтение сворачивается ДО подмены клиента, а не после.
        //
        // `RawTopic`/`RawQueue` внутри `pending_read` — сырые указатели,
        // выданные `rd_kafka_t*` ПРЕЖНЕГО консьюмера. Присваивание
        // `self.consumer = Some(...)` уничтожает старого (`rd_kafka_destroy`),
        // и всё, что после этого делает `close_topic` — `consume_stop`,
        // `rd_kafka_topic_destroy`, `rd_kafka_queue_destroy` — уходит в уже
        // освобождённую память.
        //
        // Раньше порядок был обратным. Подставлялось это редко (переподключение
        // прямо во время чтения), но смена Kafka-пользователя — именно оно и
        // есть, и делается на ходу, не дожидаясь конца загрузки.
        self.close_topic();

        self.consumer = Some(consumer);
        self.admin = Some(admin);
        // Продьюсер аутентифицирован ПРЕЖНЕЙ учёткой и держит соединение по её
        // кредам. Пережить смену подключения он не может ни при каких условиях:
        // иначе отправка шла бы под пользователем, которого в шапке уже нет.
        // Следующая отправка поднимет нового — из `connection` ниже.
        drop(self.producer.take());
        self.connection = Some(base_config(&payload));
        self.partition_counts.clear();
        Ok(())
    }

    fn test(payload: ClusterConnectPayload) -> Result<(), String> {
        let conf = get_cluster_config(&payload);
        let fatal = Arc::new(FatalError::default());
        // Свой измеритель, в общий не пишем: проверка связи — не чтение, и
        // засчитывать её в измеренную скорость кластера нечего.
        let context = MeteredContext {
            meter: Arc::new(QuotaMeter::new()),
            fatal: Arc::clone(&fatal),
        };
        let consumer: BaseConsumer<MeteredContext> = conf
            .create_with_context(context)
            .map_err(|e| format!("can't create consumer: {e}"))?;
        // Создание клиента ещё ничего не доказывает — librdkafka соединяется
        // лениво. Ходим в кластер по-настоящему.
        wait_until_ready(&consumer, &fatal)
    }

    fn disconnect(&mut self) {
        self.close_topic();
        drop(self.consumer.take());
        drop(self.admin.take());
        drop(self.producer.take());
        self.connection = None;
        self.partition_counts.clear();
    }

    // --- Отправка -----------------------------------------------------------

    /// Кладёт сообщение в топик и ждёт отчёта о доставке.
    ///
    /// Ждём намеренно, а не отвечаем «поставлено в очередь». Партиция и офсет,
    /// которые показываются пользователю, известны только из отчёта: до него не
    /// известно ни куда партишенер направил сообщение, ни удалось ли оно вообще
    /// (отказ по ACL на запись приезжает именно так). Сказать «отправлено», не
    /// дождавшись, значило бы сообщать об успехе, которого может не быть.
    fn produce(&mut self, record: ProduceRecord) -> Result<ProduceResult, String> {
        self.ensure_producer()?;
        // Ссылку берём ПОСЛЕ создания, отдельным шагом: вернуть её прямо из
        // `ensure_producer(&mut self)` значило бы растянуть изменяемый заём на
        // весь метод, и `self.delivery` рядом стал бы недоступен.
        let producer = self.producer.as_ref().expect("producer is created above");

        // Очищаем ячейку ДО отправки: в ней мог остаться отчёт от предыдущей.
        if let Ok(mut slot) = self.delivery.lock() {
            *slot = None;
        }

        let mut headers = OwnedHeaders::new_with_capacity(record.headers.len());
        for header in &record.headers {
            headers = headers.insert(Header {
                key: &header.key,
                value: Some(header.value.as_bytes()),
            });
        }

        let mut message: BaseRecord<[u8], [u8]> =
            BaseRecord::to(&record.topic).payload(&record.payload);
        if let Some(key) = &record.key {
            message = message.key(key.as_slice());
        }
        if let Some(partition) = record.partition {
            message = message.partition(partition);
        }
        if !record.headers.is_empty() {
            message = message.headers(headers);
        }

        producer
            .send(message)
            .map_err(|(e, _)| format!("can't enqueue the message: {e}"))?;

        let started = Instant::now();
        loop {
            producer.poll(DELIVERY_POLL_STEP);
            if let Some(outcome) = self.delivery.lock().ok().and_then(|mut s| s.take()) {
                return outcome;
            }
            if started.elapsed() > PRODUCE_TIMEOUT {
                // Сюда попадаем, только если librdkafka не отчиталась даже о
                // собственном `message.timeout.ms`, — то есть что-то пошло не
                // так на её стороне. Сообщение при этом могло и уехать.
                return Err("no delivery report from the cluster".to_string());
            }
        }
    }

    /// Поднимает продьюсера текущего подключения, если его ещё нет.
    fn ensure_producer(&mut self) -> Result<(), String> {
        if self.producer.is_some() {
            return Ok(());
        }
        let conf = self
            .connection
            .as_ref()
            .ok_or("not connected to a cluster")?;
        let context = ProducerCtx {
            delivery: Arc::clone(&self.delivery),
        };
        let producer: BaseProducer<ProducerCtx> = producer_config(conf)
            .create_with_context(context)
            .map_err(|e| format!("can't create producer: {e}"))?;
        self.producer = Some(producer);
        Ok(())
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

    // --- Запуск чтения ------------------------------------------------------

    /// Открывает топик: сбрасывает буфер, снимает границы партиций и запускает
    /// первый раунд. Долгая часть уходит в `continue_read`, поэтому сама
    /// команда возвращается мгновенно, а `reply` уезжает уже с результатом.
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

        let partitions = match selected_partitions(params.partitions.as_deref(), partition_count) {
            Ok(partitions) => partitions,
            Err(e) => {
                let _ = reply.send(Err(e));
                return;
            }
        };

        if let Err(e) = validate_range(&params.range) {
            let _ = reply.send(Err(e));
            return;
        }

        let (topic, queue) = match self.open_handles(&params.topic) {
            Ok(pair) => pair,
            Err(e) => {
                let _ = reply.send(Err(e));
                return;
            }
        };

        eprintln!(
            "[open_topic] {} partitions={}/{partition_count} start_from={:?} range={:?} per_partition_limit={limit}",
            params.topic,
            partitions.len(),
            params.start_from,
            params.range,
        );

        self.store.clear();
        self.filter = params.filter;
        self.sort = params.sort;
        self.newest_first = params.range.newest_first(params.start_from);
        self.open_topic = Some(params.topic.clone());
        self.cursors.clear();
        // Скорость кластера измерителю переносить между топиками можно, а вот
        // цену офсета — нет: в соседних топиках сообщения отличаются на два
        // порядка. Поэтому окно начинается заново с малого и растёт не быстрее
        // чем вдвое за раунд, пока в измерение не войдут данные нового топика.
        self.window = FIRST_ROUND_CHUNK;
        self.last_round = None;
        self.kept_per_offset = None;
        self.has_timestamps = false;
        self.rebuild_view();

        self.pending_read = Some(PendingRead {
            topic,
            queue,
            open_partitions: HashSet::new(),
            phase: ReadPhase::Watermarks(partitions),
            started: Instant::now(),
            round_started: Instant::now(),
            bytes_at_start: 0,
            reply,
            topic_name: params.topic,
            round_mark: 0,
            round_mark_bytes: 0,
            budget: limit,
            range: params.range,
        });

        let _ = self.tx.send(Command::ContinueRead);
    }

    /// "Загрузить ещё": поднимает бюджет каждой ещё не исчерпанной партиции и
    /// продолжает раунды с того окна, на котором остановилось предыдущее
    /// чтение. Watermarks уже сняты, повторно за ними не ходим.
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
        if self.cursors.is_empty() {
            let _ = reply.send(Err("nothing loaded yet".into()));
            return;
        }

        let additional = params.additional.clamp(1, 100_000) as i64;
        for cursor in self.cursors.values_mut() {
            if !cursor.exhausted {
                cursor.budget = cursor.absorbed + additional;
            }
        }

        let (topic, queue) = match self.open_handles(&topic_name) {
            Ok(pair) => pair,
            Err(e) => {
                let _ = reply.send(Err(e));
                return;
            }
        };

        eprintln!("[load_more] {topic_name} additional={additional} per partition");

        let pending = PendingRead {
            topic,
            queue,
            open_partitions: HashSet::new(),
            phase: ReadPhase::Reading(HashMap::new()),
            started: Instant::now(),
            round_started: Instant::now(),
            bytes_at_start: self.store.byte_size(),
            reply,
            topic_name,
            round_mark: self.store.len(),
            round_mark_bytes: self.store.byte_size(),
            budget: additional,
            // Диапазон уже вшит в границы курсоров — здесь его применять
            // повторно нечему и незачем.
            range: ReadRange::default(),
        };
        self.begin_round(pending);
    }

    /// Хендлы legacy-консьюмера для топика. Живут ровно одно чтение:
    /// освобождение делает `Drop`.
    fn open_handles(&self, topic_name: &str) -> Result<(RawTopic, RawQueue), String> {
        let consumer = self.consumer.as_ref().ok_or("not connected to a cluster")?;
        let client_ptr = consumer.client().native_ptr();
        let topic = RawTopic::new(client_ptr, topic_name)?;
        let queue = self.new_queue()?;
        Ok((topic, queue))
    }

    /// Разносит накопившиеся события клиента — прежде всего статистику, из
    /// которой измеряется квота.
    ///
    /// Именно `BaseConsumer::poll`, а не `rd_kafka_poll`, и это не мелочь.
    /// rdkafka-rust не ставит C-колбэк `rd_kafka_conf_set_stats_cb` вообще
    /// никогда: `BaseConsumer::new` включает event API
    /// (`rd_kafka_conf_set_events(... RD_KAFKA_EVENT_STATS ...)`), а при нём
    /// librdkafka колбэки не зовёт — складывает событие в очередь. Прямой
    /// `rd_kafka_poll` обслуживал `rk_rep`, где для статистики колбэка нет, и
    /// просто выбрасывал её: измеритель квоты не получал ни одного снимка.
    ///
    /// Очередь здесь именно главная: `group.id` мы не задаём (см.
    /// `helpers::get_cluster_config`), а без него `BaseConsumer::new` не
    /// перенаправляет главную очередь в консьюмерскую и слушает первую.
    ///
    /// Сообщения топика сюда не попадают — они идут в нашу приватную очередь
    /// через `consume_start_queue`, — так что результат ожидаемо пустой.
    fn drain_events(&self) {
        if let Some(consumer) = self.consumer.as_ref() {
            let _ = consumer.poll(Duration::ZERO);
        }
    }

    fn new_queue(&self) -> Result<RawQueue, String> {
        let consumer = self.consumer.as_ref().ok_or("not connected to a cluster")?;
        RawQueue::new(consumer.client().native_ptr())
    }

    /// Останавливает уже идущее чтение (пользователь переключил топик или
    /// закрыл его, пока предыдущее чтение ещё не закончилось). `reply` просто
    /// дропается — это отклоняет старый `invoke()` на фронте, что уже сегодня
    /// спокойно переживается флагом `cancelled` в эффекте открытия топика.
    fn cancel_pending_read(&mut self) {
        if let Some(pending) = self.pending_read.take() {
            Self::stop_all(&pending);
            // Незавершённый раунд — дырявое окно; его добыча не годится.
            self.store.truncate(pending.round_mark);
            eprintln!(
                "[worker] {} read cancelled (superseded) after {:?}",
                pending.topic_name,
                pending.started.elapsed()
            );
            // Раньше `reply` просто дропался, и на фронт прилетало
            // "kafka worker dropped the reply" — сообщение про внутреннюю
            // поломку там, где на самом деле пользователь просто ушёл с топика.
            let _ = pending.reply.send(Err(READ_SUPERSEDED.to_string()));
        }
    }

    /// Закрывает чтение всех открытых партиций. БЛОКИРУЮЩАЯ операция: каждый
    /// `consume_stop` ждёт подтверждения от брокерского потока, а тот под
    /// квотой может сидеть в придержанном фетче секундами. Поэтому зовётся
    /// ровно один раз — на завершении чтения, а не между окнами.
    fn stop_all(pending: &PendingRead) {
        for &p in &pending.open_partitions {
            pending.topic.consume_stop(p);
        }
    }

    // --- Шаги чтения --------------------------------------------------------

    /// Один шаг идущего чтения. Возвращает управление циклу `run()` после
    /// каждого шага, чтобы между ними обслуживались остальные команды.
    fn continue_read(&mut self) {
        let Some(pending) = self.pending_read.take() else {
            return;
        };

        match pending.phase {
            ReadPhase::Watermarks(_) => {
                if pending.started.elapsed() >= READ_DEADLINE {
                    eprintln!(
                        "[continue_read] {} deadline while fetching offsets",
                        pending.topic_name
                    );
                    self.finish_read(pending, true);
                    return;
                }
                self.step_watermarks(pending)
            }
            ReadPhase::Reading(_) => {
                // Общий дедлайн чтения здесь НЕ проверяется — он смотрится на
                // границе раунда (`begin_round`), где остановка ничего не
                // стоит. Здесь только страховка от зависшего раунда.
                if pending.round_started.elapsed() >= ROUND_DEADLINE {
                    eprintln!(
                        "[continue_read] {} round stalled after {:?}, dropping it",
                        pending.topic_name,
                        pending.round_started.elapsed()
                    );
                    self.abort_round(pending);
                    return;
                }
                self.step_reading(pending)
            }
        }
    }

    /// Снимает границы очередной пачки партиций. Без них нельзя нарезать окна:
    /// `RD_KAFKA_OFFSET_TAIL` дал бы только «последние N», но не сказал бы, где
    /// начинается партиция и когда читать назад больше нечего.
    fn step_watermarks(&mut self, mut pending: PendingRead) {
        let ReadPhase::Watermarks(todo) = &mut pending.phase else {
            return;
        };
        let take = WATERMARK_BATCH.min(todo.len());
        let batch: Vec<i32> = todo.drain(..take).collect();
        let remaining = todo.len();

        let Some(consumer) = self.consumer.as_ref() else {
            let _ = pending.reply.send(Err("not connected to a cluster".into()));
            return;
        };

        // Сначала собираем ответы, и только потом трогаем `self.cursors`:
        // `consumer` держит `&self`.
        let mut fetched = Vec::with_capacity(batch.len());
        for p in batch {
            match consumer.fetch_watermarks(&pending.topic_name, p, WATERMARK_TIMEOUT) {
                Ok((low, high)) => fetched.push((p, low, high)),
                Err(e) => {
                    let _ = pending
                        .reply
                        .send(Err(format!("can't read offsets of partition {p}: {e}")));
                    return;
                }
            }
        }

        let budget = pending.budget;
        let newest_first = self.newest_first;
        for (p, low, high) in fetched {
            self.cursors.insert(
                p,
                PartitionCursor {
                    low,
                    high,
                    next: if newest_first { high } else { low },
                    absorbed: 0,
                    budget,
                    // Чтение ещё не открыто: где стоит читатель — не вопрос.
                    read_position: None,
                    exhausted: low >= high,
                    frontier_ts: None,
                    barren_rounds: 0,
                },
            );
        }

        if remaining > 0 {
            self.pending_read = Some(pending);
            let _ = self.tx.send(Command::ContinueRead);
            return;
        }

        let total: i64 = self.cursors.values().map(|c| c.high - c.low).sum();
        eprintln!(
            "[watermarks] {} ready in {:?}, {total} messages in the topic",
            pending.topic_name,
            pending.started.elapsed()
        );

        // Границы сняты — теперь их можно сузить до запрошенного диапазона.
        // Отдельным шагом, а не по ходу снятия: границы по времени известны
        // только целиком, `offsets_for_times` спрашивается одним запросом на
        // все партиции сразу, и до его ответа резать нечего.
        if let Err(e) = self.apply_range(&pending) {
            Self::stop_all(&pending);
            let _ = pending.reply.send(Err(e));
            return;
        }

        pending.phase = ReadPhase::Reading(HashMap::new());
        pending.round_mark = self.store.len();
        pending.round_mark_bytes = self.store.byte_size();
        self.begin_round(pending);
    }

    /// Сужает уже снятые границы партиций до запрошенного диапазона.
    ///
    /// После этого о диапазоне не знает больше никто: окна, бюджеты, признак
    /// «дочитано», `load_more` — всё работает с `[low, high)` курсора и потому
    /// за диапазон не выходит по построению, а не по проверке в каждой ветке.
    fn apply_range(&mut self, pending: &PendingRead) -> Result<(), String> {
        let range = pending.range;
        if range.is_empty() {
            return Ok(());
        }

        // Время → офсеты. Ищет брокер: `ListOffsets` по времени возвращает
        // первый офсет со временем не раньше запрошенного, так что городить
        // поверх этого бинарный поиск по офсетам незачем.
        let by_time = range.by_timestamp();
        let from_at = self.offsets_at(&pending.topic_name, range.from_timestamp)?;
        // Верхняя граница inclusive, а exclusive конец диапазона — это первое
        // сообщение ПОСЛЕ неё: спрашиваем на миллисекунду позже.
        let to_at = self.offsets_at(&pending.topic_name, exclusive(range.to_timestamp))?;

        // Считаем всё до единой записи в курсоры: если диапазон окажется
        // пустым, сообщение об этом должно опираться на настоящие границы
        // партиций, а не на уже урезанные.
        let computed: Vec<(i32, i64, i64)> = self
            .cursors
            .iter()
            .map(|(&p, c)| {
                let (from, to) = if by_time {
                    (
                        // Момента нет в партиции — значит всё, что в ней есть,
                        // старше запрошенного начала, и брать нечего.
                        range.from_timestamp.map(|_| resolved(&from_at, p, c.high)),
                        // А вот отсутствие ВЕРХНЕЙ границы значит обратное:
                        // после неё сообщений нет, и читать надо до конца.
                        range.to_timestamp.map(|_| resolved(&to_at, p, c.high)),
                    )
                } else {
                    (range.from_offset, exclusive(range.to_offset))
                };
                let (low, high) = intersect(c.low, c.high, from, to);
                (p, low, high)
            })
            .collect();

        if computed.iter().all(|&(_, low, high)| low >= high) {
            return Err(if by_time {
                format!(
                    "no messages in the selected time range ({})",
                    describe_bounds(&self.cursors)
                )
            } else {
                format!(
                    "the selected offset range is outside the topic ({})",
                    describe_bounds(&self.cursors)
                )
            });
        }

        let newest_first = self.newest_first;
        for (p, low, high) in computed {
            let Some(cursor) = self.cursors.get_mut(&p) else {
                continue;
            };
            cursor.low = low;
            cursor.high = high;
            cursor.next = if newest_first { high } else { low };
            cursor.exhausted = low >= high;
            // Явный диапазон — это законченный запрос «всё между A и B», а не
            // открытое чтение постранично. Дефолтный `budget` (лимит страницы
            // для `newest`/`oldest`) здесь не должен обрывать раньше настоящей
            // границы диапазона — иначе «to» молча превращалось бы в очередной
            // «Load more», хотя пользователь его не просил. Раунд всё равно
            // остановят дедлайн и байтовый бюджет чтения, если диапазон и
            // правда огромен.
            cursor.budget = cursor.budget.max(high - low);
        }
        Ok(())
    }

    /// С какого офсета начинается указанный момент времени, по партициям.
    /// Пустая карта — момент не задан.
    fn offsets_at(&self, topic: &str, at: Option<i64>) -> Result<HashMap<i32, i64>, String> {
        let Some(at) = at else {
            return Ok(HashMap::new());
        };
        let consumer = self.consumer.as_ref().ok_or("not connected to a cluster")?;

        let mut request = TopicPartitionList::new();
        for &p in self.cursors.keys() {
            request
                .add_partition_offset(topic, p, Offset::Offset(at))
                .map_err(|e| format!("can't build a timestamp lookup: {e}"))?;
        }

        let resolved = consumer
            .offsets_for_times(request, WATERMARK_TIMEOUT)
            .map_err(|e| format!("can't look up offsets by time: {e}"))?;

        Ok(resolved
            .elements()
            .iter()
            // Отрицательный офсет — это `RD_KAFKA_OFFSET_END`: сообщений не
            // раньше запрошенного момента в партиции нет. Значение отбрасываем,
            // а что оно означает, решает вызывающий: для нижней границы это
            // «брать нечего», для верхней — «читать до конца».
            .filter_map(|e| e.offset().to_raw().filter(|o| *o >= 0).map(|o| (e.partition(), o)))
            .collect())
    }

    /// Нарезает окна очередного раунда и запускает по ним чтение.
    ///
    /// Здесь же — единственная точка штатной остановки чтения. Обрывать раунд
    /// на середине нельзя без потерь (см. `abort_round`), поэтому и дедлайн, и
    /// байтовый бюджет проверяются именно тут, на границе.
    fn begin_round(&mut self, mut pending: PendingRead) {
        let newest_first = self.newest_first;

        if pending.started.elapsed() >= READ_DEADLINE {
            eprintln!(
                "[begin_round] {} stopping at round boundary: deadline after {:?}",
                pending.topic_name,
                pending.started.elapsed()
            );
            self.finish_read(pending, true);
            return;
        }
        let read_bytes = self
            .store
            .byte_size()
            .saturating_sub(pending.bytes_at_start);
        if read_bytes >= READ_BYTE_BUDGET {
            eprintln!(
                "[begin_round] {} stopping at round boundary: {read_bytes} bytes read",
                pending.topic_name
            );
            self.finish_read(pending, true);
            return;
        }

        let active = self
            .cursors
            .values()
            .filter(|c| !c.exhausted && c.absorbed < c.budget)
            .count();
        let floor = window_floor(self.kept_per_offset);
        let chunk = next_window(
            self.window,
            self.quota.estimate(),
            active,
            self.last_round,
            floor,
        );
        self.window = chunk;

        let mut round: HashMap<i32, RoundPartition> = HashMap::new();
        for (&p, c) in &self.cursors {
            if let Some((start, end)) = c.next_window(newest_first, chunk) {
                round.insert(
                    p,
                    RoundPartition {
                        start,
                        end,
                        absorbed: 0,
                        done: false,
                    },
                );
            }
        }

        if round.is_empty() {
            self.finish_read(pending, false);
            return;
        }

        // Партиция открывается один раз за чтение, дальше только
        // переставляется. Ни `consume_stop`, ни новой очереди здесь нет
        // намеренно: и то, и другое стоило многосекундных пауз под квотой.
        for (&p, rp) in &round {
            let position = self.cursors.get(&p).and_then(|c| c.read_position);

            let outcome = if !pending.open_partitions.contains(&p) {
                let opened = pending
                    .topic
                    .consume_start_queue(p, rp.start, &pending.queue);
                if opened.is_ok() {
                    pending.open_partitions.insert(p);
                }
                opened
            } else if position == Some(rp.start) {
                // Окна идут встык (режим `oldest`) — читатель уже там.
                Ok(())
            } else {
                pending.topic.seek(p, rp.start)
            };

            match outcome {
                Ok(()) => {
                    if let Some(c) = self.cursors.get_mut(&p) {
                        c.read_position = Some(rp.start);
                    }
                }
                Err(e) => {
                    Self::stop_all(&pending);
                    let _ = pending
                        .reply
                        .send(Err(format!("can't position partition {p}: {e}")));
                    return;
                }
            }
        }

        pending.round_mark = self.store.len();
        pending.round_mark_bytes = self.store.byte_size();
        pending.round_started = Instant::now();
        pending.phase = ReadPhase::Reading(round);
        self.pending_read = Some(pending);
        let _ = self.tx.send(Command::ContinueRead);
    }

    /// Один `consume_batch` в рамках текущего раунда.
    fn step_reading(&mut self, mut pending: PendingRead) {
        let batch = raw_consumer::consume_batch(&pending.queue, POLL_TICK_MS, BATCH_SIZE);

        let ReadPhase::Reading(round) = &mut pending.phase else {
            return;
        };
        let newest_first = self.newest_first;
        let mut buffer_full = false;
        // Ошибка не обрабатывается на месте: пока жив `round`, `pending`
        // заимствован и его нельзя ни отдать, ни разобрать на части.
        let mut failure: Option<String> = None;

        for msg in batch {
            let p = msg.partition();
            match msg.err() {
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR => {
                    let Some(rp) = round.get_mut(&p) else {
                        continue;
                    };
                    if rp.done {
                        // Партиция своё окно добрала и просто ждёт остальных.
                        // Останавливать её здесь нельзя — `consume_stop`
                        // блокирующий (см. `RawTopic::seek`); то, что она
                        // успеет натаскать сверх окна, отбросит барьер версии
                        // при переходе к следующему окну.
                        continue;
                    }
                    if msg.offset() >= rp.end {
                        // Окно вычитано целиком — остальное принадлежит уже
                        // показанному (newest) либо следующему раунду (oldest).
                        rp.done = true;
                        continue;
                    }
                    if msg.offset() < rp.start {
                        // Хвост со старой позиции, который барьер версии
                        // почему-то не отбросил. Не должно случаться; молча
                        // проглотить такое значило бы испортить порядок.
                        eprintln!(
                            "[step_reading] {} p{p} stale offset {} outside window [{}..{})",
                            pending.topic_name,
                            msg.offset(),
                            rp.start,
                            rp.end
                        );
                        continue;
                    }
                    if !absorb(&mut self.store, &msg) {
                        eprintln!(
                            "[step_reading] {} buffer budget hit at {} messages",
                            pending.topic_name,
                            self.store.len()
                        );
                        buffer_full = true;
                        break;
                    }
                    rp.absorbed += 1;

                    let ts = msg.timestamp_millis();
                    if ts > 0 {
                        self.has_timestamps = true;
                    }
                    if let Some(c) = self.cursors.get_mut(&p) {
                        c.observe(newest_first, ts);
                    }
                }
                RDKafkaRespErr::RD_KAFKA_RESP_ERR__PARTITION_EOF => {
                    // Конец лога. В `oldest` это настоящее дно партиции;
                    // в `newest` — просто верхняя граница первого окна.
                    if let Some(rp) = round.get_mut(&p) {
                        rp.done = true;
                    }
                    if !newest_first {
                        if let Some(c) = self.cursors.get_mut(&p) {
                            c.exhausted = true;
                        }
                    }
                }
                other => {
                    let e = raw_consumer::err_str(other);
                    eprintln!("[step_reading] {} READ ERROR: {e}", pending.topic_name);
                    failure = Some(format!("read failed: {e}"));
                    break;
                }
            }
        }

        if let Some(e) = failure {
            Self::stop_all(&pending);
            self.store.truncate(pending.round_mark);
            let _ = pending.reply.send(Err(e));
            return;
        }
        if buffer_full {
            // Дочитать раунд уже не выйдет: следующий `push` тоже не влезет.
            // Отматываем дырявое окно и закрываемся тем, что опубликовано.
            self.abort_round(pending);
            return;
        }

        let round_complete = match &pending.phase {
            ReadPhase::Reading(round) => round.values().all(|rp| rp.done),
            _ => false,
        };
        if !round_complete {
            self.pending_read = Some(pending);
            let _ = self.tx.send(Command::ContinueRead);
            return;
        }

        self.close_round(&mut pending);
        self.publish();
        self.begin_round(pending);
    }

    /// Раунд дочитан целиком: двигаем курсоры на следующее окно.
    fn close_round(&mut self, pending: &mut PendingRead) {
        let newest_first = self.newest_first;
        let elapsed = pending.round_started.elapsed();
        let bytes = self.store.byte_size() - pending.round_mark_bytes;
        let window = self.window;
        let quota = self.quota.estimate();
        let ReadPhase::Reading(round) = &mut pending.phase else {
            return;
        };

        let absorbed: i64 = round.values().map(|rp| rp.absorbed).sum();
        let offsets: i64 = round.values().map(|rp| rp.end - rp.start).sum();
        // Офсеты за раунд — вторая половина измерения: она превращает «байт в
        // секунду» в «офсетов в секунду», уже с учётом и размера сообщений, и
        // плотности, и предвыборки, которую пришлось выбросить.
        self.quota.record_offsets(offsets);
        eprintln!(
            "[round] {} {} partitions, window {window}, +{absorbed} messages of {offsets} \
             offsets, +{bytes} bytes in {elapsed:?}{}",
            pending.topic_name,
            round.len(),
            match quota {
                // «kept» против «paid» — главный индикатор впустую потраченной
                // квоты: брокер шлёт по `fetch.message.max.bytes` на партицию
                // независимо от размера окна, и всё, что не влезло в окно,
                // оплачено и выброшено. Растущий разрыв означает, что окно
                // мельче одного фетча — см. `window_floor`.
                Some(q) => format!(
                    ", cluster gives {:.0} KB/s, {:.0} B/offset paid vs {:.0} kept ({:.1}x waste), \
                     throttle up to {:?}",
                    q.bytes_per_sec / 1024.0,
                    q.bytes_per_offset,
                    if offsets > 0 { bytes as f64 / offsets as f64 } else { 0.0 },
                    if offsets > 0 && bytes > 0 {
                        q.bytes_per_offset * offsets as f64 / bytes as f64
                    } else {
                        0.0
                    },
                    q.peak_throttle
                ),
                // Печатается явно: молчащий измеритель — это не «пока мало
                // данных», а вполне возможная поломка, и один раз она уже
                // стоила разогнавшегося вслепую окна.
                None => ", quota not measured yet".to_string(),
            }
        );
        // Окно, отдавшее заметно меньше сообщений, чем в нём офсетов, — это
        // либо compacted-топик, либо ошибка в нарезке. Отличить одно от
        // другого можно только по конкретным диапазонам, поэтому печатаем их.
        if absorbed * 2 < offsets {
            let detail: Vec<String> = round
                .iter()
                .map(|(p, rp)| format!("p{p} [{}..{}) -> {}", rp.start, rp.end, rp.absorbed))
                .collect();
            eprintln!("[round] {} thin: {}", pending.topic_name, detail.join(", "));
        }

        for (&p, rp) in round.iter() {
            if let Some(c) = self.cursors.get_mut(&p) {
                c.advance(newest_first, rp);
            }
        }
        round.clear();
        self.last_round = Some(elapsed);
        if offsets > 0 {
            self.kept_per_offset = Some(bytes as f64 / offsets as f64);
        }
    }

    /// Раунд оборвался на середине (дедлайн, переполнение буфера, ошибка).
    /// Его окно вычитано частично, а частичное окно — это дырка в порядке:
    /// в `newest` непрочитанным остался как раз САМЫЙ СВЕЖИЙ его край, который
    /// обязан стоять выше. Выбрасываем добычу раунда и закрываем чтение —
    /// курсоры остались на границе окна, `load_more` перечитает его целиком.
    fn abort_round(&mut self, pending: PendingRead) {
        Self::stop_all(&pending);
        self.store.truncate(pending.round_mark);
        self.finish_read(pending, true);
    }

    /// Отдаёт итог чтения. `interrupted` — чтение оборвалось, а не упёрлось в
    /// естественный конец: значит в топике заведомо есть ещё.
    fn finish_read(&mut self, pending: PendingRead, interrupted: bool) {
        self.publish();
        let all_done = self.cursors.values().all(|c| c.exhausted);
        let truncated = interrupted || !all_done;

        eprintln!(
            "[finish_read] {} in {:?}: published={} staged={} bytes={} truncated={truncated}",
            pending.topic_name,
            pending.started.elapsed(),
            self.store.committed_len(),
            self.store.staged_len(),
            self.store.byte_size()
        );

        let _ = pending.reply.send(Ok(OpenTopicResult {
            total: self.view.len(),
            loaded: self.store.committed_len(),
            buffer_bytes: self.store.byte_size(),
            memory: self.memory_usage(),
            truncated,
            quota: self.quota_info(),
        }));
    }

    // --- Публикация ---------------------------------------------------------

    /// Насколько глубоко можно публиковать прямо сейчас.
    ///
    /// Партиция, вычитанная до `frontier_ts`, дальше выдаст только более старое
    /// (в `newest`) — значит всё, что СТРОГО новее самой отстающей из живых
    /// партиций, своё место в порядке уже заняло и никем не будет подвинуто.
    fn safe_frontier(&self) -> Frontier {
        let mut frontier: Option<i64> = None;
        for c in self.cursors.values() {
            if c.exhausted {
                continue;
            }
            let Some(ts) = c.frontier_ts else {
                // Из партиции ещё ни одного сообщения: она может принести что
                // угодно, включая самое свежее. Придерживаем весь отстойник.
                // Бывает на дырявых (compacted) партициях, чьё окно оказалось
                // пустым; разрешается само, как только оттуда что-то приедет.
                return Frontier::Nothing;
            };
            frontier = Some(match frontier {
                None => ts,
                Some(f) if self.newest_first => f.max(ts),
                Some(f) => f.min(ts),
            });
        }
        match frontier {
            // Ни одной живой партиции — придерживать не от кого.
            None => Frontier::Everything,
            Some(ts) => Frontier::Beyond(ts),
        }
    }

    /// Досортировывает отстойник и публикует всё, что уже не может быть
    /// подвинуто. Опубликованное больше не двигается никогда — на этом и
    /// держится стабильность таблицы на фронте.
    fn publish(&mut self) {
        if self.store.staged_len() == 0 {
            return;
        }
        self.store.sort_staged(self.newest_first);

        let published = if !self.has_timestamps {
            // Брокер не отдал времени ни по одному сообщению: ни сортировать,
            // ни придерживать не по чему — показываем в порядке приезда,
            // иначе таблица так и осталась бы пустой.
            self.store.commit_all()
        } else {
            match self.safe_frontier() {
                Frontier::Nothing => 0,
                Frontier::Everything => self.store.commit_all(),
                Frontier::Beyond(f) if self.newest_first => {
                    self.store.commit_staged_while(|m| m.timestamp > f)
                }
                Frontier::Beyond(f) => self.store.commit_staged_while(|m| m.timestamp < f),
            }
        };

        if published > 0 {
            self.rebuild_view();
        }
    }

    /// Снимок хода ещё не завершённого чтения — для опроса с фронта, пока
    /// `open_topic`/`load_more` не ответили финальным результатом.
    /// Измеренная скорость кластера для показа в шапке.
    fn quota_info(&self) -> QuotaInfo {
        match self.quota.estimate() {
            Some(est) => QuotaInfo {
                read_bytes_per_sec: Some(est.bytes_per_sec as u64),
                peak_throttle_ms: est.peak_throttle.as_millis() as u64,
                active_brokers: est.brokers,
                known_brokers: est.known_brokers,
            },
            None => QuotaInfo::default(),
        }
    }

    fn memory_usage(&self) -> MemoryUsage {
        MemoryUsage {
            memory_bytes: self.store.memory_bytes(),
            memory_limit: self.store.max_bytes(),
        }
    }

    fn progress(&self) -> OpenTopicProgress {
        OpenTopicProgress {
            topic: self.open_topic.clone(),
            quota: self.quota_info(),
            buffer_bytes: self.store.byte_size(),
            memory: self.memory_usage(),
            loaded: self.store.committed_len(),
            total: self.view.len(),
            truncated: self.pending_read.is_some(),
            done: self.pending_read.is_none(),
        }
    }

    fn close_topic(&mut self) {
        self.cancel_pending_read();
        self.open_topic = None;
        self.filter = MessageFilter::default();
        self.sort = None;
        self.view.clear();
        self.cursors.clear();
        self.has_timestamps = false;
        // Схема принадлежит топику. Оставить её — значит показать следующий
        // топик через чужой контракт, если фронт не успеет прислать свою.
        self.decoder = None;
        // Освобождаем буфер целиком: держать сотни мегабайт, пока пользователь
        // ничего не смотрит, незачем.
        self.store.release();
    }

    /// Пересобирает отфильтрованное представление по ОПУБЛИКОВАННОЙ части
    /// буфера. Работает в памяти, без единого сетевого запроса — поэтому смена
    /// фильтра мгновенна.
    ///
    /// Публикация только дописывает записи в хвост, поэтому и `view` только
    /// растёт: индексы уже показанных строк не меняются, и кэш окон на фронте
    /// остаётся валидным.
    fn rebuild_view(&mut self) {
        // Забираем вектор себе: иначе `self.view.push` конфликтует с
        // одновременным заимствованием `self.filter` и `self.store`.
        // Ёмкость при этом сохраняется, повторных аллокаций нет.
        let mut view = std::mem::take(&mut self.view);
        view.clear();
        let total = self.store.committed_len();

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

        if let Some(sort) = self.sort {
            self.sort_view(&mut view, sort);
        }

        self.view = view;
    }

    /// Сортировка по клику на заголовок колонки, поверх фильтра.
    ///
    /// `sort_by` — стабильная сортировка: сообщения с одинаковым значением
    /// столбца остаются в том порядке, в котором их поставил обычный порядок
    /// чтения (время, а внутри него — офсет), а не в произвольном. Пока идёт
    /// прогрессивная подгрузка, эта сортировка пересчитывается на весь `view`
    /// при каждом новом раунде — если она активна, уже показанные строки
    /// перестают быть застрахованы от переезда, в отличие от обычного порядка.
    fn sort_view(&self, view: &mut [u32], sort: SortSpec) {
        let desc = sort.direction == SortDirection::Desc;
        view.sort_by(|&a, &b| {
            let ma = self
                .store
                .get(a as usize)
                .expect("view index out of sync with store");
            let mb = self
                .store
                .get(b as usize)
                .expect("view index out of sync with store");
            let cmp = match sort.column {
                SortColumn::Partition => ma.partition.cmp(&mb.partition),
                SortColumn::Offset => ma.offset.cmp(&mb.offset),
                SortColumn::Timestamp => ma.timestamp.cmp(&mb.timestamp),
                SortColumn::Key => self.store.key(a as usize).cmp(self.store.key(b as usize)),
            };
            if desc {
                cmp.reverse()
            } else {
                cmp
            }
        });
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
                let meta = self
                    .store
                    .get(i)
                    .expect("view index out of sync with store");
                let value = self.store.value(i);
                let shown = self.render(value);
                RowPreview {
                    index: start + offset_in_window,
                    partition: meta.partition,
                    offset: meta.offset,
                    timestamp: meta.timestamp,
                    key: text::preview(self.store.key(i), PREVIEW_BYTES),
                    preview: match &shown.decoded {
                        // Обрезаем уже декодированное: строке таблицы нужны
                        // первые пару сотен символов, а не всё тело.
                        Some(json) => text::preview(json.as_bytes(), PREVIEW_BYTES),
                        None => text::preview(value, PREVIEW_BYTES),
                    },
                    value_size: value.len(),
                    binary: shown.binary(value),
                    decode_error: shown.error,
                }
            })
            .collect()
    }

    /// Прогоняет тело через декодер, если он задан.
    ///
    /// Декодирование ленивое — только для строк, которые действительно уезжают
    /// на экран, и для сообщения, которое действительно открыли. Ни кэша, ни
    /// второй копии тела в памяти: буфер и так держит десятки тысяч сообщений,
    /// и класть рядом их разобранные представления значило бы удвоить его цену
    /// ради данных, которые переживут один экран прокрутки.
    fn render(&self, value: &[u8]) -> Rendered {
        let Some(decoder) = &self.decoder else {
            return Rendered::default();
        };
        match decoder.decode(value) {
            Ok(json) => Rendered {
                decoded: Some(json),
                error: None,
            },
            // Схему не трогаем и топик не закрываем: одно сообщение чужого
            // формата — обычное дело в топике, который переживал смену
            // контракта. Показываем его текстом и говорим, что случилось.
            Err(e) => Rendered {
                decoded: None,
                error: Some(e),
            },
        }
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
        let shown = self.render(value);
        // Только у декодированного тела есть enum, с которым можно сверяться:
        // на самостоятельном JSON или тексте это был бы список из чужой схемы.
        let enum_values = match &shown.decoded {
            Some(_) => self
                .decoder
                .as_ref()
                .map(|d| d.enum_values().to_vec())
                .unwrap_or_default(),
            None => Vec::new(),
        };

        Ok(FullMessage {
            partition: meta.partition,
            offset: meta.offset,
            timestamp: meta.timestamp,
            key: text::decode(self.store.key(store_index)),
            binary: shown.binary(value),
            value: match shown.decoded {
                Some(json) => json,
                None => text::decode(value),
            },
            value_size: value.len(),
            decode_error: shown.error,
            enum_values,
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

#[cfg(test)]
mod tests {
    use super::*;

    const NEWEST: bool = true;
    const OLDEST: bool = false;

    fn cursor(low: i64, high: i64, newest_first: bool, budget: i64) -> PartitionCursor {
        PartitionCursor {
            low,
            high,
            next: if newest_first { high } else { low },
            absorbed: 0,
            budget,
            read_position: None,
            exhausted: low >= high,
            frontier_ts: None,
            barren_rounds: 0,
        }
    }

    /// Прокручивает раунды до упора и отдаёт список окон в порядке чтения.
    /// Каждое окно считается вычитанным целиком (по офсету на сообщение).
    fn walk(mut c: PartitionCursor, newest_first: bool, chunk: i64) -> Vec<(i64, i64)> {
        let mut windows = Vec::new();
        while let Some((start, end)) = c.next_window(newest_first, chunk) {
            windows.push((start, end));
            c.advance(
                newest_first,
                &RoundPartition {
                    start,
                    end,
                    absorbed: end - start,
                    done: true,
                },
            );
            assert!(windows.len() < 1000, "раунды не сходятся");
        }
        windows
    }

    #[test]
    fn newest_walks_windows_backwards_from_the_end() {
        let windows = walk(cursor(0, 1000, NEWEST, 1000), NEWEST, 250);
        assert_eq!(windows, vec![(750, 1000), (500, 750), (250, 500), (0, 250)]);
    }

    #[test]
    fn oldest_walks_windows_forward_from_the_start() {
        let windows = walk(cursor(0, 1000, OLDEST, 1000), OLDEST, 250);
        assert_eq!(windows, vec![(0, 250), (250, 500), (500, 750), (750, 1000)]);
    }

    /// Партиция, обрезанная retention: офсеты начинаются не с нуля, и окна не
    /// должны заезжать ниже реального начала лога.
    #[test]
    fn windows_never_cross_the_partition_bounds() {
        assert_eq!(
            walk(cursor(400, 1000, NEWEST, 10_000), NEWEST, 250),
            vec![(750, 1000), (500, 750), (400, 500)],
        );
        assert_eq!(
            walk(cursor(400, 1000, OLDEST, 10_000), OLDEST, 250),
            vec![(400, 650), (650, 900), (900, 1000)],
        );
    }

    #[test]
    fn budget_caps_the_last_window_and_stops_the_walk() {
        // Бюджет 300 при шаге 250: второе окно урезано до остатка, третьего нет.
        assert_eq!(
            walk(cursor(0, 1000, NEWEST, 300), NEWEST, 250),
            vec![(750, 1000), (700, 750)],
        );
        assert_eq!(
            walk(cursor(0, 1000, OLDEST, 300), OLDEST, 250),
            vec![(0, 250), (250, 300)],
        );
    }

    /// Регрессия: явный диапазон "от A до B" останавливался на дефолтном
    /// лимите страницы задолго до B — `apply_range` сужал `[low, high)` до
    /// запрошенных границ, но не трогал унаследованный от `open_topic`
    /// `budget`, и `next_window` бросал раунды, даже не дойдя до настоящего
    /// конца диапазона. Фикс раздвигает `budget` под весь узкий диапазон —
    /// точно так же, как это теперь делает `apply_range`.
    #[test]
    fn a_bounded_range_walks_all_the_way_to_its_far_edge() {
        // Диапазон [0, 1000) при странично-мелком бюджете 300 обрывался бы
        // на 300, не дойдя до 1000 — см. `budget_caps_the_last_window_and_stops_the_walk`.
        let mut c = cursor(0, 1000, OLDEST, 300);
        c.budget = c.budget.max(c.high - c.low);
        assert_eq!(
            walk(c, OLDEST, 250),
            vec![(0, 250), (250, 500), (500, 750), (750, 1000)],
        );

        let mut c = cursor(0, 1000, NEWEST, 300);
        c.budget = c.budget.max(c.high - c.low);
        assert_eq!(
            walk(c, NEWEST, 250),
            vec![(750, 1000), (500, 750), (250, 500), (0, 250)],
        );
    }

    #[test]
    fn all_partitions_when_nothing_is_selected() {
        assert_eq!(selected_partitions(None, 4).unwrap(), vec![0, 1, 2, 3]);
        // Пустой список — то же самое: состояния «читать неоткуда» быть не должно.
        assert_eq!(selected_partitions(Some(&[]), 3).unwrap(), vec![0, 1, 2]);
    }

    /// Дубликат означал бы `consume_start` по уже открытой партиции — ошибку
    /// librdkafka на ровном месте.
    #[test]
    fn selection_is_sorted_and_deduplicated() {
        assert_eq!(
            selected_partitions(Some(&[5, 1, 5, 1, 3]), 8).unwrap(),
            vec![1, 3, 5],
        );
    }

    /// Топик сменился, а выбор партиций остался от прежнего — читать «партицию
    /// 7» из двухпартиционного топика нельзя, и молчать об этом тоже.
    #[test]
    fn selection_outside_the_topic_is_rejected() {
        assert!(selected_partitions(Some(&[0, 7]), 2).is_err());
        assert!(selected_partitions(Some(&[-1]), 2).is_err());
    }

    /// Границы вводит человек и вводит их inclusive; внутри всё считается
    /// полуинтервалами. Съехать здесь на единицу — значит потерять ровно то
    /// сообщение, ради которого диапазон и задавали.
    #[test]
    fn an_inclusive_upper_bound_keeps_the_message_it_names() {
        // Партиция 100..1000, просим 200..300 включительно.
        assert_eq!(
            intersect(100, 1000, Some(200), exclusive(Some(300))),
            (200, 301),
        );
    }

    #[test]
    fn range_is_clipped_to_what_the_partition_actually_holds() {
        // Начало раньше retention — читаем с реального начала.
        assert_eq!(intersect(100, 1000, Some(0), None), (100, 1000));
        // Конец за горизонтом — читаем до реального конца.
        assert_eq!(intersect(100, 1000, None, exclusive(Some(i64::MAX))), (100, 1000));
        // Диапазон целиком мимо партиции — пусто, но без паники и переполнений.
        assert_eq!(intersect(100, 1000, Some(5000), None), (5000, 5000));
        assert_eq!(intersect(100, 1000, None, exclusive(Some(10))), (100, 100));
    }

    /// Названа только верхняя граница — пользователь указал точку, ОТ которой
    /// смотрит назад, и порядок должен быть по убыванию.
    #[test]
    fn only_an_upper_bound_reads_backwards() {
        let to_only = ReadRange {
            to_offset: Some(500),
            ..ReadRange::default()
        };
        assert!(to_only.newest_first(StartFrom::Oldest));

        let from_only = ReadRange {
            from_offset: Some(500),
            ..ReadRange::default()
        };
        assert!(!from_only.newest_first(StartFrom::Newest));

        let both = ReadRange {
            from_offset: Some(1),
            to_offset: Some(500),
            ..ReadRange::default()
        };
        assert!(!both.newest_first(StartFrom::Newest));

        // Границ нет — решает селектор.
        let none = ReadRange::default();
        assert!(none.newest_first(StartFrom::Newest));
        assert!(!none.newest_first(StartFrom::Oldest));
    }

    /// Перевёрнутый диапазон вернул бы пустую таблицу без единого намёка на
    /// причину — а причина здесь целиком во введённом.
    #[test]
    fn inverted_bounds_are_rejected_with_a_reason() {
        assert!(validate_range(&ReadRange {
            from_offset: Some(500),
            to_offset: Some(100),
            ..ReadRange::default()
        })
        .is_err());
        assert!(validate_range(&ReadRange {
            from_timestamp: Some(2),
            to_timestamp: Some(1),
            ..ReadRange::default()
        })
        .is_err());
        assert!(validate_range(&ReadRange {
            from_offset: Some(-1),
            ..ReadRange::default()
        })
        .is_err());
        assert!(validate_range(&ReadRange {
            from_offset: Some(100),
            to_offset: Some(100),
            ..ReadRange::default()
        })
        .is_ok());
    }

    #[test]
    fn empty_partition_yields_no_windows() {
        assert!(walk(cursor(0, 0, NEWEST, 1000), NEWEST, 250).is_empty());
        assert!(walk(cursor(500, 500, OLDEST, 1000), OLDEST, 250).is_empty());
    }

    /// Пустое (полностью compacted) окно не должно вешать чтение: курсор
    /// обязан двигаться даже когда сообщений в окне не оказалось.
    #[test]
    fn a_window_that_yielded_nothing_still_advances_the_cursor() {
        let mut c = cursor(0, 500, NEWEST, 1000);
        let (start, end) = c.next_window(NEWEST, 250).unwrap();
        c.advance(
            NEWEST,
            &RoundPartition {
                start,
                end,
                absorbed: 0,
                done: true,
            },
        );
        assert_eq!(c.absorbed, 0);
        assert_eq!(c.next_window(NEWEST, 250), Some((0, 250)));
    }

    /// `load_more` поднимает бюджет — и чтение продолжается ровно с того окна,
    /// на котором остановилось, без нахлёста на уже прочитанное.
    #[test]
    fn raising_the_budget_resumes_without_overlap() {
        let mut c = cursor(0, 1000, NEWEST, 250);
        let (start, end) = c.next_window(NEWEST, 250).unwrap();
        assert_eq!((start, end), (750, 1000));
        c.advance(
            NEWEST,
            &RoundPartition {
                start,
                end,
                absorbed: 250,
                done: true,
            },
        );
        assert_eq!(c.next_window(NEWEST, 250), None, "бюджет выбран");

        c.budget = c.absorbed + 250;
        assert_eq!(c.next_window(NEWEST, 250), Some((500, 750)));
    }

    fn estimate(bytes_per_sec: f64, bytes_per_offset: f64) -> Option<QuotaEstimate> {
        Some(QuotaEstimate {
            bytes_per_sec,
            bytes_per_offset,
            peak_throttle: Duration::ZERO,
            brokers: 1,
            known_brokers: 1,
        })
    }

    /// Быстрый раунд, не мешающий росту.
    const QUICK: Option<Duration> = Some(Duration::from_millis(500));
    /// Пол окна, ничего не ограничивающий: тесты ниже проверяют саму формулу
    /// подбора, а пол от размера сообщений — отдельно, в тестах `window_floor`.
    const NO_FLOOR: i64 = MIN_ROUND_CHUNK;

    /// Ровно тот сценарий, который вылез на кластере: измеритель молчит
    /// (колбэк статистики висел на неопрошенной очереди), и единственная
    /// обратная связь — длительность раунда. Окно обязано перестать расти.
    #[test]
    fn a_slow_round_stops_the_window_from_growing_even_without_measurements() {
        // Наблюдавшаяся последовательность: 20 -> 40 -> 80, и раунд на 10.4 с.
        assert_eq!(next_window(20, None, 8, QUICK, NO_FLOOR), 40);
        assert_eq!(next_window(40, None, 8, QUICK, NO_FLOOR), 80);

        // Здесь прошлая версия выдавала 160 и разгонялась дальше до 640.
        let after_slow = next_window(80, None, 8, Some(Duration::from_secs_f64(10.4)), NO_FLOOR);
        assert!(
            after_slow < 20,
            "окно должно было рухнуть, а не расти: {after_slow}"
        );
    }

    /// Ужимание пропорционально переработке, а не фиксированным шагом: за
    /// пятикратный перебор нельзя расплачиваться пятью медленными раундами.
    #[test]
    fn overrun_shrinks_the_window_in_proportion() {
        let previous = 600;
        let w = next_window(
            previous,
            None,
            8,
            Some(Duration::from_secs_f64(TARGET_ROUND_SECS * 4.0)),
            NO_FLOOR,
        );
        assert_eq!(w, previous / 4);
    }

    /// Окно считается прямо из измеренной скорости, а не подбирается вслепую.
    #[test]
    fn window_is_computed_from_the_measured_quota() {
        // 200 КБ/с, 12 КБ на офсет, 4 партиции, цель — 2 секунды:
        // 400 КБ на раунд, это ~33 офсета, по 8 на партицию.
        let previous = 100;
        let w = next_window(previous, estimate(200_000.0, 12_000.0), 4, QUICK, NO_FLOOR);
        assert_eq!(
            w,
            (200_000.0 * TARGET_ROUND_SECS / 12_000.0 / 4.0).round() as i64
        );

        // Тот же кластер, но мелкие сообщения — окно кратно больше.
        let small = next_window(previous, estimate(200_000.0, 400.0), 4, QUICK, NO_FLOOR);
        assert!(small > w * 10, "{small} vs {w}");

        // Ровно наблюдавшийся случай: 8 партиций по 12 КБ на офсет при 200 КБ/с
        // схлопывают саму формулу в абсолютный минимум. В боевом коде до такого
        // окна дело не доходит — его перебивает `window_floor`, см.
        // `the_floor_stops_the_shrinking_death_spiral`.
        assert_eq!(
            next_window(previous, estimate(200_000.0, 12_000.0), 8, QUICK, NO_FLOOR),
            MIN_ROUND_CHUNK
        );
    }

    /// Чем больше партиций читается разом, тем меньше достаётся каждой:
    /// бюджет раунда общий на всех.
    #[test]
    fn window_splits_the_quota_across_partitions() {
        let est = estimate(2_000_000.0, 1_000.0);
        assert!(next_window(10_000, est, 16, QUICK, NO_FLOOR) < next_window(10_000, est, 4, QUICK, NO_FLOOR));
    }

    /// Именно это и сломалось на кластере с квотой: пока измеритель не набрал
    /// окно шире квотного, он видит burst-кредит и завышает оценку. Расти
    /// быстрее чем вдвое за раунд нельзя, иначе одна такая оценка превращается
    /// в раунд с многосекундной паузой.
    #[test]
    fn window_growth_is_capped_even_on_a_wildly_optimistic_estimate() {
        let burst = estimate(50_000_000.0, 100.0);
        assert_eq!(next_window(10, burst, 8, QUICK, NO_FLOOR), 20);
        assert_eq!(next_window(20, burst, 8, QUICK, NO_FLOOR), 40);
        // Без измерений — то же удвоение от достигнутого.
        assert_eq!(next_window(40, None, 8, QUICK, NO_FLOOR), 80);
    }

    /// А вниз — сразу: расплата за перебор квоты приходит одной длинной
    /// паузой, и растягивать ужимание на несколько раундов значит платить
    /// этой паузой несколько раз.
    #[test]
    fn window_shrinks_in_a_single_step() {
        let slow = estimate(200_000.0, 12_000.0);
        assert!(
            next_window(2_000, slow, 8, QUICK, NO_FLOOR) < 10,
            "должно было рухнуть сразу"
        );
    }

    #[test]
    fn window_stays_within_bounds() {
        // Кластер почти не отдаёт — но мельче минимума не дробим.
        assert_eq!(
            next_window(1_000, estimate(1.0, 1_000_000.0), 64, QUICK, NO_FLOOR),
            MIN_ROUND_CHUNK
        );
        // И никакая скорость не разгоняет окно выше потолка.
        let mut w = 10;
        for _ in 0..50 {
            w = next_window(w, estimate(f64::MAX, 1.0), 1, QUICK, NO_FLOOR);
        }
        assert_eq!(w, MAX_ROUND_CHUNK);
    }

    /// Главный вывод из боевого лога: окно мельче одного фетча не экономит
    /// квоту, а только уменьшает добычу — брокер всё равно шлёт свои 256 КБ на
    /// партицию. Пол окна обязан следовать за размером сообщений.
    #[test]
    fn window_floor_covers_one_fetch_worth_of_offsets() {
        // Замер с `fireg.securities`: ~8 КБ полезных на офсет.
        // 262144 / 8000 = 33 офсета — а прежний пол был 5.
        let floor = window_floor(Some(8_000.0));
        assert_eq!(floor, (FETCH_MESSAGE_MAX_BYTES / 8_000.0).round() as i64);
        assert!(
            floor > MIN_ROUND_CHUNK * 6,
            "пол должен быть кратно выше прежних пяти офсетов: {floor}"
        );
    }

    #[test]
    fn window_floor_falls_back_to_the_absolute_minimum_without_a_measurement() {
        assert_eq!(window_floor(None), MIN_ROUND_CHUNK);
        assert_eq!(window_floor(Some(0.0)), MIN_ROUND_CHUNK);
        // Гигантские сообщения: один фетч не покрывает даже офсета.
        assert_eq!(window_floor(Some(10_000_000.0)), MIN_ROUND_CHUNK);
    }

    #[test]
    fn window_floor_is_capped_on_tiny_messages() {
        // 40 байт на офсет — один фетч покрыл бы 6500 офсетов; столько тянуть
        // МИНИМАЛЬНЫМ окном незачем.
        assert_eq!(window_floor(Some(40.0)), MAX_WINDOW_FLOOR);
    }

    /// Регрессия на сам обвал: при измеренной цене офсета, какую выдал
    /// боевой кластер после нескольких мелких окон, формула просит 5 офсетов —
    /// и пол обязан это перебить.
    #[test]
    fn the_floor_stops_the_shrinking_death_spiral() {
        // Оценка кластера в момент обвала: ~1 МБ/с, 101 КБ за офсет (из
        // которых полезных — восемь).
        let collapsed = estimate(1_000_000.0, 101_746.0);
        let floor = window_floor(Some(8_000.0));

        let unbounded = next_window(10, collapsed, 8, QUICK, NO_FLOOR);
        assert_eq!(unbounded, MIN_ROUND_CHUNK, "формула сама по себе схлопывается");

        let bounded = next_window(10, collapsed, 8, QUICK, floor);
        assert_eq!(bounded, floor);
        assert!(bounded > unbounded * 6, "{bounded} vs {unbounded}");
    }

    /// Окно без единого сообщения (compacted-топик) должно расширяться, а не
    /// сужаться: за фетч мы платим в любом случае, и мельчить — значит платить
    /// за ту же пустоту много раз.
    #[test]
    fn barren_windows_widen_the_next_one() {
        let mut c = cursor(0, 100_000, NEWEST, 10_000);
        let (start, end) = c.next_window(NEWEST, 100).unwrap();
        assert_eq!(end - start, 100);

        c.advance(
            NEWEST,
            &RoundPartition {
                start,
                end,
                absorbed: 0,
                done: true,
            },
        );
        let (start, end) = c.next_window(NEWEST, 100).unwrap();
        assert_eq!(end - start, 200, "пустое окно должно было расшириться");

        // Пришли данные — разгон сбрасывается.
        c.advance(
            NEWEST,
            &RoundPartition {
                start,
                end,
                absorbed: 7,
                done: true,
            },
        );
        assert_eq!(c.barren_rounds, 0);
        let (start, end) = c.next_window(NEWEST, 100).unwrap();
        assert_eq!(end - start, 100);
    }

    #[test]
    fn frontier_tracks_the_deepest_point_reached_in_each_direction() {
        let mut c = cursor(0, 1000, NEWEST, 1000);
        for ts in [300, 100, 200] {
            c.observe(NEWEST, ts);
        }
        // Назад по времени: интересует самое старое из увиденного.
        assert_eq!(c.frontier_ts, Some(100));

        let mut c = cursor(0, 1000, OLDEST, 1000);
        for ts in [100, 300, 200] {
            c.observe(OLDEST, ts);
        }
        assert_eq!(c.frontier_ts, Some(300));
    }
}
