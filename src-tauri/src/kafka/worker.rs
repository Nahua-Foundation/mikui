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

use rdkafka::admin::{AdminClient, AdminOptions, ResourceSpecifier};
use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::{Header, Message, OwnedHeaders};
use rdkafka::producer::{BaseProducer, BaseRecord, DeliveryResult, ProducerContext};
use rdkafka::statistics::Statistics;
use rdkafka::topic_partition_list::TopicPartitionList;
use rdkafka::types::RDKafkaRespErr;
use rdkafka::Offset;
use tokio::sync::oneshot;

use super::filter::Needle;
use super::lens::{self, LensMode};
use super::quota::{QuotaEstimate, QuotaMeter};
use super::raw_consumer::{self, RawQueue, RawTopic};
use super::store::MessageStore;
use super::text::{self, PREVIEW_BYTES};
use super::types::*;
use crate::helpers::{base_config, consumer_config, producer_config, PRODUCE_TIMEOUT};
use crate::schema::Decoder;

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
/// Копия `fetch.message.max.bytes` из `helpers::consumer_config`. Держать её
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
/// Сколько ждать ответа на `DescribeConfigs`. Короче остальных: настройки
/// спрашивают, глядя на открывшееся окно, и десять секунд серого экрана там
/// неотличимы от зависшего приложения.
const DESCRIBE_TIMEOUT: Duration = Duration::from_secs(5);
/// Сколько всего времени отводится на снятие границ ВСЕХ партиций топика ради
/// счётчика сообщений. Запрос идёт отдельный на каждую, а воркер на это время
/// стоит — в том числе поперёк идущего чтения. Не уложились — показываем то,
/// что успели.
const OFFSETS_BUDGET: Duration = Duration::from_secs(2);
/// Потолок ожидания границ ОДНОЙ партиции.
const OFFSETS_TIMEOUT: Duration = Duration::from_secs(2);

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

/// Сколько ждать `cluster.id`. Короткий и без последствий: метаданные к этому
/// моменту уже пришли (`wait_until_ready`), так что ответ лежит в кэше
/// librdkafka, а неудача стоит ровно одного — ссылкой на сообщение из этого
/// сеанса не поделиться. Задерживать ради этого подключение нельзя.
const CLUSTER_ID_TIMEOUT: Duration = Duration::from_secs(2);

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
///
/// Список — про НАСТРОЙКИ подключения, и это не случайно: отметку читает
/// только `wait_until_ready`, то есть окно между «нажали Connect» и «кластер
/// ответил». Всё, что здесь перечислено, за это окно само не исправится.
///
/// `SSL` (это `_SSL`, -181) добавлен потому, что без него самая частая ошибка
/// первой настройки TLS — не тот CA, не то имя в сертификате, не тот
/// клиентский ключ — выглядела как «can't reach cluster: BrokerTransportFailure»
/// после пяти секунд ретраев. Точную причину librdkafka называет сразу и
/// словами, но она уходила в stderr, а пользователю доставался таймаут.
/// Ровно так же `UnsupportedSASLMechanism` и `IllegalSASLState` — ответ брокера
/// «этот механизм я не умею», который повторять бессмысленно.
fn is_fatal(error: &KafkaError) -> bool {
    matches!(
        error,
        KafkaError::Global(RDKafkaErrorCode::Authentication)
            | KafkaError::Global(RDKafkaErrorCode::SaslAuthenticationFailed)
            | KafkaError::Global(RDKafkaErrorCode::SSL)
            | KafkaError::Global(RDKafkaErrorCode::UnsupportedSASLMechanism)
            | KafkaError::Global(RDKafkaErrorCode::IllegalSASLState)
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

/// Означает ли конец лога, что в этой партиции читать больше нечего.
///
/// Не всегда, и это стоило внятного бага. Читатель бежит ВПЕРЕДИ окна: он
/// открыт один раз на всё чтение и тянет предвыборку сколько разрешает
/// `queued.min.messages`, а окно ограничивает только то, что мы у него берём.
/// На небольшом топике предвыборка добирается до конца лога ещё в первом же
/// окне — и `PARTITION_EOF` приезжает тогда, когда прочитана едва двадцатая
/// часть запрошенного диапазона.
///
/// Раньше EOF в режиме `oldest` объявлял партицию дочитанной безусловно.
/// Выглядело это так: топик открывается, показывает первое окно и говорит, что
/// это всё; сообщение по ссылке на офсет 50 «не найдено», хотя оно в топике
/// есть. Правильный признак — где именно кончился лог: если не дальше правой
/// границы окна, то читать и правда больше нечего.
///
/// В `newest` EOF не значит вообще ничего: там окна идут назад от конца, и
/// первое же из них упирается в конец лога по построению.
fn eof_exhausts(newest_first: bool, end_of_log: i64, window_end: i64) -> bool {
    !newest_first && end_of_log <= window_end
}

/// Почему запрошенных офсетов в топике нет.
///
/// Три ответа вместо одного, и разница между ними не косметическая. «Офсета
/// больше нет» — это retention или compaction, то есть данные были и уехали;
/// «офсета ещё нет» — это чужой или опечатанный номер. Особенно заметно на
/// ссылке из переписки, где пользователь номер не набирал и подсказать ему
/// «проверьте, что ввели» бессмысленно: он ничего не вводил.
fn describe_missing_offsets(range: &ReadRange, cursors: &HashMap<i32, PartitionCursor>) -> String {
    let what = match (range.from_offset, range.to_offset) {
        (Some(from), Some(to)) if from == to => format!("offset {from}"),
        (Some(from), Some(to)) => format!("offsets {from}..{to}"),
        (Some(from), None) => format!("offsets from {from}"),
        (None, Some(to)) => format!("offsets up to {to}"),
        (None, None) => "the selected offset range".to_string(),
    };
    let bounds = describe_bounds(cursors);

    // Верхней границей проверяем «уже съели»: если даже она старше начала
    // партиции, то и всё, что ниже, тем более.
    let top = range.to_offset.or(range.from_offset);
    if top.is_some_and(|top| cursors.values().all(|c| top < c.low)) {
        return format!(
            "{what} is no longer in the topic — retention or compaction dropped it ({bounds})"
        );
    }
    // И симметрично нижней: она за концом партиции — значит таких записей ещё
    // не существует.
    let bottom = range.from_offset.or(range.to_offset);
    if bottom.is_some_and(|bottom| cursors.values().all(|c| bottom >= c.high)) {
        return format!("{what} is not in the topic yet ({bounds})");
    }
    format!("{what} is outside the topic ({bounds})")
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

/// Ответ на глубокий поиск, остановленный пользователем. Отдельно от
/// `READ_SUPERSEDED`: там чтение стало не нужно, а здесь оно было ОТМЕНЕНО, и
/// фронт на это обязан отреагировать — перечитать топик обычным способом.
pub const SEARCH_STOPPED: &str = "search stopped";

/// Насколько глубоко читать и что оставлять в арене.
///
/// Два способа читать один топик, и различаются они не размером, а тем, что
/// считается результатом.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadDepth {
    /// Обычное открытие топика (и «Load more»): по `limit` сообщений на
    /// партицию, в арену ложится ВСЁ вычитанное, чтение упирается в дедлайн и
    /// байтовый бюджет. Фильтр применяется потом, к тому, что уже лежит.
    Window,
    /// Глубокий поиск: топик до конца, в арену ложатся ТОЛЬКО совпадения с
    /// фильтром, дедлайна и байтового бюджета нет.
    ///
    /// Отбрасывание непопавшего — не оптимизация, а условие, без которого
    /// операция невозможна. Арена — 256 МБ (`store::DEFAULT_MAX_BYTES`), и на
    /// топике с телами по 20–70 КБ это 4–13 тысяч сообщений: чтение упёрлось бы
    /// в буфер, не дойдя и до десятой части такого топика. Плата за это —
    /// буфер, в котором больше нет «всего прочитанного», поэтому снятие
    /// фильтра после поиска требует перечитать топик заново.
    WholeTopic,
}

type Reply<T> = oneshot::Sender<T>;

pub enum Command {
    Connect(ClusterConnectPayload, Reply<Result<(), String>>),
    Test(ClusterConnectPayload, Reply<Result<(), String>>),
    Disconnect(Reply<()>),
    /// Чем кластер представился при подключении (`cluster.id` из метаданных).
    /// `None` — не подключены либо кластер идентификатора не назвал.
    ///
    /// Отдельной командой, а не полем в ответе `Connect`: спрашивают его в
    /// момент, когда делятся ссылкой, то есть спустя произвольное время после
    /// подключения, и хранить его на фронте значило бы завести второй ответ на
    /// вопрос «к чему мы подключены» — расходящийся с первым при каждой смене
    /// учётки.
    ClusterId(Reply<Option<String>>),
    ListTopics(Reply<Result<Vec<TopicInfo>, String>>),
    /// Устройство и настройки одного топика. Спрашивается по клику в списке и
    /// к чтению отношения не имеет — открытый топик от неё не меняется.
    DescribeTopic(String, Reply<Result<TopicDetails, String>>),
    OpenTopic(OpenTopicParams, Reply<Result<OpenTopicResult, String>>),
    /// Самоадресованная команда: следующий шаг уже идущего чтения. Никогда не
    /// шлётся снаружи воркера.
    ContinueRead,
    LoadMore(LoadMoreParams, Reply<Result<OpenTopicResult, String>>),
    /// Прочитать топик до конца, оставив в арене только совпадения с фильтром.
    ///
    /// Параметры те же, что у `OpenTopic`, и это не совпадение: поиск ЗАНОВО
    /// открывает топик с того же конца и теми же границами. Продолжить с места,
    /// где встало обычное чтение, нельзя — в арене к тому моменту лежит
    /// непросеянное, и места под находки в ней уже может не быть.
    DeepSearch(OpenTopicParams, Reply<Result<OpenTopicResult, String>>),
    /// Остановить глубокий поиск. Арена и курсоры сбрасываются: буфер после
    /// поиска содержит только находки, и оставлять его как есть значило бы
    /// показать пользователю таблицу, из которой пропало всё непопавшее.
    /// Перечитать топик обычным способом — дело фронта, он это и так умеет.
    ///
    /// Отвечает тем, было ли что останавливать. `false` — поиск успел
    /// закончиться сам, пока летела команда; перечитывать топик в этом случае
    /// не надо, иначе клик по «стоп» стирал бы только что найденное.
    StopSearch(Reply<bool>),
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
    /// Где в таблице стоит сообщение с такими координатами. `None` — в буфере
    /// его нет: не вычитали, либо отфильтровали.
    ///
    /// Нужна ссылке на сообщение: она называет партицию и офсет, а тело
    /// отдаётся по индексу строки — как и всё остальное, что показывает
    /// таблица. Считать этот индекс на фронте нечем, там нет ни буфера, ни
    /// текущего фильтра.
    FindMessage {
        partition: i32,
        offset: i64,
        reply: Reply<Result<Option<usize>, String>>,
    },
    /// Сообщение сырыми байтами — для архива сохранённых. См. `Worker::raw`.
    GetRaw(usize, Reply<Result<RawBody, String>>),
    /// Чем декодировать тела открытого топика и чем — его ключи. `None` —
    /// отдавать как есть.
    ///
    /// Два декодера, а не один: ключ сериализуется отдельно от тела, и схема у
    /// него своя (см. `schema::store::key_decoder`). Приезжают вместе, потому
    /// что относятся к одному топику и разъехаться не должны.
    ///
    /// Отдельная команда, а не поле в `OpenTopic`: выбор message в настройках
    /// топика обязан примениться сразу, а перечитывать ради этого весь топик из
    /// Kafka — значит платить квотой на чтение за смену способа показа.
    SetDecoder {
        value: Option<Arc<Decoder>>,
        key: Option<Arc<Decoder>>,
        /// Что делать с конвертом Debezium/Connect. Приезжает вместе с
        /// декодерами по той же причине, что и они друг с другом: линза
        /// работает ПОВЕРХ разобранного тела, и разъехаться с тем, чем его
        /// разбирают, не должна.
        lens: LensMode,
        reply: Reply<()>,
    },
    /// Положить сообщение в топик. Тело приезжает уже байтами: кодированием по
    /// схеме заведует `lib.rs`, воркер про схемы не знает.
    Produce(ProduceRecord, Reply<Result<ProduceResult, String>>),
    CloseTopic(Reply<()>),
}

/// Воркер упал и поднят заново.
///
/// Событием, а не только ответом на команду: ответ достаётся ровно тому
/// вызову, который заметил смерть, а подключение потеряно у ВСЕГО приложения.
/// Без этого шапка продолжала бы показывать кластер подключённым, а список
/// топиков — топики, которых на новом воркере нет.
pub const WORKER_RESTARTED: &str = "mikui://worker-restarted";

/// Сколько ждать завершения потока, прежде чем считать его живым.
/// Зачем это нужно — в `explain_missing_reply`.
const DEATH_GRACE: Duration = Duration::from_millis(50);

/// Что отвечаем, когда воркер упал и поднят заново.
///
/// Состояние с ним ушло целиком — подключение, открытый топик, арена, — и
/// делать вид, что команда просто не удалась, нельзя: фронт повторил бы её на
/// чистом воркере и получил бы пустую таблицу вместо объяснения. Поэтому текст
/// говорит и что случилось, и что делать.
fn worker_restarted() -> String {
    // Именно свежий файл, а не папка: под рукой у человека оказывается ровно
    // то, что нужно прислать, без выбора между двумя десятками падений.
    match crate::diag::latest_crash() {
        Some(path) => format!(
            "kafka worker crashed and was restarted — reconnect to the cluster to continue. \
             The stack trace is in {}; send it over and it will be fixed",
            path.display()
        ),
        None => "kafka worker crashed and was restarted — reconnect to the cluster to continue"
            .to_string(),
    }
}

/// Живой воркер: канал к нему, его поток и номер поколения.
struct Live {
    /// Растёт на каждый перезапуск. По нему `restart` понимает, не поднял ли
    /// воркер кто-то другой, пока мы обнаруживали смерть этого.
    generation: u64,
    tx: Sender<Command>,
    /// Только ради `is_finished`: по нему видно, воркер упал или ответ уронили
    /// намеренно. Присоединяться к потоку мы не собираемся.
    thread: std::thread::JoinHandle<()>,
}

fn spawn_worker() -> Live {
    let (tx, rx) = mpsc::channel();
    let worker_tx = tx.clone();
    let thread = std::thread::Builder::new()
        .name("kafka-worker".into())
        .spawn(move || Worker::new(worker_tx).run(rx))
        .expect("failed to spawn kafka worker thread");
    Live {
        generation: 0,
        tx,
        thread,
    }
}

/// Ручка воркера. Кладётся в Tauri state.
///
/// Мьютекс — не из-за `Sender` (он `Sync` начиная с Rust 1.72), а потому что
/// воркер приходится ЗАМЕНЯТЬ: паника в его потоке оставляла приложение без
/// воркера навсегда, и помогал только перезапуск.
pub struct WorkerHandle {
    live: Mutex<Live>,
    /// Через что оповестить фронт о перезапуске.
    ///
    /// `OnceLock`, потому что ручка кладётся в state ДО того, как у приложения
    /// появляется `AppHandle`: `manage` вызывается на билдере, `setup` — уже
    /// после. Пока он не проставлен, перезапуск просто пройдёт молча — это
    /// возможно только до первого кадра, когда и сообщать ещё некому.
    app: std::sync::OnceLock<tauri::AppHandle>,
}

impl WorkerHandle {
    pub fn spawn() -> Self {
        Self {
            live: Mutex::new(spawn_worker()),
            app: std::sync::OnceLock::new(),
        }
    }

    /// Даёт ручке связь с окном. Зовётся из `setup`, один раз.
    pub fn attach(&self, app: tauri::AppHandle) {
        let _ = self.app.set(app);
    }

    /// Отравленный мьютекс здесь ничего не значит: под ним лежат счётчик,
    /// канал и хендл потока, и паника, случившаяся у кого-то под локом, их не
    /// портит. А отказ работать из-за чужой паники — ровно та беда, от которой
    /// этот тип и заведён.
    fn live(&self) -> std::sync::MutexGuard<'_, Live> {
        self.live.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Отправляет команду и ждёт ответ, не занимая поток исполнителя.
    pub async fn call<T, F>(&self, make: F) -> Result<T, String>
    where
        F: FnOnce(Reply<T>) -> Command,
    {
        let (reply_tx, reply_rx) = oneshot::channel();
        let (generation, tx) = {
            let live = self.live();
            (live.generation, live.tx.clone())
        };

        // Канал закрыт — поток воркера уже завершился.
        if tx.send(make(reply_tx)).is_err() {
            return Err(self.restart(generation));
        }

        match reply_rx.await {
            Ok(value) => Ok(value),
            Err(_) => Err(self.explain_missing_reply(generation).await),
        }
    }

    /// Ответ не пришёл — надо понять, паника это или намеренная отмена.
    ///
    /// Различать обязательно. Живой воркер роняет `reply` сам, когда чтение
    /// стало ненужным, — так отменяется предыдущее чтение при смене топика
    /// (см. `READ_SUPERSEDED`). Считать это падением значило бы убивать
    /// здорового воркера на каждом переключении.
    ///
    /// Отсрочка — из-за порядка событий при панике. `reply` уносится на ПЕРВЫХ
    /// кадрах раскрутки, а `is_finished` становится истиной только после
    /// последнего; между этими моментами микросекунды, но ждущая задача успевает
    /// проснуться внутри них. Без ожидания падение иногда читалось бы как
    /// намеренная отмена, и воркер поднимался бы только со следующей командой —
    /// то есть ровно в случае «нажал кнопку и смотрю» выглядело бы поломкой.
    ///
    /// Ждём уступая процессор, а не занимая его: поток тут не при чём, ждём мы
    /// ЧУЖОЙ поток, и ему как раз надо дать досчитать. Полную отсрочку
    /// выбирает только намеренная отмена, которой в сегодняшнем коде нет ни
    /// одной — все ветки отвечают явно.
    async fn explain_missing_reply(&self, generation: u64) -> String {
        let deadline = Instant::now() + DEATH_GRACE;
        loop {
            {
                let live = self.live();
                // Сменившееся поколение — это «его уже подняли без нас»: наш
                // ответ унесло вместе с прежним потоком.
                if live.generation != generation || live.thread.is_finished() {
                    break;
                }
            }
            if Instant::now() >= deadline {
                return "the worker dropped the reply".to_string();
            }
            tokio::task::yield_now().await;
        }
        // Лок здесь уже отпущен: `restart` берёт его сам.
        self.restart(generation)
    }

    /// Поднимает воркер заново — но только если умерший всё ещё числится
    /// текущим.
    ///
    /// Сверка поколений не педантизм: команды идут параллельно, и на одну
    /// панику смерть обнаружат сразу несколько. Без неё каждая поднимала бы
    /// своего воркера, а лишние остались бы висеть навсегда — поток воркера
    /// держит копию собственного `Sender`, поэтому сам по себе из `recv` он не
    /// выйдет никогда.
    fn restart(&self, dead: u64) -> String {
        let replaced = {
            let mut live = self.live();
            let mine = live.generation == dead;
            if mine {
                eprintln!("[worker] kafka worker died, starting a new one");
                let next = live.generation.wrapping_add(1);
                *live = spawn_worker();
                live.generation = next;
            }
            mine
        };

        // Оповещаем один раз на перезапуск, а не на каждого заметившего: лок к
        // этому моменту отпущен, и обработчик на фронте сбрасывает состояние
        // подключения, не гоняя его туда-обратно на каждую упавшую команду.
        if replaced {
            if let Some(app) = self.app.get() {
                use tauri::Emitter;
                let _ = app.emit(WORKER_RESTARTED, ());
            }
        }
        worker_restarted()
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
        // `read_position` здесь намеренно НЕ трогается: где стоит читатель —
        // это наблюдение, а не следствие того, что окно закрыто. Ставить его
        // на правую границу окна было ошибкой: чтобы окно закрылось, из
        // очереди приходится ЗАБРАТЬ сообщение за этой границей, а вместе с
        // ним библиотека отдаёт и всю предвыборку за ним. Читатель после
        // закрытого окна стоит дальше его края — иногда на весь топик дальше.
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

    /// Где стоит читатель: следующее сообщение он отдаст с этого офсета.
    ///
    /// Пишется на КАЖДОЕ изъятое из очереди событие, в том числе на выброшенное
    /// за краем окна и на сам конец лога: изъятое библиотека второй раз не
    /// отдаст, и притворяться, что читатель остался на месте, нельзя — иначе
    /// следующее окно решит, что оно уже на своей позиции, не сделает `seek` и
    /// продолжит с того места, куда убежала предвыборка. В `oldest` это дырка
    /// в таблице ровно на всё, что успело приехать сверх окна.
    fn note_read_position(&mut self, offset: i64) {
        self.read_position = Some(offset);
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
    target.min(ceiling).clamp(
        floor.clamp(MIN_ROUND_CHUNK, MAX_WINDOW_FLOOR),
        MAX_ROUND_CHUNK,
    )
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
fn selected_partitions(
    selected: Option<&[i32]>,
    partition_count: usize,
) -> Result<Vec<i32>, String> {
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

/// Чем глубокий поиск просеивает сообщения на входе.
///
/// Готовится ОДИН РАЗ на всё чтение, а не на каждом шаге, и не берётся из
/// `Worker::filter` на лету — по двум причинам. Первая: `PreparedFilter::new`
/// аллоцирует иглы, и на топике в сто тысяч сообщений это было бы двести
/// аллокаций на ровном месте. Вторая важнее — `SetFilter` может прилететь
/// посреди поиска (поле в шапке на это время заперто, но отложенная отправка
/// могла уже быть в пути), и тогда половина буфера оказалась бы просеяна одним
/// запросом, половина другим. Запрос, с которым поиск начался, — тот же, с
/// которым он закончится.
struct Sieve {
    filter: PreparedFilter,
    /// Своя копия ссылки на декодер: `search_decoded` разбирает тело, а
    /// одалживать `Worker::decoder` посреди цикла, где занят `store`, неудобно.
    decoder: Option<Arc<Decoder>>,
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
    /// Обычное чтение или глубокий поиск. Решает две вещи: останавливаться ли
    /// по дедлайну и по байтовому бюджету.
    depth: ReadDepth,
    /// Чем просеивать на входе. `None` — не просеивать, в арену идёт всё.
    sieve: Option<Sieve>,
    /// Сколько байт ПРОСМОТРЕНО за текущий раунд — вся добыча окна, включая
    /// выброшенную фильтром. Не то же, что прирост арены: при глубоком поиске
    /// в арену попадают единицы, а цена офсета считается по всему просмотренному
    /// (см. `MessageStore::footprint` и `window_floor`).
    round_scanned_bytes: usize,
    /// Сколько сообщений просмотрено за текущий раунд. Оборванный раунд
    /// отматывается целиком — вместе с этим счётчиком, иначе «просмотрено» в
    /// шкале учло бы окно, которое будет вычитано заново.
    round_scanned: u64,
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
    /// Чем кластер представился (`cluster.id`). Снимается один раз при
    /// подключении: значение постоянное, а спрашивают его на каждое «поделиться
    /// ссылкой» — то есть посреди работы, когда ходить за ним в сеть незачем.
    cluster_id: Option<String>,
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
    /// Сколько записей стора уже просеяно в `view`. Опубликованный префикс
    /// заморожен и только дописывается, поэтому всё до этой границы
    /// пересматривать не надо — см. `extend_view`.
    view_scanned: usize,
    /// Сколько сообщений ПРОСМОТРЕНО с момента открытия топика.
    ///
    /// Отдельно от `store.committed_len()`, и при глубоком поиске эти два
    /// расходятся на порядки: просмотрено пятьдесят тысяч, в буфере лежат три
    /// находки. Обычному чтению они равны, но и там это разные величины —
    /// одна про топик, другая про буфер.
    scanned: u64,
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
    decoder: Option<Arc<Decoder>>,
    /// Чем декодировать ключи. Приезжает той же командой и живёт по тем же
    /// правилам; отдельно от `decoder`, потому что схема у ключа своя.
    key_decoder: Option<Arc<Decoder>>,
    /// Что делать с конвертом Debezium/Connect в строке таблицы. Приезжает той
    /// же командой и живёт по тем же правилам.
    lens: LensMode,
}

/// Разбирает воркер в порядке, обратном зависимости: сначала чтение, потом
/// клиент.
///
/// Поля дропаются в порядке объявления, а объявлен `consumer` вторым —
/// задолго до `pending_read`. Для сырых указателей внутри чтения этот порядок
/// ровно обратный нужному: `RawTopic` и `RawQueue` выданы `rd_kafka_t*` этого
/// самого консьюмера, и после `rd_kafka_destroy` их собственные
/// `rd_kafka_topic_destroy`/`rd_kafka_queue_destroy` уходят в освобождённую
/// память. Процесс на этом умирает целиком — не паникой, которую можно
/// поймать и пережить, а сигналом.
///
/// В `connect` и `disconnect` тот же порядок выставлен руками (см. комментарий
/// там), но есть путь, где руками его не выставить: РАСКРУТКА СТЕКА. Паника
/// воркера обязана оставаться в его потоке — на этом держится всё
/// пересоздание, — а до этой правки паника посреди чтения уносила приложение:
/// журнал падения успевал записаться (хук зовётся до раскрутки), и сразу за
/// ним процесс получал сигнал уже в чужом коде. Без открытого топика
/// (`pending_read: None`) висеть нечему, поэтому на пустом воркере паника
/// вела себя правильно — из-за этого и выглядело, будто дело в самой панике.
impl Drop for Worker {
    fn drop(&mut self) {
        // `consume_stop` здесь, в отличие от `close_topic`, нарочно не
        // зовётся: он ждёт подтверждения от брокерского потока и под квотой
        // стоит секундами, а сюда мы приходим в том числе посреди раскрутки
        // паники. Незакрытые партиции снимет `rd_kafka_destroy` — он на то и
        // есть, чтобы снести клиент со всем, что на нём открыто.
        drop(self.pending_read.take());
    }
}

impl Worker {
    fn new(tx: Sender<Command>) -> Self {
        Self {
            tx,
            consumer: None,
            admin: None,
            connection: None,
            cluster_id: None,
            producer: None,
            delivery: Arc::new(Mutex::new(None)),
            partition_counts: HashMap::new(),
            store: MessageStore::default(),
            view: Vec::new(),
            view_scanned: 0,
            scanned: 0,
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
            key_decoder: None,
            lens: LensMode::default(),
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
                Command::ClusterId(reply) => {
                    let _ = reply.send(self.cluster_id.clone());
                }
                Command::ListTopics(reply) => {
                    let _ = reply.send(self.list_topics());
                }
                Command::DescribeTopic(topic, reply) => {
                    let _ = reply.send(self.describe_topic(&topic));
                }
                Command::OpenTopic(params, reply) => {
                    self.start_open_topic(params, ReadDepth::Window, reply)
                }
                Command::ContinueRead => self.continue_read(),
                Command::LoadMore(params, reply) => self.start_load_more(params, reply),
                Command::DeepSearch(params, reply) => {
                    self.start_open_topic(params, ReadDepth::WholeTopic, reply)
                }
                Command::StopSearch(reply) => {
                    let stopped = self.stop_search();
                    let _ = reply.send(stopped);
                }
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
                Command::FindMessage {
                    partition,
                    offset,
                    reply,
                } => {
                    let _ = reply.send(self.find_message(partition, offset));
                }
                Command::GetRaw(index, reply) => {
                    let _ = reply.send(self.raw(index));
                }
                Command::SetDecoder {
                    value,
                    key,
                    lens,
                    reply,
                } => {
                    self.decoder = value;
                    self.key_decoder = key;
                    self.lens = lens;
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
        // Разбираем payload ровно один раз, а консьюмера и продьюсера
        // достраиваем поверх результата: второй разбор был бы вторым местом,
        // где настройки безопасности могут разъехаться.
        let base = base_config(&payload)?;
        let mut conf = consumer_config(&base);
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

        // Чем кластер себя называет. Спрашиваем здесь, а не по требованию:
        // метаданные только что пришли, ответ лежит в кэше librdkafka и не
        // стоит ни round-trip'а, ни ожидания. `None` — кластер идентификатора
        // не назвал (так ведут себя эмуляции Kafka-протокола); тогда ссылкой
        // из этого сеанса просто не поделиться, всё остальное работает.
        let cluster_id = consumer.client().fetch_cluster_id(CLUSTER_ID_TIMEOUT);

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
        self.connection = Some(base);
        self.cluster_id = cluster_id;
        self.partition_counts.clear();
        Ok(())
    }

    fn test(payload: ClusterConnectPayload) -> Result<(), String> {
        let conf = consumer_config(&base_config(&payload)?);
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
        self.cluster_id = None;
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

    /// Устройство и настройки топика.
    ///
    /// Два источника, и они не равнозначны. Метаданные говорят, из чего топик
    /// состоит (партиции, реплики, ISR) — в `DescribeConfigs` этого нет вовсе.
    /// Настройки — отдельный запрос под отдельным правом, которого у учётки
    /// может и не быть; его неудача не отменяет метаданных, а становится
    /// строчкой `config_error` рядом с ними.
    fn describe_topic(&self, topic: &str) -> Result<TopicDetails, String> {
        let consumer = self.consumer.as_ref().ok_or("not connected to a cluster")?;
        // Метаданные одного топика, а не всего кластера: на кластере с тысячами
        // топиков полный ответ — мегабайты ради одной строки.
        let md = consumer
            .fetch_metadata(Some(topic), METADATA_TIMEOUT)
            .map_err(|e| format!("can't load metadata of {topic}: {e}"))?;
        let meta = md
            .topics()
            .iter()
            .find(|t| t.name() == topic)
            .ok_or_else(|| format!("topic {topic} is not in the cluster metadata"))?;
        if let Some(err) = meta.error() {
            return Err(format!(
                "can't describe {topic}: {}",
                raw_consumer::err_str(err)
            ));
        }

        let bounds = self.partition_bounds(topic, meta.partitions());

        let mut partitions: Vec<PartitionDetails> = meta
            .partitions()
            .iter()
            .map(|p| {
                let (low, high) = bounds.get(&p.id()).copied().unzip();
                PartitionDetails {
                    id: p.id(),
                    leader: p.leader(),
                    replicas: p.replicas().len(),
                    in_sync: p.isr().len(),
                    low,
                    high,
                }
            })
            .collect();
        // Метаданные приезжают в порядке брокера, а читают эту таблицу по
        // номеру партиции.
        partitions.sort_by_key(|p| p.id);

        let known: Vec<i64> = partitions
            .iter()
            .filter_map(|p| Some(p.high? - p.low?))
            .collect();

        let (config, config_error) = match self.topic_config(topic) {
            Ok(config) => (config, None),
            Err(e) => {
                eprintln!("[describe_topic] {topic} settings unavailable: {e}");
                (Vec::new(), Some(e))
            }
        };

        // Выборка для оценки размера на диске — только у открытого топика и
        // только из того, что уже перекачано ради показа. Ни одного лишнего
        // байта из сети: за эти сообщения квота списана один раз, и сравнить
        // сжатый объём с разжатым второй раз ничего не стоит.
        let sampled = (self.open_topic.as_deref() == Some(topic))
            .then(|| self.quota.transfer())
            .flatten();

        Ok(TopicDetails {
            name: topic.to_string(),
            // Максимум, а не значение первой партиции: у топика, которому
            // добавляли партиции при другой настройке кластера, они могут
            // отличаться, и меньшее из чисел соврало бы про топик целиком.
            replication_factor: partitions.iter().map(|p| p.replicas).max(),
            under_replicated: partitions.iter().filter(|p| p.in_sync < p.replicas).count(),
            messages: (!known.is_empty()).then(|| known.iter().sum()),
            offsets_partial: known.len() < partitions.len(),
            sample_wire_bytes: sampled.map(|s| s.wire_bytes),
            sample_messages: sampled.map(|s| s.messages),
            sample_bytes: sampled.map(|s| s.message_bytes),
            partitions,
            config,
            config_error,
        })
    }

    /// Границы `[low, high)` каждой партиции.
    ///
    /// `ListOffsets` записей не переносит, поэтому квоту на чтение это не
    /// тратит — но запрос идёт отдельный на каждую партицию, а воркер на это
    /// время стоит. Отсюда общий бюджет: на топике в несколько сотен партиций
    /// не уложились — показываем то, что успели, и говорим об этом
    /// (`offsets_partial`). Заставлять человека ждать полминуты ради счётчика
    /// сообщений, когда он открыл окно посмотреть retention, незачем.
    ///
    /// Партиция, по которой запрос не удался, просто выпадает из карты: одна
    /// недоступная не повод не показывать остальные девятнадцать.
    fn partition_bounds(
        &self,
        topic: &str,
        partitions: &[rdkafka::metadata::MetadataPartition],
    ) -> HashMap<i32, (i64, i64)> {
        let Some(consumer) = self.consumer.as_ref() else {
            return HashMap::new();
        };
        let started = Instant::now();
        let mut bounds = HashMap::with_capacity(partitions.len());

        for p in partitions {
            if started.elapsed() >= OFFSETS_BUDGET {
                eprintln!(
                    "[describe_topic] {topic}: offsets budget spent after {}/{} partitions",
                    bounds.len(),
                    partitions.len()
                );
                break;
            }
            match consumer.fetch_watermarks(topic, p.id(), OFFSETS_TIMEOUT) {
                Ok((low, high)) => {
                    bounds.insert(p.id(), (low, high));
                }
                Err(e) => eprintln!(
                    "[describe_topic] {topic} p{}: can't read offsets: {e}",
                    p.id()
                ),
            }
        }
        bounds
    }

    /// Настройки топика через `DescribeConfigs`.
    ///
    /// Ответ ждём прямо здесь, блокируя воркер, — как это уже делают
    /// `fetch_metadata` и `fetch_watermarks`. Дробить на кооперативные шаги
    /// нечего: запрос ровно один, ждать его дольше `DESCRIBE_TIMEOUT` мы всё
    /// равно не будем, а делается он по клику, когда чтения обычно нет.
    ///
    /// Исполнитель нужен только затем, что rdkafka отдаёт результат будущим.
    /// Ни таймеров, ни ввода-вывода этому будущему не нужно: его будит
    /// собственный поток админского клиента, поэтому хватает самого простого
    /// однопоточного рантайма.
    fn topic_config(&self, topic: &str) -> Result<Vec<TopicConfigEntry>, String> {
        let admin = self.admin.as_ref().ok_or("not connected to a cluster")?;
        let options = AdminOptions::new().request_timeout(Some(DESCRIBE_TIMEOUT));
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .map_err(|e| format!("can't wait for the cluster reply: {e}"))?;

        let described = runtime
            .block_on(admin.describe_configs(&[ResourceSpecifier::Topic(topic)], &options))
            .map_err(|e| format!("can't read the settings of {topic}: {e}"))?;

        let resource = described
            .into_iter()
            .next()
            .ok_or_else(|| format!("the cluster returned no settings for {topic}"))?
            .map_err(|e| format!("can't read the settings of {topic}: {e}"))?;

        let mut entries: Vec<TopicConfigEntry> = resource
            .entries
            .into_iter()
            .map(|entry| TopicConfigEntry {
                name: entry.name,
                value: entry.value,
                is_default: entry.is_default,
                is_read_only: entry.is_read_only,
                is_sensitive: entry.is_sensitive,
            })
            .collect();
        // Брокер отдаёт их в своём порядке, а ищут в этом списке по имени.
        entries.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(entries)
    }

    // --- Запуск чтения ------------------------------------------------------

    /// Открывает топик: сбрасывает буфер, снимает границы партиций и запускает
    /// первый раунд. Долгая часть уходит в `continue_read`, поэтому сама
    /// команда возвращается мгновенно, а `reply` уезжает уже с результатом.
    fn start_open_topic(
        &mut self,
        params: OpenTopicParams,
        depth: ReadDepth,
        reply: Reply<Result<OpenTopicResult, String>>,
    ) {
        // Поиск без запроса — это чтение всего топика в буфер без дедлайна и
        // без байтового бюджета, то есть ровно то, от чего эти два ограничения
        // и защищают. Сито в таком поиске пропускает всё, и он упрётся в
        // память — просто не через полминуты, а молча и надолго. UI такого не
        // предлагает; отказ здесь — на случай, если однажды предложит.
        //
        // Проверка стоит ДО `cancel_pending_read`: отклонённый запрос не должен
        // рушить идущее чтение.
        if depth == ReadDepth::WholeTopic
            && params.filter.key.is_empty()
            && params.filter.value.is_empty()
        {
            let _ = reply.send(Err(
                "a deep search needs a filter: without one there is nothing to sift, \
                 and the whole topic would go into the buffer"
                    .into(),
            ));
            return;
        }

        self.cancel_pending_read();

        // Глубокий поиск бюджетом не ограничен вовсе: он идёт до конца топика,
        // и остановить его может только находка буфера, конец партиций или
        // пользователь. `limit` из параметров при этом игнорируется — фронт
        // присылает те же параметры, что и при обычном открытии.
        let limit = match depth {
            ReadDepth::Window => params.limit.clamp(1, 100_000) as i64,
            ReadDepth::WholeTopic => i64::MAX,
        };
        let partition_count = match self.partition_counts.get(&params.topic).copied() {
            Some(c) => c,
            None => {
                // Обе причины названы намеренно, и различить их нельзя не по
                // лени: Kafka НЕ показывает в метаданных топики, на которые у
                // учётки нет права `Describe`, — неавторизованный топик
                // выглядит ровно как несуществующий. Написать одно «unknown
                // topic» значило бы отправить человека искать опечатку в имени,
                // когда на самом деле ему нужна другая учётка.
                let _ = reply.send(Err(format!(
                    "topic '{}' is not on this cluster, or the current Kafka user has no access to it",
                    params.topic
                )));
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
            "[open_topic] {} partitions={}/{partition_count} start_from={:?} range={:?} \
             per_partition_limit={limit} depth={depth:?}",
            params.topic,
            partitions.len(),
            params.start_from,
            params.range,
        );

        self.store.clear();
        self.scanned = 0;
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
        // Соотношение «сжатое к разжатому» — свойство ТОПИКА, в отличие от
        // скорости кластера: в соседних топиках и сообщения другие, и кодек
        // может быть другим. Наблюдение за передачей начинается заново.
        self.quota.mark();
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
            depth,
            // Строится ПОСЛЕ `self.filter = params.filter` выше — из того же
            // запроса, с которым топик открывается, и тем же `prepared_filter`,
            // которым потом просеивается буфер под таблицу.
            sieve: match depth {
                ReadDepth::Window => None,
                ReadDepth::WholeTopic => Some(Sieve {
                    filter: self.prepared_filter(),
                    decoder: self.decoder.clone(),
                }),
            },
            round_scanned_bytes: 0,
            round_scanned: 0,
        });

        let _ = self.tx.send(Command::ContinueRead);
    }

    /// Останавливает глубокий поиск по требованию пользователя.
    ///
    /// Сбрасывает буфер, а не оставляет находки: после поиска в арене лежат
    /// ТОЛЬКО они, и оставить её как есть значило бы показать таблицу, из
    /// которой молча исчезло всё непопавшее под фильтр. Топик после этого
    /// перечитывает фронт — обычным `open_topic`, как будто его только что
    /// открыли.
    ///
    /// Обычное чтение не трогает: остановка предлагается только на поиске, а
    /// прилететь эта команда может и позже, когда чтение уже другое. Отвечает
    /// тем, было ли что останавливать.
    fn stop_search(&mut self) -> bool {
        let is_search = self
            .pending_read
            .as_ref()
            .is_some_and(|p| p.depth == ReadDepth::WholeTopic);
        if !is_search {
            // Поиск успел закончиться сам — трогать нечего, и в особенности
            // нечего сбрасывать: в буфере лежит его законный результат.
            return false;
        }

        if let Some(pending) = self.pending_read.take() {
            Self::stop_all(&pending);
            eprintln!(
                "[stop_search] {} stopped after {:?}, {} scanned, {} kept",
                pending.topic_name,
                pending.started.elapsed(),
                self.scanned,
                self.store.len()
            );
            // Именно ошибкой, а не результатом: результат означал бы «поиск
            // закончился, вот что нашлось», и фронт показал бы находки в
            // таблице, из которой пропало непопавшее.
            let _ = pending.reply.send(Err(SEARCH_STOPPED.to_string()));
        }

        // Топик остаётся открытым (`open_topic` не снимаем): фронт сейчас
        // перечитает его, и снимать имя ради двух команд значило бы на это
        // время оставить воркер без ответа на вопрос «что открыто».
        self.store.release();
        self.cursors.clear();
        self.view.clear();
        self.view_scanned = 0;
        self.scanned = 0;
        self.window = FIRST_ROUND_CHUNK;
        self.last_round = None;
        self.kept_per_offset = None;
        self.has_timestamps = false;
        true
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
            // «Load more» продолжает обычное чтение: в арену по-прежнему
            // ложится всё. Дочитывать под фильтр — это глубокий поиск, и он
            // начинается с начала топика, а не с этого места.
            depth: ReadDepth::Window,
            sieve: None,
            round_scanned_bytes: 0,
            round_scanned: 0,
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
    /// `helpers::consumer_config`), а без него `BaseConsumer::new` не
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
            self.rollback_round(&pending);
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
                describe_missing_offsets(&range, &self.cursors)
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
            .filter_map(|e| {
                e.offset()
                    .to_raw()
                    .filter(|o| *o >= 0)
                    .map(|o| (e.partition(), o))
            })
            .collect())
    }

    /// Нарезает окна очередного раунда и запускает по ним чтение.
    ///
    /// Здесь же — единственная точка штатной остановки чтения. Обрывать раунд
    /// на середине нельзя без потерь (см. `abort_round`), поэтому и дедлайн, и
    /// байтовый бюджет проверяются именно тут, на границе.
    fn begin_round(&mut self, mut pending: PendingRead) {
        let newest_first = self.newest_first;

        // Дедлайн и байтовый бюджет — это обещание «вернуть управление
        // пользователю через полминуты». Глубокому поиску они противоречат по
        // смыслу: он и есть длинная операция, о чём пользователь предупреждён,
        // а прерви его здесь — он остановится ровно там, где ничего ещё не
        // нашлось, и выглядеть это будет как «поиск не работает». Остановить
        // его может конец топика, полный буфер находок или кнопка «стоп».
        if pending.depth == ReadDepth::Window {
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
                // Читатель уже ровно здесь — переставлять нечего.
                //
                // Так бывает в режиме `oldest`, где окна идут встык, но ТОЛЬКО
                // если прошлое окно закрылось, не забрав из очереди ничего
                // лишнего. Обычно забирает: чтобы понять, что окно кончилось,
                // приходится взять сообщение за его краем, а с ним приезжает и
                // предвыборка. Поэтому условие проверяется по наблюдённой
                // позиции (`note_read_position`), а не выводится из того, что
                // окна соседние: раньше выводилось, и всё, что librdkafka
                // успела прислать сверх окна, пропадало из таблицы вместе с
                // куском топика за ним.
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
        pending.round_scanned_bytes = 0;
        pending.round_scanned = 0;
        pending.round_started = Instant::now();
        pending.phase = ReadPhase::Reading(round);
        self.pending_read = Some(pending);
        let _ = self.tx.send(Command::ContinueRead);
    }

    /// Отмечает позицию читателя партиции. Свободной функцией по полю, а не
    /// методом воркера: зовётся из цикла, где `pending` уже заимствован.
    fn note_position(cursors: &mut HashMap<i32, PartitionCursor>, p: i32, offset: i64) {
        if let Some(c) = cursors.get_mut(&p) {
            c.note_read_position(offset);
        }
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

        // Сито готово с начала чтения и за него не меняется — см. `Sieve`.
        // Забираем его себе на время цикла: `pending.phase` рядом занят
        // изменяемо, и одалживать соседнее поле сквозь него неудобно.
        let sieve = pending.sieve.take();
        let mut scanned = 0u64;
        let mut scanned_bytes = 0usize;

        for msg in batch {
            let p = msg.partition();
            match msg.err() {
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR => {
                    // Читатель сдвинулся — независимо от того, возьмём мы это
                    // сообщение или выбросим. Из очереди оно уже изъято, и
                    // второй раз библиотека его не отдаст.
                    Self::note_position(&mut self.cursors, p, msg.offset() + 1);
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
                    // Всё, что дошло сюда, ПРОСМОТРЕНО — вне зависимости от
                    // того, оставим мы его или выбросим. Отсюда и счётчики, и
                    // цена офсета, и отметка времени партиции: они описывают
                    // топик, а не буфер, и от фильтра зависеть не должны.
                    // Считать их по одним находкам значило бы сказать окну, что
                    // офсеты почти бесплатны, — и раунд разросся бы до дедлайна
                    // (см. `window_floor`).
                    rp.absorbed += 1;
                    scanned += 1;

                    let ts = msg.timestamp_millis();
                    if ts > 0 {
                        self.has_timestamps = true;
                    }
                    if let Some(c) = self.cursors.get_mut(&p) {
                        c.observe(newest_first, ts);
                    }

                    // Ключ, тело и заголовки разбираются РОВНО ОДИН РАЗ:
                    // `RawMessage::headers` каждый раз обходит C-список и
                    // аллоцирует вектор, а на глубоком поиске через это место
                    // проходит весь топик.
                    let key = msg.key().unwrap_or(&[]);
                    let value = msg.payload().unwrap_or(&[]);
                    let headers = msg.headers();
                    scanned_bytes += MessageStore::footprint(key, value, &headers);

                    // Сито стоит ПОСЛЕ учёта и ДО арены: непопавшее в топике
                    // было, а в буфере его не будет.
                    if let Some(sieve) = &sieve {
                        if !sieve.filter.matches(key, value, sieve.decoder.as_deref()) {
                            continue;
                        }
                    }

                    let stored =
                        self.store
                            .push(msg.partition(), msg.offset(), ts, key, value, &headers);
                    if !stored {
                        eprintln!(
                            "[step_reading] {} buffer budget hit at {} messages",
                            pending.topic_name,
                            self.store.len()
                        );
                        buffer_full = true;
                        break;
                    }
                }
                RDKafkaRespErr::RD_KAFKA_RESP_ERR__PARTITION_EOF => {
                    // Конец лога. Офсет события — позиция, на которой лог
                    // кончился, то есть ровно то место, где стоит читатель.
                    let end_of_log = msg.offset();
                    Self::note_position(&mut self.cursors, p, end_of_log);
                    if let Some(rp) = round.get_mut(&p) {
                        rp.done = true;
                        if eof_exhausts(newest_first, end_of_log, rp.end) {
                            if let Some(c) = self.cursors.get_mut(&p) {
                                c.exhausted = true;
                            }
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

        // Заимствование `round` кончилось вместе с циклом — только теперь
        // `pending` снова целиком наш. Сито возвращается на место: следующий
        // шаг того же чтения просеивает тем же самым.
        pending.sieve = sieve;
        self.scanned += scanned;
        pending.round_scanned += scanned;
        pending.round_scanned_bytes += scanned_bytes;

        if let Some(e) = failure {
            Self::stop_all(&pending);
            self.rollback_round(&pending);
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
        let kept_bytes = self.store.byte_size() - pending.round_mark_bytes;
        // Мерить цену офсета надо по ПРОСМОТРЕННОМУ, а не по осевшему в арене.
        // При обычном чтении это одно и то же число (кладётся всё, что
        // просмотрено, и `footprint` считает ровно то же, что занимает `push`),
        // а при глубоком поиске они расходятся на порядки — и по осевшему
        // офсеты выглядели бы почти бесплатными.
        let bytes = pending.round_scanned_bytes;
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
             offsets, +{bytes} bytes scanned{} in {elapsed:?}{}",
            pending.topic_name,
            round.len(),
            // Второе число печатается только когда расходится с первым, то есть
            // только на глубоком поиске: там видно, сколько из просмотренного
            // сито оставило, и это главная цифра его эффективности.
            if kept_bytes == bytes {
                String::new()
            } else {
                format!(" ({kept_bytes} kept)")
            },
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
        self.rollback_round(&pending);
        self.finish_read(pending, true);
    }

    /// Отматывает недоделанный раунд: и добычу, и счётчик просмотренного.
    ///
    /// Второе — не педантизм. Окно оборванного раунда будет вычитано ЗАНОВО
    /// (курсоры остались на его границе), и оставленный счётчик посчитал бы эти
    /// сообщения дважды — шкала «просмотрено N из ~M» уехала бы за собственный
    /// знаменатель.
    fn rollback_round(&mut self, pending: &PendingRead) {
        self.store.truncate(pending.round_mark);
        self.scanned = self.scanned.saturating_sub(pending.round_scanned);
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
            scope: self.read_scope(),
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
            self.extend_view();
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
            scope: self.read_scope(),
            searching: self
                .pending_read
                .as_ref()
                .is_some_and(|p| p.depth == ReadDepth::WholeTopic),
        }
    }

    /// Сколько топика просмотрено и сколько его всего.
    ///
    /// Знаменатель — сумма `high - low` по курсорам, то есть уже с учётом и
    /// выбранных партиций, и заданных границ чтения: диапазон вшивается в
    /// курсоры сразу после watermarks. Спрашивать топик целиком было бы
    /// неверно — прогресс шёл бы к числу, до которого чтение и не собиралось.
    fn read_scope(&self) -> ReadScope {
        ReadScope {
            scanned: self.scanned,
            // Пустые курсоры — границы ещё не сняты. Ноль здесь означал бы
            // «в топике ничего нет», а это совсем другая новость.
            approx_total: if self.cursors.is_empty() {
                None
            } else {
                Some(self.cursors.values().map(|c| c.high - c.low).sum())
            },
        }
    }

    fn close_topic(&mut self) {
        self.cancel_pending_read();
        self.open_topic = None;
        self.filter = MessageFilter::default();
        self.sort = None;
        self.view.clear();
        self.view_scanned = 0;
        self.scanned = 0;
        self.cursors.clear();
        self.has_timestamps = false;
        // Схема принадлежит топику. Оставить её — значит показать следующий
        // топик через чужой контракт, если фронт не успеет прислать свою.
        self.decoder = None;
        self.key_decoder = None;
        // Линза принадлежит топику ровно так же, как схема.
        self.lens = LensMode::default();
        // Освобождаем буфер целиком: держать сотни мегабайт, пока пользователь
        // ничего не смотрит, незачем.
        self.store.release();
    }

    /// Пересобирает отфильтрованное представление по ОПУБЛИКОВАННОЙ части
    /// буфера с нуля. Работает в памяти, без единого сетевого запроса —
    /// поэтому смена фильтра мгновенна.
    ///
    /// Нужна там, где старый `view` больше не годится целиком: сменился фильтр
    /// или порядок. После публикации новой порции хватает `extend_view`.
    fn rebuild_view(&mut self) {
        // Забираем вектор себе: иначе `view.push` конфликтует с одновременным
        // заимствованием `self.filter` и `self.store`. Ёмкость при этом
        // сохраняется, повторных аллокаций нет.
        let mut view = std::mem::take(&mut self.view);
        view.clear();
        self.view_scanned = self.grow_from(&mut view, 0);
        self.view = view;
    }

    /// Досевает в `view` то, что опубликовалось с прошлого раза.
    ///
    /// Опубликованный префикс `[0, committed)` заморожен и только дописывается
    /// в хвост (см. границу `committed` в `store.rs`), поэтому пересматривать
    /// его начало не надо. Без этого фильтр перебирал бы весь буфер заново на
    /// каждом раунде подгрузки — а с включённым `search_decoded` ещё и
    /// декодировал бы его целиком, то есть квадратично от числа раундов.
    fn extend_view(&mut self) {
        let total = self.store.committed_len();
        debug_assert!(
            total >= self.view_scanned,
            "опубликованное не может убывать вне clear/release"
        );
        // Страховка на случай, если инвариант выше когда-нибудь нарушат:
        // лучше лишняя пересборка, чем `view` с индексами в никуда.
        if total < self.view_scanned {
            self.rebuild_view();
            return;
        }
        if total == self.view_scanned {
            return;
        }

        let mut view = std::mem::take(&mut self.view);
        self.view_scanned = self.grow_from(&mut view, self.view_scanned);
        self.view = view;
    }

    /// Разобранный фильтр из текущего состояния воркера.
    ///
    /// Одно место на оба применения фильтра — по буферу (`grow_from`) и на
    /// входе (сито глубокого поиска, см. `Sieve`). Разойдись эти два, и поиск
    /// клал бы в арену не то, что таблица потом из неё показывает: например,
    /// просеивал бы по сырым байтам там, где показ ищет ещё и по разобранному
    /// телу, — и найденное сообщение не доехало бы до экрана.
    ///
    /// Готовится каждый раз заново, а не хранится полем: `decoder` меняется
    /// командой `SetDecoder` без пересборки представления, и закэшированный
    /// рядом флаг «есть чем разбирать» протух бы молча. Две аллокации на проход
    /// по буферу — не та цена, за которую стоит держать производное состояние.
    fn prepared_filter(&self) -> PreparedFilter {
        PreparedFilter::new(&self.filter, self.decoder.is_some())
    }

    /// Обёртка над свободной `grow_view`: собирает разобранный фильтр из
    /// текущего состояния воркера, а саму работу отдаёт наружу.
    fn grow_from(&self, view: &mut Vec<u32>, from: usize) -> usize {
        let prepared = self.prepared_filter();
        grow_view(
            &self.store,
            &prepared,
            self.decoder.as_deref(),
            self.sort,
            from,
            view,
        )
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
                let shown = text::render(self.decoder.as_deref(), value);
                // Линза работает по УЖЕ разобранному телу: Debezium с
                // Avro-сериализатором — обычное дело, и смотреть в сырые байты
                // там было бы не на что.
                let body: &[u8] = match &shown.decoded {
                    Some(json) => json.as_bytes(),
                    None => value,
                };
                let lensed = lens::apply(self.lens, body);
                RowPreview {
                    index: start + offset_in_window,
                    partition: meta.partition,
                    offset: meta.offset,
                    timestamp: meta.timestamp,
                    // Ключ — своей схемой: у avro-топика он тоже закодирован, и
                    // сырыми байтами перед ним ехали бы длина строки и признак
                    // union'а.
                    key: text::preview(
                        text::key(self.key_decoder.as_deref(), self.store.key(i)).as_bytes(),
                        PREVIEW_BYTES,
                    ),
                    // Обрезаем уже разобранное и, если линза сработала,
                    // снятое с конверта: строке таблицы нужны первые пару
                    // сотен символов, а не всё тело.
                    preview: match &lensed.preview {
                        Some(inner) => text::preview(inner.as_bytes(), PREVIEW_BYTES),
                        None => text::preview(body, PREVIEW_BYTES),
                    },
                    value_size: value.len(),
                    binary: shown.binary(value),
                    decode_error: shown.error,
                    lens_tag: lensed.tag,
                }
            })
            .collect()
    }

    /// Индекс в буфере по индексу в текущем отфильтрованном представлении.
    fn store_index(&self, view_index: usize) -> Result<usize, String> {
        self.view
            .get(view_index)
            .map(|&i| i as usize)
            .ok_or_else(|| "message index out of range".to_string())
    }

    /// Где в таблице стоит сообщение с такими координатами.
    ///
    /// Проход по `view`, а не по всему буферу: индекс нужен именно тот, которым
    /// пользуется таблица, — то есть уже с учётом фильтра и сортировки. Ищем
    /// линейно и не заводим карту: `view` — плотный массив четырёхбайтовых
    /// индексов, сотня тысяч записей просматривается быстрее, чем строится
    /// хэш-таблица, а зовут это ровно один раз на переход по ссылке.
    ///
    /// `None` — сообщения в буфере нет. Причин две, и различить их отсюда
    /// нельзя (обе выглядят одинаково): либо его не вычитали, либо его
    /// выбросил фильтр. Объясняет это вызывающий, который знает, чем открывал.
    fn find_message(&self, partition: i32, offset: i64) -> Result<Option<usize>, String> {
        if self.open_topic.is_none() {
            return Err("no topic is open".into());
        }
        Ok(self.view.iter().position(|&i| {
            self.store
                .get(i as usize)
                .is_some_and(|m| m.partition == partition && m.offset == offset)
        }))
    }

    /// Полное тело — только когда пользователь открыл конкретное сообщение.
    fn body(&self, view_index: usize) -> Result<FullMessage, String> {
        let store_index = self.store_index(view_index)?;
        let meta = self
            .store
            .get(store_index)
            .ok_or("message index out of range")?;
        let value = self.store.value(store_index);
        let shown = text::body(self.decoder.as_deref(), value);

        Ok(FullMessage {
            partition: meta.partition,
            offset: meta.offset,
            timestamp: meta.timestamp,
            key: text::key(self.key_decoder.as_deref(), self.store.key(store_index)),
            binary: shown.binary,
            value: shown.value,
            value_size: value.len(),
            decode_error: shown.decode_error,
            enum_values: shown.enum_values,
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

    /// Сообщение в том виде, в каком оно пришло из Kafka: тело — сырыми
    /// байтами, без всякой схемы.
    ///
    /// Нужно ровно одному вызывающему — архиву сохранённых. Сохранять наш способ
    /// показа вместо самих данных значило бы потерять их навсегда: тело, не
    /// разобравшееся схемой, уезжает в UI через `from_utf8_lossy`, и загруженная
    /// назавтра схема разбирать было бы уже нечего.
    fn raw(&self, view_index: usize) -> Result<RawBody, String> {
        let store_index = self.store_index(view_index)?;
        let meta = self
            .store
            .get(store_index)
            .ok_or("message index out of range")?;
        Ok(RawBody {
            partition: meta.partition,
            offset: meta.offset,
            timestamp: meta.timestamp,
            // Ключ и здесь через схему: в архив он уезжает уже строкой (см.
            // `RawBody`), и уехать туда он обязан тем же, чем показан в
            // таблице, — иначе сохранённое сообщение выглядело бы иначе, чем
            // то, которое сохраняли.
            key: text::key(self.key_decoder.as_deref(), self.store.key(store_index)),
            value: self.store.value(store_index).to_vec(),
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

// --- Фильтрация и порядок ----------------------------------------------------
//
// Свободные функции, а не методы `Worker`: воркер держит хендлы rdkafka и в
// тесте не строится, а `MessageStore` строится руками. Логика отбора строк —
// ровно то, что стоит проверять юнит-тестами, и запирать её внутри
// непостроимого типа значило бы оставить её непокрытой.

/// Разобранный запрос: иглы готовятся один раз на проход по буферу, а не на
/// каждое сообщение.
struct PreparedFilter {
    key: Needle,
    value: Needle,
    /// Разбирать ли тело перед поиском. Флаг фронта сам по себе ничего не
    /// значит — без схемы разбирать нечем.
    decode_value: bool,
}

impl PreparedFilter {
    fn new(filter: &MessageFilter, has_decoder: bool) -> Self {
        Self {
            key: Needle::new(&filter.key, filter.case_sensitive),
            value: Needle::new(&filter.value, filter.case_sensitive),
            decode_value: filter.search_decoded && has_decoder,
        }
    }

    /// Пусты ли обе иглы, то есть «не фильтровать».
    ///
    /// `search_decoded` сюда намеренно не входит: он задаёт, ГДЕ искать, а не
    /// ЧТО. Учитывать его значило бы на голом флаге с пустыми иглами уйти в
    /// медленный путь по всему буферу с запросом, который и так совпадает со
    /// всем подряд.
    fn is_empty(&self) -> bool {
        self.key.is_empty() && self.value.is_empty()
    }

    fn matches(&self, key: &[u8], value: &[u8], decoder: Option<&Decoder>) -> bool {
        // Ключ ищем только по сырым байтам: он не protobuf, разбирать нечего.
        if !self.key.matches(key) {
            return false;
        }
        if self.value.matches(value) {
            return true;
        }
        // Сначала сырые байты, декодирование — только на промахе. Строковые
        // поля protobuf лежат на проводе непрерывным UTF-8 и находятся без
        // разбора, поэтому обычный запрос по содержимому не платит за декодер
        // вовсе; платит только настоящий промах или запрос по имени поля.
        //
        // Это важно: `SetFilter` пересобирает представление на каждое нажатие
        // клавиши, и полный разбор буфера там встал бы поперёк того же потока,
        // который отдаёт окна на экран.
        self.decode_value
            && decoder.is_some_and(|d| {
                d.decode(value)
                    .is_ok_and(|json| self.value.matches(json.as_bytes()))
            })
    }
}

/// Просеивает `store[from..committed_len)` в хвост `view` и заново
/// упорядочивает результат. Возвращает новую границу просеянного.
fn grow_view(
    store: &MessageStore,
    prepared: &PreparedFilter,
    decoder: Option<&Decoder>,
    sort: Option<SortSpec>,
    from: usize,
    view: &mut Vec<u32>,
) -> usize {
    let total = store.committed_len();
    if prepared.is_empty() {
        view.extend(from as u32..total as u32);
    } else {
        append_view(store, prepared, decoder, from, view);
    }

    // Сортируем ВЕСЬ view, а не только дописанный хвост — как и раньше.
    //
    // Инкрементальный досев даёт при этом ровно ту же перестановку, что
    // сортировка с нуля, и вот почему. `sort_by` стабильна; дописанные индексы
    // строго больше всех уже лежащих; прошлый `view` сам был стабильной
    // сортировкой возрастающих индексов. Значит внутри любой группы равных
    // ключей конкатенация уже идёт по возрастанию индекса — ровно в том
    // порядке, который дала бы свежая сортировка `0..total`. Ключи
    // опубликованных записей при этом не меняются: префикс заморожен.
    if let Some(sort) = sort {
        sort_view(store, view, sort);
    }
    total
}

/// Просеивает `store[from..committed_len)` в хвост `view`.
fn append_view(
    store: &MessageStore,
    prepared: &PreparedFilter,
    decoder: Option<&Decoder>,
    from: usize,
    view: &mut Vec<u32>,
) {
    for i in from..store.committed_len() {
        if prepared.matches(store.key(i), store.value(i), decoder) {
            view.push(i as u32);
        }
    }
}

/// Сортировка по клику на заголовок колонки, поверх фильтра.
///
/// `sort_by` — стабильная сортировка: сообщения с одинаковым значением
/// столбца остаются в том порядке, в котором их поставил обычный порядок
/// чтения (время, а внутри него — офсет), а не в произвольном. Пока идёт
/// прогрессивная подгрузка, эта сортировка пересчитывается на весь `view`
/// при каждом новом раунде — если она активна, уже показанные строки
/// перестают быть застрахованы от переезда, в отличие от обычного порядка.
fn sort_view(store: &MessageStore, view: &mut [u32], sort: SortSpec) {
    let desc = sort.direction == SortDirection::Desc;
    view.sort_by(|&a, &b| {
        let ma = store
            .get(a as usize)
            .expect("view index out of sync with store");
        let mb = store
            .get(b as usize)
            .expect("view index out of sync with store");
        let cmp = match sort.column {
            SortColumn::Partition => ma.partition.cmp(&mb.partition),
            SortColumn::Offset => ma.offset.cmp(&mb.offset),
            SortColumn::Timestamp => ma.timestamp.cmp(&mb.timestamp),
            SortColumn::Key => store.key(a as usize).cmp(store.key(b as usize)),
        };
        if desc {
            cmp.reverse()
        } else {
            cmp
        }
    });
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

    /// Воркер без кластера — годится всему, что считается по своим полям.
    fn offline_worker() -> Worker {
        let (tx, _rx) = mpsc::channel();
        Worker::new(tx)
    }

    /// Знаменатель шкалы поиска — сумма по КУРСОРАМ, а не по топику: партиции
    /// выбирает пользователь, границы чтения тоже, и прогресс обязан идти к
    /// тому, что читается на самом деле.
    #[test]
    fn approx_total_counts_only_the_partitions_being_read() {
        let mut worker = offline_worker();
        worker.cursors.insert(0, cursor(10, 110, NEWEST, 1000));
        worker.cursors.insert(3, cursor(0, 50, NEWEST, 1000));

        let scope = worker.read_scope();
        // 100 офсетов в первой, 50 во второй; `low` вычитается — то, что съел
        // retention, читать всё равно не будут.
        assert_eq!(scope.approx_total, Some(150));
        assert_eq!(scope.scanned, 0);
    }

    /// Пустые курсоры — это «границы ещё не сняты», а не «в топике ноль».
    /// Ноль здесь превратился бы в шкалу «0 из 0», то есть в готовый поиск,
    /// который ещё даже не начинался.
    #[test]
    fn approx_total_is_unknown_before_the_watermarks_are_in() {
        assert_eq!(offline_worker().read_scope().approx_total, None);
    }

    /// Глубокий поиск ставит бюджет партиции в `i64::MAX` — окна от этого не
    /// должны ни переполниться, ни разъехаться: партиция читается до конца
    /// ровно теми же окнами, что и с обычным бюджетом.
    #[test]
    fn an_unbounded_budget_still_walks_the_partition_in_normal_windows() {
        let windows = walk(cursor(0, 25, OLDEST, i64::MAX), OLDEST, 10);
        assert_eq!(windows, vec![(0, 10), (10, 20), (20, 25)]);

        let backwards = walk(cursor(0, 25, NEWEST, i64::MAX), NEWEST, 10);
        assert_eq!(backwards, vec![(15, 25), (5, 15), (0, 5)]);
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

    /// Регрессия. Читатель бежит впереди окна, и на топике в несколько десятков
    /// сообщений предвыборка добирается до конца лога ещё в первом окне. EOF,
    /// объявлявший партицию дочитанной безусловно, обрывал чтение на первом же
    /// окне: топик из 60 сообщений показывал 10 и говорил, что это всё.
    ///
    /// Найдено на ссылке в `fireg.cryptocurrencies`: офсет 50 «не найден»,
    /// хотя в топике он есть, а вычитано было ровно окно [0, 20).
    #[test]
    fn eof_beyond_the_window_does_not_finish_the_partition() {
        // Конец лога на 60, окно кончается на 20 — читать ещё есть что.
        assert!(!eof_exhausts(OLDEST, 60, 20));
        // Конец лога внутри окна — вот теперь действительно всё.
        assert!(eof_exhausts(OLDEST, 60, 60));
        assert!(eof_exhausts(OLDEST, 55, 60));
        // В `newest` EOF не значит ничего: первое же окно упирается в конец
        // лога по построению.
        assert!(!eof_exhausts(NEWEST, 60, 60));
    }

    /// Регрессия к тому же случаю. Закрытое окно НЕ означает, что читатель
    /// стоит на его правой границе: чтобы окно закрылось, из очереди берётся
    /// сообщение за краем, а с ним и вся предвыборка. Пока `advance` это
    /// утверждал, следующее окно в `oldest` считало себя уже позиционированным,
    /// не делало `seek` — и всё, что librdkafka успела прислать сверх окна,
    /// пропадало вместе с куском топика за ним.
    #[test]
    fn a_closed_window_does_not_claim_where_the_reader_stands() {
        let mut c = cursor(0, 1000, OLDEST, 1000);
        c.note_read_position(137); // читатель убежал за край окна
        c.advance(
            OLDEST,
            &RoundPartition {
                start: 0,
                end: 20,
                absorbed: 20,
                done: true,
            },
        );

        assert_eq!(c.next, 20, "курсор двигается по окнам, а не по читателю");
        assert_eq!(
            c.read_position,
            Some(137),
            "позиция читателя — наблюдение, и закрытие окна её не выдумывает"
        );
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
        assert_eq!(
            intersect(100, 1000, None, exclusive(Some(i64::MAX))),
            (100, 1000)
        );
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
        assert!(
            next_window(10_000, est, 16, QUICK, NO_FLOOR)
                < next_window(10_000, est, 4, QUICK, NO_FLOOR)
        );
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
        assert_eq!(
            unbounded, MIN_ROUND_CHUNK,
            "формула сама по себе схлопывается"
        );

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

    // --- Фильтрация ----------------------------------------------------------
    //
    // Проверяется через свободные функции: воркер держит хендлы rdkafka и в
    // тесте не строится, а `MessageStore` строится руками. `grow_view` здесь —
    // ровно та функция, которую зовёт воркер, а не её пересказ.

    /// Кладёт строки в стор и публикует их: фильтр смотрит только на
    /// опубликованный префикс.
    fn store_of(rows: &[(i32, i64, i64, &str, &[u8])]) -> MessageStore {
        let mut store = MessageStore::new(1 << 20);
        for &(partition, offset, ts, key, value) in rows {
            assert!(store.push(partition, offset, ts, key.as_bytes(), value, &[]));
        }
        store.commit_all();
        store
    }

    fn text_rows(rows: &[(&str, &str)]) -> Vec<(i32, i64, i64, String, Vec<u8>)> {
        rows.iter()
            .enumerate()
            .map(|(i, (k, v))| (0, i as i64, i as i64, k.to_string(), v.as_bytes().to_vec()))
            .collect()
    }

    fn store_of_text(rows: &[(&str, &str)]) -> MessageStore {
        let owned = text_rows(rows);
        let refs: Vec<_> = owned
            .iter()
            .map(|(p, o, t, k, v)| (*p, *o, *t, k.as_str(), v.as_slice()))
            .collect();
        store_of(&refs)
    }

    fn filter_of(key: &str, value: &str) -> MessageFilter {
        MessageFilter {
            key: key.to_string(),
            value: value.to_string(),
            case_sensitive: false,
            search_decoded: false,
        }
    }

    fn view_of(
        store: &MessageStore,
        filter: &MessageFilter,
        decoder: Option<&Decoder>,
    ) -> Vec<u32> {
        let prepared = PreparedFilter::new(filter, decoder.is_some());
        let mut view = Vec::new();
        grow_view(store, &prepared, decoder, None, 0, &mut view);
        view
    }

    #[test]
    fn an_empty_filter_shows_everything() {
        let store = store_of_text(&[("k1", "v1"), ("k2", "v2")]);
        assert_eq!(view_of(&store, &filter_of("", ""), None), vec![0, 1]);
        // Голый `search_decoded` фильтром не считается: иглы-то пустые.
        let mut bare = filter_of("", "");
        bare.search_decoded = true;
        assert_eq!(view_of(&store, &bare, None), vec![0, 1]);
    }

    #[test]
    fn key_and_value_needles_are_combined_with_and() {
        let store = store_of_text(&[
            ("order-1", "paid"),
            ("order-2", "shipped"),
            ("refund-1", "paid"),
        ]);
        assert_eq!(view_of(&store, &filter_of("order", ""), None), vec![0, 1]);
        assert_eq!(view_of(&store, &filter_of("", "paid"), None), vec![0, 2]);
        assert_eq!(view_of(&store, &filter_of("order", "paid"), None), vec![0]);
    }

    #[test]
    fn cyrillic_filter_finds_rows_in_any_case() {
        // Тот самый баг: ASCII-фолдинг не складывал кириллицу, и поиск в
        // нижнем регистре не находил ничего.
        let store = store_of_text(&[
            ("клиент-1", "Сбербанк"),
            ("КЛИЕНТ-2", "ГАЗПРОМ"),
            ("client-3", "Yandex"),
        ]);
        assert_eq!(view_of(&store, &filter_of("клиент", ""), None), vec![0, 1]);
        assert_eq!(view_of(&store, &filter_of("КЛИЕНТ", ""), None), vec![0, 1]);
        assert_eq!(view_of(&store, &filter_of("", "сбербанк"), None), vec![0]);
        assert_eq!(view_of(&store, &filter_of("", "газпром"), None), vec![1]);
    }

    #[test]
    fn case_sensitive_filter_still_distinguishes_cyrillic() {
        let store = store_of_text(&[("k", "Сбербанк")]);
        let mut exact = filter_of("", "сбербанк");
        exact.case_sensitive = true;
        assert!(view_of(&store, &exact, None).is_empty());
        exact.value = "Сбербанк".to_string();
        assert_eq!(view_of(&store, &exact, None), vec![0]);
    }

    // --- Поиск по разобранному телу ------------------------------------------

    const FILTER_PROTO: &str = r#"
        syntax = "proto3";
        package demo;
        message Event { string customer = 1; int32 amount = 2; }
    "#;

    /// Кодирует `Event { customer, amount }` руками по wire-формату:
    /// генератор кода сюда не подключён, а формат достаточно прост.
    fn event_bytes(customer: &str, amount: i32) -> Vec<u8> {
        let mut out = vec![0x0a, customer.len() as u8];
        out.extend_from_slice(customer.as_bytes());
        // Поле 2, wire type 0 (varint). Значения в тестах однобайтовые.
        out.extend_from_slice(&[0x10, amount as u8]);
        out
    }

    /// Каталог схемы живёт во временном каталоге и удаляется вызывающим.
    fn decoder_for(tag: &str) -> (std::path::PathBuf, Arc<Decoder>) {
        let dir = std::env::temp_dir().join(format!("mikui-worker-filter-{tag}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let source = dir.join("event.proto");
        std::fs::write(&source, FILTER_PROTO).unwrap();
        crate::schema::store::add_files(
            &dir,
            "cluster",
            "topic",
            &[source.to_string_lossy().into_owned()],
        )
        .unwrap();
        let decoder = crate::schema::store::decoder(&dir, "cluster", "topic")
            .unwrap()
            .unwrap();
        (dir, decoder)
    }

    #[test]
    fn string_field_content_is_found_without_decoding() {
        // Строковое поле protobuf лежит на проводе непрерывным UTF-8, поэтому
        // находится и без схемы. Это и есть короткое замыкание, которое спасает
        // обычный запрос от полного разбора буфера.
        let store = store_of(&[
            (0, 0, 0, "a", &event_bytes("Сбербанк", 10)),
            (0, 1, 1, "b", &event_bytes("Тинькофф", 20)),
        ]);
        assert_eq!(view_of(&store, &filter_of("", "сбербанк"), None), vec![0]);
        assert_eq!(view_of(&store, &filter_of("", "ТИНЬКОФФ"), None), vec![1]);
    }

    #[test]
    fn a_field_name_is_found_only_when_decoding_is_asked_for() {
        let (dir, decoder) = decoder_for("field-name");
        let store = store_of(&[
            (0, 0, 0, "a", &event_bytes("Сбербанк", 10)),
            (0, 1, 1, "b", &event_bytes("Тинькофф", 20)),
        ]);

        // Имени поля в wire-формате нет — без флага его не найти даже со схемой.
        let plain = filter_of("", "customer");
        assert!(view_of(&store, &plain, Some(&decoder)).is_empty());

        let mut decoded = plain.clone();
        decoded.search_decoded = true;
        assert_eq!(view_of(&store, &decoded, Some(&decoder)), vec![0, 1]);

        // И числа, которых в виде текста на проводе тоже нет.
        let mut amount = filter_of("", "20");
        amount.search_decoded = true;
        assert_eq!(view_of(&store, &amount, Some(&decoder)), vec![1]);

        // Флаг без схемы ничего не включает: разбирать нечем.
        assert!(view_of(&store, &decoded, None).is_empty());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn an_undecodable_body_falls_back_to_raw_bytes() {
        let (dir, decoder) = decoder_for("undecodable");
        // Второе тело — просто текст, схемой не разбирается.
        let store = store_of(&[
            (0, 0, 0, "a", &event_bytes("Сбербанк", 10)),
            (0, 1, 1, "b", b"plain text payload"),
        ]);

        let mut decoded = filter_of("", "payload");
        decoded.search_decoded = true;
        // Строка не выпала из выдачи из-за того, что разбор её тела не удался.
        assert_eq!(view_of(&store, &decoded, Some(&decoder)), vec![1]);

        let _ = std::fs::remove_dir_all(&dir);
    }

    // --- Эквивалентность инкрементального досева ------------------------------

    /// Тот самый тест, ради которого `extend_view` вообще можно считать
    /// безопасным: досев тремя порциями обязан дать тот же `view`, что и одна
    /// полная пересборка в конце — при любом порядке сортировки.
    #[test]
    fn growing_the_view_in_tranches_equals_one_full_rebuild() {
        // Партиции и отметки времени намеренно с повторами: именно на группах
        // равных ключей стабильность сортировки и проверяется.
        let rows: Vec<(i32, i64, i64, String, Vec<u8>)> = vec![
            (0, 10, 100, "ключ-a".into(), b"alpha".to_vec()),
            (1, 11, 100, "ключ-b".into(), b"beta".to_vec()),
            (0, 12, 200, "ключ-a".into(), b"alpha".to_vec()),
            (2, 13, 100, "ключ-c".into(), b"gamma".to_vec()),
            (1, 14, 200, "ключ-b".into(), b"alpha".to_vec()),
            (0, 15, 300, "ключ-a".into(), b"beta".to_vec()),
            (2, 16, 200, "ключ-c".into(), b"alpha".to_vec()),
            (1, 17, 300, "ключ-b".into(), b"gamma".to_vec()),
            (0, 18, 100, "ключ-a".into(), b"alpha".to_vec()),
        ];
        let columns = [
            None,
            Some(SortColumn::Partition),
            Some(SortColumn::Offset),
            Some(SortColumn::Timestamp),
            Some(SortColumn::Key),
        ];
        let directions = [SortDirection::Asc, SortDirection::Desc];

        for filter in [
            filter_of("", ""),
            filter_of("", "alpha"),
            filter_of("КЛЮЧ-A", ""),
        ] {
            for column in columns {
                for direction in directions {
                    let sort = column.map(|column| SortSpec { column, direction });
                    let prepared = PreparedFilter::new(&filter, false);

                    // Инкрементально: три порции по три строки, между ними —
                    // публикация, как её делает `publish`.
                    let mut store = MessageStore::new(1 << 20);
                    let mut incremental = Vec::new();
                    let mut scanned = 0usize;
                    for chunk in rows.chunks(3) {
                        for (p, o, ts, k, v) in chunk {
                            assert!(store.push(*p, *o, *ts, k.as_bytes(), v, &[]));
                        }
                        store.commit_all();
                        scanned =
                            grow_view(&store, &prepared, None, sort, scanned, &mut incremental);
                    }
                    assert_eq!(scanned, rows.len());

                    // Одной пересборкой по тому же самому стору.
                    let mut full = Vec::new();
                    grow_view(&store, &prepared, None, sort, 0, &mut full);

                    assert_eq!(
                        incremental, full,
                        "порции разошлись с полной пересборкой: {column:?} {direction:?}"
                    );
                }
            }
        }
    }

    /// Воркер с подключением и открытым чтением — но БЕЗ кластера.
    ///
    /// Кластер тут и не нужен: проверяется ниже одно — в каком порядке
    /// освобождаются объекты клиента, и от брокера это не зависит вообще.
    /// Список брокеров поэтому пустой: так librdkafka никуда и не пойдёт, а
    /// её жалобы на недоступный адрес не осядут в выводе теста.
    fn worker_mid_read() -> Worker {
        let consumer: BaseConsumer<MeteredContext> = ClientConfig::new()
            .set("bootstrap.servers", "")
            // Своё «брокеров не задано» librdkafka печатает сама, ещё до
            // того, как у клиента появится очередь событий, — порогом её и
            // унимаем, иначе она будет в выводе каждого прогона.
            .set("log_level", "3")
            .create_with_context(MeteredContext {
                meter: Arc::new(QuotaMeter::new()),
                fatal: Arc::new(FatalError::default()),
            })
            .expect("клиент создаётся и без кластера");

        let (tx, _rx) = mpsc::channel();
        let mut worker = Worker::new(tx);
        worker.consumer = Some(consumer);

        // Через тот же `open_handles`, которым их берёт настоящее чтение:
        // указатели обязаны быть выданы именно этим консьюмером, иначе
        // проверять нечего.
        let (topic, queue) = worker.open_handles("mid-read").expect("хендлы топика");
        // Партиция именно ЗАПУЩЕНА, и `consume_stop` для неё дальше не будет
        // — так же, как при панике посреди чтения. Брокер для этого не нужен:
        // `consume_start_queue` только ставит операцию в очередь партиции.
        topic
            .consume_start_queue(0, 0, &queue)
            .expect("чтение партиции запускается и без брокера");
        let (reply, _receiver) = oneshot::channel();
        worker.pending_read = Some(PendingRead {
            topic,
            queue,
            open_partitions: HashSet::from([0]),
            phase: ReadPhase::Reading(HashMap::new()),
            started: Instant::now(),
            round_started: Instant::now(),
            bytes_at_start: 0,
            reply,
            topic_name: "mid-read".to_string(),
            round_mark: 0,
            round_mark_bytes: 0,
            budget: 1,
            range: ReadRange::default(),
            depth: ReadDepth::Window,
            sieve: None,
            round_scanned_bytes: 0,
            round_scanned: 0,
        });
        worker
    }

    /// Воркер, разобранный посреди чтения, обязан пережить сам себя — см.
    /// `Drop for Worker`.
    ///
    /// Сюда же приходит и паника воркера: раскрутка стека дропает `self` в
    /// `run` ровно этим путём. Именно так приложение и умирало целиком —
    /// журнал падения записывался, а следом процесс получал сигнал в
    /// `rd_kafka_topic_destroy_final`, которая пишет в `rkt->rkt_rk` (снимает
    /// топик со списка клиента), а клиента к тому моменту уже не было:
    /// обратные указатели на `rd_kafka_t` в librdkafka не считаются ссылками
    /// (`rkt_rk`/`rkq_rk` проставляются без `rd_kafka_keep`), так что клиент
    /// уходит первым, ничего об открытых на нём хендлах не зная.
    ///
    /// Проверка тут грубая, тоньше не выйдет: неверный порядок — не ошибка,
    /// которую можно вернуть и сравнить, а обращение в освобождённую память.
    /// Тест либо доходит до конца, либо роняет весь тестовый бинарник —
    /// проверено снятием `Drop`, падает так:
    ///
    /// ```text
    /// Assertion failed: (r == 0), function rwlock_wrlock,
    ///     file tinycthread_extra.c, line 181
    /// (signal: 6, SIGABRT)
    /// ```
    ///
    /// То есть тот самый симптом, только в CI, а не у пользователя.
    #[test]
    fn a_worker_torn_down_mid_read_outlives_its_client() {
        let worker = worker_mid_read();
        assert!(
            worker.pending_read.is_some() && worker.consumer.is_some(),
            "фикстура без чтения на живом клиенте ничего не проверяет"
        );

        drop(worker);

        // Дожили — значит хендлы ушли раньше клиента. Заодно повторяем всё
        // ещё раз: порча кучи первым проходом часто остаётся незамеченной, а
        // на втором уже нет.
        drop(worker_mid_read());
    }
}
