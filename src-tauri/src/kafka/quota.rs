//! Косвенное измерение квоты на чтение.
//!
//! # Почему косвенное
//!
//! Спросить у кластера настроенную квоту нельзя. В протоколе для этого есть
//! `DescribeClientQuotas` (KIP-546), но её не реализует ни librdkafka
//! (в `rdkafka_proto.h` лежит только имя API key для логов), ни обёртка
//! `rdkafka` — а сама операция вдобавок требует ACL `DescribeConfigs` на
//! ресурсе `Cluster`, то есть админской привилегии, которой у просмотрщика
//! заведомо нет.
//!
//! Зато брокер сам сообщает `throttle_time_ms` в каждом ответе, а librdkafka
//! складывает это вместе с объёмом принятого в периодическую статистику.
//! Отсюда квота выводится косвенно — и такая оценка даже лучше настроенного
//! значения: она сразу учитывает, какая из перекрывающихся квот (на
//! пользователя, на client-id или на их пару) в действительности применилась.
//!
//! # Что именно меряется
//!
//! **Принятое по сети**, а не осевшее в буфере. Квота списывается за то, что
//! брокер отправил, включая всё, что мы выбросили, уйдя читать следующее окно.
//! Читаем мы окнами, так что расхождение между этими двумя числами реальное, и
//! платим мы именно за первое.
//!
//! **По часам чтения**, а не по настенным. Между чтениями трафика нет, и по
//! настенным часам оценка уползала бы в ноль за то время, пока пользователь
//! просто смотрит на таблицу.
//!
//! **Окном заведомо шире, чем окно квоты у брокера.** Kafka считает нагрузку
//! по скользящему окну (по умолчанию 11 секунд) и позволяет его «перебрать»,
//! расплачиваясь потом одной длинной паузой — на реальном кластере
//! наблюдались всплески до 2 МБ/с, за которыми следовала девятисекундная
//! остановка. Усреднение короче этого цикла принимает burst-кредит за
//! настоящую скорость и тут же за это платит.
//!
//! **Суммарно по всем брокерам — и отдельно их число.** Это не мелочь учёта:
//! `consumer_byte_rate` в Kafka применяет КАЖДЫЙ брокер самостоятельно, к
//! своей доле трафика. Клиент, читающий партиции с семи брокеров под квотой
//! 200 КБ/с, законно получает около 1.4 МБ/с суммарно — и измеренная цифра
//! выглядит противоречащей настроенной квоте, пока рядом не стоит число
//! брокеров, на которое она делится.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use rdkafka::statistics::Statistics;

/// Ширина окна усреднения — с запасом больше квотного окна брокера.
const WINDOW: Duration = Duration::from_secs(20);
/// Пока накоплено меньше, оценке верить нельзя: она почти наверняка снята на
/// burst-кредите.
const MIN_SPAN: Duration = Duration::from_secs(3);

/// Что кластер на самом деле даёт.
#[derive(Debug, Clone, Copy)]
pub struct QuotaEstimate {
    /// Байт в секунду. При включённом квотировании сходится к самой квоте.
    pub bytes_per_sec: f64,
    /// Сколько сетевых байт приходится на один пройденный офсет. Учитывает и
    /// размер сообщений, и плотность офсетов, и то, что часть присланного мы
    /// выбрасываем — то есть настоящую цену офсета в единицах квоты.
    pub bytes_per_offset: f64,
    /// Самая длинная задержка, которую брокер накладывал на ответ. Ненулевая
    /// означает: медленно не потому, что приложение тормозит.
    pub peak_throttle: Duration,
    /// Сколько брокеров отдавало данные. `bytes_per_sec` — сумма по всем ним,
    /// а квота применяется каждым по отдельности; без этого числа измеренная
    /// скорость необъяснимо превышает настроенную квоту.
    pub brokers: u32,
    /// Сколько брокеров кластер вообще показал в метаданных. Само по себе
    /// число отдающих проверить не с чем: «семь» выглядит правдоподобно при
    /// любом составе кластера. Рядом со «семь из десяти» видно и то, что
    /// лидеры читаемых партиций сидят на подмножестве узлов, и то, что цифра
    /// не выдумана.
    pub known_brokers: u32,
}

/// Один интервал статистики.
struct Bucket {
    elapsed: Duration,
    rx_bytes: u64,
    offsets: i64,
    /// Сколько брокеров прислало хоть что-то за этот интервал.
    brokers: u32,
}

#[derive(Default)]
struct Meter {
    buckets: VecDeque<Bucket>,
    span: Duration,
    rx_bytes: u64,
    offsets: i64,
    /// Предыдущий снимок — статистика librdkafka кумулятивна.
    last_rx_total: Option<u64>,
    /// То же по каждому брокеру: нужно, чтобы отличить брокера, который
    /// действительно отдаёт данные, от того, с кем просто есть соединение.
    last_rx_by_broker: HashMap<i32, u64>,
    last_tick: Option<Instant>,
    /// Офсеты, пройденные с прошлого тика статистики.
    pending_offsets: i64,
    /// Идёт ли чтение прямо сейчас и шло ли оно на прошлом тике. Интервал
    /// засчитывается, только если чтение шло всё это время.
    reading: bool,
    was_reading: bool,
    peak_throttle: Duration,
}

pub struct QuotaMeter {
    inner: Mutex<Meter>,
}

impl Default for QuotaMeter {
    fn default() -> Self {
        Self::new()
    }
}

impl QuotaMeter {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Meter::default()),
        }
    }

    /// Новое подключение — прошлые измерения относятся к другому кластеру.
    pub fn reset(&self) {
        *self.lock() = Meter::default();
    }

    /// Идёт ли чтение. Останавливает и запускает часы измерителя.
    pub fn set_reading(&self, reading: bool) {
        let mut m = self.lock();
        if !reading {
            // Незасчитанные офсеты вместе с чтением и уходят: интервал, в
            // котором чтение прервалось, всё равно не годится в выборку.
            m.pending_offsets = 0;
        }
        m.reading = reading;
    }

    /// Сколько офсетов прошёл только что завершившийся раунд.
    pub fn record_offsets(&self, offsets: i64) {
        self.lock().pending_offsets += offsets;
    }

    /// Очередной снимок статистики librdkafka.
    pub fn observe(&self, stats: &Statistics) {
        let brokers: Vec<(i32, u64)> = stats
            .brokers
            .values()
            // `nodeid` -1 — это ещё не опознанный bootstrap-узел: соединение
            // есть, данных по нему нет, и в число отдающих он не входит.
            .filter(|b| b.nodeid >= 0)
            .map(|b| (b.nodeid, b.rxbytes))
            .collect();
        let throttle_ms = stats
            .brokers
            .values()
            .filter_map(|b| b.throttle.as_ref())
            .map(|w| w.max.max(0) as u64)
            .max()
            .unwrap_or(0);
        self.tick(&brokers, Duration::from_millis(throttle_ms), Instant::now());
    }

    /// Отделено от `observe`, чтобы измеритель проверялся без конструирования
    /// статистики librdkafka.
    fn tick(&self, brokers: &[(i32, u64)], throttle: Duration, now: Instant) {
        let mut m = self.lock();
        m.peak_throttle = m.peak_throttle.max(throttle);

        let rx_total: u64 = brokers.iter().map(|&(_, bytes)| bytes).sum();
        // Брокеры, чей счётчик вырос с прошлого тика. Ровно они и делят между
        // собой измеренную скорость, и ровно на их число делится квота.
        let mut active: u32 = 0;
        for &(id, total) in brokers {
            let previous = m.last_rx_by_broker.insert(id, total);
            if previous.is_some_and(|p| total > p) {
                active += 1;
            }
        }

        let (Some(last_rx), Some(last_tick)) = (m.last_rx_total, m.last_tick) else {
            m.last_rx_total = Some(rx_total);
            m.last_tick = Some(now);
            m.was_reading = m.reading;
            return;
        };

        let elapsed = now.saturating_duration_since(last_tick);
        let rx_bytes = rx_total.saturating_sub(last_rx);
        m.last_rx_total = Some(rx_total);
        m.last_tick = Some(now);

        let countable = m.reading && m.was_reading;
        m.was_reading = m.reading;
        if !countable || elapsed.is_zero() {
            m.pending_offsets = 0;
            return;
        }

        let offsets = std::mem::take(&mut m.pending_offsets);
        m.span += elapsed;
        m.rx_bytes += rx_bytes;
        m.offsets += offsets;
        m.buckets.push_back(Bucket {
            elapsed,
            rx_bytes,
            offsets,
            brokers: active,
        });

        while m.span > WINDOW && m.buckets.len() > 1 {
            let Some(oldest) = m.buckets.pop_front() else {
                break;
            };
            m.span -= oldest.elapsed;
            m.rx_bytes -= oldest.rx_bytes;
            m.offsets -= oldest.offsets;
        }
    }

    /// `None` — измерений пока недостаточно, чтобы на них опираться.
    pub fn estimate(&self) -> Option<QuotaEstimate> {
        let m = self.lock();
        if m.span < MIN_SPAN || m.rx_bytes == 0 || m.offsets <= 0 {
            return None;
        }
        Some(QuotaEstimate {
            bytes_per_sec: m.rx_bytes as f64 / m.span.as_secs_f64(),
            bytes_per_offset: m.rx_bytes as f64 / m.offsets as f64,
            peak_throttle: m.peak_throttle,
            // Максимум, а не среднее: отдельный брокер вполне может промолчать
            // один интервал, и среднее занижало бы делитель квоты именно там,
            // где на него смотрят.
            brokers: m.buckets.iter().map(|b| b.brokers).max().unwrap_or(0),
            known_brokers: m.last_rx_by_broker.len() as u32,
        })
    }

    /// Мьютекс держится только на время арифметики и никогда — во время
    /// обращений к Kafka, так что отравить его может лишь паника внутри самого
    /// измерителя. Такую восстанавливаем: терять из-за неё подключение незачем.
    fn lock(&self) -> std::sync::MutexGuard<'_, Meter> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Прогоняет измеритель через последовательность тиков по секунде.
    /// Каждый элемент — (принято байт за секунду, офсетов за секунду).
    /// Всё приходит с одного брокера.
    fn run(ticks: &[(u64, i64)]) -> QuotaMeter {
        let meter = QuotaMeter::new();
        meter.set_reading(true);
        let start = Instant::now();
        let mut rx_total = 0;
        // Первый тик только задаёт точку отсчёта, второй — первый засчитанный.
        meter.tick(&[(0, rx_total)], Duration::ZERO, start);
        for (i, &(bytes, offsets)) in ticks.iter().enumerate() {
            rx_total += bytes;
            meter.record_offsets(offsets);
            meter.tick(
                &[(0, rx_total)],
                Duration::ZERO,
                start + Duration::from_secs(i as u64 + 1),
            );
        }
        meter
    }

    #[test]
    fn no_estimate_until_there_is_enough_to_measure() {
        assert!(QuotaMeter::new().estimate().is_none());
        // Двух секунд мало.
        assert!(run(&[(100_000, 10), (100_000, 10)]).estimate().is_none());
        // Четырёх уже хватает.
        assert!(run(&[(100_000, 10); 4]).estimate().is_some());
    }

    #[test]
    fn measures_steady_throughput_and_the_price_of_an_offset() {
        let est = run(&[(200_000, 20); 10]).estimate().unwrap();
        assert!((est.bytes_per_sec - 200_000.0).abs() < 1.0, "{est:?}");
        assert!((est.bytes_per_offset - 10_000.0).abs() < 1.0, "{est:?}");
    }

    /// Ровно тот случай, ради которого всё и затевалось: кластер даёт
    /// всплеск, потом надолго замолкает. Оценка обязана показать среднее по
    /// циклу, а не скорость всплеска — иначе следующий раунд будет размером с
    /// весь burst-кредит и упрётся в многосекундную паузу.
    #[test]
    fn burst_followed_by_a_stall_averages_out_to_the_quota() {
        let mut ticks = vec![(2_000_000, 200); 2];
        ticks.extend(vec![(0, 0); 18]);
        let est = run(&ticks).estimate().unwrap();

        // 4 МБ за 20 секунд — 200 КБ/с, а не 2 МБ/с.
        assert!(
            est.bytes_per_sec > 150_000.0 && est.bytes_per_sec < 250_000.0,
            "оценка поймала burst вместо квоты: {est:?}"
        );
    }

    #[test]
    fn old_measurements_leave_the_window() {
        // Долгая медленная работа полностью вытесняет ранний всплеск.
        let mut ticks = vec![(10_000_000, 1000); 3];
        ticks.extend(vec![(100_000, 10); 40]);
        let est = run(&ticks).estimate().unwrap();
        assert!((est.bytes_per_sec - 100_000.0).abs() < 5_000.0, "{est:?}");
    }

    /// Часы измерителя стоят, пока не читаем: иначе пауза, во время которой
    /// пользователь просто смотрит на таблицу, засчиталась бы как нулевая
    /// скорость и следующее чтение начиналось бы с самого мелкого окна.
    #[test]
    fn idle_time_does_not_count_as_slowness() {
        let meter = QuotaMeter::new();
        let start = Instant::now();
        meter.set_reading(true);
        meter.tick(&[(0, 0)], Duration::ZERO, start);
        for i in 1..=6 {
            meter.record_offsets(20);
            meter.tick(
                &[(0, 200_000 * i)],
                Duration::ZERO,
                start + Duration::from_secs(i),
            );
        }
        let before = meter.estimate().unwrap();

        // Полчаса простоя без единого байта.
        meter.set_reading(false);
        meter.tick(
            &[(0, 1_200_000)],
            Duration::ZERO,
            start + Duration::from_secs(1800),
        );

        let after = meter.estimate().unwrap();
        assert_eq!(
            before.bytes_per_sec.round(),
            after.bytes_per_sec.round(),
            "простой не должен влиять на оценку"
        );
    }

    #[test]
    fn remembers_the_worst_throttling_seen() {
        let meter = QuotaMeter::new();
        let start = Instant::now();
        meter.set_reading(true);
        meter.tick(&[(0, 0)], Duration::ZERO, start);
        for i in 1..=4 {
            meter.record_offsets(10);
            meter.tick(
                &[(0, 100_000 * i)],
                Duration::from_millis(if i == 2 { 9_000 } else { 120 }),
                start + Duration::from_secs(i),
            );
        }
        assert_eq!(
            meter.estimate().unwrap().peak_throttle,
            Duration::from_millis(9_000)
        );
    }

    /// Ровно то, из-за чего измеренная скорость выглядит противоречащей
    /// настроенной квоте: `consumer_byte_rate` применяет каждый брокер
    /// самостоятельно, и суммарная скорость клиента кратна их числу.
    #[test]
    fn counts_the_brokers_the_bytes_came_from() {
        let meter = QuotaMeter::new();
        let start = Instant::now();
        meter.set_reading(true);

        // Пять брокеров отдают по 200 КБ/с каждый, шестой только подключён.
        let snapshot = |second: u64| -> Vec<(i32, u64)> {
            let mut brokers: Vec<(i32, u64)> =
                (0..5).map(|id| (id, 200_000 * second)).collect();
            brokers.push((5, 0));
            brokers
        };

        meter.tick(&snapshot(0), Duration::ZERO, start);
        for second in 1..=5 {
            meter.record_offsets(100);
            meter.tick(
                &snapshot(second),
                Duration::ZERO,
                start + Duration::from_secs(second),
            );
        }

        let est = meter.estimate().unwrap();
        assert_eq!(est.brokers, 5, "молчащий брокер не отдаёт данные: {est:?}");
        // Но в составе кластера он есть, и знаменатель это показывает.
        assert_eq!(est.known_brokers, 6, "{est:?}");
        // Один миллион в секунду суммарно — при квоте 200 КБ/с на брокера.
        assert!((est.bytes_per_sec - 1_000_000.0).abs() < 1.0, "{est:?}");
    }

    #[test]
    fn reset_forgets_the_previous_cluster() {
        let meter = run(&[(200_000, 20); 10]);
        assert!(meter.estimate().is_some());
        meter.reset();
        assert!(meter.estimate().is_none());
    }
}
