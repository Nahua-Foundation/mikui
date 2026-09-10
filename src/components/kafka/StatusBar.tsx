import { formatBytes } from './format';
import { OpenTopicResult } from './types';

/**
 * Полоска состояния под таблицей.
 *
 * Всё это раньше жило в шапке крупным шрифтом и на длинном топике не влезало
 * туда вместе с селекторами. Сведения о том, СКОЛЬКО уже прочитано и чего это
 * стоило, — фоновые: смотреть на них постоянно не нужно, а вот пропадать из
 * вида они не должны, иначе медленное чтение под квотой неотличимо от
 * зависшего приложения.
 */

/** Доля буфера, после которой шкала перестаёт быть просто индикатором. */
const MEMORY_WARN = 0.75;

/** Задержка «5 ms» и задержка «5 s» — это две разные новости. */
function formatDuration(millis: number): string {
  if (millis < 1000) return `${millis} ms`;
  return `${(millis / 1000).toFixed(millis < 10_000 ? 1 : 0)} s`;
}

/**
 * Что именно показывает цифра скорости — вопрос, который она вызывает первым:
 * измеренные мегабайты в секунду под квотой в сотни килобайт выглядят
 * невозможными, пока не сказано, что это сумма по брокерам.
 */
function throughputHint(stats: OpenTopicResult): string {
  const { read_bytes_per_sec: rate, active_brokers: active, known_brokers: known } = stats;
  const base =
    'Bytes received from the network, averaged over up to 20 s of active reading' +
    ' (the clock stops between reads, so this is the last measured rate).';
  if (rate === null || active <= 1) return base;

  // Знаменатель тут не для полноты: без «из десяти» число отдающих не с чем
  // сверить, а лидеры читаемых партиций почти всегда сидят на подмножестве
  // узлов — и «семь» при десяти брокерах не ошибка, а норма.
  const of = known > active ? ` of ${known} in the cluster` : '';
  return (
    `${base} Summed across ${active} brokers${of} — about ` +
    `${formatBytes(Math.round(rate / active))}/s each. Kafka enforces a consumer ` +
    'byte-rate quota per broker, so the total legitimately exceeds a per-broker limit.'
  );
}

interface StatusBarProps {
  stats: OpenTopicResult | null;
}

export function StatusBar({ stats }: StatusBarProps) {
  if (!stats) return null;

  const limit = stats.memory_limit || 1;
  const used = Math.min(stats.memory_bytes / limit, 1);
  const warn = used >= MEMORY_WARN;

  return (
    <div className="box-border flex flex-row items-center justify-between gap-4 w-full shrink-0 border-t border-edge px-4 py-1.5 font-mono text-xs text-dim">
      <div className="flex flex-row items-center gap-2 min-w-0 truncate">
        <span className="text-soft">{stats.loaded.toLocaleString()} msg</span>

        {/* После глубокого поиска в буфере лежат ОДНИ находки, и «3 msg» без
            второго числа читается как «в топике три сообщения». Показываем,
            сколько за этими тремя просмотрено. Пока числа совпадают — то есть
            при обычном чтении — второго нет: там оно ничего не добавляет. */}
        {stats.scanned > stats.loaded && (
          <>
            <span>·</span>
            <span
              title={
                'How many messages were read from the topic to produce the rows above. ' +
                'A deep search keeps only the matches, so it looks at far more than it shows.'
              }
            >
              {stats.scanned.toLocaleString()} scanned
              {stats.approx_total !== null && ` of ~${stats.approx_total.toLocaleString()}`}
            </span>
          </>
        )}

        {/* Скорость, которую даёт кластер. Без неё медленная загрузка
            неотличима от зависшего приложения — а на кластере с квотой на
            чтение она медленная всегда.

            Число брокеров стоит рядом не для полноты: `consumer_byte_rate`
            применяет КАЖДЫЙ брокер к своей доле трафика, поэтому суммарная
            цифра кратна их числу и без делителя выглядит противоречащей
            настроенной квоте. */}
        {stats.read_bytes_per_sec !== null && (
          <>
            <span>·</span>
            <span title={throughputHint(stats)}>
              {formatBytes(stats.read_bytes_per_sec)}/s
              {stats.active_brokers > 1 && (
                <span className="text-dim">
                  {' '}
                  ({stats.active_brokers}×
                  {formatBytes(Math.round(stats.read_bytes_per_sec / stats.active_brokers))})
                </span>
              )}
            </span>
          </>
        )}

        {/* Слово «truncated» ничего не сообщало о том, что делать: прочитано
            не всё, и остальное берётся кнопкой «Load more». */}
        {stats.truncated && (
          <>
            <span>·</span>
            <span
              className="text-brand"
              title="Not the whole topic was read: the per-partition limit, the memory limit or the round deadline came first. Use Load more to continue."
            >
              partial read
            </span>
          </>
        )}

        {/* Красным, а не акцентным: throttled объясняет, почему всё идёт
            медленно. С величиной — потому что 5 мс и 5 с это две разные
            новости, а слово для них было одно. */}
        {stats.peak_throttle_ms > 0 && (
          <>
            <span>·</span>
            <span
              className="text-danger"
              title={`The broker held a response back for as long as ${formatDuration(
                stats.peak_throttle_ms,
              )} — that is the worst single delay seen, not a total. A read quota is in effect.`}
            >
              throttled {formatDuration(stats.peak_throttle_ms)}
            </span>
          </>
        )}
      </div>

      <div
        className="flex flex-row items-center gap-2 shrink-0"
        title={`Message buffer: ${formatBytes(stats.memory_bytes)} of ${formatBytes(
          stats.memory_limit,
        )} (payload ${formatBytes(stats.buffer_bytes)}). Reading stops at the limit.`}
      >
        <span>memory</span>
        <div className="relative h-1.5 w-24 rounded-full bg-edge overflow-hidden">
          <div
            className={`absolute left-0 top-0 h-full rounded-full transition-[width] duration-300 ${
              warn ? 'bg-danger' : 'bg-brand'
            }`}
            // Ширина считается от данных и в классы Tailwind не укладывается.
            style={{ width: `${Math.max(used * 100, used > 0 ? 2 : 0)}%` }}
          />
        </div>
        <span className={warn ? 'text-danger' : 'text-soft'}>
          {formatBytes(stats.memory_bytes)}
        </span>
      </div>
    </div>
  );
}
