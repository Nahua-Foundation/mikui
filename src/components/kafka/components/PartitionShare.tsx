import { useEffect, useRef, useState } from 'react';
import { PartitionDetails } from '../types';

/**
 * Как распределены сообщения по партициям — полоса на сто процентов.
 *
 * Отвечает ровно на один вопрос: ровно ли лёг ключ. Перекос несёт ШИРИНА
 * сектора, и это главное; цвет только отделяет секторы друг от друга и
 * связывает их со строками таблицы под полосой.
 */

/**
 * Категориальная палитра для тёмной поверхности, в ФИКСИРОВАННОМ порядке.
 *
 * Порядок — не украшение, а механизм: именно он разводит СОСЕДНИЕ секторы для
 * тех, кто различает цвета иначе. Проверено валидатором против поверхности
 * приложения (`--color-surface`, #0f172b): худшая соседняя пара по CVD
 * ΔE 8.4, по обычному зрению 19.3, контраст всех восьми к фону выше 3:1.
 * Менять состав или порядок — только прогнав валидатор заново.
 *
 * `ink` — чем писать ПОВЕРХ заливки. Подпись внутри цветного пятна
 * единственная в интерфейсе не носит текстовый токен, поэтому цвет выбран по
 * яркости заливки: так каждая держит не меньше 4.5:1.
 */
const SERIES: { fill: string; ink: string }[] = [
  { fill: '#3987e5', ink: '#0f172b' },
  { fill: '#d95926', ink: '#0f172b' },
  { fill: '#199e70', ink: '#0f172b' },
  { fill: '#c98500', ink: '#0f172b' },
  { fill: '#d55181', ink: '#0f172b' },
  { fill: '#008300', ink: '#ffffff' },
  { fill: '#9085e9', ink: '#0f172b' },
  { fill: '#e66767', ink: '#0f172b' },
];

/**
 * Со скольки секторов полоса перестаёт быть полосой.
 *
 * Просветы между заливками берут по два пикселя из общей ширины независимо от
 * того, сколько данных в секторе. На двух сотнях партиций они съедают больше
 * половины окна, и «распределение» превращается в частокол. Столько партиций
 * — случай для таблицы, она рядом.
 */
const MAX_SEGMENTS = 48;

/** Просвет фона между заливками. Именно он разделяет секторы: обводка вокруг
 *  каждого добавила бы чернил, которых нет в данных. */
const GAP_PX = 2;
/** Ширина знака Fira Code при `text-[10px]`: advance 0.6em. */
const CHAR_PX = 6;
/** Сколько воздуха обязано остаться по краям подписи внутри сектора. */
const LABEL_PADDING_PX = 8;

/** Сколько сообщений в партиции. `null` — границы не сняты, и это НЕ ноль:
 *  про такую партицию не известно ничего, включая то, пуста ли она. */
export function messagesIn(partition: PartitionDetails): number | null {
  return partition.low === null || partition.high === null ? null : partition.high - partition.low;
}

/**
 * Какой партиции какой цвет. Ключ — номер партиции.
 *
 * Раскладка одна на полосу и на таблицу под ней: таблица служит легендой, и
 * разъехаться этим двум местам нельзя.
 *
 * Цвета идут по кругу, когда секторов больше восьми, и это осознанный размен.
 * Восемь — предел, в котором цвет способен ОПОЗНАВАТЬ: девятый оттенок,
 * достаточно далёкий от всех восьми сразу, взять неоткуда, а два десятка
 * цветов всё равно не удержать в голове. Зато цвет остаётся способен
 * РАЗДЕЛЯТЬ, и на топике из двенадцати партиций нужно именно это. Круг
 * безопасен потому, что порядок слотов подобран по СОСЕДНИМ парам: любые два
 * подряд идущих сектора различимы, включая пару на замыкании круга
 * (проверено валидатором, худшая соседняя пара не меняется). Опознание при
 * этом забирают номер на секторе и таблица — по номеру, а не по цвету.
 *
 * Ключ — номер партиции, а не позиция в исходном массиве: сектор получает
 * только партиция, в которой есть сообщения, и при дырках в нумерации
 * индексация по позиции выдавала бы двум СОСЕДНИМ секторам один цвет.
 *
 * Партиции без сектора в раскладку не попадают: цвет здесь значит «вот этот
 * сектор», и красить им нечего.
 */
export function partitionColors(
  partitions: PartitionDetails[],
): Map<number, { fill: string; ink: string }> {
  return new Map(
    partitions
      .filter((partition) => (messagesIn(partition) ?? 0) > 0)
      .map((partition, i) => [partition.id, SERIES[i % SERIES.length]]),
  );
}

interface PartitionShareProps {
  partitions: PartitionDetails[];
}

export function PartitionShare({ partitions }: PartitionShareProps) {
  const barRef = useRef<HTMLDivElement>(null);
  /** Ширина полосы в пикселях. Меряется, а не берётся из предположения о
   *  размере окна: от неё зависит, поместится ли подпись, а обрезанная подпись
   *  («12%» из «112%») врёт убедительнее, чем её отсутствие. */
  const [width, setWidth] = useState(0);

  useEffect(() => {
    const bar = barRef.current;
    if (!bar) return;
    const observer = new ResizeObserver(([entry]) => setWidth(entry.contentRect.width));
    observer.observe(bar);
    return () => observer.disconnect();
  }, []);

  // Пустая партиция не занимает места в распределении, партиция без снятых
  // границ в нём не участвует вовсе — но это разные вещи, и вторая делает
  // проценты неполными. Обе остаются в таблице.
  const counted = partitions
    .map((partition) => ({ partition, messages: messagesIn(partition) ?? 0 }))
    .filter((entry) => entry.messages > 0);
  const unmeasured = partitions.filter((partition) => messagesIn(partition) === null).length;

  const total = counted.reduce((sum, entry) => sum + entry.messages, 0);
  if (total === 0 || counted.length > MAX_SEGMENTS) return null;

  const colors = partitionColors(partitions);

  /** Пиксели, которые делят между собой заливки: просветы забирают своё
   *  первыми, сколько бы данных ни было в секторах. */
  const usable = Math.max(0, width - GAP_PX * (counted.length - 1));

  /** Подпись, которая ПОМЕСТИТСЯ в сектор такой доли, — иначе ничего. */
  const fitting = (share: number, full: string, short: string): string | null => {
    const room = share * usable;
    if (room >= full.length * CHAR_PX + LABEL_PADDING_PX) return full;
    if (room >= short.length * CHAR_PX + LABEL_PADDING_PX) return short;
    return null;
  };

  return (
    <div className="space-y-1.5">
      {/* Когда границы сняты не у всех партиций, проценты считаются от
          измеренного, а не от топика. Промолчать об этом значило бы выдать
          часть за целое — а полоса на сто процентов именно так и читается. */}
      <div className="font-mono text-[11px] text-dim">
        messages per partition
        {unmeasured > 0 && (
          <span
            className="text-brand"
            title={`Offsets of ${unmeasured} partition${
              unmeasured > 1 ? 's' : ''
            } could not be read in time, so these shares are of the measured part, not of the whole topic.`}
          >
            {' '}
            · {partitions.length - unmeasured} of {partitions.length} measured
          </span>
        )}
      </div>
      <div
        ref={barRef}
        className="flex h-5 w-full flex-row"
        style={{ gap: `${GAP_PX}px` }}
      >
        {counted.map(({ partition, messages }) => {
          const share = messages / total;
          const percent = Math.round(share * 100);
          const { fill, ink } = colors.get(partition.id) ?? SERIES[0];
          return (
            <div
              key={partition.id}
              // Долями, а не процентной шириной: просветы съедают место
              // первыми, и проценты в сумме давали бы переполнение.
              style={{ flex: `${share} 1 0`, backgroundColor: fill, color: ink }}
              className="flex items-center justify-center whitespace-nowrap font-mono text-[10px] leading-none first:rounded-l last:rounded-r"
              title={`Partition ${partition.id}: ${messages.toLocaleString()} messages, ${
                share < 0.01 ? '<1' : percent
              }% of the topic`}
            >
              {fitting(share, `${partition.id} · ${percent}%`, String(partition.id))}
            </div>
          );
        })}
      </div>
    </div>
  );
}
