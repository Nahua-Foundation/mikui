import { memo, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { ReadScope, RowPreview, SortColumn, SortDirection, SortSpec } from './types';
import { formatTimestamp } from './format';
import { OP_LABELS } from './lens';
import { Virtuoso } from 'react-virtuoso';

/**
 * Цвет метки операции CDC.
 *
 * Не украшение: в потоке изменений глаз ищет удаления, и одинаково серые метки
 * заставляли бы читать каждую. Вставка и удаление — по краям смысла, обновление
 * и снапшот-чтение нейтральны.
 */
const OP_COLORS: Record<string, string> = {
  c: 'text-syntax-string',
  u: 'text-brand',
  d: 'text-danger',
  r: 'text-dim',
  t: 'text-danger',
  m: 'text-dim',
};

const COLUMNS = ['partition', 'offset', 'key', 'message', 'timestamp'] as const;
const DEFAULT_WIDTHS = ['80px', '100px', '120px', '1fr', '190px'];
const MIN_COLUMN_PX = 60;

/** `message` не несёт своего значения для сортировки — это склеенный из
 *  нескольких полей превью тела, кликать по нему незачем. */
function sortColumnFor(label: (typeof COLUMNS)[number]): SortColumn | null {
  return label === 'message' ? null : label;
}

/** unsorted → asc → desc → unsorted. */
function nextDirection(current: SortDirection | null): SortDirection | null {
  if (current === null) return 'asc';
  if (current === 'asc') return 'desc';
  return null;
}

interface MessageRowProps {
  row: RowPreview;
  onSelect: (index: number) => void;
  /** Строка, ради которой открыли топик по ссылке. */
  highlighted: boolean;
  /** Курсор дошёл до подсвеченной строки — подсказка своё отработала. */
  onHighlightSeen: () => void;
}

/** memo: при подгрузке чанка перерисовываются только новые строки,
 *  а не весь видимый список. */
const MessageRow = memo(function MessageRow({
  row,
  onSelect,
  highlighted,
  onHighlightSeen,
}: MessageRowProps) {
  return (
    <div
      // Подсветка — тот же `hover:bg-elevated`, только не по курсору: строка,
      // найденную по ссылке, надо показать среди соседей, а рисовать ради
      // этого отдельный цвет значило бы заводить второй язык выделения.
      // Гаснет, как только человек до неё дотянулся курсором: дальше он и сам
      // знает, где она.
      className={`border-b border-edge cursor-pointer hover:bg-elevated grid gap-4 p-3 text-sm ${
        highlighted ? 'bg-elevated' : ''
      }`}
      style={{ gridTemplateColumns: 'var(--mikui-grid)' }}
      onClick={() => onSelect(row.index)}
      onMouseEnter={highlighted ? onHighlightSeen : undefined}
    >
      <div className="font-mono text-brand">{row.partition}</div>
      <div className="font-mono text-soft">{row.offset}</div>
      <div className="font-mono text-soft truncate">{row.key}</div>
      <div className="font-mono text-soft truncate">
        {/* Операция CDC. Тем же приёмом, что и метки ниже, а не отдельной
            колонкой: колонки и их растягивание трогать ради этого не за чем, а
            рядом с телом метке и место — она объясняет, ЧТО именно за тело
            дальше (у удаления это прежняя строка, а не новая). */}
        {row.lens_tag && (
          <span
            className={`mr-2 ${OP_COLORS[row.lens_tag] ?? 'text-soft'}`}
            title={`Debezium: ${OP_LABELS[row.lens_tag] ?? row.lens_tag}`}
          >
            [{row.lens_tag}]
          </span>
        )}
        {/* Схема есть, но это сообщение по ней не разобралось. Метка нужна
            затем, что дальше идёт обычный текст, и без неё строка выглядела бы
            так, будто схема к топику вовсе не загружена. */}
        {row.decode_error && (
          <span className="text-brand mr-2" title={row.decode_error}>
            [undecoded]
          </span>
        )}
        {row.binary && !row.decode_error && (
          <span
            className="text-brand mr-2"
            title={`${row.value_size} bytes, not valid UTF-8`}
          >
            [binary]
          </span>
        )}
        {row.preview}
      </div>
      <div className="font-mono text-soft truncate">{formatTimestamp(row.timestamp)}</div>
    </div>
  );
});

/** Строка, чей чанк ещё летит. Появляется редко — соседние чанки подгружаются
 *  заранее, — но пустоту вместо строки показывать нельзя. */
function PlaceholderRow() {
  return (
    <div
      className="border-b border-edge grid gap-4 p-3 text-sm"
      style={{ gridTemplateColumns: 'var(--mikui-grid)' }}
    >
      {COLUMNS.map((c) => (
        <div key={c} className="h-4 rounded bg-edge/40 animate-pulse" />
      ))}
    </div>
  );
}

/**
 * Ход глубокого поиска: сколько просмотрено, сколько нашлось, и кнопка «стоп».
 *
 * Шкала здесь не украшение. Поиск идёт минутами и почти всё это время таблица
 * под ним пуста — без знаменателя «просмотрено 12 345» неотличимо от зависшего
 * приложения, а именно на этом кластере медленное чтение под квотой выглядит
 * зависанием чаще, чем хотелось бы.
 *
 * Знаменатель приблизительный, и в подписи это сказано словом «~», а не
 * умолчанием: это число офсетов, а на компактированных партициях их больше, чем
 * сообщений (см. `ReadScope.approx_total`). Дойдя до конца, поиск честно
 * остановится «на 80 000 из ~92 000» — и объяснение этому лежит в подсказке.
 */
function SearchProgress({
  scope,
  found,
  onStop,
}: {
  scope: ReadScope;
  found: number;
  onStop: () => void;
}) {
  const { scanned, approx_total: approx } = scope;
  // Доля считается только когда знаменатель есть И он осмысленный. Пока
  // границы партиций не сняты, шкалы нет вовсе: полоса, стоящая на нуле,
  // выглядит как «не двигается», а не как «ещё не знаем сколько».
  const share = approx && approx > 0 ? Math.min(scanned / approx, 1) : null;

  return (
    <div className="flex flex-row items-center gap-3 border-b border-edge bg-surface px-3 py-1.5 font-mono text-xs shrink-0">
      <span className="text-brand shrink-0">searching…</span>
      <span className="text-soft shrink-0">
        {scanned.toLocaleString()}
        {approx !== null && ` of ~${approx.toLocaleString()}`} scanned
      </span>
      <span className="text-dim shrink-0">·</span>
      <span className={found > 0 ? 'text-strong shrink-0' : 'text-dim shrink-0'}>
        {found.toLocaleString()} found
      </span>

      {share !== null && (
        <div
          className="relative h-1.5 min-w-16 flex-1 overflow-hidden rounded-full bg-edge"
          title={
            `Scanned ${scanned.toLocaleString()} of about ${approx?.toLocaleString()} messages. ` +
            'The total is an estimate from partition offsets: on a compacted topic there are ' +
            'more offsets than surviving messages, so the search can legitimately finish short ' +
            'of it.'
          }
        >
          <div
            className="absolute left-0 top-0 h-full rounded-full bg-brand transition-[width] duration-300"
            // Ширина считается от данных и в классы Tailwind не укладывается.
            style={{ width: `${Math.max(share * 100, share > 0 ? 2 : 0)}%` }}
          />
        </div>
      )}

      <button
        type="button"
        onClick={onStop}
        title="Stop the search and re-read the topic the usual way"
        className="ml-auto shrink-0 cursor-pointer rounded border border-edge px-2 py-0.5 text-soft hover:bg-elevated hover:text-strong"
      >
        Stop
      </button>
    </div>
  );
}

/**
 * Насколько глубоко в топик заглянул фильтр.
 *
 * Одна фраза на два места — пустую таблицу и футер под найденным. Числа в них
 * одни и те же, и разойтись их формулировкам нельзя: пользователь сверяет
 * «нашлось два» именно с этой строкой, решая, довериться результату или искать
 * дальше.
 */
function coverage(scanned: number, approx: number | null, searched: boolean): string {
  const of = approx !== null && approx > scanned ? ` of about ${approx.toLocaleString()}` : '';
  // «Просмотрено» против «загружено»: после глубокого поиска в буфере лежат
  // единицы, а просмотрены были десятки тысяч, и «загружено» показало бы не ту
  // цифру, которой мерится проделанная работа.
  return searched
    ? `${scanned.toLocaleString()} messages searched${of}`
    : `${scanned.toLocaleString()} messages read so far${of}`;
}

/**
 * Пустая таблица с активным фильтром.
 *
 * До этого экрана здесь было просто «No messages», и это был тупик: кнопка
 * «Load more» живёт в футере списка, а списка при нуле строк нет вовсе —
 * дочитать топик было нельзя никак, кроме сброса фильтра. Теперь тут сказано,
 * ЧТО именно просмотрено, и предложено единственное действие, которое имеет
 * смысл дальше.
 */
function NothingMatched({
  scanned,
  approx,
  canSearchDeeper,
  searchedBuffer,
  onDeepSearch,
}: {
  scanned: number;
  approx: number | null;
  canSearchDeeper: boolean;
  searchedBuffer: boolean;
  onDeepSearch: () => void;
}) {
  return (
    <div className="mx-auto max-w-md p-6 text-center font-mono">
      <div className="text-soft text-sm">
        Nothing matched — {coverage(scanned, approx, searchedBuffer)}.
      </div>

      {canSearchDeeper ? (
        <>
          {/* Сказано и то, сколько это стоит, и то, что операция отменяемая:
              без первого кнопку не нажмут на большом топике, без второго
              нажмут и испугаются, что приложение зависло. */}
          <div className="mt-3 text-xs text-dim leading-relaxed">
            {searchedBuffer
              ? 'The search did not reach the end of the topic. Running it again starts over ' +
                'from the same end — narrow the filter first if it stopped on a full buffer.'
              : 'The rest of the topic has not been read yet. A deep search reads it through ' +
                'and keeps only the matches — this takes a while on a large topic, and you ' +
                'can stop it at any time.'}
          </div>
          <button
            type="button"
            onClick={onDeepSearch}
            className="mt-4 cursor-pointer rounded bg-brand px-4 py-2 text-sm text-surface hover:bg-brand-hover"
          >
            {searchedBuffer ? 'Search again' : 'Search the whole topic'}
          </button>
        </>
      ) : (
        <div className="mt-3 text-xs text-dim leading-relaxed">
          {searchedBuffer
            ? 'The whole topic has been searched — nothing in it matches this filter.'
            : 'The whole topic has been read — there is nothing else to search.'}
        </div>
      )}
    </div>
  );
}

interface MessagesPanelProps {
  total: number;
  getRow: (index: number) => RowPreview | undefined;
  onRangeChanged: (startIndex: number, endIndex: number) => void;
  onSelectMessage: (index: number) => void;
  isLoading: boolean;
  /** Растёт при подгрузке чанка — сигнал перерисовать видимые строки. */
  version: number;
  /** Можно предложить обычную догрузку. Уже, чем `truncated`: в буфер после
   *  глубокого поиска дописывать непросеянное нельзя. */
  canLoadMore: boolean;
  /** В топике осталось непрочитанное — независимо от того, чем его дочитывать. */
  truncated: boolean;
  isLoadingMore: boolean;
  onLoadMore: () => void;
  /** Фильтр не пуст. От этого зависит, что означает пустая таблица: «в топике
   *  ничего нет» или «ничего не подошло». */
  hasFilter: boolean;
  /** Идёт глубокий поиск по всему топику. */
  isSearching: boolean;
  /** В буфере лежит добыча поиска, а не обычное чтение: непопавшее под фильтр
   *  в него не клали. Меняет смысл счётчиков под таблицей. */
  searchedBuffer: boolean;
  /** Сколько просмотрено и сколько всего — знаменатель приблизительный
   *  (см. `ReadScope`). `null` — открытого топика нет. */
  scope: ReadScope | null;
  onDeepSearch: () => void;
  onStopSearch: () => void;
  /** Клик по заголовку колонки. `null` — обычный порядок чтения. */
  sort: SortSpec | null;
  onSortChange: (sort: SortSpec | null) => void;
  /**
   * Сообщение, ради которого топик открыли по ссылке. `null` — обычное чтение.
   *
   * Координатами, а не индексом строки: индекс живёт до ближайшей смены
   * фильтра или сортировки, а подсветка должна пережить и то, и другое —
   * сообщение от этого не перестаёт быть тем, за которым пришли.
   */
  highlight: { partition: number; offset: number } | null;
  onHighlightSeen: () => void;
}

export function MessagesPanel({
  total,
  getRow,
  onRangeChanged,
  onSelectMessage,
  isLoading,
  version,
  canLoadMore,
  truncated,
  isLoadingMore,
  onLoadMore,
  hasFilter,
  isSearching,
  searchedBuffer,
  scope,
  onDeepSearch,
  onStopSearch,
  sort,
  onSortChange,
  highlight,
  onHighlightSeen,
}: MessagesPanelProps) {
  const [colWidths, setColWidths] = useState<string[]>(DEFAULT_WIDTHS);
  const rootRef = useRef<HTMLDivElement>(null);
  const dragging = useRef<{ index: number; startX: number; startW: number; dir: 1 | -1 } | null>(null);
  const pendingWidths = useRef<string[] | null>(null);

  const gridTemplate = useMemo(() => colWidths.join(' '), [colWidths]);

  // Ширины колонок живут в CSS-переменной. Раньше здесь был setState на каждый
  // mousemove, то есть полная перерисовка таблицы на каждое движение мыши.
  // Теперь во время перетаскивания React не участвует вообще, а в state
  // результат попадает один раз — на отпускании кнопки.
  const applyGrid = useCallback((template: string) => {
    rootRef.current?.style.setProperty('--mikui-grid', template);
  }, []);

  useEffect(() => applyGrid(gridTemplate), [gridTemplate, applyGrid]);

  const onMouseMove = useCallback(
    (e: MouseEvent) => {
      const drag = dragging.current;
      if (!drag) return;
      const width = Math.max(MIN_COLUMN_PX, drag.startW + drag.dir * (e.clientX - drag.startX));
      const next = [...colWidths];
      next[drag.index] = `${width}px`;
      pendingWidths.current = next;
      applyGrid(next.join(' '));
    },
    [colWidths, applyGrid],
  );

  const stopDragging = useCallback(() => {
    if (!dragging.current) return;
    dragging.current = null;
    window.removeEventListener('mousemove', onMouseMove);
    window.removeEventListener('mouseup', stopDragging);
    if (pendingWidths.current) {
      setColWidths(pendingWidths.current);
      pendingWidths.current = null;
    }
  }, [onMouseMove]);

  const startDragging = useCallback(
    (index: number, dir: 1 | -1, e: React.MouseEvent) => {
      // Гибкая колонка растягивается на всё оставшееся место, её не тянем.
      if (colWidths[index] === '1fr') return;
      const current = colWidths[index];
      const startW = current.endsWith('px')
        ? parseInt(current, 10) || MIN_COLUMN_PX
        : MIN_COLUMN_PX;
      dragging.current = { index, startX: e.clientX, startW, dir };
      window.addEventListener('mousemove', onMouseMove);
      window.addEventListener('mouseup', stopDragging);
      e.preventDefault();
      e.stopPropagation();
    },
    [colWidths, onMouseMove, stopDragging],
  );

  useEffect(
    () => () => {
      window.removeEventListener('mousemove', onMouseMove);
      window.removeEventListener('mouseup', stopDragging);
    },
    [onMouseMove, stopDragging],
  );

  const handleHeaderClick = useCallback(
    (label: (typeof COLUMNS)[number]) => {
      const column = sortColumnFor(label);
      if (!column) return;
      const current = sort?.column === column ? sort.direction : null;
      const direction = nextDirection(current);
      onSortChange(direction ? { column, direction } : null);
    },
    [sort, onSortChange],
  );

  const handleRangeChanged = useCallback(
    ({ startIndex, endIndex }: { startIndex: number; endIndex: number }) =>
      onRangeChanged(startIndex, endIndex),
    [onRangeChanged],
  );

  const renderItem = useCallback(
    (index: number) => {
      const row = getRow(index);
      if (!row) return <PlaceholderRow />;
      const highlighted =
        !!highlight && row.partition === highlight.partition && row.offset === highlight.offset;
      return (
        <MessageRow
          row={row}
          onSelect={onSelectMessage}
          highlighted={highlighted}
          onHighlightSeen={onHighlightSeen}
        />
      );
    },
    // version в зависимостях намеренно: подгрузился чанк — перерисовываем.
    [getRow, onSelectMessage, version, highlight, onHighlightSeen],
  );

  /**
   * Что стоит под последней найденной строкой.
   *
   * Главное здесь — то, чего тут раньше не было вовсе: пока топик дочитан не до
   * конца, найденное НЕ ЗНАЧИТ «всё найденное». Две подошедшие строки выглядят
   * как исчерпывающий ответ, хотя это ответ по той тысяче сообщений на
   * партицию, которую успели прочитать. Поэтому предложение искать глубже стоит
   * здесь независимо от того, нашлось что-нибудь или нет, — как и объяснение,
   * по какой части топика получен текущий ответ.
   */
  const Footer = useCallback(() => {
    // Поиск оборвался, не дойдя до конца топика, — почти всегда потому, что
    // буфер заполнился находками. Обычной догрузкой это не лечится (она
    // дописала бы непросеянное), поэтому вместо кнопок — что делать дальше.
    //
    // `!isSearching` обязателен: пока поиск ИДЁТ, «в топике есть ещё» верно по
    // определению, и без этой проверки объявление о том, что он остановился,
    // висело бы под таблицей всё время его работы.
    if (searchedBuffer && truncated && !isSearching) {
      return (
        <div className="p-3 text-center font-mono text-xs text-dim">
          The search stopped before the end of the topic — the buffer is full of matches.
          Narrow the filter and search again to get past this point.
        </div>
      );
    }

    // Глубже искать нечего, дочитывать нечего — футер пуст, как и раньше.
    if (!canLoadMore) return null;

    const offerSearch = hasFilter && !searchedBuffer;
    return (
      <div className="flex flex-col items-center gap-2 p-3 font-mono">
        {offerSearch && scope && (
          <div className="text-center text-xs text-dim">
            These matches are from the {coverage(scope.scanned, scope.approx_total, false)} —
            the rest of the topic has not been read yet.
          </div>
        )}
        <div className="flex flex-row items-center gap-2">
          <button
            type="button"
            onClick={onLoadMore}
            disabled={isLoadingMore}
            className="px-4 py-2 text-sm font-mono rounded border border-edge text-soft hover:bg-elevated disabled:opacity-50 disabled:cursor-not-allowed"
          >
            {isLoadingMore ? 'Loading…' : 'Load more'}
          </button>
          {/* Рядом с «Load more», а не вместо неё: это два разных ответа на
              «мало нашлось». Дочитать ещё тысячу на партицию дёшево и часто
              достаточно; пройти топик целиком дорого и отменяемо. Выбор между
              ними зависит от того, насколько редкое ищут, — а этого знать
              отсюда нельзя. */}
          {offerSearch && (
            <button
              type="button"
              onClick={onDeepSearch}
              disabled={isLoadingMore}
              title="Read the topic through to the end, keeping only the matches. Takes a while on a large topic; you can stop it at any time."
              className="rounded bg-brand px-4 py-2 text-sm text-surface hover:bg-brand-hover disabled:opacity-50 disabled:cursor-not-allowed"
            >
              Search the whole topic
            </button>
          )}
        </div>
      </div>
    );
  }, [
    canLoadMore,
    truncated,
    searchedBuffer,
    hasFilter,
    scope,
    isSearching,
    isLoadingMore,
    onLoadMore,
    onDeepSearch,
  ]);

  const virtuosoComponents = useMemo(() => ({ Footer }), [Footer]);

  return (
    <div
      ref={rootRef}
      // `h-full` здесь больше нет: под таблицей появилась полоска состояния,
      // и высота в 100% родителя спорила бы с ней за место.
      className="flex-1 min-h-0 flex flex-col"
      style={{ '--mikui-grid': gridTemplate } as React.CSSProperties}
    >
      <div className="border-b border-edge bg-surface">
        <div
          className="grid gap-4 p-3 text-sm font-mono text-soft select-none"
          style={{ gridTemplateColumns: 'var(--mikui-grid)' }}
        >
          {COLUMNS.map((label, i) => {
            // Гибкая колонка (1fr) сама не тянется; всё, что до неё, растёт
            // за счёт ручки справа, всё, что после — за счёт ручки слева
            // (её правая ручка висела бы за пределами таблицы).
            const flexIndex = colWidths.indexOf('1fr');
            const showRightHandle = colWidths[i] !== '1fr' && (flexIndex === -1 || i < flexIndex);
            const showLeftHandle = colWidths[i] !== '1fr' && flexIndex !== -1 && i > flexIndex;
            const column = sortColumnFor(label);
            const active = column && sort?.column === column ? sort.direction : null;
            return (
              <div key={label} className="relative">
                <div
                  className={column ? 'cursor-pointer hover:text-brand' : undefined}
                  onClick={column ? () => handleHeaderClick(label) : undefined}
                >
                  {label}
                  {active && <span className="ml-1">{active === 'asc' ? '▲' : '▼'}</span>}
                </div>
                {showRightHandle && (
                  <div
                    onMouseDown={(e) => startDragging(i, 1, e)}
                    className="absolute top-0 right-[-8px] h-full w-4 cursor-col-resize"
                    style={{
                      backgroundImage:
                        'linear-gradient(to right, transparent 7px, var(--color-edge) 7px, var(--color-edge) 8px, transparent 8px)',
                    }}
                    title="Drag to resize"
                  />
                )}
                {showLeftHandle && (
                  <div
                    onMouseDown={(e) => startDragging(i, -1, e)}
                    className="absolute top-0 left-[-16px] h-full w-4 cursor-col-resize"
                    style={{
                      backgroundImage:
                        'linear-gradient(to right, transparent 7px, var(--color-edge) 7px, var(--color-edge) 8px, transparent 8px)',
                    }}
                    title="Drag to resize"
                  />
                )}
              </div>
            );
          })}
        </div>
      </div>

      <div className="flex-1 min-h-0 overflow-hidden h-full flex flex-col">
        {/* Поиск идёт своей полосой и ВСЕГДА, в том числе когда таблица под ней
            пуста: пустая таблица — обычное состояние поиска на протяжении
            большей части его работы, и именно тогда шкала нужна больше всего.
            Полоса обычного чтения показывается по прежнему правилу — только
            когда что-то уже нашлось; до первых строк своё сообщение есть ниже. */}
        {isSearching ? (
          // `scope` подставляется пустым, а не гасит полосу: без неё исчезла бы
          // и кнопка «стоп», а долгая операция без способа её прервать — это
          // то самое, чего здесь быть не должно.
          <SearchProgress
            scope={scope ?? { scanned: 0, approx_total: null }}
            found={total}
            onStop={onStopSearch}
          />
        ) : (
          isLoading &&
          total > 0 && (
            <div className="px-3 py-1 text-xs font-mono text-soft border-b border-edge bg-surface shrink-0">
              Loading… {total} so far
            </div>
          )
        )}

        {total > 0 ? (
          <Virtuoso
            style={{ height: '100%' }}
            totalCount={total}
            rangeChanged={handleRangeChanged}
            itemContent={renderItem}
            components={virtuosoComponents}
          />
        ) : isSearching ? (
          // Коротко и без объяснений: сколько просмотрено, сколько нашлось и
          // как остановиться — всё это уже сказано полосой прямо над этим
          // местом, и повторять её здесь значило бы занять экран дважды.
          <div className="p-4 text-center font-mono text-dim">No matches yet</div>
        ) : isLoading ? (
          <div className="p-4 text-center text-soft font-mono">Reading from Kafka…</div>
        ) : hasFilter ? (
          <NothingMatched
            scanned={scope?.scanned ?? 0}
            approx={scope?.approx_total ?? null}
            // Именно `truncated`, а не `canLoadMore`: обычная догрузка после
            // поиска запрещена, а вот повторить поиск можно всегда, пока в
            // топике осталось непрочитанное.
            canSearchDeeper={truncated}
            searchedBuffer={searchedBuffer}
            onDeepSearch={onDeepSearch}
          />
        ) : (
          <div className="p-4 text-center text-dim font-mono">No messages</div>
        )}
      </div>
    </div>
  );
}
