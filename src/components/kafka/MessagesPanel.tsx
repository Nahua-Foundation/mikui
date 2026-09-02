import { memo, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { RowPreview, SortColumn, SortDirection, SortSpec } from './types';
import { formatTimestamp } from './format';
import { Virtuoso } from 'react-virtuoso';

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
}

/** memo: при подгрузке чанка перерисовываются только новые строки,
 *  а не весь видимый список. */
const MessageRow = memo(function MessageRow({ row, onSelect }: MessageRowProps) {
  return (
    <div
      className="border-b border-edge cursor-pointer hover:bg-elevated grid gap-4 p-3 text-sm"
      style={{ gridTemplateColumns: 'var(--mikui-grid)' }}
      onClick={() => onSelect(row.index)}
    >
      <div className="font-mono text-brand">{row.partition}</div>
      <div className="font-mono text-soft">{row.offset}</div>
      <div className="font-mono text-soft truncate">{row.key}</div>
      <div className="font-mono text-soft truncate">
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

interface MessagesPanelProps {
  total: number;
  getRow: (index: number) => RowPreview | undefined;
  onRangeChanged: (startIndex: number, endIndex: number) => void;
  onSelectMessage: (index: number) => void;
  isLoading: boolean;
  /** Растёт при подгрузке чанка — сигнал перерисовать видимые строки. */
  version: number;
  /** В топике есть ещё непрочитанные сообщения — показать кнопку. */
  canLoadMore: boolean;
  isLoadingMore: boolean;
  onLoadMore: () => void;
  /** Клик по заголовку колонки. `null` — обычный порядок чтения. */
  sort: SortSpec | null;
  onSortChange: (sort: SortSpec | null) => void;
}

export function MessagesPanel({
  total,
  getRow,
  onRangeChanged,
  onSelectMessage,
  isLoading,
  version,
  canLoadMore,
  isLoadingMore,
  onLoadMore,
  sort,
  onSortChange,
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
      return row ? <MessageRow row={row} onSelect={onSelectMessage} /> : <PlaceholderRow />;
    },
    // version в зависимостях намеренно: подгрузился чанк — перерисовываем.
    [getRow, onSelectMessage, version],
  );

  const Footer = useCallback(() => {
    if (!canLoadMore) return null;
    return (
      <div className="p-3 flex justify-center">
        <button
          type="button"
          onClick={onLoadMore}
          disabled={isLoadingMore}
          className="px-4 py-2 text-sm font-mono rounded border border-edge text-soft hover:bg-elevated disabled:opacity-50 disabled:cursor-not-allowed"
        >
          {isLoadingMore ? 'Loading…' : 'Load more'}
        </button>
      </div>
    );
  }, [canLoadMore, isLoadingMore, onLoadMore]);

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
        {isLoading && total > 0 && (
          <div className="px-3 py-1 text-xs font-mono text-soft border-b border-edge bg-surface shrink-0">
            Loading… {total} so far
          </div>
        )}
        {isLoading && total === 0 ? (
          <div className="p-4 text-center text-soft font-mono">Reading from Kafka…</div>
        ) : total === 0 ? (
          <div className="p-4 text-center text-dim font-mono">No messages</div>
        ) : (
          <Virtuoso
            style={{ height: '100%' }}
            totalCount={total}
            rangeChanged={handleRangeChanged}
            itemContent={renderItem}
            components={virtuosoComponents}
          />
        )}
      </div>
    </div>
  );
}
