import { ArrowRight } from 'lucide-react';
import { highlightLine, NO_ENUM_VALUES } from '../syntax';
import { DiffRow, Envelope, OP_LABELS, diffRows } from '../lens';

/**
 * Содержимое конверта: диф для Debezium, полезная нагрузка для Kafka Connect.
 *
 * Отдельной вкладкой, а не вместо тела. Вкладка `payload` продолжает показывать
 * конверт ЦЕЛИКОМ — и это не осторожность, а требование: в CDC-событии половина
 * смысла лежит в `source` и `ts_ms` (какая таблица, какой LSN, когда), и диф без
 * них отвечает лишь на половину вопросов.
 */
export function EnvelopeView({ envelope }: { envelope: Envelope }) {
  if (envelope.kind === 'connect') {
    return <Payload value={envelope.payload} />;
  }

  const rows = diffRows(envelope.before, envelope.after);
  return (
    <div className="space-y-4">
      <div className="flex items-center gap-2 font-mono text-xs">
        <span className="text-dim">operation</span>
        <span className="text-brand">{OP_LABELS[envelope.op] ?? envelope.op}</span>
        {/* Источник события — таблица и время. Мелочью и рядом с операцией:
            это первое, что спрашивают у CDC-сообщения после «что изменилось». */}
        <Source rest={envelope.rest} />
      </div>

      {rows.length > 0 ? (
        <DiffTable rows={rows} op={envelope.op} />
      ) : (
        <div className="font-mono text-xs text-dim">
          Nothing to compare: this event carries neither a before nor an after row.
        </div>
      )}
    </div>
  );
}

/** Таблица дифа: поле, было, стало. */
function DiffTable({ rows, op }: { rows: DiffRow[]; op: string }) {
  const changedCount = rows.filter((r) => r.changed).length;
  return (
    <div className="space-y-2">
      <div className="grid grid-cols-[minmax(0,1fr)_minmax(0,1.2fr)_auto_minmax(0,1.2fr)] gap-x-3 font-mono text-[11px] text-dim">
        <div>field</div>
        <div>before</div>
        <div />
        <div>after</div>
      </div>
      <div className="divide-y divide-edge border-y border-edge">
        {rows.map((row) => (
          <div
            key={row.field}
            className="grid grid-cols-[minmax(0,1fr)_minmax(0,1.2fr)_auto_minmax(0,1.2fr)] gap-x-3 py-1.5 font-mono text-xs items-baseline"
          >
            {/* Изменившееся поле выделено именем, а не фоном: строк бывает
                много, и полоса заливки в них читается хуже, чем один яркий
                столбец слева. */}
            <div className={`truncate ${row.changed ? 'text-brand' : 'text-dim'}`}>
              {row.field}
            </div>
            <div className={`break-all ${row.changed ? 'text-soft' : 'text-dim'}`}>
              {row.before ?? <span className="text-dim">—</span>}
            </div>
            <div className="text-dim">
              {row.changed ? <ArrowRight className="size-3" /> : null}
            </div>
            <div className={`break-all ${row.changed ? 'text-slate-50' : 'text-dim'}`}>
              {row.after ?? <span className="text-dim">—</span>}
            </div>
          </div>
        ))}
      </div>
      <div className="font-mono text-[11px] text-dim">
        {/* У вставки и удаления «изменилось всё», и говорить это отдельно —
            шум. Счётчик осмыслен там, где сравнивать и правда есть что. */}
        {op === 'u'
          ? `${changedCount} of ${rows.length} field${rows.length === 1 ? '' : 's'} changed`
          : `${rows.length} field${rows.length === 1 ? '' : 's'}`}
      </div>
    </div>
  );
}

/** Таблица и время события из `source`, если они там есть. */
function Source({ rest }: { rest: Record<string, unknown> }) {
  const source = rest.source;
  const table =
    typeof source === 'object' && source !== null
      ? [
          (source as Record<string, unknown>).db,
          (source as Record<string, unknown>).schema,
          (source as Record<string, unknown>).table,
        ]
          .filter((part): part is string => typeof part === 'string' && part.length > 0)
          .join('.')
      : '';
  const at = typeof rest.ts_ms === 'number' ? new Date(rest.ts_ms).toISOString() : null;

  if (!table && !at) return null;
  return (
    <>
      {table && (
        <>
          <span className="text-dim">·</span>
          <span className="text-soft truncate">{table}</span>
        </>
      )}
      {at && (
        <>
          <span className="text-dim">·</span>
          <span className="text-dim">{at}</span>
        </>
      )}
    </>
  );
}

/** Полезная нагрузка Connect-конверта, раскрашенная тем же кодом, что и тело. */
function Payload({ value }: { value: unknown }) {
  const text = JSON.stringify(value, null, 2) ?? 'null';
  return (
    <div className="font-mono leading-6 whitespace-pre">
      {text.split('\n').map((line, index) => (
        <div key={index}>{highlightLine(line, NO_ENUM_VALUES)}</div>
      ))}
    </div>
  );
}
