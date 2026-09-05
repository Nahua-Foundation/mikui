/**
 * Разбор конверта Debezium / Kafka Connect для окна просмотра.
 *
 * Зеркало `kafka::lens` в Rust, и это осознанная цена решения показывать линзу
 * в двух местах. В Rust линза меняет ПРЕВЬЮ СТРОКИ: там её не сделать иначе —
 * в `RowPreview.preview` приезжает 256 байт, собранных на той стороне, и у
 * Connect-топика это всегда кусок `schema`. Здесь линза работает по ПОЛНОМУ
 * телу и делает то, чего превью не может: диф `before`/`after`.
 *
 * Тело при этом не подменяется. Вкладка `payload` показывает конверт целиком:
 * диф полезен ровно вместе с `source` и `ts_ms`, и прятать их было бы потерей.
 *
 * Правила распознавания обязаны совпадать с Rust — иначе строка таблицы и
 * открытое из неё сообщение расходятся в том, что считать конвертом. Набор
 * примеров в тестах там и здесь один и тот же.
 */

/** Операции Debezium: create, update, delete, read (снапшот), truncate, message. */
const OPS = new Set(['c', 'u', 'd', 'r', 't', 'm']);

/** Человеческое имя операции — в таблице и в шапке дифа. */
export const OP_LABELS: Record<string, string> = {
  c: 'create',
  u: 'update',
  d: 'delete',
  r: 'read',
  t: 'truncate',
  m: 'message',
};

export interface DebeziumEnvelope {
  kind: 'debezium';
  op: string;
  before: unknown;
  after: unknown;
  /** Остальное содержимое конверта: `source`, `ts_ms`, `transaction`. */
  rest: Record<string, unknown>;
}

export interface ConnectEnvelope {
  kind: 'connect';
  payload: unknown;
}

export type Envelope = DebeziumEnvelope | ConnectEnvelope;

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

/**
 * Снимает конверт Kafka Connect.
 *
 * Признак — `schema` и `payload` на верхнем уровне, причём `schema.type ===
 * 'struct'`. Проверять тип обязательно: пара полей с такими именами
 * встречается и в обычных прикладных сообщениях, а `"type": "struct"` рядом с
 * ними — уже почерк конвертера.
 */
function unwrapConnect(value: unknown): unknown | null {
  if (!isObject(value)) return null;
  const schema = value.schema;
  if (!isObject(schema) || schema.type !== 'struct') return null;
  return 'payload' in value ? value.payload : null;
}

/**
 * Похоже ли тело на конверт Debezium.
 *
 * Известная операция ПЛЮС хотя бы одно из полей конверта. Одного `op` мало:
 * поле с таким именем есть в куче прикладных сообщений.
 */
function isDebezium(value: unknown): value is Record<string, unknown> {
  if (!isObject(value)) return false;
  if (typeof value.op !== 'string' || !OPS.has(value.op)) return false;
  return 'before' in value || 'after' in value || 'source' in value;
}

/**
 * Что за конверт перед нами. `null` — обычное тело.
 *
 * Debezium ЖИВЁТ ВНУТРИ Connect-конверта, когда у коннектора включены схемы,
 * поэтому сначала снимается внешний, а потом смотрится содержимое.
 */
export function detectEnvelope(body: string): Envelope | null {
  let parsed: unknown;
  try {
    parsed = JSON.parse(body);
  } catch {
    return null;
  }

  const unwrapped = unwrapConnect(parsed);
  const inner = unwrapped === null ? parsed : unwrapped;

  if (isDebezium(inner)) {
    const { op, before, after, ...rest } = inner;
    return {
      kind: 'debezium',
      op: op as string,
      before: before ?? null,
      after: after ?? null,
      rest,
    };
  }
  if (unwrapped !== null) {
    return { kind: 'connect', payload: unwrapped };
  }
  return null;
}

/** Одна строка дифа: поле и то, чем оно было и стало. */
export interface DiffRow {
  field: string;
  before: string | null;
  after: string | null;
  changed: boolean;
}

/**
 * Построчный диф `before` / `after`.
 *
 * Поля берутся из обеих сторон и в порядке появления: у вставки нет `before`, у
 * удаления — `after`, и показывать надо ту сторону, которая есть. Сравнение по
 * напечатанному значению, а не по ссылке: вложенные объекты иначе всегда
 * считались бы изменившимися.
 */
export function diffRows(before: unknown, after: unknown): DiffRow[] {
  const left = isObject(before) ? before : {};
  const right = isObject(after) ? after : {};
  const fields = [...Object.keys(left), ...Object.keys(right).filter((k) => !(k in left))];

  return fields.map((field) => {
    const b = field in left ? render(left[field]) : null;
    const a = field in right ? render(right[field]) : null;
    return { field, before: b, after: a, changed: b !== a };
  });
}

/** Значение в одну строку. Строки — без кавычек: в таблице дифа они лишний шум. */
function render(value: unknown): string {
  if (value === null || value === undefined) return 'null';
  if (typeof value === 'string') return value;
  return JSON.stringify(value);
}
