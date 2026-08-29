import { useCallback, useEffect, useReducer, useRef } from 'react';
import { invoke } from '@tauri-apps/api/core';
import { RowPreview } from './types';

/** Размер одной подгрузки. Кратно перекрывает любой реальный вьюпорт. */
const CHUNK = 200;
/** Потолок кэша: 8 * 200 = 1600 строк в JS одновременно, независимо от того,
 *  50 сообщений в топике или 500 тысяч. Всё остальное живёт в Rust. */
const MAX_CHUNKS = 8;

interface CachedChunk {
  rows: RowPreview[];
  /** Каким был `total` на момент запроса. Чанк, упёршийся в конец списка,
   *  вернулся неполным — и станет полным, когда данных подъедет ещё. */
  totalAtFetch: number;
}

/**
 * Оконная подгрузка строк из буфера на стороне Rust.
 *
 * Смысл: в JS никогда не оказывается больше пары тысяч строк. Virtuoso знает
 * общее число элементов и спрашивает только видимый диапазон, а мы подтягиваем
 * его чанками с запасом на соседей, чтобы при прокрутке не мелькали пустые
 * строки.
 *
 * `generation` меняется, когда меняется САМ СПИСОК — другой топик, партиция,
 * направление чтения, фильтр. Кэш при этом сбрасывается, а ответы на
 * устаревшие запросы отбрасываются, иначе быстрый перебор топиков приводил бы
 * к тому, что старый ответ перезаписывает новый.
 *
 * Фоновая догрузка сообщений generation НЕ меняет: бэкенд только дописывает
 * строки в конец и никогда не переставляет уже показанные, поэтому кэш
 * остаётся валидным. Раньше generation дёргался на каждый тик прогресса, кэш
 * обнулялся четыре раза в секунду — отсюда и были вечно мигающие плейсхолдеры.
 */
export function useMessageWindow(generation: number, total: number) {
  const chunks = useRef(new Map<number, CachedChunk>());
  const inFlight = useRef(new Set<number>());
  const genRef = useRef(generation);
  const totalRef = useRef(total);
  /** Последний диапазон, о котором сообщил Virtuoso. */
  const range = useRef({ start: 0, end: 0 });
  const [version, bump] = useReducer((n: number) => n + 1, 0);

  totalRef.current = total;

  // Сброс делается здесь, в теле рендера, а не в useEffect — и это важно.
  // Virtuoso вызывает rangeChanged из своего layout-эффекта, который может
  // отработать раньше нашего. Со сбросом в эффекте получалась гонка: хук видел
  // ещё не очищенный кэш, считал нужные чанки загруженными и не запрашивал их,
  // после чего кэш очищался — и строки оставались пустыми до следующей
  // прокрутки. К моменту любого колбэка кэш обязан соответствовать generation.
  if (genRef.current !== generation) {
    genRef.current = generation;
    chunks.current.clear();
    inFlight.current.clear();
    range.current = { start: 0, end: 0 };
  }

  const ensureRange = useCallback((startIndex: number, endIndex: number) => {
    range.current = { start: startIndex, end: endIndex };
    const gen = genRef.current;
    const totalNow = totalRef.current;
    if (totalNow === 0) return;

    // По соседнему чанку с каждой стороны — префетч под инерционную прокрутку.
    const first = Math.max(0, Math.floor(startIndex / CHUNK) - 1);
    const last = Math.min(
      Math.floor(endIndex / CHUNK) + 1,
      Math.floor((totalNow - 1) / CHUNK),
    );

    for (let chunk = first; chunk <= last; chunk++) {
      if (inFlight.current.has(chunk)) continue;
      const cached = chunks.current.get(chunk);
      // Полный чанк переспрашивать незачем. Неполный — только если список с
      // тех пор вырос: иначе последний чанк перезапрашивался бы вечно.
      if (cached && (cached.rows.length === CHUNK || cached.totalAtFetch >= totalNow)) {
        continue;
      }
      inFlight.current.add(chunk);

      invoke<RowPreview[]>('get_window', { start: chunk * CHUNK, count: CHUNK })
        .then((rows) => {
          // Пока летел ответ, топик или фильтр могли смениться.
          if (genRef.current !== gen) return;
          chunks.current.set(chunk, { rows, totalAtFetch: totalNow });
          evictFarChunks(chunks.current, chunk);
          bump();
        })
        .catch((e) => console.error('get_window failed', e))
        .finally(() => inFlight.current.delete(chunk));
    }
  }, []);

  // Список подрос в фоне. Virtuoso на это rangeChanged не зовёт — видимый
  // диапазон не изменился, — но хвостовой чанк мог быть недобран.
  useEffect(() => {
    ensureRange(range.current.start, range.current.end);
  }, [total, generation, ensureRange]);

  const getRow = useCallback(
    (index: number): RowPreview | undefined =>
      chunks.current.get(Math.floor(index / CHUNK))?.rows[index % CHUNK],
    [],
  );

  return { ensureRange, getRow, version };
}

/** Выбрасывает чанки, самые далёкие от текущей позиции прокрутки. */
function evictFarChunks(map: Map<number, CachedChunk>, current: number) {
  if (map.size <= MAX_CHUNKS) return;
  const byDistance = [...map.keys()].sort(
    (a, b) => Math.abs(b - current) - Math.abs(a - current),
  );
  while (map.size > MAX_CHUNKS) {
    const victim = byDistance.shift();
    if (victim === undefined) break;
    map.delete(victim);
  }
}
