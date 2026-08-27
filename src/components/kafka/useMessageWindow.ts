import { useCallback, useReducer, useRef } from 'react';
import { invoke } from '@tauri-apps/api/core';
import { RowPreview } from './types';

/** Размер одной подгрузки. Кратно перекрывает любой реальный вьюпорт. */
const CHUNK = 200;
/** Потолок кэша: 8 * 200 = 1600 строк в JS одновременно, независимо от того,
 *  50 сообщений в топике или 500 тысяч. Всё остальное живёт в Rust. */
const MAX_CHUNKS = 8;

/**
 * Оконная подгрузка строк из буфера на стороне Rust.
 *
 * Смысл: в JS никогда не оказывается больше пары тысяч строк. Virtuoso знает
 * общее число элементов и спрашивает только видимый диапазон, а мы подтягиваем
 * его чанками с запасом на соседей, чтобы при прокрутке не мелькали пустые
 * строки.
 *
 * `generation` меняется при любой смене содержимого (другой топик, другой
 * фильтр). Кэш при этом сбрасывается, а ответы на устаревшие запросы
 * отбрасываются — иначе быстрый перебор топиков приводил бы к тому, что старый
 * ответ перезаписывает новый.
 */
export function useMessageWindow(generation: number) {
  const chunks = useRef(new Map<number, RowPreview[]>());
  const inFlight = useRef(new Set<number>());
  const genRef = useRef(generation);
  const [version, bump] = useReducer((n: number) => n + 1, 0);

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
  }

  const ensureRange = useCallback((startIndex: number, endIndex: number) => {
    const gen = genRef.current;
    // По соседнему чанку с каждой стороны — префетч под инерционную прокрутку.
    const first = Math.max(0, Math.floor(startIndex / CHUNK) - 1);
    const last = Math.floor(endIndex / CHUNK) + 1;

    for (let chunk = first; chunk <= last; chunk++) {
      if (chunks.current.has(chunk) || inFlight.current.has(chunk)) continue;
      inFlight.current.add(chunk);

      invoke<RowPreview[]>('get_window', { start: chunk * CHUNK, count: CHUNK })
        .then((rows) => {
          // Пока летел ответ, топик или фильтр могли смениться.
          if (genRef.current !== gen) return;
          chunks.current.set(chunk, rows);
          evictFarChunks(chunks.current, chunk);
          bump();
        })
        .catch((e) => console.error('get_window failed', e))
        .finally(() => inFlight.current.delete(chunk));
    }
  }, []);

  const getRow = useCallback(
    (index: number): RowPreview | undefined =>
      chunks.current.get(Math.floor(index / CHUNK))?.[index % CHUNK],
    [],
  );

  return { ensureRange, getRow, version };
}

/** Выбрасывает чанки, самые далёкие от текущей позиции прокрутки. */
function evictFarChunks(map: Map<number, RowPreview[]>, current: number) {
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
