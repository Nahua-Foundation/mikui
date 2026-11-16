/**
 * Модальные окна, которых нет в стартовом куске бандла.
 *
 * Все окна приложения висят в дереве `KafkaExplorerPortfolio` постоянно — так
 * у каждого сохраняется своё состояние между открытиями, и так закрытие
 * доигрывает анимацию. Платой за это было то, что их код разбирался движком до
 * первого кадра: около семидесяти килобайт минифицированного JS, который нужен
 * только после явного действия пользователя. На RDP-ферме, где процессорное
 * время поделено, это заметная часть «приложение запускается пару секунд».
 *
 * Здесь это разделено на два независимых решения:
 *
 *   - `lazy` уводит каждое окно в свой кусок бандла. В стартовом куске от него
 *     остаётся заглушка.
 *   - `Deferred` не монтирует содержимое, пока окно не открыли ВПЕРВЫЕ, и не
 *     размонтирует после. То есть прежнее свойство — состояние и анимация
 *     закрытия живут — сохраняется начиная со второго открытия, а до первого
 *     нет и самого компонента.
 *
 * Два окна остались в стартовом куске намеренно:
 *
 *   - `MessageDetailsModal` — это главное, ради чего приложение открывают, и
 *     оно же лежит на пути холодного старта по ссылке `mikui://message/…`.
 *   - `OpenLinkModal` — на том же пути, и открывается оно раньше, чем успевает
 *     отработать предзагрузка ниже.
 */
import { ReactNode, Suspense, lazy, useEffect, useRef } from 'react';

// Переименование в `default` расписано у каждого окна, а не спрятано в общий
// помощник: `lazy` выводит типы пропсов только из такой формы, а помощник,
// принимающий имя экспорта строкой, стирает их в пустой объект — и все пропсы
// окон перестают проверяться.
//
// Пути ОБЯЗАНЫ вести прямо в файл окна, а не в `./index`. Бочка
// `kafka/index.ts` реэкспортирует всё подряд, и `import('./index')` затянул бы
// в кусок весь набор — то есть ровно то, от чего эта затея избавляется.
export const TopicConfigModal = lazy(() =>
  import('./modals/TopicConfigModal').then((m) => ({ default: m.TopicConfigModal })),
);
export const TopicInfoModal = lazy(() =>
  import('./modals/TopicInfoModal').then((m) => ({ default: m.TopicInfoModal })),
);
export const ClusterConfigModal = lazy(() =>
  import('./modals/ClusterConfigModal').then((m) => ({ default: m.ClusterConfigModal })),
);
export const ClusterArchiveModal = lazy(() =>
  import('./modals/ClusterArchiveModal').then((m) => ({ default: m.ClusterArchiveModal })),
);
export const ClusterUsersModal = lazy(() =>
  import('./modals/ClusterUsersModal').then((m) => ({ default: m.ClusterUsersModal })),
);
export const ProduceMessageModal = lazy(() =>
  import('./modals/ProduceMessageModal').then((m) => ({ default: m.ProduceMessageModal })),
);
export const FavoritesModal = lazy(() =>
  import('./modals/FavoritesModal').then((m) => ({ default: m.FavoritesModal })),
);
export const SavedMessageModal = lazy(() =>
  import('./modals/SavedMessageModal').then((m) => ({ default: m.SavedMessageModal })),
);
export const ShareLinkModal = lazy(() =>
  import('./modals/ShareLinkModal').then((m) => ({ default: m.ShareLinkModal })),
);

/** Те же загрузчики списком — для предзагрузки. Порядок примерно по тому,
 *  насколько рано окно обычно открывают. */
const PRELOAD = [
  () => import('./modals/ClusterArchiveModal'),
  () => import('./modals/TopicConfigModal'),
  () => import('./modals/TopicInfoModal'),
  () => import('./modals/ClusterConfigModal'),
  () => import('./modals/FavoritesModal'),
  () => import('./modals/ProduceMessageModal'),
  () => import('./modals/ClusterUsersModal'),
  () => import('./modals/SavedMessageModal'),
  () => import('./modals/ShareLinkModal'),
];

/**
 * Подтягивает куски окон, когда движку нечем заняться.
 *
 * Без этого шага затея меняла бы шило на мыло: стартовый кусок стал бы меньше,
 * но первое открытие каждого окна ждало бы загрузки своего куска. Куски лежат
 * в самом приложении (Tauri отдаёт их из бинаря), так что ожидание это
 * миллисекунды — но мигание пустотой на первом клике заметно, а убирается оно
 * бесплатно.
 *
 * По одному за заход, а не `Promise.all`: смысл в том, чтобы разбор чужого
 * кода не конкурировал с первой отрисовкой и с открытием топика, а не в том,
 * чтобы он закончился поскорее.
 *
 * `requestIdleCallback` есть и в WebView2, и в WebKitGTK; в WKWebView на macOS
 * его нет — отсюда запасной `setTimeout`. Задержка в нём не выверена и не
 * должна быть: это просто «не сейчас».
 */
export function usePreloadModals() {
  // Один раз на процесс. `StrictMode` в разработке монтирует всё дважды, и без
  // этого предзагрузка запускалась бы двумя наборами.
  const started = useRef(false);

  useEffect(() => {
    if (started.current) return;
    started.current = true;

    let cancelled = false;
    let queued = 0;
    const idle: (cb: () => void) => number =
      typeof window.requestIdleCallback === 'function'
        ? (cb) => window.requestIdleCallback(() => cb())
        : (cb) => window.setTimeout(cb, 200);
    const cancelIdle =
      typeof window.cancelIdleCallback === 'function'
        ? window.cancelIdleCallback.bind(window)
        : window.clearTimeout.bind(window);

    const next = (i: number) => {
      if (cancelled || i >= PRELOAD.length) return;
      queued = idle(() => {
        // Ошибку глотаем молча и идём дальше: это предзагрузка, и её неудача
        // ничего не ломает — окно загрузит свой кусок при открытии, как и
        // загружало бы без неё.
        PRELOAD[i]()
          .catch(() => {})
          .then(() => next(i + 1));
      });
    };
    next(0);

    return () => {
      cancelled = true;
      cancelIdle(queued);
    };
  }, []);
}

/**
 * Монтирует содержимое только после первого открытия.
 *
 * Через ref, а не через state: `open` переключает родитель, то есть в момент
 * перехода в `true` он и так перерисовывается — отметка ставится и читается в
 * одном проходе, без лишнего рендера.
 *
 * Своя граница `Suspense` на каждое окно обязательна. Общая означала бы, что
 * окно, чей кусок ещё летит, гасит остальные, — а открыты они бывают и по
 * двое (ссылка показывается поверх сообщения, на которое она ведёт).
 */
export function Deferred({ open, children }: { open: boolean; children: ReactNode }) {
  const everOpen = useRef(false);
  if (open) everOpen.current = true;
  if (!everOpen.current) return null;
  return <Suspense fallback={null}>{children}</Suspense>;
}
