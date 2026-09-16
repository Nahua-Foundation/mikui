import { useEffect } from "react";

/**
 * Смахивание тоста двумя пальцами по трекпаду.
 *
 * Своё у sonner только «зажать и потащить» — оно живёт на pointer-событиях, а
 * два пальца по трекпаду шлют `wheel`, то есть прокрутку. Поэтому здесь жест
 * не обрабатывается заново, а ПЕРЕВОДИТСЯ в то самое перетаскивание: колесо
 * накапливается и отправляется тосту синтетическими pointer-событиями.
 *
 * Так, а не своей анимацией, ради того, чтобы не держать вторую копию правил.
 * У sonner на перетаскивании висит всё: затухание в запрещённых
 * направлениях, порог, скорость броска, вылет за край и снятие со стопки.
 * Повторить это значило бы разойтись с ним на первом же обновлении.
 *
 * Только горизонтальные жесты, и это не сужение ради простоты. Колесо мыши
 * шлёт вертикаль крупными порциями — одного щелчка хватает, чтобы перевалить
 * порог, и тост улетал бы от попытки прокрутить то, что лежит под ним.
 * Горизонталь колесом почти не встречается, а для тоста в правом нижнем углу
 * это ещё и направление к ближайшему краю.
 */

/** Порог sonner (`SWIPE_THRESHOLD`) с запасом на округление долей пикселя. */
const DISMISS_AT = 48;
/** Жест считается законченным, если колесо молчит столько миллисекунд. */
const GESTURE_END_MS = 120;
/** Синтетическому указателю нужен какой-нибудь id: живого за ним нет. */
const POINTER_ID = 1;

/** Элемент, у которого мы на одну отправку перекрываем захват указателя. */
type WithCapture = { setPointerCapture?: (pointerId: number) => void };

export function useTrackpadToastSwipe() {
  useEffect(() => {
    /** Тост, который сейчас тянут. */
    let target: HTMLElement | null = null;
    /**
     * Тост, который уже отпущен за порог и улетает.
     *
     * Инерцию macOS досылает почти секунду после того, как пальцы оторвали, и
     * без этой памяти она начинала бы по нему новый жест. Одного `data-swipe-out`
     * не хватает: его ставит состояние React, то есть следующей отрисовкой, а
     * первое инерционное событие может успеть раньше.
     */
    let spent: HTMLElement | null = null;
    /** Сколько он уже проехал вправо от начала жеста. */
    let travelled = 0;
    let origin = { x: 0, y: 0 };
    let idle: number | undefined;

    /** Идёт наша отправка — по этому признаку её и отличает заслонка ниже. */
    let dispatching = false;

    const send = (type: "pointerdown" | "pointermove" | "pointerup", x: number) => {
      const node = target;
      if (!node) return;
      dispatching = true;
      try {
        node.dispatchEvent(
          new PointerEvent(type, {
            // Всплытие обязательно: обработчики висят не на самом узле, а на
            // контейнере, куда React делегирует события портала (`body`).
            bubbles: true,
            cancelable: true,
            pointerId: POINTER_ID,
            pointerType: "mouse",
            isPrimary: true,
            button: 0,
            buttons: type === "pointerup" ? 0 : 1,
            clientX: x,
            clientY: origin.y,
          }),
        );
      } finally {
        // `dispatchEvent` синхронный, так что признак снимается сразу за
        // последним обработчиком и ни одного чужого события не захватит.
        dispatching = false;
      }
    };

    /**
     * Дальше `body` наши события не идут.
     *
     * `DismissableLayer` у Radix слушает `pointerdown` на `document` и всё, что
     * пришло не через дерево слоя, понимает как «ткнули мимо окна»: открытая
     * модалка закрывалась от смахивания тоста. А события эти адресованы тосту,
     * и видеть их за пределами тостера не должен никто.
     *
     * Гасить именно на `body` — потому что React делегирует события портала
     * ровно туда (см. `createPortal` в `sonner.tsx`). До этого узла событие
     * доходит, sonner своё получает; `document` — уже нет. Соседей по узлу
     * `stopPropagation` не отменяет (это умеет только
     * `stopImmediatePropagation`), поэтому порядок с обработчиком React
     * неважен.
     */
    const containAtBody = (event: Event) => {
      if (dispatching) event.stopPropagation();
    };

    const startDrag = (toast: HTMLElement) => {
      const box = toast.getBoundingClientRect();
      target = toast;
      travelled = 0;
      origin = { x: box.left + box.width / 2, y: box.top + box.height / 2 };

      // `setPointerCapture` с выдуманным id бросает `NotFoundError`, и бросает
      // ВНУТРИ обработчика sonner — до того, как тот запомнит начало жеста, то
      // есть перетаскивание не начнётся вовсе. Своё свойство перекрывает метод
      // прототипа только у этого элемента и только на одну отправку.
      (toast as unknown as WithCapture).setPointerCapture = () => {};
      send("pointerdown", origin.x);
      delete (toast as unknown as WithCapture).setPointerCapture;
    };

    const release = () => {
      window.clearTimeout(idle);
      idle = undefined;
      if (!target) return;
      send("pointerup", origin.x + travelled);
      target = null;
      travelled = 0;
    };

    const onWheel = (event: WheelEvent) => {
      if (Math.abs(event.deltaX) <= Math.abs(event.deltaY)) return;

      const node = (event.target as Element | null)?.closest?.("[data-sonner-toast]");
      if (!(node instanceof HTMLElement)) return;
      // Неснимаемый тост и тот, что уже улетает: по второму жест пришёл бы
      // из инерции, доезжающей после броска, и дёргал бы его посреди вылета.
      if (node.dataset.dismissible === "false") return;
      if (node.dataset.swipeOut === "true" || node.dataset.removed === "true") return;

      // Иначе macOS уведёт горизонтальный жест себе — на шаг назад по истории.
      // Гасим и остаточную инерцию: тост её уже не касается, а истории она
      // касалась бы по-прежнему.
      event.preventDefault();
      if (node === spent) return;

      if (node !== target) {
        release();
        spent = null;
        startDrag(node);
      }

      // Знак обратный: при «естественной» прокрутке (на macOS она по
      // умолчанию) пальцы вправо дают ОТРИЦАТЕЛЬНЫЙ `deltaX`.
      travelled -= event.deltaX;
      send("pointermove", origin.x + travelled);

      window.clearTimeout(idle);
      if (travelled >= DISMISS_AT) {
        // Порог взят — отпускаем сразу, не дожидаясь тишины. После броска
        // macOS ещё почти секунду досылает инерционные события, и тост всё
        // это время висел бы натянутым, вместо того чтобы улететь.
        spent = node;
        release();
      } else {
        // Жест кончился, не добрав до порога: sonner вернёт тост на место.
        idle = window.setTimeout(release, GESTURE_END_MS);
      }
    };

    // `passive: false` — ради `preventDefault`. Перехват на `window`, а не на
    // контейнере тостов: контейнер уезжает порталом в `body`, и держать на
    // него ссылку только ради слушателя незачем — цель и так проверяется по
    // `data-sonner-toast`.
    window.addEventListener("wheel", onWheel, { passive: false, capture: true });
    // Все три типа, а не только `pointerdown`: за модалку отвечает он один, но
    // адресованы тостеру они все, и чужому обработчику на `document` не место
    // ни в одном из них.
    const body = document.body;
    body.addEventListener("pointerdown", containAtBody);
    body.addEventListener("pointermove", containAtBody);
    body.addEventListener("pointerup", containAtBody);

    return () => {
      window.clearTimeout(idle);
      window.removeEventListener("wheel", onWheel, { capture: true });
      body.removeEventListener("pointerdown", containAtBody);
      body.removeEventListener("pointermove", containAtBody);
      body.removeEventListener("pointerup", containAtBody);
    };
  }, []);
}
