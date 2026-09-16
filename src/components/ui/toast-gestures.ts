import { useEffect } from "react";

/**
 * Всё, что нужно тосту, чтобы его жесты работали и никого не задевали.
 *
 * Две заботы, и обе про одно: указатель на тосте адресован тосту, а остальное
 * приложение не должно принимать его на свой счёт.
 *
 * --- 1. Смахивание двумя пальцами по трекпаду ---
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
 *
 * --- 2. Тост не считается «кликом мимо» ---
 *
 * Самый естественный способ убрать тост мышью — навести, зажать и потянуть в
 * сторону. Это работало, но заодно закрывало открытое модальное окно: Radix
 * слушает `pointerdown` на `document` и всё, что пришло не через дерево слоя,
 * понимает как «ткнули мимо». То есть попытка убрать уведомление отменяла
 * работу, ради которой оно и появилось.
 *
 * Поэтому события, адресованные тостеру, до `document` не доходят вовсе —
 * ни настоящие, ни синтетические из пункта 1. См. `containAtBody`.
 *
 * --- 3. Текст тоста выделяется, а не тянется ---
 *
 * Курсор над тостом — `text`, потому что это обычный текст в обычном блоке, и
 * обещание он давал верное: сообщение об ошибке хочется выделить и скопировать.
 * Выделить, однако, не удавалось, и не из-за `user-select`: его никто не
 * запрещал. Мешало само перетаскивание. Тост на зажатой кнопке едет за
 * указателем ОДИН К ОДНОМУ, значит буква под курсором остаётся та же, значит
 * выделение не растёт ни на символ. У sonner на этот случай есть защита — на
 * `pointermove` он выходит сразу, если выделение непустое, — но дожить до неё
 * невозможно: пустым оно и остаётся.
 *
 * Развязка по месту нажатия, а не по хитрой эвристике: на однострочном тосте
 * выделение и смахивание — это один и тот же жест (зажать и потянуть по
 * горизонтали), различить их по движению нельзя, а различить по тому, за что
 * взялись, — можно и нужно. Нажатие на заголовок или описание до sonner не
 * доходит вовсе (`yieldToSelection`), и браузер делает своё обычное дело.
 * Смахивание мышью осталось за всем остальным: отбивки, иконка, пустое место
 * справа от текста, — а жест с трекпада (пункт 1) работает и над текстом,
 * потому что живёт на `wheel` и выделению не мешает.
 *
 * Курсоры после этого говорят правду: `text` над текстом, `grab` над тем, что
 * и правда можно потащить. См. `classNames` в `sonner.tsx`.
 *
 * Отобрать выделение может ещё и модальное окно — своей ловушкой фокуса. Про
 * это `holdFocusOnText`.
 *
 * Отсчёт до закрытия на время выделения держит пауза от наведения (`expanded`
 * у sonner), а не от нажатия (`interacting`): нажатие до тостера тоже не
 * доходит. Разница заметна в одном случае — если, не отпуская кнопку, увести
 * указатель за края тоста: отсчёт возобновится, и выделенное можно не успеть
 * скопировать. Чинить это значило бы подделывать тостеру `mouseleave`, а
 * проглоченный `mouseleave` — это тост, который больше никогда сам не уйдёт.
 */

/** Порог sonner (`SWIPE_THRESHOLD`) с запасом на округление долей пикселя. */
const DISMISS_AT = 48;
/** Жест считается законченным, если колесо молчит столько миллисекунд. */
const GESTURE_END_MS = 120;
/** Синтетическому указателю нужен какой-нибудь id: живого за ним нет. */
const POINTER_ID = 1;

/** Элемент, у которого мы на одну отправку перекрываем захват указателя. */
type WithCapture = { setPointerCapture?: (pointerId: number) => void };

/** Пришло ли событие из тостера. */
function fromToaster(event: Event): boolean {
  const node = event.target;
  return node instanceof Element && node.closest("[data-sonner-toaster]") !== null;
}

/** Пришло ли событие из текста тоста — заголовка или описания. */
function fromToastText(event: Event): boolean {
  const node = event.target;
  if (!(node instanceof Element)) return false;
  const part = node.closest("[data-title],[data-description]");
  return part !== null && part.closest("[data-sonner-toast]") !== null;
}

/**
 * Снять выделение, если оно внутри тостера.
 *
 * Указатель делает это сам: выделение снимает `mousedown`, который браузер
 * шлёт вслед за `pointerdown`. У жеста с трекпада такого события нет, а
 * защита sonner от «тянут, а на самом деле выделяют» смотрит на выделение по
 * всему документу. Без этого только что скопированный текст запирал бы тост:
 * смахнуть его жестом уже нельзя.
 *
 * Только своё выделение: выделенное в приложении жест по тосту трогать не
 * вправе, хоть той же защите оно и помешает.
 */
function dropSelectionInsideToaster(): void {
  const selection = window.getSelection();
  if (!selection || selection.toString().length === 0) return;
  const anchor = selection.anchorNode;
  const node = anchor instanceof Element ? anchor : anchor?.parentElement;
  if (node?.closest("[data-sonner-toaster]")) selection.removeAllRanges();
}

export function useToastGestures() {
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
    /**
     * Нажатие на текст тоста, которое ещё не отпустили.
     *
     * Хранится здесь, а не выводится из выделения: на первом `pointerdown`
     * выделять ещё нечего, а решать, чьё сейчас нажатие, надо уже тогда.
     */
    let selecting = false;
    let origin = { x: 0, y: 0 };
    let idle: number | undefined;

    const send = (type: "pointerdown" | "pointermove" | "pointerup", x: number) => {
      const node = target;
      if (!node) return;
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
    };

    /**
     * Дальше `body` события тостера не идут.
     *
     * Отбор по ЦЕЛИ, а не по тому, свои это события или чужие. Раньше здесь
     * гасились только синтетические из пункта 1 — те, что отправляет `send`, —
     * и «зажать мышью и потянуть» по-прежнему закрывало модалку. А разницы
     * между настоящим указателем и переведённым жестом для остального
     * приложения нет никакой: и то и другое адресовано тосту. Заодно отпала
     * нужда помечать собственную отправку признаком: цель у неё тот же тост.
     *
     * Гасить именно на `body` — потому что React делегирует события портала
     * ровно туда (см. `createPortal` в `sonner.tsx`). До этого узла событие
     * доходит, sonner своё получает; `document` — уже нет. Соседей по узлу
     * `stopPropagation` не отменяет (это умеет только
     * `stopImmediatePropagation`), поэтому порядок с обработчиком React
     * неважен: и перетаскивание, и крестик на тосте работают как раньше.
     */
    const containAtBody = (event: Event) => {
      if (fromToaster(event)) event.stopPropagation();
    };

    /**
     * Нажатие на текст тоста до sonner не доходит — см. пункт 3.
     *
     * На перехвате (`capture`) и на `body`: до этого узла событие ещё только
     * спускается, а обработчики React сидят на нём же и слушают всплытие, то
     * есть погашенное здесь событие не достанется ни `onPointerDown` тоста
     * (тот самый, что начинает перетаскивание), ни `document`, где его ждёт
     * Radix. Значит попутно снимается и забота пункта 2. Выделения запрет
     * распространения не касается вовсе: оно — действие браузера по
     * умолчанию, а не обработчик, и отменял бы его только `preventDefault`.
     *
     * `isTrusted` отделяет живой указатель от переведённого жеста: тот
     * отправляет события самому тосту (`send` ниже), а не тексту в нём, — но
     * если однажды начнёт отправлять вглубь, глушить их здесь нельзя, иначе
     * смахивание с трекпада перестанет работать.
     */
    const yieldToSelection = (event: Event) => {
      selecting = event.isTrusted && fromToastText(event);
      if (selecting) event.stopPropagation();
    };

    /**
     * Пока выделяют — фокус у тоста не отбирать.
     *
     * Это про тост поверх модального окна: там выделение не начиналось, хотя
     * смахивание работало. Виновата ловушка фокуса Radix. Нажатие на текст
     * переводит фокус на сам тост (у него `tabindex=0`), `FocusScope` слышит
     * `focusin`/`focusout` на `document`, видит фокус вне модалки и возвращает
     * его назад — причём вызовом `focus(..., { select: true })`, то есть, если
     * это поле ввода, ещё и с `input.select()`.
     *
     * Дальше всё сходится. Выделение в документе одно на всех: у WebKit это
     * одна и та же `FrameSelection`, и её база уезжает в поле модалки, а
     * протяжка мышью расширяет именно базу, — значит расширять в тосте уже
     * нечего. А смахивание при этом целёхонько: выделенное внутри `input`
     * живёт в теневом дереве, `getSelection().toString()` отдаёт пустую
     * строку, и защита sonner «тянут, а на самом деле выделяют» молчит.
     *
     * Поэтому на время нажатия события фокуса, адресованные тостеру, до
     * `document` не доходят — как и указатель в пункте 2. Дальше `pointerup`
     * не глушим: щелчок мимо или Tab дадут `focusin` уже не из тостера,
     * Radix его увидит и вернёт фокус в модалку сам. То есть ловушка не
     * ломается, а лишь пропускает одно нажатие — ровно то, которым выделяют.
     */
    const holdFocusOnText = (event: FocusEvent) => {
      if (!selecting) return;
      const next = event.relatedTarget;
      const toToaster = next instanceof Element && next.closest("[data-sonner-toaster]") !== null;
      // `focusin` приходит на тост, `focusout` — с поля модалки, и там тостер
      // значится не целью, а тем, КУДА фокус уходит.
      if (fromToaster(event) || toToaster) event.stopPropagation();
    };

    /** Нажатие кончилось: с этого момента фокус снова живёт по правилам Radix. */
    const endSelecting = () => {
      selecting = false;
    };

    const startDrag = (toast: HTMLElement) => {
      dropSelectionInsideToaster();
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

    // Список ровно по тому, что слушает `DismissableLayer` у Radix:
    // `pointerdown` — им он и закрывает слой, а `click` держит одноразовым
    // слушателем для касаний, где решение отложено до щелчка. Оба гасим.
    //
    // `pointermove` и `pointerup` — не про Radix; на `document` их никто не
    // ждёт. Оставлены затем, что адресованы они тостеру ровно так же, и
    // выпускать половину жеста наружу было бы непоследовательно.
    //
    // `focusin` в списке НЕТ намеренно, хотя Radix слушает и его. Модальное
    // окно и без нас не закрывается по уходу фокуса — `DialogContent` гасит
    // `onFocusOutside` сам, — а вот ловушка фокуса (`FocusScope`) держится
    // как раз на `focusin`. Погасив его, мы обменяли бы закрывающуюся модалку
    // на модалку, из которой уезжает фокус, то есть на ошибку хуже исходной.
    const body = document.body;
    const contained = ["pointerdown", "pointermove", "pointerup", "click"];
    for (const type of contained) {
      body.addEventListener(type, containAtBody);
    }
    body.addEventListener("pointerdown", yieldToSelection, { capture: true });
    for (const type of ["focusin", "focusout"] as const) {
      body.addEventListener(type, holdFocusOnText);
    }
    // На `window` и на перехвате: отпустить кнопку могут и за пределами тоста,
    // а `pointercancel` — единственный признак того, что жест отняли (системный
    // жест, потеря окна), и другого конца у нажатия тогда не будет.
    window.addEventListener("pointerup", endSelecting, { capture: true });
    window.addEventListener("pointercancel", endSelecting, { capture: true });

    return () => {
      window.clearTimeout(idle);
      window.removeEventListener("wheel", onWheel, { capture: true });
      for (const type of contained) {
        body.removeEventListener(type, containAtBody);
      }
      body.removeEventListener("pointerdown", yieldToSelection, { capture: true });
      for (const type of ["focusin", "focusout"] as const) {
        body.removeEventListener(type, holdFocusOnText);
      }
      window.removeEventListener("pointerup", endSelecting, { capture: true });
      window.removeEventListener("pointercancel", endSelecting, { capture: true });
    };
  }, []);
}
