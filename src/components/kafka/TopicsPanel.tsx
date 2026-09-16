import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { ChevronDown, ChevronRight, Search, Star, X } from 'lucide-react';
import { Input } from '../ui/input';
import { Topic } from './types';
import { Virtuoso } from 'react-virtuoso';
import { TopicRow } from './components/TopicRow';
import { TopicPeek, PeekAnchor } from './components/TopicPeek';
import { FilterHelp } from './components/FilterHelp';
import { TopicQuery, matchesQuery, narrowsToMatch, parseTopicQuery } from './topicFilter';

const DEFAULT_WIDTH = 220;
const MIN_WIDTH = 160;
const MAX_WIDTH = 480;

/** Ниже этого в избранном не видно уже ни одной строки — только заголовок.
 *  Считая мёртвую зону над разделителем: она съедает высоту, но строк в ней
 *  нет. */
const MIN_FAVORITES_HEIGHT = 66;

/**
 * Сколько курсор должен простоять на строке, прежде чем в ней появятся кнопки.
 *
 * Пауза здесь не украшение. Список прокручивают и просматривают курсором, и
 * кнопки, выскакивающие под каждым проездом мыши, — это мельтешение во всю
 * панель. Задержка отделяет «веду курсор вниз по списку» от «остановился на
 * этом топике», а второе и есть намерение, ради которого кнопки нужны.
 */
const REVEAL_DELAY_MS = 200;
/** Длительность сдвига имени — та же, что в классах `TopicRow`. */
const REVEAL_SHIFT_MS = 400;

/** В каком из двух списков находится строка. */
type ListKind = 'favorites' | 'all';

/**
 * Чем строка отличается от остальных.
 *
 * Не именем: избранный топик стоит в ОБОИХ списках, и по имени кнопки
 * появлялись бы разом в двух местах — там, где курсор, и там, где его нет.
 */
const rowKey = (list: ListKind, name: string) => `${list}:${name}`;

interface TopicsPanelProps {
  topics: Topic[];
  /**
   * Чей это список: кластер и учётка. `null` — не подключены.
   *
   * Нужен ради одного — сбросить поиск, когда список стал другим. Под другой
   * учёткой видно другие топики, и оставшийся в поле запрос относится уже не
   * к тому, что в списке: человек переключил пользователя и видит «No topics
   * match "orders"» там, где топиков может быть полторы тысячи.
   */
  scope: string | null;
  selectedTopic: Topic | null;
  onTopicSelect: (topic: Topic) => void;
  /** Показать устройство и настройки топика — не обязательно открытого. */
  onTopicInfo: (topic: Topic) => void;
  /** Имена избранных топиков ЭТОГО кластера. */
  favoriteTopics: Set<string>;
  onToggleFavorite: (topic: Topic) => void;
}

/** Один экземпляр на модуль: `localeCompare` создаёт коллатор на каждый вызов,
 *  что на тысячах топиков заметно. */
const COLLATOR = new Intl.Collator(undefined, { numeric: true, sensitivity: 'base' });

/** Отбор и порядок — общие для обоих списков: избранное отличается только тем,
 *  из чего выбирает, и поиск обязан просеивать их одинаково. */
function arrange(topics: Topic[], query: TopicQuery): Topic[] {
  const kept = query.groups.length > 0 ? topics.filter((t) => matchesQuery(t.name, query)) : [...topics];
  // Короткие совпадения обычно релевантнее — но только когда что-то ищут.
  // Фильтр из одних исключений ничего не ищет: это тот же полный список без
  // лишнего, и пересборка по длине перетасовала бы привычный алфавит ни за чем.
  return narrowsToMatch(query)
    ? kept.sort((a, b) => a.name.length - b.name.length || COLLATOR.compare(a.name, b.name))
    : kept.sort((a, b) => COLLATOR.compare(a.name, b.name));
}

export function TopicsPanel({
  topics,
  scope,
  selectedTopic,
  onTopicSelect,
  onTopicInfo,
  favoriteTopics,
  onToggleFavorite,
}: TopicsPanelProps) {
  const [topicFilter, setTopicFilter] = useState('');

  // Список сменился целиком — поиск по нему больше ни к чему не относится.
  // Остальная раскладка панели (ширина, избранное) переключение переживает:
  // это настройки вида, а не запрос к содержимому.
  useEffect(() => {
    setTopicFilter('');
  }, [scope]);
  const [width, setWidth] = useState(DEFAULT_WIDTH);
  const rootRef = useRef<HTMLDivElement>(null);
  const dragging = useRef<{ startX: number; startW: number } | null>(null);
  const pendingWidth = useRef<number | null>(null);
  const [peek, setPeek] = useState<PeekAnchor | null>(null);
  /** Строка, в которой показаны кнопки. Ровно одна: курсор не бывает на двух
   *  строках сразу. */
  const [revealed, setRevealed] = useState<string | null>(null);
  const revealTimer = useRef<number | null>(null);
  /** Строка под курсором — с ней работает отложенное появление кнопок. */
  const hovered = useRef<{ row: HTMLElement; name: string; key: string } | null>(null);

  const [favoritesCollapsed, setFavoritesCollapsed] = useState(false);
  /**
   * До какой высоты избранному позволено расти. `null` — до потолка панели:
   * пока топиков немного, подбирать что-то руками незачем.
   *
   * Именно потолок, а не высота: сама секция всегда по содержимому. Прибитая
   * высота держала бы под удалёнными топиками пустое место — человек убрал из
   * избранного половину, а секция осталась прежней.
   *
   * Живёт до закрытия окна, как и ширина панели: это раскладка текущего
   * сеанса, а не настройка, ради которой стоит ходить на диск.
   */
  const [favoritesHeight, setFavoritesHeight] = useState<number | null>(null);
  /** Потолок — половина места под оба списка, чтобы избранное не вытесняло
   *  тот список, ради которого панель и открыта. Меряем, а не задаём в CSS
   *  процентами: проценты от высоты flex-элемента разные движки разрешают
   *  по-разному, а Tauri — это три разных движка. */
  const [maxFavoritesHeight, setMaxFavoritesHeight] = useState<number | null>(null);
  const listsRef = useRef<HTMLDivElement>(null);
  const favoritesRef = useRef<HTMLDivElement>(null);
  const favoritesScrollRef = useRef<HTMLDivElement>(null);
  const favoritesDrag = useRef<{ startY: number; startH: number; ceiling: number } | null>(null);
  const pendingFavoritesHeight = useRef<number | null>(null);

  // Как и в MessagesPanel: во время перетаскивания React не участвует,
  // ширина применяется напрямую к DOM, в state попадает один раз на mouseup.
  const applyWidth = useCallback((w: number) => {
    if (rootRef.current) rootRef.current.style.width = `${w}px`;
  }, []);

  const onMouseMove = useCallback(
    (e: MouseEvent) => {
      const drag = dragging.current;
      if (!drag) return;
      const next = Math.min(MAX_WIDTH, Math.max(MIN_WIDTH, drag.startW + (e.clientX - drag.startX)));
      pendingWidth.current = next;
      applyWidth(next);
    },
    [applyWidth],
  );

  const stopDragging = useCallback(() => {
    if (!dragging.current) return;
    dragging.current = null;
    window.removeEventListener('mousemove', onMouseMove);
    window.removeEventListener('mouseup', stopDragging);
    if (pendingWidth.current !== null) {
      setWidth(pendingWidth.current);
      pendingWidth.current = null;
    }
  }, [onMouseMove]);

  const startDragging = useCallback(
    (e: React.MouseEvent) => {
      dragging.current = { startX: e.clientX, startW: width };
      window.addEventListener('mousemove', onMouseMove);
      window.addEventListener('mouseup', stopDragging);
      e.preventDefault();
      e.stopPropagation();
    },
    [width, onMouseMove, stopDragging],
  );

  useEffect(
    () => () => {
      window.removeEventListener('mousemove', onMouseMove);
      window.removeEventListener('mouseup', stopDragging);
    },
    [onMouseMove, stopDragging],
  );

  // Потолок избранного держится за живой размер панели: окно меняют, и
  // половина от вчерашней высоты сегодня значит не то же самое.
  useEffect(() => {
    const lists = listsRef.current;
    if (!lists) return;
    const observer = new ResizeObserver(([entry]) => {
      setMaxFavoritesHeight(Math.round(entry.contentRect.height / 2));
    });
    observer.observe(lists);
    return () => observer.disconnect();
  }, []);

  // Через ref: обработчик перетаскивания висит на window, и пересоздаваться на
  // каждое изменение размера окна ему незачем.
  const maxFavoritesHeightRef = useRef<number | null>(null);
  maxFavoritesHeightRef.current = maxFavoritesHeight;

  // Высота избранного — тем же приёмом, что и ширина панели: во время
  // перетаскивания правим DOM напрямую, в state кладём один раз на mouseup.
  // Правим `maxHeight`, а не `height`: тянут именно потолок.
  const onFavoritesMouseMove = useCallback((e: MouseEvent) => {
    const drag = favoritesDrag.current;
    if (!drag) return;
    const next = Math.min(
      drag.ceiling,
      Math.max(MIN_FAVORITES_HEIGHT, drag.startH + (e.clientY - drag.startY)),
    );
    pendingFavoritesHeight.current = next;
    if (favoritesRef.current) favoritesRef.current.style.maxHeight = `${next}px`;
  }, []);

  const stopFavoritesDragging = useCallback(() => {
    if (!favoritesDrag.current) return;
    favoritesDrag.current = null;
    window.removeEventListener('mousemove', onFavoritesMouseMove);
    window.removeEventListener('mouseup', stopFavoritesDragging);
    if (pendingFavoritesHeight.current !== null) {
      setFavoritesHeight(pendingFavoritesHeight.current);
      pendingFavoritesHeight.current = null;
    }
  }, [onFavoritesMouseMove]);

  const startFavoritesDragging = useCallback(
    (e: React.MouseEvent) => {
      // От измеренной высоты, а не от `favoritesHeight`: там потолок, а секция
      // стоит по содержимому и обычно ниже его. Тянуть надо оттуда, где ручка
      // сейчас, иначе она прыгнет под курсором на первом же движении.
      const section = favoritesRef.current;
      const scroller = favoritesScrollRef.current;
      const startH = section?.getBoundingClientRect().height ?? MIN_FAVORITES_HEIGHT;
      // Выше своего содержимого секция всё равно не станет, и тянуть туда
      // некуда: ручка осталась бы стоять, отставая от курсора на сколь угодно
      // много. Потолок — что ниже: весь список целиком или половина панели.
      // За время перетаскивания содержимое не меняется, так что меряем один раз.
      const content = scroller ? startH - scroller.clientHeight + scroller.scrollHeight : startH;
      const ceiling = Math.max(
        MIN_FAVORITES_HEIGHT,
        Math.min(content, maxFavoritesHeightRef.current ?? content),
      );
      favoritesDrag.current = { startY: e.clientY, startH, ceiling };
      window.addEventListener('mousemove', onFavoritesMouseMove);
      window.addEventListener('mouseup', stopFavoritesDragging);
      e.preventDefault();
      e.stopPropagation();
    },
    [onFavoritesMouseMove, stopFavoritesDragging],
  );

  useEffect(
    () => () => {
      window.removeEventListener('mousemove', onFavoritesMouseMove);
      window.removeEventListener('mouseup', stopFavoritesDragging);
    },
    [onFavoritesMouseMove, stopFavoritesDragging],
  );

  // useMemo: раньше фильтрация и сортировка гонялись на каждый рендер —
  // включая рендеры, вызванные подгрузкой сообщений в соседней панели.
  // И `topics.sort()` мутировал пропс на месте.
  const query = useMemo(() => parseTopicQuery(topicFilter), [topicFilter]);
  const filteredTopics = useMemo(() => arrange(topics, query), [topics, query]);
  /** Избранное — из того же списка, что и всё остальное: отмеченного топика
   *  может уже не быть на кластере, и рисовать строку, за которой ничего нет,
   *  значило бы предлагать открыть несуществующее. Из настроек имя при этом не
   *  вычёркивается: топик мог уехать вместе с временно недоступным кластером. */
  const favoriteRows = useMemo(
    () => arrange(topics.filter((topic) => favoriteTopics.has(topic.name)), query),
    [topics, favoriteTopics, query],
  );

  const closePeek = useCallback(() => setPeek(null), []);

  /** Курсор зашёл на строку списка. Дорисовываем имя, только если оно реально
   *  не поместилось: над короткими именами вынос был бы мельтешением без
   *  пользы — там и так всё видно.
   *
   *  Левый край берётся от самого текста, а не от строки: вынос продолжает
   *  имя ровно оттуда, где оно начинается в списке, и не накрывает кнопки
   *  слева, так что те остаются настоящими и подсвечиваются под курсором. */
  const openPeek = useCallback((row: HTMLElement, name: string) => {
    const label = row.querySelector<HTMLElement>('[data-topic-name]');
    if (!label || label.scrollWidth <= label.clientWidth) {
      setPeek(null);
      return;
    }
    const rowRect = row.getBoundingClientRect();
    const labelRect = label.getBoundingClientRect();
    setPeek({ name, left: labelRect.left, top: rowRect.top, height: rowRect.height });
  }, []);

  const cancelReveal = useCallback(() => {
    if (revealTimer.current === null) return;
    window.clearTimeout(revealTimer.current);
    revealTimer.current = null;
  }, []);

  /** Курсор зашёл на строку: вынос имени сразу, кнопки — если задержится. */
  const enterRow = useCallback(
    (row: HTMLElement, name: string, key: string) => {
      hovered.current = { row, name, key };
      openPeek(row, name);
      cancelReveal();
      revealTimer.current = window.setTimeout(() => {
        revealTimer.current = null;
        setRevealed(key);
        // Имя уехало вправо и, возможно, только теперь перестало помещаться —
        // вынос обязан переехать вместе с ним. Ждём конца сдвига: посреди него
        // геометрия промежуточная, и вынос встал бы не там, где имя.
        window.setTimeout(() => {
          const still = hovered.current;
          if (still?.key === key) openPeek(still.row, name);
        }, REVEAL_SHIFT_MS);
      }, REVEAL_DELAY_MS);
    },
    [openPeek, cancelReveal],
  );

  /** Курсор ушёл со строки — и с неё же снимается всё, что он вызвал. */
  const leaveRow = useCallback(() => {
    hovered.current = null;
    cancelReveal();
    setRevealed(null);
    closePeek();
  }, [cancelReveal, closePeek]);

  // Прокрутка и изменение размера двигают список под уже снятой геометрией,
  // а пересчитывать её на лету незачем: курсор в этот момент всё равно уходит
  // со строки. Скролл слушаем в фазе capture — он всплывает не от window,
  // а от внутреннего скроллера Virtuoso (и от скроллера избранного).
  useEffect(() => {
    if (!peek && !revealed) return;
    window.addEventListener('scroll', leaveRow, true);
    window.addEventListener('resize', leaveRow);
    return () => {
      window.removeEventListener('scroll', leaveRow, true);
      window.removeEventListener('resize', leaveRow);
    };
  }, [peek, revealed, leaveRow]);

  // Смена фильтра перетасовывает список под неподвижным курсором: строка на
  // том же месте — уже другой топик, а mouseenter об этом не сообщит. То же
  // самое делает сворачивание избранного: списки под курсором разъезжаются.
  useEffect(() => {
    leaveRow();
    // leaveRow в зависимостях не нужен: сбрасывать надо на смену фильтра, а
    // не всякий раз, когда пересоздалась функция.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [topicFilter, favoritesCollapsed]);

  // Отложенное появление кнопок переживает размонтирование панели, если его
  // не снять: таймер выстрелит в пустоту, а ссылка на строку не отпустит DOM.
  useEffect(() => cancelReveal, [cancelReveal]);

  /**
   * Звезда, поставленная или снятая прямо под курсором.
   *
   * Списки после неё перестраиваются: строка появляется в избранном или
   * исчезает из него, а остальные разъезжаются. Курсор при этом не двигался —
   * `mouseleave` по удалённой строке не приходит вовсе, — и вынос имени остался
   * бы висеть над пустым местом, а кнопки — над чужой строкой.
   */
  const toggleFavorite = useCallback(
    (topic: Topic) => {
      leaveRow();
      onToggleFavorite(topic);
    },
    [leaveRow, onToggleFavorite],
  );

  /** Обёртка строки: та же и в избранном, и в общем списке — вплоть до
   *  обработчиков курсора. */
  const row = (topic: Topic, list: ListKind) => {
    const key = rowKey(list, topic.name);
    return (
      <div
        key={key}
        className="relative shrink-0 w-full p-2"
        data-name="topic item"
        onMouseEnter={(e) => enterRow(e.currentTarget, topic.name, key)}
        // Закрывает вынос в том числе когда курсор уходит ВПРАВО за границу
        // панели: справа строки уже нет, а хвост выноса сквозной и мышь не
        // ловит.
        onMouseLeave={leaveRow}
      >
        <TopicRow
          topic={topic}
          isSelected={selectedTopic?.name === topic.name}
          revealed={revealed === key}
          isFavorite={favoriteTopics.has(topic.name)}
          onSelect={() => onTopicSelect(topic)}
          onInfo={onTopicInfo}
          onToggleFavorite={toggleFavorite}
        />
      </div>
    );
  };

  const favoritesOpen = !favoritesCollapsed && favoriteRows.length > 0;

  /** Докуда избранному позволено расти: что ниже — натянутое руками или
   *  половина панели. Обоих может ещё не быть — до первого перетаскивания и до
   *  первого замера соответственно. */
  const favoritesCeiling = useMemo(() => {
    const limits = [favoritesHeight, maxFavoritesHeight].filter((v) => v !== null);
    return limits.length > 0 ? Math.min(...limits) : undefined;
  }, [favoritesHeight, maxFavoritesHeight]);

  return (
    <div
      ref={rootRef}
      className="relative shrink-0 flex flex-col h-full border-r border-edge"
      style={{ width }}
    >
      {/* Topics Filter.
          Обе кнопки живут ВНУТРИ поля, а не рядом с ним: панель бывает шириной
          в 160 пикселей, и вынесенные наружу они отъедали бы у ввода треть
          именно тогда, когда его и так не хватает. */}
      <div className="p-2 border-b border-edge">
        <div className="relative">
          <Search className="absolute left-2 top-1/2 transform -translate-y-1/2 size-4 text-dim" />
          <Input
            value={topicFilter}
            onChange={(e) => setTopicFilter(e.target.value)}
            placeholder="Filter topics..."
            // Место под крестик освобождается только когда он есть: пустому
            // полю лишний отступ ни к чему.
            className={`bg-surface border-edge text-strong font-mono placeholder:text-dim pl-8 ${
              topicFilter ? 'pr-14' : 'pr-8'
            }`}
          />
          {topicFilter && (
            <button
              type="button"
              onClick={() => setTopicFilter('')}
              title="Clear filter"
              className="absolute right-[26px] top-1/2 -translate-y-1/2 flex items-center text-dim hover:text-strong transition-colors cursor-pointer p-0 border-none bg-transparent"
            >
              <X className="size-4" />
            </button>
          )}
          <FilterHelp />
        </div>
      </div>

      <div ref={listsRef} className="flex-1 min-h-0 flex flex-col">
        {/* Favorites.
            Отдельной секцией над общим списком, а не отдельным его порядком:
            отмеченные топики нужны под рукой ПОСТОЯННО, а список прокручен
            неизвестно куда. Из общего списка они при этом не исчезают — там
            они на своём месте по алфавиту, и искать их привычным способом
            остаётся можно. */}
        {/* `pb-2.5` — мёртвая зона над разделителем. Список внутри обрезается
            по краю своего скроллера, и без неё низ обрезанной строки упирался
            прямо в границу, а сразу под ней начиналась настоящая строка общего
            списка: верх одного имени и низ другого читались как одно кривое.
            Нужна она только раскрытой секции — свёрнутой обрезать нечего. */}
        <div
          ref={favoritesRef}
          className={`relative shrink-0 flex flex-col min-h-0 border-b border-edge ${
            favoritesOpen ? 'pb-2.5' : ''
          }`}
          style={{ maxHeight: favoritesOpen ? favoritesCeiling : undefined }}
        >
          <button
            type="button"
            onClick={() => setFavoritesCollapsed((collapsed) => !collapsed)}
            title={favoritesCollapsed ? 'Expand favorites' : 'Collapse favorites'}
            className="flex items-center gap-1 shrink-0 w-full px-2 py-1.5 text-left font-mono text-xs text-dim hover:text-strong bg-transparent border-none cursor-pointer"
          >
            {favoritesCollapsed ? (
              <ChevronRight className="size-3.5 shrink-0" />
            ) : (
              <ChevronDown className="size-3.5 shrink-0" />
            )}
            {/* Та же звезда с заливкой, что стоит в отмеченной строке, и того
                же цвета — по ней секция и опознаётся как «то самое избранное».
                На размер меньше строчной: рядом с `text-xs` заголовка звезда в
                `size-4` читалась бы кнопкой, а нажимается здесь вся шапка.
                Цвет свой, не от заголовка: тот под курсором светлеет, а метка
                секции от наведения меняться не должна. */}
            <Star className="size-3 shrink-0 fill-current text-brand" />
            {/* Счётчик показывает то, что в секции ВИДНО: под активным поиском
                она просеяна тем же словом, что и список под ней. */}
            Favorites ({favoriteRows.length})
          </button>

          {/* `flex-auto`, а не `flex-1`: пока высоту не тянули, у секции её
              нет — она ровно по содержимому, а базис `0` у `flex-1` нечему
              задать эту высоту и в WebKit схлопывает список до нуля. Базис
              `auto` берётся от самих строк, растёт до натянутой высоты и
              сжимается до потолка, отдавая остальное скроллу. */}
          {favoritesOpen && (
            <div ref={favoritesScrollRef} className="flex-auto min-h-0 overflow-auto">
              {favoriteRows.map((topic) => row(topic, 'favorites'))}
            </div>
          )}

          {/* Тянуть высоту не за что, когда секция свёрнута или пуста.
              Полоса тоньше, чем у ширины панели: она лежит поперёк списка, и
              каждый её пиксель — это пиксель строки топика, который перестал
              нажиматься. Вниз она выступает на три — ровно чтобы попасть
              курсором в границу, а не в первую строку под ней. */}
          {favoritesOpen && (
            <div
              onMouseDown={startFavoritesDragging}
              className="absolute bottom-[-3px] left-0 w-full h-[6px] cursor-row-resize z-10"
              style={{
                backgroundImage:
                  'linear-gradient(to bottom, transparent 2px, var(--color-edge) 2px, var(--color-edge) 3px, transparent 3px)',
              }}
              title="Drag to resize"
            />
          )}
        </div>

        {/* Topics List with Virtuoso.
            `pt-2.5` — вторая половина мёртвой зоны: скроллер начинается ниже
            разделителя, и обрезанная сверху строка не подпирает его так же,
            как обрезанная снизу строка избранного — снизу. */}
        <div className="flex-1 min-h-0 pt-2.5">
          {filteredTopics.length === 0 && topicFilter ? (
            <div className="flex items-center justify-center py-4 text-dim font-mono text-sm">
              No topics match "{topicFilter}"
            </div>
          ) : (
            <Virtuoso
              className="h-full"
              totalCount={filteredTopics.length}
              itemContent={(index) => row(filteredTopics[index], 'all')}
            />
          )}
        </div>
      </div>

      {peek && <TopicPeek anchor={peek} />}

      <div
        onMouseDown={startDragging}
        className="absolute top-0 right-[-8px] h-full w-4 cursor-col-resize z-10"
        style={{
          backgroundImage:
            'linear-gradient(to right, transparent 7px, var(--color-edge) 7px, var(--color-edge) 8px, transparent 8px)',
        }}
        title="Drag to resize"
      />
    </div>
  );
}
