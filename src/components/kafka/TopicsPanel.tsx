import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { Search } from 'lucide-react';
import { Input } from '../ui/input';
import { Topic } from './types';
import { Virtuoso } from 'react-virtuoso';
import { TopicRow } from './components/TopicRow';
import { TopicPeek, PeekAnchor } from './components/TopicPeek';

const DEFAULT_WIDTH = 220;
const MIN_WIDTH = 160;
const MAX_WIDTH = 480;

/**
 * Сколько курсор должен простоять на строке, прежде чем в ней появится кнопка
 * информации.
 *
 * Пауза здесь не украшение. Список прокручивают и просматривают курсором, и
 * кнопка, выскакивающая под каждым проездом мыши, — это мельтешение во всю
 * панель. Задержка отделяет «веду курсор вниз по списку» от «остановился на
 * этом топике», а второе и есть намерение, ради которого кнопка нужна.
 */
const REVEAL_DELAY_MS = 1000;
/** Длительность сдвига имени — та же, что в классах `TopicRow`. */
const REVEAL_SHIFT_MS = 150;

interface TopicsPanelProps {
  topics: Topic[];
  selectedTopic: Topic | null;
  onTopicSelect: (topic: Topic) => void;
  /** Показать устройство и настройки топика — не обязательно открытого. */
  onTopicInfo: (topic: Topic) => void;
}

/** Один экземпляр на модуль: `localeCompare` создаёт коллатор на каждый вызов,
 *  что на тысячах топиков заметно. */
const COLLATOR = new Intl.Collator(undefined, { numeric: true, sensitivity: 'base' });

export function TopicsPanel({ topics, selectedTopic, onTopicSelect, onTopicInfo }: TopicsPanelProps) {
  const [topicFilter, setTopicFilter] = useState('');
  const [width, setWidth] = useState(DEFAULT_WIDTH);
  const rootRef = useRef<HTMLDivElement>(null);
  const dragging = useRef<{ startX: number; startW: number } | null>(null);
  const pendingWidth = useRef<number | null>(null);
  const [peek, setPeek] = useState<PeekAnchor | null>(null);
  /** Имя топика, в строке которого показана кнопка информации. Ровно одно:
   *  курсор не бывает на двух строках сразу. */
  const [revealed, setRevealed] = useState<string | null>(null);
  const revealTimer = useRef<number | null>(null);
  /** Строка под курсором — с ней работает отложенное появление кнопки. */
  const hovered = useRef<{ row: HTMLElement; name: string } | null>(null);

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

  // useMemo: раньше фильтрация и сортировка гонялись на каждый рендер —
  // включая рендеры, вызванные подгрузкой сообщений в соседней панели.
  // И `topics.sort()` мутировал пропс на месте.
  const filteredTopics = useMemo(() => {
    const needle = topicFilter.trim().toLowerCase();
    if (!needle) {
      return [...topics].sort((a, b) => COLLATOR.compare(a.name, b.name));
    }
    return topics
      .filter((topic) => topic.name.toLowerCase().includes(needle))
      // При активном поиске короткие совпадения обычно релевантнее.
      .sort((a, b) => a.name.length - b.name.length || COLLATOR.compare(a.name, b.name));
  }, [topics, topicFilter]);

  const closePeek = useCallback(() => setPeek(null), []);

  /** Курсор зашёл на строку списка. Дорисовываем имя, только если оно реально
   *  не поместилось: над короткими именами вынос был бы мельтешением без
   *  пользы — там и так всё видно.
   *
   *  Левый край берётся от самого текста, а не от строки: вынос продолжает
   *  имя ровно оттуда, где оно начинается в списке, и не накрывает кнопку
   *  слева, так что та остаётся настоящей и подсвечивается под курсором. */
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

  /** Курсор зашёл на строку: вынос имени сразу, кнопка — если задержится. */
  const enterRow = useCallback(
    (row: HTMLElement, name: string) => {
      hovered.current = { row, name };
      openPeek(row, name);
      cancelReveal();
      revealTimer.current = window.setTimeout(() => {
        revealTimer.current = null;
        setRevealed(name);
        // Имя уехало вправо и, возможно, только теперь перестало помещаться —
        // вынос обязан переехать вместе с ним. Ждём конца сдвига: посреди него
        // геометрия промежуточная, и вынос встал бы не там, где имя.
        window.setTimeout(() => {
          const still = hovered.current;
          if (still?.name === name) openPeek(still.row, name);
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
  // а от внутреннего скроллера Virtuoso.
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
  // том же месте — уже другой топик, а mouseenter об этом не сообщит.
  useEffect(() => {
    leaveRow();
    // leaveRow в зависимостях не нужен: сбрасывать надо на смену фильтра, а
    // не всякий раз, когда пересоздалась функция.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [topicFilter]);

  // Отложенное появление кнопки переживает размонтирование панели, если его
  // не снять: таймер выстрелит в пустоту, а ссылка на строку не отпустит DOM.
  useEffect(() => cancelReveal, [cancelReveal]);

  return (
    <div
      ref={rootRef}
      className="relative shrink-0 flex flex-col h-full border-r border-edge"
      style={{ width }}
    >
      {/* Topics Filter */}
      <div className="p-2 border-b border-edge">
        <div className="relative">
          <Search className="absolute left-2 top-1/2 transform -translate-y-1/2 size-4 text-dim" />
          <Input
            value={topicFilter}
            onChange={(e) => setTopicFilter(e.target.value)}
            placeholder="Filter topics..."
            className="bg-surface border-edge text-slate-50 font-mono placeholder:text-dim pl-8"
          />
        </div>
      </div>
      
      {/* Topics List with Virtuoso */}
      <div className="flex-1 min-h-0">
        {filteredTopics.length === 0 && topicFilter ? (
          <div className="flex items-center justify-center py-4 text-dim font-mono text-sm">
            No topics match "{topicFilter}"
          </div>
        ) : (
          <Virtuoso
            className="h-full"
            totalCount={filteredTopics.length}
            itemContent={(index) => (
              <div
                key={filteredTopics[index].name}
                className="relative shrink-0 w-full p-2"
                data-name="topic item"
                onMouseEnter={(e) => enterRow(e.currentTarget, filteredTopics[index].name)}
                // Закрывает вынос в том числе когда курсор уходит ВПРАВО за
                // границу панели: справа строки уже нет, а хвост выноса
                // сквозной и мышь не ловит.
                onMouseLeave={leaveRow}
              >
                <TopicRow
                  topic={filteredTopics[index]}
                  isSelected={selectedTopic?.name === filteredTopics[index].name}
                  revealed={revealed === filteredTopics[index].name}
                  onSelect={() => onTopicSelect(filteredTopics[index])}
                  onInfo={onTopicInfo}
                />
              </div>
            )}
          />
        )}
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