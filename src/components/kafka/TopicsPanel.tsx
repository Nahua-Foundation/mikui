import { memo, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { Search, Edit } from 'lucide-react';
import { Input } from '../ui/input';
import { Topic } from './types';
import { Virtuoso } from 'react-virtuoso';

const DEFAULT_WIDTH = 220;
const MIN_WIDTH = 160;
const MAX_WIDTH = 480;

interface TopicItemProps {
  topic: Topic; 
  isSelected: boolean; 
  onClick: () => void;
  onConfigClick: (topic: Topic) => void;
}

const TopicItem = memo(function TopicItem({
  topic,
  isSelected,
  onClick,
  onConfigClick,
}: TopicItemProps) {
  const handleEditClick = (e: React.MouseEvent) => {
    e.stopPropagation();
    onConfigClick(topic);
  };

  return (
    <div className="relative shrink-0 w-full cursor-pointer" data-name="folder" onClick={onClick}>
      <div className="flex flex-row items-center relative size-full">
        <div className="box-border content-stretch flex flex-row gap-1.5 items-center justify-start px-2 py-1 relative w-full">
          <button
            onClick={handleEditClick}
            className="size-4 text-dim hover:text-brand transition-colors cursor-pointer p-0 border-none bg-transparent"
          >
            <Edit className="size-4" />
          </button>
          <div className={`basis-0 font-mono font-[450] grow leading-[0] min-h-px min-w-px relative shrink-0 text-[14px] text-left ${
            isSelected ? 'text-slate-50' : 'text-soft hover:text-slate-50'
          }`}>
            <p className="block leading-[20px]" style={{whiteSpace: "nowrap"}}>{topic.name}</p>
          </div>
        </div>
      </div>
    </div>
  );
});

interface TopicsPanelProps {
  topics: Topic[];
  selectedTopic: Topic | null;
  onTopicSelect: (topic: Topic) => void;
  onTopicConfig: (topic: Topic) => void;
}

/** Один экземпляр на модуль: `localeCompare` создаёт коллатор на каждый вызов,
 *  что на тысячах топиков заметно. */
const COLLATOR = new Intl.Collator(undefined, { numeric: true, sensitivity: 'base' });

export function TopicsPanel({ topics, selectedTopic, onTopicSelect, onTopicConfig }: TopicsPanelProps) {
  const [topicFilter, setTopicFilter] = useState('');
  const [width, setWidth] = useState(DEFAULT_WIDTH);
  const rootRef = useRef<HTMLDivElement>(null);
  const dragging = useRef<{ startX: number; startW: number } | null>(null);
  const pendingWidth = useRef<number | null>(null);

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
              >
                <TopicItem
                  topic={filteredTopics[index]}
                  isSelected={selectedTopic?.name === filteredTopics[index].name}
                  onClick={() => onTopicSelect(filteredTopics[index])}
                  onConfigClick={onTopicConfig}
                />
              </div>
            )}
          />
        )}
      </div>

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