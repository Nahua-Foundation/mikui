import { memo } from 'react';
import { Edit } from 'lucide-react';
import { Topic } from '../types';

interface TopicRowProps {
  topic: Topic;
  isSelected: boolean;
  onSelect: () => void;
  onConfig: (topic: Topic) => void;
}

/** Одна строка списка топиков. Имя, которое не поместилось, обрезается
 *  многоточием; целиком его показывает `TopicPeek` под курсором. */
export const TopicRow = memo(function TopicRow({
  topic,
  isSelected,
  onSelect,
  onConfig,
}: TopicRowProps) {
  const handleEditClick = (e: React.MouseEvent) => {
    e.stopPropagation();
    onConfig(topic);
  };

  return (
    <div className="relative shrink-0 w-full cursor-pointer" data-name="folder" onClick={onSelect}>
      <div className="flex flex-row items-center relative size-full">
        <div className="box-border content-stretch flex flex-row gap-1.5 items-center justify-start px-2 py-1 relative w-full">
          <button
            onClick={handleEditClick}
            className="size-4 shrink-0 text-dim hover:text-brand transition-colors cursor-pointer p-0 border-none bg-transparent"
          >
            <Edit className="size-4" />
          </button>
          <div
            className={`basis-0 font-mono font-[450] grow leading-[0] min-h-px min-w-px relative shrink-0 text-[14px] text-left ${
              isSelected ? 'text-slate-50' : 'text-soft hover:text-slate-50'
            }`}
          >
            {/* `data-topic-name` — по нему список находит элемент, чтобы
                сравнить scrollWidth с clientWidth и понять, обрезано ли имя,
                и от него же берёт левый край для выноса. */}
            <p
              data-topic-name
              className="block leading-[20px] whitespace-nowrap overflow-hidden text-ellipsis"
            >
              {topic.name}
            </p>
          </div>
        </div>
      </div>
    </div>
  );
});
