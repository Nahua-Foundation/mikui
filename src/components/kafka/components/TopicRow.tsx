import { memo } from 'react';
import { Info } from 'lucide-react';
import { Topic } from '../types';

interface TopicRowProps {
  topic: Topic;
  isSelected: boolean;
  /** Курсор простоял на строке достаточно долго — показываем кнопку. */
  revealed: boolean;
  onSelect: () => void;
  onInfo: (topic: Topic) => void;
}

/**
 * Одна строка списка топиков. Имя, которое не поместилось, обрезается
 * многоточием; целиком его показывает `TopicPeek` под курсором.
 *
 * Кнопки информации в разметке нет, пока строку не подержали под курсором.
 * Постоянно занятое под неё место стоило имени тридцати пикселей отступа на
 * КАЖДОЙ строке — в панели шириной в две сотни это заметная часть того, ради
 * чего список и открыт. Появляется она слева, а не справа: вынос полного
 * имени начинается от левого края текста и накрывает всё правее него.
 */
export const TopicRow = memo(function TopicRow({
  topic,
  isSelected,
  revealed,
  onSelect,
  onInfo,
}: TopicRowProps) {
  const handleInfoClick = (e: React.MouseEvent) => {
    e.stopPropagation();
    onInfo(topic);
  };

  return (
    <div className="relative shrink-0 w-full cursor-pointer" data-name="folder" onClick={onSelect}>
      <div className="flex flex-row items-center relative size-full">
        {/* Имя уезжает вправо ровно на ширину кнопки с зазором, освобождая ей
            место. Отступом, а не сдвигом: сдвиг вынес бы хвост длинного имени
            за край панели, а отступ пересчитывает многоточие как надо. */}
        <div
          className={`box-border content-stretch flex flex-row items-center justify-start py-1 pr-2 relative w-full transition-[padding-left] duration-150 ease-out ${
            revealed ? 'pl-[30px]' : 'pl-2'
          }`}
        >
          {revealed && (
            <button
              onClick={handleInfoClick}
              title="Topic info"
              className="absolute left-2 top-0 bottom-0 flex items-center text-dim hover:text-brand transition-colors cursor-pointer p-0 border-none bg-transparent animate-reveal"
            >
              <Info className="size-4" />
            </button>
          )}
          <div
            className={`basis-0 font-mono font-[450] grow leading-[0] min-h-px min-w-px relative shrink-0 text-[14px] text-left ${
              isSelected ? 'text-strong' : 'text-soft hover:text-strong'
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
