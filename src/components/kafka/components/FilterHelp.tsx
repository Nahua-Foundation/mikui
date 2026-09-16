import { CircleHelp } from 'lucide-react';
import { Popover, PopoverContent, PopoverTrigger } from '../../ui/popover';

/** Оператор и что он делает. Порядок — от самого частого к самому редкому. */
const SYNTAX: [token: string, meaning: string][] = [
  ['text', 'contains "text"'],
  ['^text', 'starts with "text"'],
  ['text$', 'ends with "text"'],
  ['!text', 'does not contain "text"'],
  ['a|b', 'a or b'],
  ['a b', 'both at once'],
];

/**
 * Шпаргалка по синтаксису фильтра — иконка в правом краю поля поиска.
 *
 * Без неё операторы не существуют: угадать, что `$` привязывает к концу имени,
 * нельзя, а placeholder такого не вместит. Попоувер, а не подсказка при
 * наведении: здесь таблица, которую читают, иногда — не отпуская мышь с поля.
 *
 * Пример внизу не дублирует таблицу, а показывает единственное, чего по ней не
 * видно: что условия составляются. Взят настоящий — имена, различающиеся одним
 * хвостом, и есть повод, по которому всё это заведено.
 */
export function FilterHelp() {
  return (
    <Popover>
      <PopoverTrigger asChild>
        <button
          type="button"
          title="Filter syntax"
          className="absolute right-2 top-1/2 -translate-y-1/2 flex items-center text-dim hover:text-strong transition-colors cursor-pointer p-0 border-none bg-transparent"
        >
          <CircleHelp className="size-4" />
        </button>
      </PopoverTrigger>
      <PopoverContent
        align="start"
        className="p-3 bg-surface border-edge text-soft shadow-peek"
      >
        <div className="font-mono text-xs">
          <div className="mb-2 text-strong">Filter syntax</div>

          <div className="grid grid-cols-[auto_1fr] gap-x-3 gap-y-1">
            {SYNTAX.map(([token, meaning]) => (
              <div key={token} className="contents">
                <span className="text-brand">{token}</span>
                <span className="text-dim">{meaning}</span>
              </div>
            ))}
          </div>

          <div className="mt-3 pt-2 border-t border-edge text-dim">
            <span className="text-brand">text$ !name-2</span> — ends with "text", but not the "name-2" ones
          </div>
        </div>
      </PopoverContent>
    </Popover>
  );
}
