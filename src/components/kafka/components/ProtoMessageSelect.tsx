import { useState } from 'react';
import { Check, ChevronsUpDown } from 'lucide-react';
import { Button } from '../../ui/button';
import { Popover, PopoverContent, PopoverTrigger } from '../../ui/popover';
import { Command, CommandEmpty, CommandInput, CommandItem, CommandList } from '../../ui/command';

/**
 * Отбор по подстроке, а не нечёткое совпадение, которое cmdk делает по
 * умолчанию.
 *
 * Имена message длинные и составные (`com.example.orders.OrderCreated`), и
 * нечёткий поиск на таких уверенно находит почти всё: буквы запроса
 * рассыпаются по точкам и частям имени. Тот же довод, по которому и фильтр
 * топиков ищет подстроку, а не похожее.
 */
const matchesSearch = (value: string, search: string) =>
  value.toLowerCase().includes(search.toLowerCase()) ? 1 : 0;

interface ProtoMessageSelectProps {
  /** Имена message из загруженных .proto. Пустой список — выбирать не из чего. */
  messages: string[];
  value: string | null;
  onChange: (name: string) => void;
  /** Запрет по своей причине — поверх «нечего выбирать». */
  disabled?: boolean;
  /** Классы триггера. Ширину задаёт место, куда его ставят, а не он сам. */
  className?: string;
}

/**
 * Выбор типа message из загруженных .proto — с поиском по списку.
 *
 * Раньше здесь стоял `Select`, и до нужного типа приходилось листать: в
 * большом .proto объявлено под сотню сообщений, а различаются они, как
 * правило, хвостом имени. Набрать три буквы дешевле в любом случае.
 *
 * Тот же приём, что у выбора subject в Avro: `Popover` с `Command` внутри.
 * Поле поиска живёт внутри попоувера, поэтому в закрытом виде занимает ровно
 * столько же места, сколько занимал селектор.
 *
 * Один компонент на два места (схема топика и отправка сообщения), потому что
 * длинное имя в узком триггере — это три отдельных правки вёрстки
 * (`min-w-0`, обрезка, подсказка с полным именем), и разъехаться им нельзя.
 */
export function ProtoMessageSelect({
  messages,
  value,
  onChange,
  disabled = false,
  className = 'w-full',
}: ProtoMessageSelectProps) {
  const [open, setOpen] = useState(false);
  const empty = messages.length === 0;

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <Button
          variant="outline"
          role="combobox"
          disabled={disabled || empty}
          // Полное имя — подсказкой: в триггере оно обрезано, а знать, что
          // именно выбрано, нужно целиком.
          title={value ?? undefined}
          // `min-w-0` обязателен: триггер стоит flex-элементом в строке с
          // другими кнопками, и без него его автоминимум равен заданной
          // ширине — сжаться нечем, и он выезжает из строки вправо вместе с
          // рамкой и стрелкой.
          className={`justify-between min-w-0 bg-surface border-edge text-strong font-mono disabled:opacity-50 ${className}`}
        >
          <span className={`truncate ${value ? '' : 'text-dim'}`}>
            {value ?? (empty ? 'Load a .proto file first' : 'Choose a message')}
          </span>
          <ChevronsUpDown className="size-4 shrink-0 opacity-50" />
        </Button>
      </PopoverTrigger>
      {/* Шире триггера, но не шире разумного: триггер бывает узким, а имена
          тут длинные, и обрезать их в самом списке — значит обрезать ровно то,
          по чему в нём и выбирают. */}
      <PopoverContent
        align="start"
        className="w-auto min-w-(--radix-popover-trigger-width) max-w-[36rem] p-0 bg-surface border-edge"
      >
        <Command filter={matchesSearch} className="bg-surface">
          <CommandInput placeholder="Search messages…" className="font-mono text-strong" />
          <CommandList>
            <CommandEmpty className="p-3 font-mono text-xs text-dim">Nothing found</CommandEmpty>
            {messages.map((name) => (
              <CommandItem
                key={name}
                value={name}
                title={name}
                onSelect={() => {
                  setOpen(false);
                  if (name !== value) onChange(name);
                }}
                className="font-mono text-xs text-strong data-[selected=true]:bg-edge"
              >
                <Check
                  className={`size-3.5 shrink-0 ${name === value ? 'opacity-100' : 'opacity-0'}`}
                />
                <span className="truncate">{name}</span>
              </CommandItem>
            ))}
          </CommandList>
        </Command>
      </PopoverContent>
    </Popover>
  );
}
