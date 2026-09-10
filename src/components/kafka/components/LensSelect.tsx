import { ChevronDown, Eye, EyeOff, GitCompare, Package, Wand2 } from 'lucide-react';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from '../../ui/dropdown-menu';
import { LensSetting } from '../types';

/** `null` — «не выбирали»: конверт распознаётся по самому телу. */
type Choice = LensSetting | null;

const LENSES: { value: Choice; label: string; icon: typeof Eye; hint: string }[] = [
  {
    value: null,
    label: 'auto',
    icon: Wand2,
    hint: 'Recognise a Debezium or Kafka Connect envelope by the body itself',
  },
  {
    value: 'off',
    label: 'off',
    icon: EyeOff,
    hint: 'Show bodies exactly as they arrive, envelope and all',
  },
  {
    value: 'debezium',
    label: 'debezium',
    icon: GitCompare,
    hint: 'Show the changed row and the operation instead of the CDC envelope',
  },
  {
    value: 'connect',
    label: 'connect',
    icon: Package,
    hint: 'Show the payload instead of the schema that precedes it',
  },
];

interface LensSelectProps {
  lens: Choice;
  onChange: (lens: Choice) => void;
}

/**
 * Что делать с конвертом Debezium / Kafka Connect.
 *
 * Отдельный селектор рядом с форматом, а не пункт внутри него: это независимые
 * настройки. Формат отвечает на вопрос «как превратить байты в JSON», линза —
 * «что из этого JSON показать»; Debezium с Avro-сериализатором обычное дело, и
 * их сочетание должно выражаться.
 *
 * По умолчанию `auto`: конверт распознаётся сам, и включать его руками на
 * каждом CDC-топике никто не должен. `off` при этом — полноценный ответ, а не
 * возврат к умолчанию: распознавание его не отменяет.
 */
export function LensSelect({ lens, onChange }: LensSelectProps) {
  const current = LENSES.find((l) => l.value === lens) ?? LENSES[0];

  return (
    <DropdownMenu>
      <DropdownMenuTrigger className="font-mono text-xs text-soft hover:text-strong bg-transparent hover:bg-transparent p-0 h-auto gap-1.5 flex items-start border-none outline-none cursor-pointer">
        {/* Два уровня, как у соседних селекторов: подпись сверху, значение под
            ней. */}
        <div className="flex flex-col items-start leading-tight">
          <span className="text-dim">envelope</span>
          <span className="text-soft">{current.label}</span>
        </div>
        <ChevronDown className="size-3.5 mt-0.5" />
      </DropdownMenuTrigger>
      <DropdownMenuContent className="bg-surface border-edge min-w-44" align="end">
        {LENSES.map(({ value, label, icon: Icon, hint }) => (
          <DropdownMenuItem
            key={label}
            title={hint}
            className={`font-mono cursor-pointer ${
              value === lens
                ? 'bg-edge text-strong'
                : 'text-soft hover:bg-edge hover:text-strong'
            }`}
            onClick={() => onChange(value)}
          >
            <Icon className="size-4" />
            {label}
          </DropdownMenuItem>
        ))}
      </DropdownMenuContent>
    </DropdownMenu>
  );
}
