import { Braces, ChevronDown, FileJson, FileText, Settings2, Shapes, Type } from 'lucide-react';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from '../../ui/dropdown-menu';
import { BodyFormat } from '../types';

const FORMATS: { value: BodyFormat; label: string; icon: typeof Braces }[] = [
  { value: 'json', label: 'json', icon: Braces },
  { value: 'text', label: 'text', icon: Type },
  { value: 'proto', label: 'proto', icon: FileText },
  { value: 'avro', label: 'avro', icon: Shapes },
  { value: 'jsonschema', label: 'json schema', icon: FileJson },
];

/**
 * Формат, которому нужна схема, — а значит и дверь в её настройки рядом с
 * селектором. Ни json, ни text ничего к себе не требуют: их видно как есть.
 *
 * `jsonschema` здесь тоже, хотя ПОКАЗУ схема ему не нужна: тело за
 * confluent-заголовком читается и без неё. Дверь ему нужна ради отправки — там
 * схема единственное, что отличает валидное сообщение от текста, который
 * потребитель топика не прочитает.
 */
export function needsSchema(format: BodyFormat): boolean {
  return format === 'proto' || format === 'avro' || format === 'jsonschema';
}

interface FormatSelectProps {
  format: BodyFormat;
  onChange: (format: BodyFormat) => void;
  /** Открыть настройки схемы топика — кнопка появляется только там, где схема
   *  вообще нужна. */
  onOpenSchema: () => void;
}

/**
 * Чем показывать тела сообщений открытого топика.
 *
 * Стоит в шапке, а не только в настройках топика, потому что это выбор
 * ПОКАЗА, а не свойство данных: одно и то же тело смотрят то разобранным по
 * схеме, то сырым текстом, и ради переключения между ними открывать модальное
 * окно незачем. Выбор при этом тот же самый и хранится там же — в схеме
 * топика на диске, — поэтому переживает и смену топика, и перезапуск.
 */
export function FormatSelect({ format, onChange, onOpenSchema }: FormatSelectProps) {
  return (
    <div className="flex flex-row items-center gap-2">
      {/* Два уровня — как у соседнего селектора партиций: подпись постоянной
          ширины сверху, значение под ней. */}
      <DropdownMenu>
        <DropdownMenuTrigger className="font-mono text-xs text-soft hover:text-slate-50 bg-transparent hover:bg-transparent p-0 h-auto gap-1.5 flex items-start border-none outline-none cursor-pointer">
          <div className="flex flex-col items-start leading-tight">
            <span className="text-dim">format</span>
            {/* Подпись из того же списка, что и пункты меню: у `jsonschema`
                значение и подпись расходятся, и печатать здесь сырое значение
                значило бы показывать в шапке не то, что выбрано в списке. */}
            <span className="text-soft">
              {FORMATS.find((f) => f.value === format)?.label ?? format}
            </span>
          </div>
          <ChevronDown className="size-3.5 mt-0.5" />
        </DropdownMenuTrigger>
        <DropdownMenuContent className="bg-surface border-edge min-w-32" align="end">
          {FORMATS.map(({ value, label, icon: Icon }) => (
            <DropdownMenuItem
              key={value}
              className={`font-mono cursor-pointer ${
                value === format
                  ? 'bg-edge text-slate-50'
                  : 'text-soft hover:bg-edge hover:text-slate-50'
              }`}
              onClick={() => onChange(value)}
            >
              <Icon className="size-4" />
              {label}
            </DropdownMenuItem>
          ))}
        </DropdownMenuContent>
      </DropdownMenu>

      {needsSchema(format) && (
        <button
          onClick={onOpenSchema}
          className="text-soft hover:text-brand bg-transparent border-none outline-none cursor-pointer p-0 transition-colors"
          title="Schema settings"
        >
          <Settings2 className="size-4" />
        </button>
      )}
    </div>
  );
}
