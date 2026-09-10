import { useState } from 'react';
import { ArrowDown, ArrowUp, ChevronDown, Clock, Hash } from 'lucide-react';
import { Button } from '../../ui/button';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuSub,
  DropdownMenuSubContent,
  DropdownMenuSubTrigger,
  DropdownMenuTrigger,
} from '../../ui/dropdown-menu';
import { Input } from '../../ui/input';
import { Label } from '../../ui/label';
import { EMPTY_RANGE, ReadMode, ReadRange } from '../types';

/** Незаданная граница в подписи. */
const OPEN_BOUND = '…';

/**
 * Пустое поле — это «границы нет», и это законно. Неразобранный ввод — совсем
 * другое дело, и молча приравнивать его к пустому нельзя: пользователь тогда
 * получит не тот диапазон, который набрал, и не узнает об этом.
 */
const INVALID = Number.NaN;

function parseOffset(raw: string): number | null {
  const text = raw.trim();
  if (!text) return null;
  return /^\d+$/.test(text) ? Number(text) : INVALID;
}

/**
 * Разбирает и то, что даёт `datetime-local`, и то, что можно вставить из лога:
 * ISO-8601, «2026-08-31 02:11:45» и голые unix-millis.
 */
function parseTimestamp(raw: string): number | null {
  const text = raw.trim();
  if (!text) return null;

  // Голые цифры — так время выглядит в самой Kafka и в логах. Десять знаков
  // это секунды: сообщение из 1970 года куда менее вероятно, чем скопированный
  // unix-timestamp в секундах.
  if (/^\d{9,}$/.test(text)) {
    const value = Number(text);
    return text.length <= 10 ? value * 1000 : value;
  }

  // `Date` берёт ISO-8601 и формат `datetime-local`; время без указанной зоны
  // он трактует как локальное — ровно то, что показано в таблице. Пробел
  // вместо `T` понимают не все движки, поэтому нормализуем сами.
  const parsed = Date.parse(text.replace(' ', 'T'));
  return Number.isNaN(parsed) ? INVALID : parsed;
}

/** `datetime-local` понимает только локальное время без зоны. */
function toPickerValue(millis: number | null): string {
  if (millis === null) return '';
  const local = new Date(millis - new Date(millis).getTimezoneOffset() * 60_000);
  return local.toISOString().slice(0, 19);
}

const TIME_LABEL = new Intl.DateTimeFormat(undefined, {
  hour: '2-digit',
  minute: '2-digit',
  second: '2-digit',
  hour12: false,
});

function bound(value: number | null, format: (v: number) => string): string {
  return value === null ? OPEN_BOUND : format(value);
}

/**
 * Подпись на кнопке: из шапки должно быть видно, что именно сейчас читается.
 *
 * Без слова «first»: стрелка рядом уже говорит, в какую сторону, и повторять
 * это словом значит занимать шапку дважды под одно и то же. Полная формулировка
 * осталась в самом меню, где выбирают.
 */
function label(mode: ReadMode, range: ReadRange): string {
  switch (mode) {
    case 'newest':
      return 'newest';
    case 'oldest':
      return 'oldest';
    case 'offset':
      return `offset ${bound(range.from_offset, String)} → ${bound(range.to_offset, String)}`;
    case 'timestamp': {
      const at = (v: number) => TIME_LABEL.format(new Date(v));
      return `time ${bound(range.from_timestamp, at)} → ${bound(range.to_timestamp, at)}`;
    }
  }
}

interface ReadOrderProps {
  mode: ReadMode;
  range: ReadRange;
  /**
   * Выбрана ли ровно одна партиция. Офсеты в каждой партиции свои, и
   * «от офсета 12345» в партиции 0 и в партиции 5 — разные сообщения; по
   * нескольким сразу такой диапазон означал бы не то, что читается как
   * его смысл. Время сквозное, и на него ограничение не распространяется.
   */
  singlePartition: boolean;
  onChange: (mode: ReadMode, range: ReadRange) => void;
}

export function ReadOrder({ mode, range, singlePartition, onChange }: ReadOrderProps) {
  const [open, setOpen] = useState(false);
  // Черновик: пока не нажат Apply, топик не перечитывается. Иначе каждая
  // набранная цифра запускала бы чтение из кластера с квотой на чтение.
  const [fromOffset, setFromOffset] = useState('');
  const [toOffset, setToOffset] = useState('');
  const [fromTime, setFromTime] = useState('');
  const [toTime, setToTime] = useState('');
  // По умолчанию — ручной ввод: нативный datetime-local неудобен для того,
  // как время обычно и получают — скопированным из лога, а не подобранным
  // в пикере по клику.
  const [manualTime, setManualTime] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Открыли меню — показываем то, что читается сейчас, а не остатки прошлой
  // попытки.
  const handleOpenChange = (next: boolean) => {
    setOpen(next);
    if (!next) return;
    setError(null);
    setFromOffset(range.from_offset === null ? '' : String(range.from_offset));
    setToOffset(range.to_offset === null ? '' : String(range.to_offset));
    setFromTime(toPickerValue(range.from_timestamp));
    setToTime(toPickerValue(range.to_timestamp));
  };

  const pick = (next: ReadMode) => {
    setOpen(false);
    onChange(next, EMPTY_RANGE);
  };

  const applyOffsets = () => {
    const from = parseOffset(fromOffset);
    const to = parseOffset(toOffset);
    if (Number.isNaN(from) || Number.isNaN(to)) {
      setError('Offsets must be whole numbers');
      return;
    }
    if (from === null && to === null) {
      setError('Fill at least one bound');
      return;
    }
    if (from !== null && to !== null && from > to) {
      setError('The first offset is after the last one');
      return;
    }
    setOpen(false);
    onChange('offset', { ...EMPTY_RANGE, from_offset: from, to_offset: to });
  };

  const applyTimestamps = () => {
    const from = parseTimestamp(fromTime);
    const to = parseTimestamp(toTime);
    if (Number.isNaN(from) || Number.isNaN(to)) {
      setError('Use YYYY-MM-DD HH:MM:SS, an ISO-8601 value or unix millis');
      return;
    }
    if (from === null && to === null) {
      setError('Fill at least one bound');
      return;
    }
    if (from !== null && to !== null && from > to) {
      setError('The start is after the end');
      return;
    }
    setOpen(false);
    onChange('timestamp', { ...EMPTY_RANGE, from_timestamp: from, to_timestamp: to });
  };

  /**
   * Меню перехватывает клавиатуру под свою навигацию и поиск по первым буквам
   * — набирать в полях внутри него без этого нельзя вовсе.
   */
  const keepKeysInInput = (apply: () => void) => (e: React.KeyboardEvent) => {
    e.stopPropagation();
    if (e.key === 'Enter') apply();
  };

  const inputClass =
    'bg-surface border-edge text-strong font-mono text-sm placeholder:text-dim h-9';

  return (
    <DropdownMenu open={open} onOpenChange={handleOpenChange}>
      <DropdownMenuTrigger className="font-mono text-xs text-soft hover:text-strong bg-transparent hover:bg-transparent p-0 h-auto gap-1.5 flex items-center border-none outline-none cursor-pointer">
        {mode === 'newest' && <ArrowDown className="size-3.5" />}
        {mode === 'oldest' && <ArrowUp className="size-3.5" />}
        {mode === 'offset' && <Hash className="size-3.5" />}
        {mode === 'timestamp' && <Clock className="size-3.5" />}
        <span className="whitespace-nowrap">{label(mode, range)}</span>
        <ChevronDown className="size-3.5" />
      </DropdownMenuTrigger>

      <DropdownMenuContent className="bg-surface border-edge w-56" align="end">
        <DropdownMenuItem
          className={`font-mono cursor-pointer ${
            mode === 'newest' ? 'bg-edge text-strong' : 'text-soft hover:bg-edge hover:text-strong'
          }`}
          onClick={() => pick('newest')}
        >
          <ArrowDown className="size-4" />
          newest first
        </DropdownMenuItem>
        <DropdownMenuItem
          className={`font-mono cursor-pointer ${
            mode === 'oldest' ? 'bg-edge text-strong' : 'text-soft hover:bg-edge hover:text-strong'
          }`}
          onClick={() => pick('oldest')}
        >
          <ArrowUp className="size-4" />
          oldest first
        </DropdownMenuItem>

        <DropdownMenuSeparator className="bg-edge" />

        <DropdownMenuSub>
          <DropdownMenuSubTrigger
            className={`font-mono flex items-center gap-2 ${
              mode === 'timestamp' ? 'bg-edge text-strong' : 'text-soft'
            }`}
          >
            <Clock className="size-4" />
            specific timestamp
          </DropdownMenuSubTrigger>
          <DropdownMenuSubContent className="bg-surface border-edge w-72 p-3 space-y-3">
            <div className="space-y-1.5">
              <Label className="font-mono text-xs text-soft">From</Label>
              <Input
                type={manualTime ? 'text' : 'datetime-local'}
                step="1"
                value={fromTime}
                onChange={(e) => setFromTime(e.target.value)}
                onKeyDown={keepKeysInInput(applyTimestamps)}
                placeholder="2026-08-31 02:11:45"
                className={inputClass}
              />
            </div>
            <div className="space-y-1.5">
              <Label className="font-mono text-xs text-soft">To</Label>
              <Input
                type={manualTime ? 'text' : 'datetime-local'}
                step="1"
                value={toTime}
                onChange={(e) => setToTime(e.target.value)}
                onKeyDown={keepKeysInInput(applyTimestamps)}
                placeholder="2026-08-31 02:15:00"
                className={inputClass}
              />
            </div>
            {/* Пикер удобнее для «примерно тогда-то», но время из лога в него
                не вставить — а именно так его обычно и получают. */}
            <p className="font-mono text-xs text-dim">
              Or{' '}
              <button
                type="button"
                onClick={() => setManualTime((v) => !v)}
                className="font-mono text-xs text-brand hover:text-brand-hover bg-transparent border-none p-0 cursor-pointer"
              >
                {manualTime ? 'use the date picker' : 'type an exact value'}
              </button>
            </p>
            {error && <p className="font-mono text-xs text-danger">{error}</p>}
            <Button
              onClick={applyTimestamps}
              size="sm"
              className="w-full bg-brand text-surface hover:bg-brand-hover font-mono"
            >
              Apply
            </Button>
          </DropdownMenuSubContent>
        </DropdownMenuSub>

        <DropdownMenuSub>
          <DropdownMenuSubTrigger
            disabled={!singlePartition}
            className={`font-mono flex items-center gap-2 ${
              mode === 'offset' ? 'bg-edge text-strong' : 'text-soft'
            } data-[disabled]:text-dim`}
          >
            <Hash className="size-4" />
            specific offset
          </DropdownMenuSubTrigger>
          <DropdownMenuSubContent className="bg-surface border-edge w-64 p-3 space-y-3">
            <div className="space-y-1.5">
              <Label className="font-mono text-xs text-soft">From offset</Label>
              <Input
                value={fromOffset}
                onChange={(e) => setFromOffset(e.target.value)}
                onKeyDown={keepKeysInInput(applyOffsets)}
                placeholder="from offset"
                className={inputClass}
              />
            </div>
            <div className="space-y-1.5">
              <Label className="font-mono text-xs text-soft">To offset</Label>
              <Input
                value={toOffset}
                onChange={(e) => setToOffset(e.target.value)}
                onKeyDown={keepKeysInInput(applyOffsets)}
                placeholder="to offset"
                className={inputClass}
              />
            </div>
            <p className="font-mono text-xs text-dim">
              Leave one side empty to read from — or up to — that offset.
            </p>
            {error && <p className="font-mono text-xs text-danger">{error}</p>}
            <Button
              onClick={applyOffsets}
              size="sm"
              className="w-full bg-brand text-surface hover:bg-brand-hover font-mono"
            >
              Apply
            </Button>
          </DropdownMenuSubContent>
        </DropdownMenuSub>

        {!singlePartition && (
          <p className="font-mono text-xs text-dim px-2 py-1.5">
            Offsets are per-partition — select a single one to use them.
          </p>
        )}
      </DropdownMenuContent>
    </DropdownMenu>
  );
}
