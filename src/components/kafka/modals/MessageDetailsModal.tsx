import { useEffect, useRef, useState } from 'react';
import { Button } from '../../ui/button';
import { Copy, Star } from 'lucide-react';
import { toast } from 'sonner';
import { Dialog, DialogHeader, DialogTitle, DialogDescription } from '../../ui/dialog';
import { DialogContentNoClose } from '../DialogContentNoClose';
import { BodyFormat, FullMessage, MessageHeader } from '../types';
import { highlightLine } from '../syntax';

/** Потолок отрисовки. Тело на 10 МБ иначе положило бы вкладку на лопатки
 *  ещё до того, как пользователь что-то увидит. */
const MAX_RENDERED_LINES = 2000;

/** `toLocaleString` секунд точнее не берёт, поэтому миллисекунды дописываются
 *  отдельно — как в таблице сообщений. */
function formatTimestamp(millis: number): string {
  if (!millis) return '—';
  const ms = String(((millis % 1000) + 1000) % 1000).padStart(3, '0');
  return `${new Date(millis).toLocaleString()}.${ms}`;
}

interface MessageDetailsModalProps {
  message: FullMessage | null;
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onAddToFavorite?: (message: FullMessage) => void;
  /**
   * Показать соседнее сообщение, не закрывая модалку: -1 — строкой выше по
   * таблице, +1 — строкой ниже. Порядок строк задаёт таблица, поэтому здесь
   * достаточно шага: сортировка учтена сама.
   *
   * Не задан, когда сообщение открыто не из таблицы (избранное) — соседей у
   * него тогда просто нет.
   */
  onNavigate?: (delta: -1 | 1) => void;
  /**
   * Формат тела, выбранный для топика. Разобранный protobuf приезжает сюда
   * компактным JSON, поэтому от JSON-топика он тут ничем не отличается —
   * особняком стоит только `text`, где раскладывать тело не просят.
   */
  format?: BodyFormat;
}

export function MessageDetailsModal({
  message,
  open,
  onOpenChange,
  onAddToFavorite,
  onNavigate,
  format = 'json',
}: MessageDetailsModalProps) {
  const [activeTab, setActiveTab] = useState<'payload' | 'headers'>('payload');

  // Стрелки: вверх/вниз — соседнее сообщение, влево/вправо — вкладка.
  //
  // Слушатель на window, а не на самой модалке: фокус после клика по строке
  // таблицы может стоять на любой кнопке внутри, и ловить событие на элементе
  // значило бы зависеть от того, где он оказался.
  useEffect(() => {
    if (!open) return;

    const onKeyDown = (event: KeyboardEvent) => {
      // Стрелка с модификатором — это уже другая команда (у macOS, например,
      // Cmd+↑ ходит по истории), и присваивать её себе нельзя.
      if (event.altKey || event.ctrlKey || event.metaKey || event.shiftKey) return;

      switch (event.key) {
        case 'ArrowUp':
          if (!onNavigate) return;
          onNavigate(-1);
          break;
        case 'ArrowDown':
          if (!onNavigate) return;
          onNavigate(1);
          break;
        case 'ArrowLeft':
          setActiveTab('payload');
          break;
        case 'ArrowRight':
          setActiveTab('headers');
          break;
        default:
          return;
      }
      // Стрелками управляет обработчик на window, а не клик по кнопке вкладки,
      // и фокус после открытия модалки (или после клика по строке таблицы)
      // остаётся на том, что оказалось сфокусировано первым — обычно на
      // кнопке payload. Дальше он там и стоит, даже когда вкладку переключили
      // стрелкой: кольцо фокуса едет на кнопку, которая уже не активна.
      (document.activeElement as HTMLElement | null)?.blur();
      // Только для разобранных клавиш: иначе вверх/вниз заодно прокрутили бы
      // тело сообщения, и переход к соседу выглядел бы как рывок текста.
      event.preventDefault();
    };

    window.addEventListener('keydown', onKeyDown);
    return () => window.removeEventListener('keydown', onKeyDown);
  }, [open, onNavigate]);

  // Прокрутка тела живёт в DOM и переживает смену сообщения: без сброса сосед
  // открывался бы на той же высоте, то есть где-то посередине своего JSON.
  const bodyRef = useRef<HTMLDivElement>(null);
  useEffect(() => {
    bodyRef.current?.scrollTo({ top: 0, left: 0 });
  }, [message?.partition, message?.offset]);

  const copyToClipboard = (text: string) => {
    navigator.clipboard.writeText(text);
    toast.success('Copied to clipboard');
  };

  const copyHeadersToClipboard = (headers: MessageHeader[]) => {
    copyToClipboard(headers.map((h) => `${h.key}: ${h.value}`).join('\n'));
  };

  /** `isJson` решает, переносить ли строки: у форматированного JSON перенос
   *  сбил бы нумерацию, а у чего угодно другого одна строка на всё тело
   *  растянулась бы в бесконечную полосу. */
  const formatJson = (jsonString: string): { text: string; isJson: boolean } => {
    // Явно выбранный Text просят показать как есть — раскладывать его по
    // строкам значило бы решать за пользователя.
    if (format === 'text') return { text: jsonString, isJson: false };
    try {
      return { text: JSON.stringify(JSON.parse(jsonString), null, 2), isJson: true };
    } catch {
      return { text: jsonString, isJson: false };
    }
  };

  const handleCopy = () => {
    if (activeTab === 'payload') {
      copyToClipboard(message!.value);
    } else {
      copyHeadersToClipboard(message!.headers);
    }
  };

  const handleSave = () => {
    if (message && onAddToFavorite) {
      onAddToFavorite(message);
    }
  };

  if (!message) return null;

  const { text: formatted, isJson } = formatJson(message.value);
  const allLines = formatted.split('\n');
  const lines = allLines.slice(0, MAX_RENDERED_LINES);
  const hiddenLines = allLines.length - lines.length;
  const enumValues = new Set(message.enum_values);

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContentNoClose className="max-w-[58rem] sm:max-w-[58rem] max-h-[90vh] bg-surface border-edge text-slate-50">
        <DialogHeader className="border-b border-edge pb-4">
          <DialogTitle className="font-mono text-soft text-lg">
            Message Details
          </DialogTitle>
          <DialogDescription className="font-mono text-soft text-sm">
            View details of the selected message
          </DialogDescription>
        </DialogHeader>
        
        <div className="space-y-6 overflow-auto">
          {/* Message Metadata */}
          <div className="grid grid-cols-2 gap-6">
            <div>
              <div className="font-mono text-sm text-soft mb-1">Partition</div>
              <div className="font-mono text-brand">{message.partition}</div>
            </div>
            <div>
              <div className="font-mono text-sm text-soft mb-1">Offset</div>
              <div className="font-mono text-brand">{message.offset}</div>
            </div>
            <div>
              <div className="font-mono text-sm text-soft mb-1">Key</div>
              <div className="font-mono text-slate-50">{message.key}</div>
            </div>
            <div>
              <div className="font-mono text-sm text-soft mb-1">Timestamp</div>
              <div className="font-mono text-slate-50">{formatTimestamp(message.timestamp)}</div>
            </div>
          </div>
          
          {/* Content Section with Tab Buttons */}
          <div>
            <div className="flex items-center justify-between mb-4">
              <div className="flex gap-1">
                <Button
                  variant="ghost"
                  size="sm"
                  className={`font-mono px-3 py-1 h-auto ${
                    activeTab === 'payload' 
                      ? 'bg-brand text-surface hover:bg-brand-hover' 
                      : 'bg-transparent text-soft hover:bg-edge hover:text-slate-50'
                  }`}
                  onClick={() => setActiveTab('payload')}
                >
                  payload
                </Button>
                <Button
                  variant="ghost"
                  size="sm"
                  className={`font-mono px-3 py-1 h-auto ${
                    activeTab === 'headers' 
                      ? 'bg-brand text-surface hover:bg-brand-hover' 
                      : 'bg-transparent text-soft hover:bg-edge hover:text-slate-50'
                  }`}
                  onClick={() => setActiveTab('headers')}
                >
                  headers
                </Button>
              </div>
              <div className="flex gap-2">
                {onAddToFavorite && (
                  <Button 
                    variant="outline" 
                    size="sm"
                    className="bg-transparent border-edge text-soft hover:bg-edge hover:text-slate-50"
                    onClick={handleSave}
                  >
                    <Star className="size-4 mr-1" />
                    Save
                  </Button>
                )}
                <Button 
                  variant="outline" 
                  size="sm"
                  className="bg-transparent border-edge text-soft hover:bg-edge hover:text-slate-50"
                  onClick={handleCopy}
                >
                  <Copy className="size-4 mr-1" />
                  Copy
                </Button>
              </div>
            </div>
            
            {/* Схема к топику есть, но это сообщение по ней не разобралось.
                Тело ниже — обычный текст, и сказать об этом надо здесь: тост
                к моменту открытия модалки давно погас. */}
            {message.decode_error && activeTab === 'payload' && (
              <div className="mb-3 rounded border border-edge bg-sunken px-3 py-2 font-mono text-xs text-brand break-words">
                {message.decode_error} — showing the raw body as text.
              </div>
            )}

            <div ref={bodyRef} className="bg-sunken border border-edge rounded-lg p-4 max-h-96 overflow-auto">
              {activeTab === 'payload' ? (
                <div className="flex gap-4">
                  {/* Номера строк. sticky: строки больше не переносятся, а
                      уезжают вправо — вместе с ними уехала бы и нумерация. */}
                  <div className="font-mono text-soft text-right leading-6 select-none sticky left-0 z-10 bg-sunken pr-1">
                    {lines.map((_, index) => (
                      <div key={index}>{index + 1}</div>
                    ))}
                  </div>

                  {/* JSON content. `whitespace-pre` — не украшение: отступы,
                      которые расставил JSON.stringify, HTML по умолчанию
                      сминает, и форматированный JSON выглядел плоским. */}
                  <div
                    className={`font-mono leading-6 flex-1 ${
                      isJson ? 'whitespace-pre' : 'whitespace-pre-wrap break-all'
                    }`}
                  >
                    {lines.map((line, index) => (
                      <div key={index}>{highlightLine(line, enumValues)}</div>
                    ))}
                    {/* whitespace-normal: обычное предложение, а не строка
                        JSON — переносить его по словам можно и нужно. */}
                    {hiddenLines > 0 && (
                      <div className="text-dim pt-2 whitespace-normal">
                        … {hiddenLines.toLocaleString()} more lines not rendered ({message.value_size.toLocaleString()} bytes total).
                        Use Copy to get the full payload.
                      </div>
                    )}
                  </div>
                </div>
              ) : (
                <div>
                  {message.headers.length > 0 ? (
                    <div className="space-y-2">
                      {message.headers.map(({ key, value }, index) => (
                        <div key={index} className="flex gap-4 font-mono leading-6">
                          <div className="text-brand min-w-0 flex-shrink-0">
                            {key}:
                          </div>
                          <div className="text-soft break-all">
                            {value}
                          </div>
                        </div>
                      ))}
                    </div>
                  ) : (
                    <div className="font-mono text-soft text-center py-8">
                      No headers found
                    </div>
                  )}
                </div>
              )}
            </div>
          </div>
        </div>
      </DialogContentNoClose>
    </Dialog>
  );
}