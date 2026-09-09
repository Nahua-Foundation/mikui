import { ReactNode, useEffect, useMemo, useRef, useState } from 'react';
import { Button } from '../../ui/button';
import { Copy, Link2, Star, StarOff } from 'lucide-react';
import { toast } from 'sonner';
import { Dialog, DialogHeader, DialogTitle, DialogDescription } from '../../ui/dialog';
import { DialogContentNoClose } from '../DialogContentNoClose';
import { BodyFormat, FullMessage, MessageHeader, showsRawBody } from '../types';
import { formatTimestamp } from '../format';
import { detectEnvelope } from '../lens';
import { EnvelopeView } from '../components/EnvelopeView';
import { highlightLine } from '../syntax';

/** Потолок отрисовки. Тело на 10 МБ иначе положило бы вкладку на лопатки
 *  ещё до того, как пользователь что-то увидит. */
const MAX_RENDERED_LINES = 2000;

/** Вкладки окна. `envelope` появляется только у тела в конверте CDC/Connect. */
type Tab = 'payload' | 'envelope' | 'headers';

/** Соседняя вкладка по кругу. Список приходит аргументом: его состав зависит
 *  от сообщения. */
function step(current: Tab, delta: 1 | -1, tabs: Tab[]): Tab {
  const index = tabs.indexOf(current);
  if (index === -1) return tabs[0];
  return tabs[(index + delta + tabs.length) % tabs.length];
}

interface MessageDetailsModalProps {
  message: FullMessage | null;
  open: boolean;
  onOpenChange: (open: boolean) => void;
  /**
   * Заголовок и подпись под ним.
   *
   * Подпись задают оба вызывающих, и оба — чтобы назвать происхождение
   * сообщения: таблица пишет топик, архив — кластер и топик, откуда запись
   * взята (открытого топика в тот момент может не быть вовсе). Значение по
   * умолчанию осталось на случай, когда назвать нечего.
   */
  title?: string;
  description?: ReactNode;
  /** Сообщение уже лежит в архиве. Тогда кнопка сохранения превращается в
   *  кнопку удаления: без этого клик по «Save» на уже сохранённом выглядел бы
   *  как «ничего не произошло». */
  saved?: boolean;
  onAddToFavorite?: () => void;
  onRemoveFavorite?: () => void;
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
   * Собрать ссылку на это сообщение и показать её.
   *
   * Не задан там, где ссылку не на что построить: у сохранённого сообщения
   * координаты есть, а подключения к его кластеру может не быть вовсе — а
   * `cluster.id`, которым ссылка называет кластер, известен только от живого
   * подключения (см. `link.rs`).
   */
  onShare?: () => void;
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
  title = 'Message Details',
  description = 'View details of the selected message',
  saved = false,
  onAddToFavorite,
  onRemoveFavorite,
  onNavigate,
  onShare,
  format = 'json',
}: MessageDetailsModalProps) {
  const [activeTab, setActiveTab] = useState<Tab>('payload');

  /**
   * Конверт Debezium/Connect, если тело в него завёрнуто.
   *
   * Разбирается только у ОТКРЫТОГО сообщения — одно тело на модалку, а не
   * двести на окно таблицы, — поэтому здесь можно позволить себе полный разбор
   * без всяких потолков, в отличие от превью строки (см. `kafka::lens`).
   *
   * `text` и `hex` в счёт не идут: у них пользователь прямо попросил показывать
   * тело как есть, и разбирать его вопреки этому значило бы не слушать. У `hex`
   * разбирать вдобавок нечего — там строка из шестнадцатеричных цифр.
   */
  const envelope = useMemo(
    () => (message && !showsRawBody(format) ? detectEnvelope(message.value) : null),
    [message, format],
  );

  /** Вкладки, которые сейчас есть. Порядок — тот же, в каком они нарисованы. */
  const tabs: Tab[] = envelope ? ['payload', 'envelope', 'headers'] : ['payload', 'headers'];

  // Вкладка принадлежит сообщению: у соседнего конверта может не быть, и
  // остаться на исчезнувшей вкладке значило бы показать пустоту.
  useEffect(() => {
    if (!envelope && activeTab === 'envelope') setActiveTab('payload');
  }, [envelope, activeTab]);

  // Через ref: обработчик стрелок висит на window и пересоздаётся только при
  // открытии модалки, а состав вкладок меняется с каждым сообщением.
  const tabsRef = useRef(tabs);
  tabsRef.current = tabs;

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

      // Слушатель висит на window, поэтому ловит стрелки и из полей ввода —
      // в том числе из окна, открытого поверх этого (ссылкой делятся именно
      // так). Там стрелки двигают курсор, и отбирать их у текста нельзя.
      const from = event.target as HTMLElement | null;
      if (
        from instanceof HTMLInputElement ||
        from instanceof HTMLTextAreaElement ||
        from?.isContentEditable
      ) {
        return;
      }

      switch (event.key) {
        case 'ArrowUp':
          if (!onNavigate) return;
          onNavigate(-1);
          break;
        case 'ArrowDown':
          if (!onNavigate) return;
          onNavigate(1);
          break;
        // По кругу, а не «влево — первая, вправо — последняя»: вкладок стало
        // три, и средняя иначе была бы недостижима с клавиатуры.
        case 'ArrowLeft':
          setActiveTab((current) => step(current, -1, tabsRef.current));
          break;
        case 'ArrowRight':
          setActiveTab((current) => step(current, 1, tabsRef.current));
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
    // Явно выбранные text и hex просят показать как есть — раскладывать их по
    // строкам значило бы решать за пользователя. У hex это ещё и единственный
    // верный ответ: тело из одних цифр (`12345678`) разобралось бы как число и
    // уехало в JSON-ветку, где его покрасили бы как число.
    if (showsRawBody(format)) return { text: jsonString, isJson: false };
    try {
      return { text: JSON.stringify(JSON.parse(jsonString), null, 2), isJson: true };
    } catch {
      return { text: jsonString, isJson: false };
    }
  };

  const handleCopy = () => {
    // С вкладки конверта копируется ПОЛНОЕ тело, а не диф: копируют, чтобы
    // переслать или воспроизвести, а диф — представление, которого в топике
    // никогда не было.
    if (activeTab === 'headers') {
      copyHeadersToClipboard(message!.headers);
    } else {
      copyToClipboard(message!.value);
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
      <DialogContentNoClose className="max-w-[58rem] sm:max-w-[58rem] max-h-[90vh] bg-surface border-edge text-strong">
        <DialogHeader className="border-b border-edge pb-4">
          <DialogTitle className="font-mono text-soft text-lg">{title}</DialogTitle>
          <DialogDescription className="font-mono text-soft text-sm">
            {description}
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
              <div className="font-mono text-strong">{message.key}</div>
            </div>
            <div>
              <div className="font-mono text-sm text-soft mb-1">Timestamp</div>
              <div className="font-mono text-strong">{formatTimestamp(message.timestamp)}</div>
            </div>
          </div>
          
          {/* Content Section with Tab Buttons */}
          <div>
            <div className="flex items-center justify-between mb-4">
              <div className="flex gap-1">
                {tabs.map((tab) => (
                  <Button
                    key={tab}
                    variant="ghost"
                    size="sm"
                    title={
                      tab === 'envelope'
                        ? envelope?.kind === 'debezium'
                          ? 'What this change did to the row'
                          : 'The payload without the schema that precedes it'
                        : undefined
                    }
                    className={`font-mono px-3 py-1 h-auto ${
                      activeTab === tab
                        ? 'bg-brand text-surface hover:bg-brand-hover'
                        : 'bg-transparent text-soft hover:bg-edge hover:text-strong'
                    }`}
                    onClick={() => setActiveTab(tab)}
                  >
                    {tab === 'envelope' && envelope?.kind === 'debezium' ? 'change' : tab}
                  </Button>
                ))}
              </div>
              <div className="flex gap-2">
                {/* Одна кнопка на два состояния, а не «Save» рядом с
                    «Delete»: сохранённость сообщения — это одно свойство, и
                    показывать его двумя кнопками значило бы заставлять
                    угадывать, какая из них сейчас что-то сделает. */}
                {saved
                  ? onRemoveFavorite && (
                      <Button
                        variant="outline"
                        size="sm"
                        className="bg-transparent border-edge text-brand hover:bg-edge hover:text-danger"
                        onClick={onRemoveFavorite}
                        title="This message is kept on disk. Click to delete it from saved."
                      >
                        <StarOff className="size-4 mr-1" />
                        Delete from saved
                      </Button>
                    )
                  : onAddToFavorite && (
                      <Button
                        variant="outline"
                        size="sm"
                        className="bg-transparent border-edge text-soft hover:bg-edge hover:text-strong"
                        onClick={onAddToFavorite}
                        title="Keep this message on disk — it will outlive the topic's retention."
                      >
                        <Star className="size-4 mr-1" />
                        Save
                      </Button>
                    )}
                {/* Ссылка отдельно от Copy: тот копирует САМО тело, а эта —
                    адрес сообщения в кластере. Одной кнопкой это было бы не
                    выразить, а разница принципиальная — телом делятся, когда
                    важно содержимое, ссылкой — когда важно место. */}
                {onShare && (
                  <Button
                    variant="outline"
                    size="sm"
                    className="bg-transparent border-edge text-soft hover:bg-edge hover:text-strong"
                    onClick={onShare}
                    title="Copy a link that opens this message in mikui"
                  >
                    <Link2 className="size-4 mr-1" />
                    Share
                  </Button>
                )}
                <Button
                  variant="outline"
                  size="sm"
                  className="bg-transparent border-edge text-soft hover:bg-edge hover:text-strong"
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
                  {/* `break-words`, а не `break-all`: перенос ищется по
                      пробелам и рвёт слово только тогда, когда оно и само в
                      строку не влезает. У hex это обязательно — `break-all`
                      разрывал бы пару цифр посреди байта (`C8` на двух
                      строках), а длинную ленту текста без пробелов обе
                      настройки переносят одинаково. */}
                  <div
                    className={`font-mono leading-6 flex-1 ${
                      isJson ? 'whitespace-pre' : 'whitespace-pre-wrap break-words'
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
              ) : activeTab === 'envelope' && envelope ? (
                <EnvelopeView envelope={envelope} />
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