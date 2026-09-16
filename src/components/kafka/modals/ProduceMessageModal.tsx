import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { ChevronDown, Plus, RotateCcw, Send, X } from 'lucide-react';
import { toast } from 'sonner';
import { Button } from '../../ui/button';
import { Input } from '../../ui/input';
import { Label } from '../../ui/label';
import { Dialog, DialogHeader, DialogTitle, DialogDescription } from '../../ui/dialog';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '../../ui/select';
import { DialogContentNoClose } from '../DialogContentNoClose';
import { highlightLine, NO_ENUM_VALUES } from '../syntax';
import { SubjectPicker } from '../components/AvroSchema';
import { ProtoMessageSelect } from '../components/ProtoMessageSelect';
import * as api from '../api';
import { describeError } from '../api';
import {
  MessageHeader,
  PayloadFormat,
  PayloadIssue,
  ProduceRequest,
  MessageForm,
  Topic,
  TopicSchema,
} from '../types';

/** Пауза перед проверкой тела. Проверка ходит в Rust (разбор .proto повторить
 *  на фронте нечем), и дёргать её на каждый символ незачем. */
const CHECK_DEBOUNCE_MS = 300;

/** Значение селектора партиций, означающее «пусть выберет партишенер».
 *  Строка, а не число: `Select` работает со строками, а пустая строка у него
 *  зарезервирована под «ничего не выбрано». */
const AUTO_PARTITION = 'auto';

/** Порядок в селекторе формата. `text` первым: он же и значение по умолчанию. */
const FORMATS: { value: PayloadFormat; label: string }[] = [
  { value: 'text', label: 'text' },
  { value: 'json', label: 'json' },
  { value: 'jsonschema', label: 'json schema' },
  { value: 'proto', label: 'proto' },
  { value: 'avro', label: 'avro' },
  { value: 'hex', label: 'hex' },
];

/** Форматы, тело которых — это JSON, и его надо раскрашивать. У proto и avro
 *  тело вводится тем же JSON, каким оно потом показывается при чтении. */
const HIGHLIGHTED: ReadonlySet<PayloadFormat> = new Set<PayloadFormat>([
  'json',
  'proto',
  'avro',
  'jsonschema',
]);

/** Форматы, чьё тело проверяет бэкенд. Текст отправляется как есть — ходить
 *  ради него в Rust на каждое нажатие клавиши не за чем. */
const CHECKED: ReadonlySet<PayloadFormat> = new Set<PayloadFormat>([
  'json',
  'proto',
  'avro',
  'jsonschema',
  'hex',
]);

/** Форматы, у которых тело кодируется по схеме, а значит есть и заготовка, и
 *  подсветка enum.
 *
 *  `jsonschema` здесь при том, что тело у него НЕ кодируется — оно и так JSON.
 *  Общего с двумя другими у него ровно то, за чем это множество и заведено:
 *  схема даёт заготовку и решает, пройдёт ли тело проверку. */
const SCHEMA_FORMATS: ReadonlySet<PayloadFormat> = new Set<PayloadFormat>([
  'proto',
  'avro',
  'jsonschema',
]);

/** Форматы, которые берут схему по subject реестра. У обоих поэтому есть и
 *  селектор subject'а, и confluent-заголовок перед телом. */
const SUBJECT_FORMATS: ReadonlySet<PayloadFormat> = new Set<PayloadFormat>([
  'avro',
  'jsonschema',
]);

/**
 * Место под полосу прокрутки резервируется всегда — и в поле ввода, и в слое
 * подсветки под ним.
 *
 * Иначе появившаяся полоса сужает поле, но не слой: строки начинают переноситься
 * в двух местах по-разному, и подсветка расходится с текстом ровно тогда, когда
 * тело переросло окно. Утилиты в Tailwind под это нет, поэтому стилем.
 */
const SCROLLBAR_GUTTER: React.CSSProperties = { scrollbarGutter: 'stable' };

/**
 * Описание выбранного типа вместе с тем, чем его выбирали, — см. `currentForm`.
 *
 * `choice` — имя message у protobuf и subject у Avro. Хранится ВМЕСТЕ с
 * ответом, потому что пока запрос летит, в состоянии лежит описание прежнего
 * типа, и отличить его от нового иначе нечем.
 */
interface DescribedMessage {
  choice: string;
  form: MessageForm;
}

const PLACEHOLDERS: Record<PayloadFormat, string> = {
  text: 'Message body',
  json: '{ "id": 1 }',
  proto: 'Pick a message type — the template appears here',
  avro: 'The template appears here once the schema is known',
  jsonschema: 'The template appears here once the schema is known',
  // Тем же видом, каким hex показывает просмотр сообщения: скопированное
  // оттуда ложится сюда как есть, и подсказка это подтверждает, а не спорит.
  hex: '0A 15 08 — spaces, colons and dashes are ignored',
};

/**
 * Ключ выбранного типа для эффекта загрузки заготовки.
 *
 * Пустая строка — законное значение у форматов, берущих схему по subject:
 * subject может быть не выбран, и тогда берётся тот, что назначен топику. У
 * protobuf пустой выбор означает «нечем», и запрос не уходит вовсе.
 */
function schemaChoice(
  format: PayloadFormat,
  protoMessage: string | null,
  subjectChoice: string | null,
): string | null {
  if (format === 'proto') return protoMessage;
  if (SUBJECT_FORMATS.has(format)) return subjectChoice ?? '';
  return null;
}

interface ProduceMessageModalProps {
  topic: Topic | null;
  /** Ключ кластера — см. `clusterKey`. null, когда подключения нет. */
  cluster: string | null;
  /** Схема ОТКРЫТОГО топика. От неё зависит, доступен ли формат proto и какие
   *  типы предлагать: без загруженных .proto кодировать нечем. */
  schema: TopicSchema | null;
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

export function ProduceMessageModal({
  topic,
  cluster,
  schema,
  open,
  onOpenChange,
}: ProduceMessageModalProps) {
  const [partition, setPartition] = useState<string>(AUTO_PARTITION);
  const [key, setKey] = useState('');
  const [headers, setHeaders] = useState<MessageHeader[]>([]);
  const [format, setFormat] = useState<PayloadFormat>('text');
  const [payload, setPayload] = useState('');
  const [protoMessage, setProtoMessage] = useState<string | null>(null);
  /** Subject, которым кодировать. null — тот, что назначен топику. */
  const [subjectChoice, setSubjectChoice] = useState<string | null>(null);
  /** Ответ бэкенда вместе с типом, к которому он относится. null — тип не
   *  выбран либо схема не разобралась. */
  const [schemaForm, setSchemaForm] = useState<DescribedMessage | null>(null);
  const [issue, setIssue] = useState<PayloadIssue | null>(null);
  const [busy, setBusy] = useState(false);
  /** Список заголовков прокручен не до конца — под ним есть ещё. */
  const [moreHeaders, setMoreHeaders] = useState(false);

  const topicName = topic?.name ?? null;
  const messages = useMemo(() => schema?.messages ?? [], [schema]);
  const canUseProto = messages.length > 0;
  const avro = schema?.avro ?? null;
  /** Кодировать Avro можно либо через реестр, либо по локальным .avsc. */
  const canUseAvro = !!avro && (avro.registry || avro.files.length > 0);
  const json = schema?.json ?? null;
  /** То же условие и по той же причине: проверить тело нечем, пока нет ни
   *  реестра, ни своей схемы. */
  const canUseJsonSchema = !!json && (json.registry || json.files.length > 0);

  /** Схемная половина, к которой относится выбранный формат. Обе устроены
   *  одинаково в том, что здесь нужно, — subject и признак реестра. */
  const bySubject = format === 'jsonschema' ? json : avro;

  /**
   * Для какого типа заготовку уже предлагали.
   *
   * Без этой отметки эффект подстановки, у которого `payload` в зависимостях,
   * возвращал бы заготовку сразу же после того, как пользователь очистил поле
   * руками. Сбрасывается при открытии модалки: на второй заход заготовка
   * предлагается снова.
   *
   * Состояние, а не ref: от него зависит не только эффект, но и то, что
   * нарисовано, — расхождение с выбранным типом видно пользователю (см.
   * `staleTemplate`).
   */
  const [offeredFor, setOfferedFor] = useState<string | null>(null);

  // Схема через ref, а не напрямую в зависимостях сброса ниже: она приезжает
  // из состояния приложения и меняет идентичность при каждом перечитывании
  // топика. Попади она в зависимости — форма обнулялась бы под руками.
  const schemaRef = useRef(schema);
  schemaRef.current = schema;

  // Форма принадлежит одному открытию, а модалка переиспользуется. Без сброса
  // в ней остались бы ключ, заголовки и тело от прошлой отправки — и второе
  // сообщение ушло бы с хвостами первого, чего никто не просил.
  useEffect(() => {
    if (!open) return;
    const current = schemaRef.current;
    setPartition(AUTO_PARTITION);
    setKey('');
    setHeaders([]);
    setPayload('');
    setIssue(null);
    setBusy(false);
    setMoreHeaders(false);
    setSchemaForm(null);
    setOfferedFor(null);
    // Тип по умолчанию — тот, которым топик читают: раз им смотрят, им скорее
    // всего и отправляют. А если топику тип не назначен, но в загруженных
    // .proto он всего один — берём его: выбирать не из чего, и предлагать
    // выбор было бы формальностью.
    const declared = current?.messages ?? [];
    setProtoMessage(current?.message ?? (declared.length === 1 ? declared[0] : null));
    setSubjectChoice(null);
    // Формат тоже: у топика со схемой начинать с текста значило бы предлагать
    // положить в него заведомо нечитаемое тело.
    if (current?.format === 'proto' && (current?.messages?.length ?? 0) > 0) {
      setFormat('proto');
    } else if (
      current?.format === 'avro' &&
      !!current.avro &&
      (current.avro.registry || current.avro.files.length > 0)
    ) {
      setFormat('avro');
    } else if (
      current?.format === 'jsonschema' &&
      !!current.json &&
      (current.json.registry || current.json.files.length > 0)
    ) {
      setFormat('jsonschema');
    } else {
      setFormat('text');
    }
  }, [open]);

  // Всё, что форме нужно знать про выбранный тип: заготовка тела и имена
  // enum-значений для подсветки. Одним запросом — обе половины описывают один
  // и тот же тип и приезжают на одно событие.
  const choice = schemaChoice(format, protoMessage, subjectChoice);
  useEffect(() => {
    if (!open || !cluster || !topicName || choice === null) {
      setSchemaForm(null);
      return;
    }

    let cancelled = false;
    const wanted = choice;
    const load =
      format === 'proto'
        ? api.protoMessageForm(cluster, topicName, wanted)
        : // Пустой выбор у форматов с subject законен: бэкенд возьмёт то, что
          // назначено топику. У protobuf такого нет — там без типа кодировать
          // нечем.
          format === 'jsonschema'
          ? api.jsonMessageForm(cluster, topicName, wanted || null)
          : api.avroMessageForm(cluster, topicName, wanted || null);

    load
      .then((loaded) => {
        // Выбор хранится ВМЕСТЕ с ответом. Пока запрос летит, в состоянии
        // лежит описание ПРЕЖНЕГО типа, и без этой отметки его нельзя отличить
        // от описания нового: подстановка успевала сработать на старом ответе,
        // и в поле оказывалась заготовка предыдущего типа, а не выбранного.
        if (!cancelled) setSchemaForm({ choice: wanted, form: loaded });
      })
      .catch((e) => {
        // Не тост: разбор схемы уже отчитался об ошибке в настройках топика, а
        // здесь это всего лишь «заготовки и подсветки не будет» — набрать тело
        // руками ничто не мешает, и проверка перед отправкой скажет своё.
        console.error('Failed to describe the schema', e);
        if (!cancelled) setSchemaForm(null);
      });

    return () => {
      cancelled = true;
    };
  }, [open, format, cluster, topicName, choice]);

  /**
   * Описание ВЫБРАННОГО сейчас типа, а не последнего приехавшего.
   *
   * Всё, что зависит от схемы — заготовка, подсветка enum, кнопка — обязано
   * смотреть сюда: пока ответ в пути, `schemaForm` относится к прежнему типу, и
   * пользоваться им значит показывать чужой контракт.
   */
  const currentForm = schemaForm && schemaForm.choice === choice ? schemaForm.form : null;

  /**
   * Subject, которым тело в итоге закодируется.
   *
   * Выбранный в форме, назначенный топику или найденный в реестре по имени
   * топика — в этом порядке. Последнее слово за бэкендом: он же и кодирует, и
   * его ответ (`currentForm.subject`) точнее любой догадки на этой стороне —
   * в частности, он пуст там, где схема локальная и никакого subject'а нет.
   */
  const usedSubject = currentForm
    ? currentForm.subject
    : (subjectChoice ?? bySubject?.subject ?? bySubject?.detected ?? null);

  /**
   * Уедет ли тело с confluent-заголовком.
   *
   * Разницу видит не отправитель, а потребитель топика, поэтому форма о ней и
   * пишет: с заголовком тело прочитает штатный десериализатор, без него — лишь
   * тот, кто знает схему заранее. Заголовок берётся из реестра, значит и
   * бывает он ровно там, где схему дал subject.
   */
  const framed =
    SUBJECT_FORMATS.has(format) && !!bySubject?.registry && usedSubject !== null;

  /** Ключ, под которым помнится «заготовку для этого уже предлагали». С
   *  форматом внутри: пустой выбор у Avro иначе не отличить от отсутствия
   *  выбора у protobuf. */
  const offerKey = choice === null ? null : `${format}:${choice}`;

  /** Ставит заготовку в поле. */
  const applyTemplate = useCallback(() => {
    if (!currentForm || offerKey === null) return;
    setOfferedFor(offerKey);
    setPayload(currentForm.template);
  }, [currentForm, offerKey]);

  // Заготовка подставляется сама, но ТОЛЬКО в пустое поле и только один раз на
  // тип. Смена типа в селекторе набранное тело не трогает: промахнуться в
  // списке легко, а потерять из-за этого заполненное сообщение нельзя — за
  // подмену отвечает кнопка рядом с селектором.
  useEffect(() => {
    if (!currentForm || offerKey === null) return;
    if (offeredFor === offerKey) return;
    if (payload !== '') return;
    setOfferedFor(offerKey);
    setPayload(currentForm.template);
  }, [currentForm, offerKey, offeredFor, payload]);

  /**
   * В поле лежит заготовка ДРУГОГО типа, чем выбран сейчас.
   *
   * Смена типа тело не трогает намеренно (см. выше), и без этой отметки
   * получалось непонятное: пользователь выбрал другой тип, ничего не набирал,
   * а поле сразу краснеет — ошибка честная, но причина её не в том, что он
   * только что сделал, а в том, чего ещё не сделал. Отметка нужна, чтобы
   * назвать причину и предложить кнопку.
   *
   * `currentForm` в условии: пока описание типа не приехало, заменить
   * заготовку всё равно нечем, а предлагать неработающую ссылку нельзя.
   */
  const staleTemplate =
    !!currentForm && offeredFor !== null && offerKey !== null && offeredFor !== offerKey;

  // Проверка тела. Талон отсекает опоздавшие ответы: печатают быстрее, чем
  // они приходят, и без него под полем оставалась бы ошибка от текста,
  // который давно исправили.
  const checkTicket = useRef(0);
  useEffect(() => {
    if (!open || !topicName) return;
    if (!CHECKED.has(format)) {
      setIssue(null);
      return;
    }

    const ticket = ++checkTicket.current;
    const timer = window.setTimeout(() => {
      api
        .checkProducePayload(cluster, {
          topic: topicName,
          partition: null,
          key: '',
          headers: [],
          format,
          payload,
          message: protoMessage,
          subject: subjectChoice,
        })
        .then((found) => {
          if (checkTicket.current === ticket) setIssue(found);
        })
        .catch((e) => {
          // Сама проверка сломалась — это не претензия к телу. Молча оставить
          // поле «чистым» честнее, чем показать чужую ошибку как ошибку ввода;
          // отправка всё равно проверит ещё раз и скажет своё.
          console.error('Failed to check the payload', e);
          if (checkTicket.current === ticket) setIssue(null);
        });
    }, CHECK_DEBOUNCE_MS);

    return () => window.clearTimeout(timer);
  }, [open, cluster, topicName, format, payload, protoMessage, subjectChoice]);

  // Список заголовков прокручиваемый, и обе беды прокрутки лечатся здесь.
  //
  // Первая: добавленная строка оказывается ниже видимой части, и нажатие "add"
  // выглядит как «ничего не произошло». Вторая: о том, что список продолжается,
  // ничего не говорит — оверлейная полоса прокрутки на macOS появляется только
  // во время самой прокрутки, то есть ровно тогда, когда подсказка уже не
  // нужна. Поэтому: прокручиваем к новой строке сами и рисуем стрелку вниз,
  // пока до низа не докрутили.
  const headersRef = useRef<HTMLDivElement>(null);

  const syncHeadersHint = useCallback(() => {
    const box = headersRef.current;
    // Запас в пиксель: дробные высоты строк иначе не дают условию сойтись, и
    // стрелка остаётся висеть на самом низу списка.
    setMoreHeaders(!!box && box.scrollTop + box.clientHeight < box.scrollHeight - 1);
  }, []);

  useEffect(syncHeadersHint, [headers, syncHeadersHint]);

  const addHeader = useCallback(() => {
    setHeaders((prev) => [...prev, { key: '', value: '' }]);
    // После коммита React'а: до него новой строки в DOM ещё нет, и прокрутка
    // упёрлась бы в прежнюю высоту.
    requestAnimationFrame(() => {
      const box = headersRef.current;
      if (box) box.scrollTop = box.scrollHeight;
      syncHeadersHint();
    });
  }, [syncHeadersHint]);

  const updateHeader = useCallback((index: number, patch: Partial<MessageHeader>) => {
    setHeaders((prev) => prev.map((h, i) => (i === index ? { ...h, ...patch } : h)));
  }, []);

  const removeHeader = useCallback((index: number) => {
    setHeaders((prev) => prev.filter((_, i) => i !== index));
  }, []);

  const handleSend = useCallback(async () => {
    if (!topicName) return;
    const request: ProduceRequest = {
      topic: topicName,
      partition: partition === AUTO_PARTITION ? null : Number(partition),
      key,
      // Заголовок без имени — это недозаполненная строка формы, а не заголовок.
      // Отправлять его значило бы класть в сообщение мусор, который пользователь
      // просто не успел стереть.
      headers: headers.filter((h) => h.key.trim() !== ''),
      format,
      payload,
      message: protoMessage,
      subject: subjectChoice,
    };

    setBusy(true);
    try {
      const result = await api.produceMessage(cluster, request);
      toast.success(
        `Sent to ${topicName} · partition ${result.partition} · offset ${result.offset}`,
      );
      onOpenChange(false);
    } catch (e) {
      console.error('Failed to produce message', e);
      toast.error(`Failed to send: ${describeError(e)}`);
    } finally {
      setBusy(false);
    }
  }, [
    topicName,
    partition,
    key,
    headers,
    format,
    payload,
    protoMessage,
    subjectChoice,
    cluster,
    onOpenChange,
  ]);

  // Подсветка живёт слоем ПОД полем ввода, а само поле прозрачное. Другого
  // способа раскрасить редактируемый текст в textarea нет, и держится это на
  // том, что оба слоя переносят строки одинаково: шрифт, отступы, высота
  // строки и правила переноса у них обязаны совпадать до пикселя.
  const highlightRef = useRef<HTMLPreElement>(null);
  const syncScroll = useCallback((event: React.UIEvent<HTMLTextAreaElement>) => {
    const layer = highlightRef.current;
    if (!layer) return;
    layer.scrollTop = event.currentTarget.scrollTop;
    layer.scrollLeft = event.currentTarget.scrollLeft;
  }, []);

  const highlighted = HIGHLIGHTED.has(format);
  const blocked = issue?.severity === 'error';

  /**
   * Имена enum-значений выбранного типа.
   *
   * Красится по совпадению С ЭТИМ списком, а не по виду строки: enum в JSON
   * записывается той же строкой в кавычках, что и обычное значение, и отличить
   * их можно только по схеме. Побочный эффект ровно тот, которого и хочется —
   * опечатка в имени константы остаётся зелёной строкой, и видно её сразу, ещё
   * до того, как ошибку подтвердит проверка под полем.
   */
  const enumValues = useMemo(
    () =>
      SCHEMA_FORMATS.has(format) && currentForm
        ? new Set(currentForm.enum_values)
        : NO_ENUM_VALUES,
    [format, currentForm],
  );

  if (!topic) return null;

  const partitions = Array.from({ length: topic.partitions }, (_, i) => i);
  // Рамка поля цветом говорит то же, что и текст под ним: оранжевая —
  // отправить можно, красная — нельзя.
  const bodyBorder = blocked
    ? 'border-danger'
    : issue
      ? 'border-brand'
      : 'border-edge';

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContentNoClose className="max-w-[52rem] sm:max-w-[52rem] max-h-[90vh] bg-surface border-edge text-strong">
        <DialogHeader className="border-b border-edge pb-4 min-w-0">
          <DialogTitle className="font-mono text-soft text-lg">Produce to topic</DialogTitle>
          {/* Имя топика — подзаголовком, а не в самом заголовке: оно бывает
              длинным, и в строке с текстом ему пришлось бы делить место с ним. */}
          <DialogDescription
            className="font-mono text-soft text-sm truncate"
            title={topic.name}
          >
            {topic.name}
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-5 overflow-auto pt-1 min-w-0">
          {/* Партиция и ключ — одной строкой: партиционирование по ключу
              связывает их, и смотреть на них надо вместе. */}
          <div className="grid grid-cols-2 gap-6">
            <div className="space-y-2">
              <Label className="font-mono text-sm text-soft">Partition</Label>
              <Select value={partition} onValueChange={setPartition}>
                <SelectTrigger className="bg-surface border-edge text-strong font-mono">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent className="bg-surface border-edge max-h-80">
                  <SelectItem
                    value={AUTO_PARTITION}
                    className="text-strong font-mono focus:bg-edge"
                  >
                    automatic
                  </SelectItem>
                  {/* Подпись СТРОКОЙ, а не числом.
                      С числом партиция 0 выбиралась, но в свёрнутом селекторе
                      оставалось пусто (в списке пункт при этом виден). `value`
                      у всех пунктов и так строка, поэтому единственное, чем
                      нулевая партиция отличалась от остальных, — подпись
                      числом `0`; она и не доезжала до поля триггера. */}
                  {partitions.map((p) => (
                    <SelectItem
                      key={p}
                      value={String(p)}
                      className="text-strong font-mono focus:bg-edge"
                    >
                      {String(p)}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
              {/* Партишенер назван поимённо не из педантизма: от него зависит,
                  ляжет ли тестовое сообщение в ту же партицию, в которую пишет
                  боевой сервис, — а это и есть смысл ручной отправки. */}
              {partition === AUTO_PARTITION && (
                <div className="font-mono text-xs text-dim">
                  murmur2 hash of the key, same as the Java client. Without a key — a random
                  partition.
                </div>
              )}
            </div>

            <div className="space-y-2">
              <Label className="font-mono text-sm text-soft">Key</Label>
              <Input
                value={key}
                onChange={(e) => setKey(e.target.value)}
                placeholder="Optional"
                className="bg-surface border-edge text-strong font-mono placeholder:text-dim"
              />
              <div className="font-mono text-xs text-dim">
                {key === '' ? 'No key — not the same as an empty one.' : 'Sent as UTF-8.'}
              </div>
            </div>
          </div>

          {/* Заголовки */}
          <div className="space-y-2">
            <div className="flex items-center justify-between">
              <Label className="font-mono text-sm text-soft">Headers</Label>
              <button
                type="button"
                onClick={addHeader}
                className="font-mono text-xs text-dim hover:text-brand bg-transparent border-none cursor-pointer flex items-center gap-1"
              >
                <Plus className="size-3.5" />
                add
              </button>
            </div>
            {headers.length > 0 ? (
              <div className="relative">
                {/* Прокручиваемый, а не растущий бесконечно: полтора десятка
                    заголовков иначе вытеснили бы из окна само поле тела. */}
                <div
                  ref={headersRef}
                  onScroll={syncHeadersHint}
                  className="max-h-36 overflow-auto rounded border border-edge divide-y divide-edge"
                >
                  {headers.map((header, index) => (
                    // pr-5, а не общий p-2: полоса прокрутки на macOS
                    // накладывается ПОВЕРХ содержимого, и крестик под ней
                    // становился почти некликабельным.
                    <div key={index} className="flex items-center gap-2 p-2 pr-5">
                      <Input
                        value={header.key}
                        onChange={(e) => updateHeader(index, { key: e.target.value })}
                        placeholder="key"
                        className="h-8 w-48 shrink-0 bg-surface border-edge text-strong font-mono text-xs placeholder:text-dim"
                      />
                      <Input
                        value={header.value}
                        onChange={(e) => updateHeader(index, { value: e.target.value })}
                        placeholder="value"
                        className="h-8 flex-1 min-w-0 bg-surface border-edge text-strong font-mono text-xs placeholder:text-dim"
                      />
                      <button
                        type="button"
                        onClick={() => removeHeader(index)}
                        className="p-1 text-dim hover:text-strong bg-transparent border-none cursor-pointer shrink-0"
                        title="Remove this header"
                      >
                        <X className="size-3.5" />
                      </button>
                    </div>
                  ))}
                </div>

                {/* «Ниже есть ещё». Без этого добавление четвёртого заголовка
                    выглядело как несработавшая кнопка: строка уезжала вниз, а
                    в интерфейсе не менялось ничего. */}
                {moreHeaders && (
                  <div className="pointer-events-none absolute inset-x-px bottom-px flex justify-center rounded-b bg-gradient-to-t from-surface via-surface to-transparent pt-5 pb-0.5">
                    <ChevronDown className="size-3.5 text-dim" />
                  </div>
                )}
              </div>
            ) : (
              <div className="rounded border border-edge border-dashed p-3 text-center font-mono text-xs text-dim">
                No headers
              </div>
            )}
          </div>

          {/* Формат и тело */}
          <div className="space-y-2">
            <div className="flex items-center justify-between gap-4">
              {/* Селектором, а не рядом кнопок: форматов шесть, и полосой они
                  занимали половину строки, оставляя выбору типа ровно столько
                  места, сколько ему не хватало. Заодно это тот же вид, каким
                  формат выбирают при чтении. */}
              <Select value={format} onValueChange={(v) => setFormat(v as PayloadFormat)}>
                <SelectTrigger className="w-40 shrink-0 bg-surface border-edge text-strong font-mono">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent className="bg-surface border-edge">
                  {FORMATS.map(({ value, label }) => {
                    // Формат без схемы кодировать нечем. Пункт не прячем, а
                    // гасим: иначе непонятно, почему у одного топика формат
                    // есть, а у соседнего нет.
                    const missing =
                      (value === 'proto' && !canUseProto) ||
                      (value === 'avro' && !canUseAvro) ||
                      (value === 'jsonschema' && !canUseJsonSchema);
                    return (
                      <SelectItem
                        key={value}
                        value={value}
                        disabled={missing}
                        className="text-strong font-mono focus:bg-edge"
                      >
                        {label}
                        {/* Причина строкой в самом пункте, а не подсказкой:
                            выключенный пункт не принимает наведение мыши, и
                            `title` на нём никогда бы не показался. */}
                        {missing && (
                          <span className="text-dim">
                            {value === 'proto'
                              ? '· load a .proto first'
                              : '· needs a registry or a schema file'}
                          </span>
                        )}
                      </SelectItem>
                    );
                  })}
                </SelectContent>
              </Select>

              {format === 'proto' && (
                <div className="flex items-center gap-3 min-w-0">
                  {/* Выход из тупика «я всё стёр»: заготовка подставляется сама
                      только пока в поле нечего терять, и без этой кнопки
                      вернуть её было бы нечем. */}
                  {currentForm && (
                    <Button
                      type="button"
                      variant="outline"
                      size="sm"
                      onClick={applyTemplate}
                      title="Replace the body with a fresh template"
                      // Обведённая кнопка в рост соседнего селектора, а не
                      // текстовая ссылка: заготовка сама подставляется только
                      // пока в поле нечего терять, и после правки тела это
                      // единственный способ вернуть её — а текстом кнопка не
                      // читалась как кнопка вообще.
                      className="h-9 shrink-0 bg-transparent border-edge text-soft hover:bg-edge hover:text-strong font-mono text-xs"
                    >
                      <RotateCcw className="size-3.5" />
                      template
                    </Button>
                  )}
                  {/* Обрезка длинного имени, подсказка с полным и `min-w-0`
                      против выезда триггера из строки — всё это теперь внутри
                      `ProtoMessageSelect`: то же самое понадобилось и в окне
                      схемы топика, а расходиться этим правкам нельзя. */}
                  <ProtoMessageSelect
                    messages={messages}
                    value={protoMessage}
                    onChange={setProtoMessage}
                    className="w-72 text-xs"
                  />
                </div>
              )}

              {SUBJECT_FORMATS.has(format) && (
                <div className="flex items-center gap-3 min-w-0">
                  {currentForm && (
                    <Button
                      type="button"
                      variant="outline"
                      size="sm"
                      onClick={applyTemplate}
                      title="Replace the body with a fresh template"
                      className="h-9 shrink-0 bg-transparent border-edge text-soft hover:bg-edge hover:text-strong font-mono text-xs"
                    >
                      <RotateCcw className="size-3.5" />
                      template
                    </Button>
                  )}
                  {/* Селектор только у реестра: локальные файлы задают схему
                      топика целиком, и выбирать в них нечего. */}
                  {bySubject?.registry && (
                    <div className="w-72 shrink-0">
                      <SubjectPicker
                        cluster={cluster ?? ''}
                        subject={subjectChoice ?? bySubject.subject}
                        // Найденный сам — подписью, а не выбором: выбора не
                        // было, и показывать его как сделанный нечестно. Но и
                        // пустое поле над работающей заготовкой врёт сильнее.
                        placeholder={usedSubject ? `${usedSubject} — found automatically` : undefined}
                        disabled={!cluster}
                        onChange={setSubjectChoice}
                      />
                    </div>
                  )}
                </div>
              )}
            </div>

            <div className={`relative rounded-lg border ${bodyBorder} bg-sunken`}>
              {/* Слой подсветки. aria-hidden: настоящее поле — textarea ниже,
                  и скринридеру этот слой только мешал бы. overflow-hidden, а не
                  auto: свою полосу прокрутки он показывать не должен, она встала
                  бы поверх настоящей — прокручиваем его мы сами, в `syncScroll`. */}
              {highlighted && (
                <pre
                  ref={highlightRef}
                  aria-hidden
                  style={SCROLLBAR_GUTTER}
                  className="absolute inset-0 m-0 overflow-hidden p-3 font-mono text-sm leading-6 whitespace-pre-wrap break-words pointer-events-none"
                >
                  {/* Строки — inline-span'ы, а перевод строки между ними
                      настоящим символом. Блоками (`div` на строку) было бы
                      короче, но пустая строка внутри тела схлопнулась бы в
                      нулевую высоту, и вся подсветка ниже неё уехала бы вверх
                      относительно поля ввода. */}
                  {payload.split('\n').map((line, index, all) => (
                    <span key={index}>
                      {highlightLine(line, enumValues)}
                      {index < all.length - 1 ? '\n' : ''}
                    </span>
                  ))}
                </pre>
              )}
              <textarea
                value={payload}
                onChange={(e) => setPayload(e.target.value)}
                onScroll={highlighted ? syncScroll : undefined}
                placeholder={PLACEHOLDERS[format]}
                spellCheck={false}
                style={SCROLLBAR_GUTTER}
                className={`relative block h-64 w-full resize-none bg-transparent p-3 font-mono text-sm leading-6 whitespace-pre-wrap break-words outline-none placeholder:text-dim ${
                  highlighted ? 'text-transparent caret-strong' : 'text-strong'
                }`}
              />
            </div>

            {issue && (
              <div
                className={`font-mono text-xs break-words ${
                  blocked ? 'text-danger' : 'text-brand'
                }`}
              >
                {issue.message}
              </div>
            )}

            {/* Причина ошибки — не в теле, а в том, что оно от прежнего типа.
                Со ссылкой, а не одним текстом: сказать «обновите заготовку» и
                оставить искать, чем именно, значит сделать полработы — ссылка
                делает то же самое, что кнопка `template` над полем. */}
            {issue && staleTemplate && (
              <div className="font-mono text-xs text-dim">
                Just changed the template? Don't forget to{' '}
                <button
                  type="button"
                  onClick={applyTemplate}
                  title="Replace the body with a fresh template for the type you picked"
                  className="bg-transparent border-none p-0 font-mono text-xs text-brand underline cursor-pointer"
                >
                  refresh
                </button>{' '}
                it
              </div>
            )}

            {/* Выход из тупика. Заблокировав отправку, приложение обязано
                сказать, что делать дальше: тело, которое не ложится на схему,
                отправить всё-таки можно — своими байтами.

                Пока заготовка от прежнего типа, этого совета здесь нет: он
                предлагает смириться с телом, которое пользователь, скорее
                всего, просто не успел обновить. */}
            {blocked && format === 'proto' && !staleTemplate && (
              <div className="font-mono text-xs text-dim">
                Need to send exactly this? Encode it yourself and paste the bytes as{' '}
                <button
                  type="button"
                  onClick={() => setFormat('hex')}
                  className="bg-transparent border-none p-0 font-mono text-xs text-brand underline cursor-pointer"
                >
                  hex
                </button>
                .
              </div>
            )}

            {/* Что именно уедет в топик. Не деталь реализации: с заголовком
                тело прочитает штатный потребитель со своим десериализатором,
                а без него — только тот, кто знает схему заранее. */}
            {SUBJECT_FORMATS.has(format) && (
              <div className="font-mono text-xs text-dim">
                {framed
                  ? 'Sent in the Confluent wire format: the schema id goes in front of the body.'
                  : format === 'jsonschema'
                    ? 'Sent as plain JSON — no schema id in front. The body is still checked against the schema, but a consumer expecting the registry format will not read it.'
                    : 'Sent as a bare Avro datum — no schema id in front. A consumer that expects the registry format will not read it.'}
              </div>
            )}
          </div>

          <div className="flex gap-3 pt-1">
            <Button
              onClick={() => onOpenChange(false)}
              variant="outline"
              className="flex-1 bg-transparent border-edge text-soft hover:bg-edge hover:text-strong font-mono"
            >
              Cancel
            </Button>
            <Button
              onClick={handleSend}
              disabled={busy || blocked}
              title={blocked ? 'Fix the body first — there are no bytes to send' : undefined}
              className="flex-1 bg-brand text-surface hover:bg-brand-hover font-mono disabled:opacity-50"
            >
              <Send className="size-4 mr-1" />
              {busy ? 'Sending…' : 'Send'}
            </Button>
          </div>
        </div>
      </DialogContentNoClose>
    </Dialog>
  );
}
