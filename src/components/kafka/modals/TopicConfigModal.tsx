import { useCallback, useEffect, useRef, useState } from 'react';
import { Button } from '../../ui/button';
import { ClipboardType, FileText, FolderOpen, Library, Upload, X } from 'lucide-react';
import { toast } from 'sonner';
import { open as openFileDialog } from '@tauri-apps/plugin-dialog';
import { Dialog, DialogHeader, DialogTitle } from '../../ui/dialog';
import { DialogContentNoClose } from '../DialogContentNoClose';
import { Label } from '../../ui/label';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '../../ui/tabs';
import { AvroSchema } from '../components/AvroSchema';
import { GuessResults } from '../components/GuessResults';
import { JsonSchemaSettings } from '../components/JsonSchemaSettings';
import { ProtoEditor } from '../components/ProtoEditor';
import { ProtoMessageSelect } from '../components/ProtoMessageSelect';
import { SchemaSetSelect } from '../components/SchemaSetSelect';
import { SchemaSourcesModal } from './SchemaSourcesModal';
import * as api from '../api';
import { describeError } from '../api';
import {
  BodyFormat,
  ProtoPreview,
  SchemaGuess,
  SourceBinding,
  SourceView,
  Topic,
  TopicSchema,
} from '../types';

interface TopicConfigModalProps {
  topic: Topic | null;
  /** Ключ кластера — см. `clusterKey`. null, когда подключения нет. */
  cluster: string | null;
  /**
   * Какой топик сейчас открыт в таблице.
   *
   * Нужен подбору типа: образцы он берёт из буфера воркера, а буфер один и
   * принадлежит открытому топику. Настройки же открывают у любого топика
   * списка, и кнопка подбора у не-открытого обязана быть выключена — иначе
   * пользователь получил бы уверенный ответ, посчитанный по чужим телам.
   * (Бэкенд такой запрос всё равно отвергнет, но объяснять это тостом, когда
   * можно объяснить выключенной кнопкой, незачем.)
   */
  openTopic: string | null;
  /**
   * Схема открытого топика — та, которой таблица прямо сейчас читает тела.
   *
   * Нужна затем, чтобы у этого топика форме не приходилось ЖДАТЬ: приложение
   * уже держит его схему и перечитывает её после каждой правки, в том числе
   * после переключения формата в шапке. А открывают настройки чаще всего именно
   * оттуда — шестерёнка стоит вплотную к селектору формата.
   *
   * Тому, что приехало, форма верит не на слово: схема несёт в себе имя своего
   * топика и кластера, и посеяно будет только совпадающее. Без этой сверки
   * нашлась бы своя щель — между выбором топика и ответом `applyTopicSchema`
   * здесь лежит схема ПРЕЖНЕГО.
   */
  openTopicSchema?: TopicSchema | null;
  open: boolean;
  onOpenChange: (open: boolean) => void;
  /**
   * Схема топика изменилась на диске.
   *
   * Зовётся по Save — и, кроме него, только из разделов avro и jsonschema, у
   * которых свои кнопки применения. Протобафная часть окна до Save на диск не
   * ходит вовсе: в этом и смысл черновика (`Draft`).
   */
  onSchemaChanged?: (topic: string, schema: TopicSchema | null) => void;
}

/**
 * Откуда топик берёт свой .proto. Три способа, и выбран может быть только один.
 *
 * Взаимоисключающи они не для порядка в интерфейсе, а по устройству: это три
 * ответа на один вопрос «чем декодировать», и сосуществуя они решались бы тем,
 * какой из них проверили раньше. Бэкенд держит то же правило — загрузка файлов
 * снимает привязку к библиотеке и стирает вставленный текст, привязка сносит
 * свои копии, вставка стирает файлы.
 */
export type ProtoMode = 'paste' | 'files' | 'library';

/**
 * Какая вкладка открыта, когда окно только открылось.
 *
 * У настроенного топика — та, которой он настроен: видеть надо то, чем он
 * читается, а не предложение настроить его заново.
 *
 * У ненастроенного выбор между «вставить текст» и библиотекой решается тем,
 * есть ли в библиотеке что выбирать. Есть — библиотека: один выбор контракта
 * дешевле любого другого способа, ради этого она и заведена. Нет — поле для
 * текста: это самый прямой путь, и он не требует ничего, кроме самого
 * контракта.
 */
export function modeOf(schema: TopicSchema | null, sources: SourceView[]): ProtoMode {
  return (
    modeFromSchema(schema) ??
    (sources.some((source) => source.sets.some((set) => !set.error)) ? 'library' : 'paste')
  );
}

/**
 * Та часть выбора вкладки, на которую отвечает САМА схема топика, — то есть всё,
 * кроме ненастроенного топика.
 *
 * Отдельно от `modeOf` ради одного: у настроенного топика ответ известен сразу,
 * как приехала схема, и ждать ради него библиотеку незачем. `null` — настроен
 * топик ничем, и тогда вкладку решает то, есть ли в библиотеке что выбирать.
 */
export function modeFromSchema(schema: TopicSchema | null): ProtoMode | null {
  if (schema?.source) return 'library';
  const files = schema?.files ?? [];
  if (files.some((file) => file.pasted)) return 'paste';
  if (files.length > 0) return 'files';
  return null;
}

/**
 * Про схему известно всё, что нужно форме, — показывать можно.
 *
 * Не хватать может ровно одного: вкладки у ненастроенного protobuf-топика, и
 * отвечает на это библиотека. Остальным форматам она не нужна вовсе, так что и
 * ждать им нечего.
 */
export function settledWithoutLibrary(schema: TopicSchema | null): boolean {
  return schema?.format !== 'proto' || modeFromSchema(schema) !== null;
}

/** Сколько ждать после последнего нажатия клавиши, прежде чем разбирать текст.
 *  Разбор идёт на бэкенде в отдельном потоке, но затевать его на каждый символ
 *  незачем: вставляют контракт целиком, а не набирают. */
const CHECK_DELAY_MS = 400;

/**
 * Контракт, набранный в окне, — по одному на каждый способ его задать.
 *
 * Все три живут одновременно, хотя выбран будет один: переключиться на соседнюю
 * вкладку посмотреть и вернуться — обычное дело, и набранный текст на этом
 * теряться не должен.
 */
export interface Draft {
  /** «Paste text»: текст в поле. */
  text: string;
  /** «Local files»: пути, выбранные в диалоге. Доисканные по импортам сюда не
   *  попадают — их находит разбор, и приезжают они в `ProtoPreview.files`. */
  paths: string[];
  /** «Library»: выбранный контракт. */
  binding: SourceBinding | null;
}

const EMPTY_DRAFT: Draft = { text: '', paths: [], binding: null };

/**
 * Задан ли контракт на этой вкладке.
 *
 * От него зависит и то, можно ли сохранять, и то, показывать ли селектор типа:
 * у пустой вкладки отвечать на вопрос «чем читать» нечем.
 */
export function draftFilled(mode: ProtoMode, draft: Draft): boolean {
  if (mode === 'paste') return draft.text.trim().length > 0;
  if (mode === 'files') return draft.paths.length > 0;
  return draft.binding !== null;
}

/**
 * Совпадает ли черновик с тем, что у топика уже сохранено.
 *
 * Нужно ради одного: у такого черновика разбор УЖЕ известен — он приехал вместе
 * со схемой, — и спрашивать его у бэкенда заново незачем. Открытие настроек
 * настроенного топика от этого не стоит ни одного запроса.
 *
 * Сравнение по тому, чем контракт задан, а не по его содержимому: текст сверяем
 * посимвольно, файлы — списком исходных путей (в том же порядке, в каком их
 * хранит схема), привязку — парой «источник плюс входной файл».
 */
export function draftMatchesSchema(
  mode: ProtoMode,
  draft: Draft,
  schema: TopicSchema | null,
): boolean {
  if (!schema || modeFromSchema(schema) !== mode) return false;
  if (mode === 'paste') return draft.text === (schema.pasted_text ?? '');
  if (mode === 'files') {
    const stored = pickedPathsOf(schema);
    return (
      draft.paths.length === stored.length && draft.paths.every((p, i) => p === stored[i])
    );
  }
  return (
    draft.binding !== null &&
    draft.binding.source === schema.source?.source &&
    draft.binding.entry === schema.source?.entry
  );
}

/**
 * Исходные пути файлов, выбранных руками.
 *
 * Доисканные по импортам (`auto`) не в счёт и вставленный текст тоже: у первых
 * нет самостоятельного выбора, у второго нет оригинала на диске. Это ровно тот
 * список, который сохранение отдаёт обратно бэкенду, — он доищет импорты сам,
 * как делал и при первой загрузке.
 */
export function pickedPathsOf(schema: TopicSchema | null): string[] {
  return (schema?.files ?? [])
    .filter((file) => !file.auto && !file.pasted && file.source)
    .map((file) => file.source);
}

/**
 * Черновик по сохранённой схеме: открыть настройки значит увидеть действующий
 * контракт, а не чистый лист.
 *
 * Заполнена при этом ровно одна вкладка — та, которой топик настроен: контракт
 * у него один, и показывать его копию в двух местах значило бы предлагать выбор,
 * которого нет.
 */
export function draftFromSchema(schema: TopicSchema | null): Draft {
  const mode = modeFromSchema(schema);
  if (!schema || !mode) return EMPTY_DRAFT;
  if (mode === 'paste') return { ...EMPTY_DRAFT, text: schema.pasted_text ?? '' };
  if (mode === 'files') return { ...EMPTY_DRAFT, paths: pickedPathsOf(schema) };
  return { ...EMPTY_DRAFT, binding: schema.source };
}

/**
 * Разбор сохранённого контракта — тот, что приехал вместе со схемой.
 *
 * Спрашивать его у бэкенда заново незачем: `TopicSchemaView` уже несёт и список
 * типов, и корень, и список файлов, причём посчитанные тем же разбором. Поэтому
 * открытие настроек настроенного топика не стоит ни одного запроса — а стоило
 * бы трёх, считай мы черновик «изменением», которое надо проверить.
 *
 * `null` — топик не настроен ничем, и разбирать нечего.
 */
export function previewFromSchema(schema: TopicSchema | null): ProtoPreview | null {
  if (!schema || !modeFromSchema(schema)) return null;
  return {
    files: schema.files,
    messages: schema.messages,
    root: schema.root,
  };
}

/**
 * Чем заполнить селектор типа, когда разбор контракта обновился.
 *
 * Прежний выбор сохраняется, если он в новом наборе есть: человек мог выбрать
 * вложенный тип намеренно, и перебивать его правилом корня нельзя. Иначе —
 * корень набора, а его-то обычно и хватает: на настоящих контрактах правило
 * корня совпало с назначенным вручную 14 раз из 14. Нет корня, а тип один —
 * берём единственный: выбирать не из чего.
 */
export function messageFor(
  preview: ProtoPreview | null,
  chosen: string | null,
): string | null {
  const messages = preview?.messages ?? [];
  if (chosen && messages.includes(chosen)) return chosen;
  if (preview?.root) return preview.root;
  return messages.length === 1 ? messages[0] : null;
}

/**
 * Вид вкладки.
 *
 * Поверх shadcn-овского `TabsTrigger`, у которого свои цвета и скругления:
 * здесь нужны те же, что у остального окна (`font-mono`, `bg-surface`,
 * `text-strong`). `dark:` для активной подписи задан отдельно — у базового
 * класса есть своё правило под тёмную тему, и без парного наша бы в ней
 * проиграла.
 */
const TAB_CLASS =
  // `min-w-0` — та же страховка, что у кнопок выбора источника: подпись не
  // должна иметь права раздвинуть полоску вкладок шире окна настроек.
  //
  // `cursor-pointer` — своим: у базового `TabsTrigger` курсор остаётся
  // стрелкой, то есть вкладка не выглядит нажимаемой, хотя она кнопка. В
  // остальном окне кнопки его ставят поимённо (`cursor-pointer` у каждой), так
  // что общего правила, которое бы сюда дотянулось, в проекте нет.
  'min-w-0 cursor-pointer font-mono text-xs rounded border border-transparent px-2 py-1.5 text-dim hover:text-soft ' +
  'data-[state=active]:bg-surface data-[state=active]:border-edge ' +
  'data-[state=active]:text-strong dark:data-[state=active]:text-strong';

/**
 * Высота панели под вкладками — одна на все три.
 *
 * Без неё окно меняло размер на каждое переключение, причём по-разному: между
 * «Local files» и «Library» расходились единицы пикселей (список файлов против
 * поля выбора), а «Paste text» был выше остальных сразу на полторы сотни.
 * А поскольку модальное окно выровнено по центру экрана, любая смена высоты
 * двигает и его ВЕРХ — то есть саму полоску вкладок, по которой в этот момент
 * и целятся.
 *
 * Задают её две вкладки вровень. «Paste text»: 8 (`pt-2`) + 226 (поле с рамкой;
 * `h-56` — ровно столько занимает заготовка из восьми строк при `leading-6`
 * плюс отступы, и ужать его значило бы её обрезать) + 8 + 16 (строка
 * состояния) = 258. «Local files» с полным списком: 8 + 14 (подпись) + 8 + 160
 * (`max-h-40`) + 8 + 36 (кнопка `h-9`) + 8 + 13 (подсказка) + 8 + 16 = 279.
 * Отсюда 288 — с небольшим запасом на случай, если арифметика где-то на пиксель
 * врёт.
 *
 * Цена — пустое место под содержимым «Library»: там всего подпись, поле выбора
 * и кнопка. Это осознанный размен: неподвижное окно стоит дороже плотной
 * вёрстки, потому что по вкладкам ходят, а в пустоту — нет.
 */
const TAB_PANEL_CLASS = 'space-y-2 pt-2 min-h-[288px]';

/** Заготовка в пустом поле: из неё видно и что вставлять, и что ждут именно
 *  текст контракта, а не путь к нему.
 *
 *  Нарочно безотносительно какой-либо предметной области: заготовка стоит перед
 *  глазами у каждого, кто открыл вкладку, и выдуманные заявки из неё читались бы
 *  как намёк, что приложение про них. Восемь строк — это ещё и ровно та высота,
 *  на которую рассчитано поле (см. `TAB_PANEL_CLASS`). */
const PASTE_PLACEHOLDER = `syntax = "proto3";

package events;

message Event {
  string event_uid = 1;
  int64 event_ts = 2;
}`;

/**
 * Что сказал разбор черновика. Одна строка под содержимым вкладки.
 *
 * Стоит ПОД полем, а не между полем и кнопкой: кнопки у вкладки больше нет, и
 * появление строки больше ничего не двигает. Раньше под неё приходилось держать
 * пустое место — иначе «checking…» на каждое затишье в наборе текста толкало
 * кнопку и всё окно.
 *
 * Порядок проверок — от срочного к приятному: сперва причина отказа, потом
 * признак, что разбор идёт, потом подтверждение, что это и есть действующий
 * контракт, и только потом — сколько типов нашлось. Последнее не украшение:
 * иначе про успешный разбор не видно вообще ничего, и непонятно, почему вдруг
 * ожил селектор типа.
 */
function DraftStatus({
  error,
  checking,
  preview,
  saved,
}: {
  error: string | null;
  checking: boolean;
  preview: ProtoPreview | null;
  saved: boolean;
}) {
  if (error) {
    return (
      <div className="font-mono text-xs text-danger break-words whitespace-pre-wrap">{error}</div>
    );
  }
  if (checking) return <div className="font-mono text-xs text-dim">checking…</div>;
  if (!preview) return null;
  if (saved) {
    return (
      <div className="font-mono text-xs text-dim">This is the contract the topic is read by.</div>
    );
  }
  const count = preview.messages.length;
  return (
    <div className="font-mono text-xs text-dim">
      Parsed — {count} message {count === 1 ? 'type' : 'types'}. Save to read the topic by it.
    </div>
  );
}

/**
 * Что стоит на месте формы, пока схема топика не приехала.
 *
 * Заготовка повторяет расстановку той формы, которая и приедет: полоска вкладок
 * и панель под ней той же высоты (`TAB_PANEL_CLASS`), — чтобы форма встала на то
 * же место, а не подвинула окно.
 *
 * Видно её ровно в одном случае: у protobuf-топика, не настроенного ничем, —
 * там вкладку решает библиотека, и до её ответа показывать нечего. Всем
 * остальным форма достаётся готовой из схемы открытого топика
 * (`openTopicSchema`), и ждать им нечего вовсе.
 *
 * Своя, а не `ui/skeleton`: тот красится в `bg-accent`, то есть мимо палитры
 * этих окон (`surface`/`sunken`/`edge`), и в тёмной теме светится.
 */
function Waiting() {
  return (
    <div className="space-y-2 min-w-0" aria-hidden>
      <div className="h-9 rounded border border-edge bg-sunken animate-pulse" />
      <div className="min-h-[288px] rounded border border-edge bg-sunken animate-pulse" />
    </div>
  );
}

function chooseMessage(schema: TopicSchema | null): string | null {
  const messages = schema?.messages ?? [];
  if (schema?.message) {
    // Пустой список — это «файлов нет вовсе», и судить по нему не о чем:
    // сохранённый выбор остаётся, иначе открытие окна у топика с временно
    // недоступными .proto стирало бы привязку. А вот если список есть и
    // выбора в нём НЕТ, то тип из файла уехал — и держать имя, которого
    // больше не существует, значит обещать декодирование, которого не будет.
    if (messages.length === 0 || messages.includes(schema.message)) return schema.message;
  }
  return messages.length === 1 ? messages[0] : null;
}

export function TopicConfigModal({
  topic,
  cluster,
  openTopic,
  openTopicSchema,
  open,
  onOpenChange,
  onSchemaChanged,
}: TopicConfigModalProps) {
  const [format, setFormat] = useState<BodyFormat>('json');
  const [message, setMessage] = useState<string | null>(null);
  const [schema, setSchema] = useState<TopicSchema | null>(null);
  /** Идёт разбор .proto на бэкенде: кнопки, которые тоже его затронут,
   *  на это время выключены. */
  const [busy, setBusy] = useState(false);
  /** Результат подбора типа по телам. null — не подбирали. */
  const [guess, setGuess] = useState<SchemaGuess | null>(null);
  const [guessing, setGuessing] = useState(false);
  /** Библиотека наборов .proto — из чего выбирать, кроме своих файлов. */
  const [sources, setSources] = useState<SourceView[]>([]);
  /** За библиотекой уже сходили. Пустой список сам по себе ответа не даёт:
   *  он одинаков и у пустой библиотеки, и у непрочитанной. */
  const [sourcesLoaded, setSourcesLoaded] = useState(false);
  /** Поверх этой формы открыта сама библиотека. Окно источников живёт здесь, а
   *  не в шапке приложения: источники нужны ровно тогда, когда настраивают
   *  proto у топика, и других дверей к ним в приложении нет. */
  const [sourcesOpen, setSourcesOpen] = useState(false);
  /** Какой из трёх способов задать контракт сейчас открыт. */
  const [mode, setMode] = useState<ProtoMode>('paste');
  /**
   * Контракт, НАБРАННЫЙ в окне и ещё не сохранённый.
   *
   * Это и есть то, чем окно отличается от прежнего: раньше каждый способ
   * применялся сразу — вставка кнопкой, файлы диалогом, контракт библиотеки
   * щелчком по списку, — и к моменту, когда человек доходил до Save, на диске
   * уже лежало всё, кроме выбранного типа. Cancel при этом не отменял ничего.
   * Теперь на диск не уходит ничто, пока не нажат Save.
   *
   * Свой у каждой вкладки, а не один на все: переключиться посмотреть и
   * вернуться — обычное дело, и терять на этом набранный текст незачем. Выбран
   * при сохранении будет тот, чья вкладка открыта, — три способа
   * взаимоисключающи, и это свойство никуда не делось.
   */
  const [draft, setDraft] = useState<Draft>(EMPTY_DRAFT);
  /**
   * Что дал разбор контракта ОТКРЫТОЙ вкладки. `null` — разбирать нечего либо
   * не разобралось.
   *
   * Отсюда берётся список типов для селектора «чем читать» — и только отсюда.
   * Раньше он брался из схемы топика, и это было видно глазами: стоишь на
   * пустой вкладке «Paste text», а в селекторе предлагаются типы из контракта,
   * привязанного через библиотеку.
   */
  const [preview, setPreview] = useState<ProtoPreview | null>(null);
  /** Чем плох контракт на открытой вкладке. null — разобрался либо пусто. */
  const [draftError, setDraftError] = useState<string | null>(null);
  /** Разбор идёт прямо сейчас. */
  const [checking, setChecking] = useState(false);
  /**
   * Про какую пару кластер-топик на экране стоит ответ.
   *
   * Несовпадение с текущей означает, что в полях чужая схема, — и это ровно то,
   * что видел человек: переключив формат с avro на proto и открыв настройки, он
   * две-три секунды смотрел на форму avro, ту самую, которую окно нарисовало в
   * прошлый раз, после чего она скачком менялась на proto.
   */
  const [shown, setShown] = useState<string | null>(null);
  /** В полях — схема ИМЕННО этого топика, а не ожидание и не чужая. */
  const [ready, setReady] = useState(false);
  /** Схему прочитать не удалось вовсе. Форма тогда не рисуется: пустые поля над
   *  живой кнопкой Save — это приглашение затереть настройку, которую окно так
   *  и не показало. */
  const [loadError, setLoadError] = useState<string | null>(null);

  const topicName = topic?.name ?? null;

  /**
   * Перечитать библиотеку. Возвращает её же: вызывающему она бывает нужна
   * значением, а не состоянием — выбор вкладки считается по ней сразу.
   *
   * Отказ молчит: без библиотеки форма работает как работала — остаются свои
   * файлы с диска и вставка текстом, — а красный тост поверх открывшихся
   * настроек читался бы как поломка самих настроек.
   */
  const loadSources = useCallback(async (): Promise<SourceView[]> => {
    try {
      const library = await api.listSchemaSources();
      setSources(library);
      setSourcesLoaded(true);
      return library;
    } catch (e) {
      console.error('Failed to list schema sources', e);
      return [];
    }
  }, []);

  // --- Чья схема сейчас в полях -----------------------------------------------
  //
  // Состояние формы принадлежит паре кластер-топик, а окно одно на все топики
  // списка, и пока новая схема не приехала, в полях остаётся ПРЕЖНЯЯ.
  //
  // Сбрасывается оно прямо в рендере, а не эффектом, и это здесь существенно:
  // эффект отрабатывает уже ПОСЛЕ отрисовки, то есть чужую форму всё равно
  // успело бы показать — ровно это и происходило. Присваивание состояния во
  // время рендера React обрывает и начинает рендер заново, ничего не выводя на
  // экран; это его собственный приём для состояния, которое зависит от пропсов.
  const wanted = open && cluster && topicName ? `${cluster} ${topicName}` : null;
  if (!open) {
    // На закрытии сбрасывается только метка, но не поля: окно закрывается с
    // анимацией и всё это время ещё нарисовано, а заготовка вместо формы в
    // уезжающем окне — такая же неправда, как чужая форма во въезжающем. Метки
    // довольно, чтобы следующее открытие считалось открытием даже у того же
    // самого топика.
    if (shown !== null) setShown(null);
  } else if (wanted !== shown) {
    setShown(wanted);
    // Схему открытого топика приложение уже держит — ей и сеем. Сверка по имени
    // обязательна: `openTopic` говорит, какой топик открыт, а схема приезжает
    // отдельным ответом, и между этими двумя событиями в ней лежит прежняя.
    const seed =
      openTopicSchema && openTopicSchema.cluster === cluster && openTopicSchema.topic === topicName
        ? openTopicSchema
        : null;
    setSchema(seed);
    setFormat(seed?.format ?? 'json');
    // Черновик заводится из того, чем топик настроен: открыть настройки значит
    // увидеть действующий контракт, а не чистый лист. Соседние вкладки при этом
    // пусты — контракт у топика один.
    setDraft(draftFromSchema(seed));
    setPreview(previewFromSchema(seed));
    setMessage(seed?.message ?? null);
    setDraftError(null);
    // Подбор относится к паре кластер-топик и к тому, что лежало в буфере в
    // момент нажатия: показывать его на следующем топике значило бы предлагать
    // ответ, посчитанный по чужим телам.
    setGuess(null);
    setMode(modeFromSchema(seed) ?? 'paste');
    setReady(!!seed && settledWithoutLibrary(seed));
    setLoadError(null);
  }

  // Чтение схемы. Ответ с диска — последнее слово в любом случае: посеянное
  // лишь избавляет от ожидания, но источником правды не становится.
  useEffect(() => {
    if (!open || !cluster || !topicName) return;
    let cancelled = false;

    void (async () => {
      let loaded: TopicSchema | null;
      try {
        loaded = await api.getTopicSchema(cluster, topicName);
      } catch (e) {
        if (cancelled) return;
        console.error('Failed to load topic schema', e);
        // Тоста мало: он уедет, а окно останется — и останется в нём заготовка,
        // которую не отличить от «ещё грузится». Причина обязана лежать на том
        // месте, где должна была быть форма.
        setLoadError(describeError(e));
        return;
      }
      if (cancelled) return;

      setSchema(loaded);
      setFormat(loaded?.format ?? 'json');
      setDraft(draftFromSchema(loaded));
      setPreview(previewFromSchema(loaded));
      setMessage(loaded?.message ?? null);
      setDraftError(null);
      if (loaded?.error) {
        toast.error(`Schema of ${topicName} is broken: ${loaded.error}`);
      }

      // Вкладку, на которую отвечает сама схема, ставим здесь же и библиотеку
      // ради неё не ждём. Ждать приходится ровно в одном случае — у
      // ненастроенного protobuf-топика, — и тогда до ответа форма показывает
      // ожидание, а не наугад выбранную вкладку.
      const decided = modeFromSchema(loaded);
      if (decided) setMode(decided);
      if (settledWithoutLibrary(loaded)) setReady(true);

      // Остальным форматам библиотека не нужна вовсе: у avro-топика ходить за
      // списком protobuf-контрактов незачем.
      if (loaded?.format !== 'proto') return;
      const library = await loadSources();
      if (cancelled || decided) return;
      setMode(modeOf(loaded, library));
      setReady(true);
    })();

    return () => {
      cancelled = true;
    };
  }, [open, cluster, topicName, loadSources]);

  /**
   * Разбор черновика открытой вкладки — то, на чём держится вся форма.
   *
   * Он отвечает сразу на три вопроса: разбирается ли контракт (иначе Save
   * выключен и под полем стоит причина), какие типы в нём объявлены (ими
   * заполняется селектор) и какие файлы доискались по импортам. Раньше ответ на
   * второй и третий давало только ПРИМЕНЕНИЕ — отсюда и бралась кнопка «Apply
   * contract», и невозможность отменить сделанное.
   *
   * Разбор тот же самый, что у сохранения: `check_proto_*` и `add_proto_*`
   * зовут одно и то же. Поэтому ответ здесь не приближение, а ровно то, что
   * скажет Save.
   *
   * Чего он НЕ делает: не ходит на бэкенд за тем, что уже известно. Черновик,
   * совпадающий с сохранённой схемой, разобран вместе с ней — у настроенного
   * топика открытие настроек не стоит ни одного запроса.
   *
   * Гонка ответов разрешается счётчиком: отпущенная клавиша отменяет ожидание,
   * но не уже улетевший запрос, и ответ на прежний текст обязан не перебить
   * ответ на нынешний.
   */
  const checkSeq = useRef(0);
  useEffect(() => {
    if (!open || !ready) return;

    // Пусто — разбирать нечего, и это не ошибка: вкладку могли просто открыть
    // посмотреть.
    if (!draftFilled(mode, draft)) {
      ++checkSeq.current;
      setPreview(null);
      setDraftError(null);
      setChecking(false);
      return;
    }

    if (draftMatchesSchema(mode, draft, schema)) {
      ++checkSeq.current;
      setPreview(previewFromSchema(schema));
      setDraftError(null);
      setChecking(false);
      return;
    }

    // Прежний ответ относился к прежнему черновику и с этого мгновения не
    // значит ничего. Оставить его на экране значило бы показывать ошибку в
    // строке, которую только что исправили, — и держать Save выключенным из-за
    // неё.
    setDraftError(null);
    setChecking(true);
    const seq = ++checkSeq.current;
    const settle = (next: ProtoPreview | null, error: string | null) => {
      if (seq !== checkSeq.current) return;
      setPreview(next);
      setDraftError(error);
      setChecking(false);
      // Тип выбирается по НОВОМУ набору: прежний остаётся, только если он в нём
      // есть, иначе его подставляет корень. Считается здесь, а не отдельным
      // эффектом по `preview`, чтобы выбор и набор менялись одним шагом.
      setMessage((chosen) => messageFor(next, chosen));
    };

    // Только текст набирают по буквам — у файлов и привязки задержка ни от чего
    // не спасает, а ощущается как задумчивость.
    const delay = mode === 'paste' ? CHECK_DELAY_MS : 0;
    const timer = setTimeout(() => {
      const checking =
        mode === 'paste'
          ? api.checkProtoText(draft.text)
          : mode === 'files'
            ? api.checkProtoFiles(draft.paths)
            : api.checkSchemaSet(draft.binding!.source, draft.binding!.entry);
      checking
        .then((parsed) => settle(parsed, null))
        .catch((e) => settle(null, describeError(e)));
    }, delay);

    return () => clearTimeout(timer);
    // `sources` в зависимостях нарочно: набор из библиотеки мог только что
    // появиться или быть перезабран, и его разбор от этого меняется. Лишним это
    // не выходит — у совпадающего с сохранённым черновика ветка выше обрывает
    // эффект, не дойдя до запроса.
  }, [open, ready, mode, draft, schema, sources]);

  /**
   * Этому топику есть по чему подбирать: его тела и лежат в буфере воркера.
   *
   * Стоит здесь, до колбэков, а не рядом с разметкой: от него зависит
   * `handleBind` — подбор после привязки имеет смысл только у открытого топика.
   */
  const isOpenTopic = !!topicName && topicName === openTopic;

  /**
   * Общий хвост операций разделов avro и jsonschema — единственных, что ещё
   * применяются сразу, своими кнопками. Протобафная часть окна сюда не ходит:
   * у неё черновик, и применяет его Save.
   *
   * Успех виден сразу: и в форме, и в уже открытой таблице. Неудача не меняет
   * ничего — на диске бэкенд её тоже не применил.
   */
  const applySchema = useCallback(
    async (
      action: () => Promise<TopicSchema | null>,
      // Функцией, а не строкой, ради одного случая: загрузка схемы может
      // подтянуть файлы, которых пользователь не выбирал, и сказать об этом
      // можно только по результату.
      success: string | ((updated: TopicSchema | null) => string),
    ): Promise<{ ok: boolean; message: string | null }> => {
      if (!cluster || !topicName) return { ok: false, message: null };
      setBusy(true);
      try {
        const updated = await action();
        setSchema(updated);
        setFormat(updated?.format ?? 'json');
        const chosen = chooseMessage(updated);
        setMessage(chosen);
        onSchemaChanged?.(topicName, updated);
        toast.success(typeof success === 'string' ? success : success(updated));
        return { ok: true, message: chosen };
      } catch (e) {
        console.error('Schema operation failed', e);
        toast.error(describeError(e));
        return { ok: false, message: null };
      } finally {
        setBusy(false);
      }
    },
    [cluster, topicName, onSchemaChanged],
  );

  /**
   * В библиотеке что-то изменилось, пока она была открыта поверх этой формы.
   *
   * Перечитывается и список источников (от него зависит выпадающий список
   * контрактов), и схема самого топика: добавленный источник мог быть ровно
   * тем, которого не хватало привязке, — и тогда топик начинает декодироваться
   * сию секунду, без Save. По той же причине об этом говорится наружу: открытая
   * таблица обязана перерисоваться.
   *
   * Чего здесь намеренно НЕ делается — не трогается НАБРАННОЕ: ни формат, ни
   * выбранный контракт, ни тип. Всё это могли выбрать минуту назад и ещё не
   * сохранить, а поход за источником к ним не относится вовсе. Разбор
   * выбранного контракта пересчитается сам — список источников у его эффекта в
   * зависимостях.
   */
  const handleSourcesChanged = useCallback(async () => {
    await loadSources();
    if (!cluster || !topicName) return;
    try {
      const reloaded = await api.getTopicSchema(cluster, topicName);
      setSchema(reloaded);
      onSchemaChanged?.(topicName, reloaded);
    } catch (e) {
      console.error('Failed to re-read the topic schema', e);
    }
  }, [cluster, topicName, onSchemaChanged, loadSources]);

  /**
   * Выбрать .proto в диалоге — и только. На диск ничего не уходит: выбранные
   * пути ложатся в черновик, а разбор следом скажет, что в них объявлено и
   * какие файлы доискались по импортам.
   *
   * Выбрать повторно — значит заменить набор, а не дополнить его: диалог
   * показывает ровно то, что человек считает контрактом этого топика.
   */
  const handlePickFiles = useCallback(async () => {
    let picked: string | string[] | null;
    try {
      picked = await openFileDialog({
        multiple: true,
        filters: [{ name: 'Proto', extensions: ['proto'] }],
      });
    } catch (e) {
      console.error('Failed to open file dialog', e);
      toast.error(`Failed to open file dialog: ${describeError(e)}`);
      return;
    }

    const paths = Array.isArray(picked) ? picked : picked ? [picked] : [];
    if (paths.length === 0) return;
    setDraft((current) => ({ ...current, paths }));
  }, []);

  /** Убрать файл из набора. Правка черновика — с диска ничего не исчезает. */
  const handleDropFile = useCallback((path: string) => {
    setDraft((current) => ({
      ...current,
      paths: current.paths.filter((have) => have !== path),
    }));
  }, []);

  /**
   * Подобрать тип по телам топика.
   *
   * Ничего не сохраняет и ничего не читает из Kafka: это подсказка, а не
   * правка. Результат остаётся на экране, пока его не перебьёт следующий
   * подбор или смена топика.
   *
   * `quiet` — подбор затеяло приложение, а не человек: сразу после привязки
   * набора, у которого корня нет. Тогда отказ молчит и список не показывается.
   * Отказов там законных хватает — топик не открыт, прочитанного ещё нет, — и
   * красный тост в ответ на выбор контракта выглядел бы поломкой, хотя не
   * сломалось ничего.
   */
  const runGuess = useCallback(
    async (quiet: boolean) => {
      if (!cluster || !topicName) return;
      setGuessing(true);
      try {
        const guessed = await api.guessTopicMessage(cluster, topicName);
        if (!quiet) setGuess(guessed);

        // Ответ, за которым и нажимали, подставляется в селектор сам.
        //
        // Сперва подбор только показывал список, и это было ошибкой: человек
        // жмёт «подобрать» не затем, чтобы почитать числа, а затем, чтобы тип
        // оказался выбран. Числа остаются рядом — проверить выбор по ним
        // можно, — но ставить его руками после того, как приложение уже
        // назвало ответ, незачем.
        //
        // Условия ровно те, при которых ответ ОДИН: кандидат из набора ЭТОГО
        // топика, равных ему нет, и он либо корень своего .proto, либо разобрал
        // все тела до единого. Ничья (`tied_at_top > 1`) выбором не
        // разрешается принципиально: два структурно одинаковых типа по байтам
        // неразличимы, и молча взять первый значит половину таких топиков
        // декодировать чужим типом.
        //
        // Чего здесь намеренно НЕТ — требования, чтобы тела объяснились целиком
        // (`fitting > 0`). Сперва оно стояло, и из-за него автовыбор не
        // срабатывал вообще никогда: загруженный .proto сплошь и рядом УРЕЗАН,
        // в нём оставлены нужные поля, а остальные выброшены, — и верный тип
        // оставляет половину байт в полях, которых не объявляет. Это нормальная
        // работа, а не промах, и запрещать по ней выбор значит запрещать его
        // всегда.
        const best = guessed.candidates[0];
        const sure = !!best && (best.root || best.fit.parsed === best.fit.samples);
        if (best && best.own && sure && guessed.tied_at_top === 1) {
          setMessage(best.message);
          // Молчаливый подбор всё-таки говорит — но только когда ответил.
          // Иначе поле заполнилось бы само, и откуда там имя, было бы неясно.
          if (quiet) {
            toast.success(`Matched ${best.message} by the bodies of ${topicName}`);
          }
        } else if (quiet) {
          // Ответа нет — показываем числа, как при ручном подборе: раз уж
          // приложение сходило за ним само, пусть будет видно, почему
          // промолчало.
          setGuess(guessed);
        }
      } catch (e) {
        console.error('Failed to guess the message type', e);
        if (!quiet) {
          toast.error(describeError(e));
          setGuess(null);
        }
      } finally {
        setGuessing(false);
      }
    },
    [cluster, topicName],
  );

  const handleGuess = useCallback(() => void runGuess(false), [runGuess]);

  /**
   * Привязать топик к набору из библиотеки — или отвязать.
   *
   * Это и есть то, ради чего библиотека заведена: вместо диалога файлов,
   * ожидания резолва импортов и выбора типа — один выбор набора. Тип
   * подставляет правило корня на бэкенде, а если корня нет — подбор по телам,
   * тут же и молча.
   *
   * Стоит ПОСЛЕ `runGuess` не из эстетики: он у неё в зависимостях, и объявлен
   * он должен быть раньше.
   */
  /** Выбрать контракт в библиотеке — правка черновика. Разбор следом скажет,
   *  какие в нём типы; на диск он уйдёт по Save. */
  const handlePickSet = useCallback((binding: SourceBinding | null) => {
    setDraft((current) => ({ ...current, binding }));
  }, []);

  /**
   * Применить всё разом — и это единственное место, где настройки топика
   * меняются на диске.
   *
   * Порядок важен: сперва контракт, потом выбранный тип. Контракт бэкенд
   * применяет и проверяет одним действием, и если он не разобрался, схема,
   * которая работала, остаётся работать — а тип записывать уже не по чему.
   *
   * Контракт не трогается вовсе, когда он совпадает с сохранённым: пришли
   * поменять один тип — значит меняется один тип, и пересобирать ради этого
   * каталог файлов незачем.
   */
  const handleSave = useCallback(async () => {
    if (!cluster || !topicName) return;
    setBusy(true);
    try {
      if (!draftMatchesSchema(mode, draft, schema)) {
        if (mode === 'paste') {
          await api.addProtoText(cluster, topicName, draft.text);
        } else if (mode === 'files') {
          await api.addProtoFiles(cluster, topicName, draft.paths);
        } else {
          await api.bindTopicSource(
            cluster,
            topicName,
            draft.binding?.source ?? null,
            draft.binding?.entry ?? null,
          );
        }
      }
      const saved = await api.saveTopicSchema(cluster, topicName, format, message);
      onSchemaChanged?.(topicName, saved);
      toast.success(`Settings saved for ${topicName}`);
      onOpenChange(false);
    } catch (e) {
      console.error('Failed to save topic settings', e);
      // Тостом, а не строкой под полем: причина почти всегда не в самом
      // контракте — его разбор к этому моменту уже удался, — а в том, что файл
      // исчез с диска или каталог не записывается.
      toast.error(describeError(e));
    } finally {
      setBusy(false);
    }
  }, [cluster, topicName, mode, draft, schema, format, message, onSchemaChanged, onOpenChange]);

  if (!topic) return null;

  /**
   * Типы, из которых выбирают, — РАЗБОР ОТКРЫТОЙ ВКЛАДКИ, а не схема топика.
   *
   * Разница видна глазами, и ровно на неё и жаловались: стоишь на пустой
   * вкладке «Paste text», а селектор предлагает типы из контракта, привязанного
   * через библиотеку, — то есть обещает выбор, который к этой вкладке не
   * относится никак.
   */
  const messages = preview?.messages ?? [];
  /**
   * Файлы, которые показывает вкладка «Local files»: выбранные плюс доисканные
   * по импортам — так их вернул разбор.
   *
   * Вставленный текст из списка убран: файл он и есть файл, но строки «откуда
   * взяли» у него нет и быть не может, а кнопка × относилась бы к тому, чего на
   * диске не существует. Видно его во вкладке «Paste text», и видно тем, чем он
   * является, — текстом.
   */
  const picked = (mode === 'files' ? (preview?.files ?? []) : []).filter((file) => !file.pasted);

  /**
   * Сколько контрактов библиотеки вообще можно выбрать.
   *
   * Нужно на один вопрос — показывать ли поле выбора: у пустой библиотеки это
   * поле, на которое нечего ответить. Сам список строит `SchemaSetSelect`, и
   * строит он его в два уровня — плоским сорок с лишним контрактов двух
   * репозиториев уже не читались.
   *
   * Наборы с ошибкой разбора не в счёт: привязать к ним нечего, а чинить их
   * надо в окне библиотеки, где и написано, что именно сломалось.
   */
  const usableSets = sources.reduce(
    (total, source) => total + source.sets.filter((set) => !set.error).length,
    0,
  );
  /**
   * Сколько наборов библиотеки не разбирается — то есть сколько пропало из
   * списка выше.
   *
   * Молчать про них нельзя, и это стоило разбирательства на живом репозитории:
   * в окне библиотеки стояло «19 contracts», здесь — один пункт, и ничто не
   * связывало одно с другим. Отфильтрованы они по-прежнему (привязать к
   * сломанному набору нечего), но теперь об этом сказано и сказано, куда идти.
   */
  const brokenSets = sources.reduce(
    (total, source) => total + source.sets.filter((set) => set.error).length,
    0,
  );
  /** Что выбрано в выпадающем списке контрактов — черновик, а не схема. */
  const bound = draft.binding;
  /**
   * В библиотеке есть на что отвечать выпадающему списку.
   *
   * Сломанные наборы в счёт идут: выбрать их нельзя, но список при них нужен —
   * рядом с ним стоит объяснение, сколько их и почему их не видно, и показывать
   * это объяснение под надписью «источников нет» было бы противоречием.
   */
  const hasLibrary = usableSets > 0 || !!bound || brokenSets > 0;
  /**
   * Выбранный контракт в библиотеке ЕСТЬ и разбирается.
   *
   * `null` — за библиотекой ещё не сходили, и судить не о чем. Третье состояние
   * здесь не из любви к ним: привязанный топик форма показывает, не дожидаясь
   * списка контрактов (вкладку ей назвала сама схема), и без этого `null` всё
   * это время селектор стоял бы у него пустым, а под ним — «контракта больше нет
   * в библиотеке». То есть окно пугало бы ровно тем, чего не случилось.
   */
  const boundIsLive = !sourcesLoaded
    ? null
    : !!bound &&
      sources.some(
        (source) =>
          source.id === bound.source &&
          source.sets.some((set) => set.entry === bound.entry && !set.error),
      );

  /** На открытой вкладке контракт задан — значит есть чем читать топик и есть
   *  что сохранять. */
  const filled = draftFilled(mode, draft);
  /** Черновик совпадает с тем, что сохранено. Нужно подбору: декодер он строит
   *  по схеме с диска, а не по набранному здесь. */
  const savedContract = draftMatchesSchema(mode, draft, schema);

  /**
   * Чего не хватает, чтобы сохранять.
   *
   * `null` — всё на месте. Иначе это текст для подсказки на выключенной кнопке:
   * выключенная кнопка без объяснения — худший из возможных ответов, человек
   * остаётся гадать, чего от него хотят.
   *
   * Порядок проверок тот же, в каком человек их проходит: сперва контракт,
   * потом его разбор, потом тип. Тип при этом обязателен не из строгости —
   * без него protobuf-тело не разобрать вовсе: в .proto нет ничего, что
   * сказало бы, каким из объявленных сообщений записан топик.
   */
  const blocked: string | null =
    format !== 'proto'
      ? null
      : !filled
        ? mode === 'paste'
          ? 'Paste the contract first'
          : mode === 'files'
            ? 'Pick the .proto files first'
            : 'Pick a contract from the library first'
        : draftError
          ? 'Fix the contract first — it does not parse'
          : checking
            ? 'Checking the contract…'
            : !message
              ? 'Pick the message type this topic carries'
              : null;

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      {/* Окно стоит по центру экрана, но ТОЧКА ПРИВЯЗКИ не зависит от его
          содержимого — и в этом вся разница.

          Обычное центрирование (`top-50%` плюс `translate-y-[-50%]`, как у
          остальных окон) отсчитывает смещение от ФАКТИЧЕСКОЙ высоты элемента.
          Значит любое изменение содержимого раздвигает окно в обе стороны и
          двигает верхнюю кромку, а с ней и полоску вкладок. По вкладкам же
          ходят — от «Library» к «Paste text» и обратно, подряд, — и целиться в
          уезжающую вверх-вниз полоску невозможно.

          Здесь смещение считается от середины экрана минус половина НОМИНАЛЬНОЙ
          высоты: 19rem — это примерно половина от 613 px, которые окно занимает
          в обычном своём виде (24 отступа + 45 шапки + 16 зазора + 504 тела + 24
          отступа). Выглядит как центрирование, но от содержимого не зависит
          вовсе. `max(1.5rem, …)` — пол: на низком экране иначе уехало бы под
          верхнюю кромку.

          Выравнивать вместо этого высоты содержимого — игра в кошки-мышки.
          Панели вкладок выровнены (`TAB_PANEL_CLASS`), и этого хватило ровно до
          первого пояснения НИЖЕ них: строка «на этой вкладке контракта нет»
          стоила двадцати пикселей, и вкладки поехали снова.

          `max-h` с прокруткой — не украшение: окно растёт вниз, а на коротком
          экране ему надо оставаться доступным до самых кнопок. Попоуверы и
          выпадающие списки внутри живут в порталах у `body`, так что прокрутка
          их не обрезает.

          И только у proto: вкладки есть ТОЛЬКО там, а номинальная высота у
          разделов разная — avro с jsonschema втрое короче, и при той же
          привязке встали бы заметно выше центра. Переключить формат внутри окна
          нельзя (селектор убран), так что выбор делается один раз на
          открытие. */}
      <DialogContentNoClose
        className={`max-w-md max-h-[86vh] overflow-y-auto bg-surface border-edge text-strong ${
          format === 'proto' ? 'top-[max(1.5rem,calc(50%_-_19rem))] translate-y-0' : ''
        }`}
        aria-describedby={undefined}
      >
        {/* Кнопки «в избранное» здесь больше нет: она показывала тост и не
            делала ничего — избранного у топиков в приложении не существует. */}
        <DialogHeader className="border-b border-edge pb-4 min-w-0">
          <DialogTitle className="font-mono text-soft text-lg truncate min-w-0" title={topic.name}>
            {topic.name}
          </DialogTitle>
        </DialogHeader>

        {/* min-w-0: содержимое окна — ячейка грида, а она по умолчанию не уже
            своего min-content. Без этого длинный путь к .proto растягивает
            колонку, и форма вылезает за подложку модалки. */}
        <div className="space-y-6 pt-4 min-w-0">
          {loadError && (
            <div className="rounded border border-edge border-dashed p-3 font-mono text-xs text-brand break-words">
              Could not read the settings of this topic: {loadError}
            </div>
          )}

          {!ready && !loadError && <Waiting />}

          {/* Схема ещё не приехала — и до тех пор форма не рисуется вовсе.
              Показывать в это время ПРЕЖНЮЮ и было той самой бедой: окно
              открывалось мгновенно, но с формой того формата, которым топик
              настраивали в прошлый раз.

              Снятием с дерева, а не классом: форма — это не картинка, а живые
              поля с кнопками, и невидимый, но нажимаемый Save успел бы
              сохранить заготовку вместо настройки. */}
          {ready && (
          <>
          {/* Селектора формата здесь больше нет, и это не упрощение вёрстки.
              Он был вторым ответом на вопрос, у которого уже есть первый:
              формат выбирают в шапке, рядом с топиком, и применяется он сразу,
              а здесь тот же выбор ждал кнопки Save — два управления на одну
              настройку расходятся уже тем, когда они срабатывают.

              Открыть это окно иначе как шестерёнкой в шапке нельзя (других
              входов в приложении нет, см. `handleOpenSchemaSettings`), а она
              стоит вплотную к тому самому селектору и показывается только у
              форматов, которым схема нужна. То есть формат к этому моменту
              выбран всегда, и выбран сознательно — секунду назад.

              Заодно ушла путаница в подписях: здесь стояло «Message Type», и
              это был ФОРМАТ, а ниже, у выбора protobuf-типа, — «Message».

              Формат по-прежнему известен (`format` приезжает со схемой) и
              по-прежнему уходит в `handleSave`: он решает, какой из трёх
              разделов ниже показать, и сохраняется неизменным вместе с типом. */}

          {/* Proto specific fields */}
          {format === 'proto' && (
            <div className="space-y-4">
              {/* Три способа задать контракт — вкладками.

                  Раньше они лежали подряд, один под другим: выбор набора из
                  библиотеки сверху, список файлов под ним, а переключались
                  между ними две еле заметные текстовые ссылки. Читалось это
                  как «библиотека — основной путь, остальное мелким шрифтом»,
                  хотя пути равноправны и взаимоисключающи. Вкладки говорят
                  ровно то, что есть на самом деле: выбрать можно один. */}
              <Tabs value={mode} onValueChange={(value) => setMode(value as ProtoMode)}>
                <TabsList className="grid w-full grid-cols-3 h-auto gap-1 rounded border border-edge bg-sunken p-1">
                  <TabsTrigger value="paste" className={TAB_CLASS}>
                    <ClipboardType className="size-3.5" />
                    Paste text
                  </TabsTrigger>
                  <TabsTrigger value="files" className={TAB_CLASS}>
                    <FolderOpen className="size-3.5" />
                    Local files
                  </TabsTrigger>
                  <TabsTrigger value="library" className={TAB_CLASS}>
                    <Library className="size-3.5" />
                    Library
                  </TabsTrigger>
                </TabsList>

                {/* --- Вставить текстом ------------------------------------ */}
                {/* Самый прямой путь: контракт прислали в переписке, он лежит в
                    вики или его только что написали — вставить и сохранить.
                    Файл при этом ровно один, и чужие импорты в нём не
                    разрешатся: рядом с вставленным текстом нет каталога, где
                    их искать. Об этом и говорит ошибка, когда до неё доходит.

                    Своей кнопки у вкладки больше нет. Раньше здесь стояла
                    «Apply contract», и стояла вынужденно: узнать, какие типы
                    объявляет контракт, можно было единственным способом —
                    применить его. Теперь это говорит разбор (`check_proto_text`),
                    и применяет контракт общий Save, как и на двух соседних
                    вкладках. */}
                <TabsContent value="paste" className={TAB_PANEL_CLASS}>
                  <ProtoEditor
                    value={draft.text}
                    onChange={(text) => setDraft((current) => ({ ...current, text }))}
                    disabled={busy}
                    placeholder={PASTE_PLACEHOLDER}
                  />
                  <DraftStatus
                    error={draftError}
                    checking={checking}
                    preview={preview}
                    saved={draftMatchesSchema('paste', draft, schema)}
                  />
                </TabsContent>

                {/* --- Файлы с диска --------------------------------------- */}
                <TabsContent value="files" className={TAB_PANEL_CLASS}>
                  <div className="flex items-center justify-between">
                    <Label className="font-mono text-sm text-soft">Proto Files</Label>
                    {/* Кнопок «перечитать» здесь больше нет, и они больше не
                        нужны: файлы читаются с диска в момент Save, то есть
                        сохранение И ЕСТЬ перечитывание. Раньше набор лежал
                        копией в каталоге топика с самого момента выбора, и
                        поправленный на диске контракт приходилось втягивать
                        заново отдельной кнопкой. */}
                  </div>

                  {picked.length > 0 ? (
                    <div className="max-h-40 overflow-auto rounded border border-edge divide-y divide-edge">
                      {picked.map((file) => (
                        <div key={file.name} className="flex items-center gap-2 p-2">
                          <FileText
                            className={`size-3.5 shrink-0 ${file.auto ? 'text-dim/60' : 'text-dim'}`}
                          />
                          <div className="min-w-0 flex-1">
                            <div className="flex items-center gap-1.5 min-w-0">
                              <div
                                className={`font-mono text-xs truncate ${file.auto ? 'text-soft' : 'text-strong'}`}
                              >
                                {file.name}
                              </div>
                              {/* Файл не выбирали — его нашли по import.
                                  Сказать об этом надо: иначе в списке появляются
                                  файлы, которых пользователь туда не клал, и
                                  непонятно, откуда они и почему без кнопок. */}
                              {file.auto && (
                                <span
                                  className="shrink-0 font-mono text-[10px] text-dim border border-edge rounded px-1"
                                  title="Found by an import in the selected files"
                                >
                                  import
                                </span>
                              )}
                            </div>
                            {/* Исходный путь: по нему работает «обновить», и
                                именно он отличает два одноимённых контракта. */}
                            <div className="font-mono text-[11px] text-dim truncate" title={file.source}>
                              {file.source}
                            </div>
                          </div>
                          {/* У зависимости кнопки нет: она держится импортом и
                              уйдёт вместе с ним, а сама по себе из набора не
                              убирается. */}
                          {!file.auto && (
                            <button
                              type="button"
                              onClick={() => handleDropFile(file.source)}
                              disabled={busy}
                              className="p-1 text-dim hover:text-strong disabled:opacity-50 bg-transparent border-none cursor-pointer shrink-0"
                              title="Drop this file from the set"
                            >
                              <X className="size-3.5" />
                            </button>
                          )}
                        </div>
                      ))}
                    </div>
                  ) : (
                    <div className="rounded border border-edge border-dashed p-3 text-center font-mono text-xs text-dim">
                      No .proto files picked
                    </div>
                  )}

                  <Button
                    onClick={handlePickFiles}
                    disabled={busy}
                    variant="outline"
                    className="w-full bg-transparent border-edge text-soft hover:bg-edge hover:text-strong font-mono"
                  >
                    <Upload className="size-4 mr-2" />
                    {busy ? 'Working…' : 'Pick Proto Files'}
                  </Button>
                  {/* Выбрать достаточно корневой файл: импорты доищутся рядом с
                      ним на диске. Сказать об этом стоит здесь — иначе человек
                      выбирает весь каталог разом. */}
                  <div className="font-mono text-[11px] text-dim break-words">
                    Pick the contract itself — files it imports are found next to it on disk.
                  </div>
                  <DraftStatus
                    error={draftError}
                    checking={checking}
                    preview={preview}
                    saved={draftMatchesSchema('files', draft, schema)}
                  />
                </TabsContent>

                {/* --- Библиотека ------------------------------------------ */}
                {/* То, ради чего она и заведена: вместо диалога файлов, ожидания
                    резолва импортов и выбора типа — один выбор контракта. Дверь
                    в саму библиотеку тоже здесь: кнопка в шапке приложения
                    убрана, потому что источники касаются одного формата из
                    шести, а шапка общая на всё. */}
                <TabsContent value="library" className={TAB_PANEL_CLASS}>
                  <div className="flex items-center justify-between gap-2">
                    <Label className="font-mono text-sm text-soft">Schema source</Label>
                    {bound && (
                      <button
                        type="button"
                        onClick={() => handlePickSet(null)}
                        disabled={busy}
                        className="font-mono text-xs text-dim hover:text-brand disabled:opacity-50 bg-transparent border-none cursor-pointer"
                        title="Clear the picked contract"
                      >
                        clear
                      </button>
                    )}
                  </div>
                  {hasLibrary ? (
                    <SchemaSetSelect
                      sources={sources}
                      value={boundIsLive === false ? null : bound}
                      onChange={handlePickSet}
                      disabled={busy}
                    />
                  ) : (
                    // Выбирать нечего, но сказать есть что: про саму возможность
                    // иначе неоткуда узнать, а стоит она одного действия.
                    <div className="rounded border border-edge border-dashed p-3 font-mono text-xs text-dim break-words">
                      No sources yet. Add a folder or a repository and its contracts become
                      selectable here for every topic, without copying .proto files topic by topic.
                    </div>
                  )}
                  {/* Настоящая кнопка, а не текстовая ссылка. Это не украшение:
                      у пустой библиотеки она единственное, что вообще можно
                      нажать, и выглядеть подписью она не имеет права. */}
                  <Button
                    onClick={() => setSourcesOpen(true)}
                    variant="outline"
                    className="w-full bg-transparent border-edge text-soft hover:bg-edge hover:text-strong font-mono"
                    title="Add, re-fetch or remove the folders and repositories that contracts come from"
                  >
                    <Library className="size-4 mr-2" />
                    Configure schema sources
                  </Button>
                  {/* Привязка есть, а набора под ней нет: источник убрали или он
                      перестал разбираться. Промолчать нельзя — топик в этом
                      состоянии не декодируется, а по виду формы непонятно
                      почему. */}
                  {bound && boundIsLive === false && (
                    <div className="font-mono text-xs text-brand break-words">
                      This topic is bound to {bound.entry}, but that contract is not in the library
                      anymore — fetch its source again in Schema sources, or pick another way to
                      give the topic its contract.
                    </div>
                  )}
                  {/* Сломанные наборы в списке не показываются — привязать к ним
                      нечего. Но и молчать о них нельзя: иначе список короче
                      библиотеки без всякого объяснения. */}
                  {brokenSets > 0 && (
                    <div className="font-mono text-[11px] text-dim break-words">
                      {brokenSets} more {brokenSets === 1 ? 'contract' : 'contracts'} in the library{' '}
                      {brokenSets === 1 ? 'does' : 'do'} not parse and {brokenSets === 1 ? 'is' : 'are'}{' '}
                      not listed here — open Schema sources to see why.
                    </div>
                  )}
                  <DraftStatus
                    error={draftError}
                    checking={checking}
                    preview={preview}
                    saved={draftMatchesSchema('library', draft, schema)}
                  />
                </TabsContent>
              </Tabs>

              {/* Сама библиотека — окном поверх этого. Стоит ВНЕ вкладок: Radix
                  размонтирует содержимое неактивной, а окно обязано пережить
                  всё, пока оно открыто. В разметке не занимает ничего — Radix
                  уносит содержимое в портал.

                  Открыта поверх, а не вместо: настройки могли быть уже
                  наполовину заполнены (формат переключён, тип выбран), и
                  закрывать их ради похода за источником значило бы терять этот
                  выбор. Об изменениях библиотека сообщает сюда — список
                  контрактов и схема топика перечитываются, не дожидаясь, пока
                  окно закроют. */}
              <SchemaSourcesModal
                open={sourcesOpen}
                onOpenChange={setSourcesOpen}
                onChanged={() => void handleSourcesChanged()}
              />

              {/* СОХРАНЁННАЯ схема на диске есть, но не разбирается — обычно
                  потому, что файлы из-под неё исчезли.
                  Одним блоком на все вкладки и БЕЗ оглядки на открытую: ошибка
                  относится к схеме ТОПИКА, а не к способу, которым её задали, и
                  прятать её на одной из вкладок значило бы прятать состояние
                  топика от того, кто пришёл его чинить. Заодно это последнее,
                  чем высота содержимого ниже вкладок от вкладки зависела.
                  С ошибкой ЧЕРНОВИКА она не путается: та стоит внутри вкладки,
                  под самим контрактом (`DraftStatus`). */}
              {schema?.error && (
                <div className="font-mono text-xs text-brand break-words">{schema.error}</div>
              )}

              <div className="space-y-2">
                <div className="flex items-center justify-between gap-2">
                  <Label className="font-mono text-sm text-soft">Message</Label>
                  {/* Подбор по телам — единственный способ ответить на вопрос
                      «каким типом это записано», когда топик голый protobuf: в
                      теле нет ни имени типа, ни id схемы, а имя топика о типе
                      ничего не обещает. */}
                  {/* Подбор работает по СОХРАНЁННОЙ схеме топика: декодер он
                      строит из неё, а не из черновика. Пока черновик от неё
                      отличается, кнопка выключена — иначе она ранжировала бы
                      типы прежнего контракта и выдавала бы ответ про него. */}
                  <button
                    type="button"
                    onClick={() => void handleGuess()}
                    disabled={guessing || busy || !isOpenTopic || !savedContract}
                    title={
                      !isOpenTopic
                        ? 'Open this topic first — the match runs on its own bodies'
                        : !savedContract
                          ? 'Save the contract first — the match runs on the saved one'
                          : 'Try every type of the saved .proto against the bodies already read'
                    }
                    className="font-mono text-xs text-dim hover:text-brand disabled:opacity-50 bg-transparent border-none cursor-pointer disabled:cursor-not-allowed"
                  >
                    {guessing ? 'matching…' : 'guess from bodies'}
                  </button>
                </div>

                {guess && <GuessResults guess={guess} chosen={message} onPick={setMessage} />}

                {/* Типы — из разбора ОТКРЫТОЙ вкладки. Раньше селектор стоял
                    живым на любой и предлагал типы из контракта, заданного на
                    ДРУГОЙ: брал он их из схемы топика, а не из того, что перед
                    глазами.

                    Почему выбирать не из чего — подписью В САМОМ селекторе, а не
                    строкой под ним: строка была единственным, чем высота этого
                    блока отличалась от вкладки к вкладке. */}
                <ProtoMessageSelect
                  messages={messages}
                  value={message}
                  onChange={setMessage}
                  disabled={busy || messages.length === 0}
                  emptyLabel={
                    filled
                      ? draftError
                        ? 'The contract does not parse'
                        : 'Reading the contract…'
                      : 'No contract on this tab yet'
                  }
                />
                {messages.length > 0 && !message && (
                  <div className="font-mono text-xs text-dim">
                    This contract declares {messages.length} message types — pick the one this
                    topic carries.
                  </div>
                )}
                {/* Выбран не корень набора. Перебивать выбор нельзя — читать
                    топик вложенным типом законно, — но и промолчать нельзя:
                    именно так в настройках оказался внутренний тип, а топик
                    сыпал ошибками декодирования, и понять почему было нечем.

                    Корень берётся из РАЗБОРА открытой вкладки, а не из схемы
                    топика. Это не придирка: вставив на соседней вкладке другой
                    контракт, человек сверял бы выбранный тип с корнем прежнего
                    — и получал бы совет перейти на тип, которого в том, что
                    перед ним, нет вовсе. */}
                {preview?.root && message && message !== preview.root && (
                  <div className="font-mono text-xs text-dim">
                    Nothing in this .proto contains{' '}
                    <button
                      type="button"
                      onClick={() => setMessage(preview.root!)}
                      disabled={busy}
                      className="font-mono text-xs text-brand hover:underline disabled:opacity-50 bg-transparent border-none p-0 cursor-pointer"
                    >
                      {preview.root}
                    </button>
                    , so that is the type this file was written around — topics are normally read
                    by it.
                  </div>
                )}
              </div>
            </div>
          )}

          {/* Avro specific fields */}
          {format === 'avro' && cluster && topicName && (
            <AvroSchema
              cluster={cluster}
              topic={topicName}
              avro={schema?.avro ?? null}
              busy={busy}
              onApply={(action, success) => void applySchema(action, success)}
            />
          )}

          {/* JSON Schema specific fields */}
          {format === 'jsonschema' && cluster && topicName && (
            <JsonSchemaSettings
              cluster={cluster}
              topic={topicName}
              json={schema?.json ?? null}
              busy={busy}
              onApply={(action, success) => void applySchema(action, success)}
            />
          )}

          {/* Реестр держит для топика protobuf-схему, а тянуть её оттуда
              приложение пока не умеет — только из локальных .proto. Промолчать
              значило бы оставить пользователя гадать, почему топик со схемой
              показывается текстом. */}
          {schema?.detected_kind === 'PROTOBUF' && format !== 'proto' && (
            <div className="rounded border border-edge bg-sunken p-3 font-mono text-xs text-dim">
              The registry holds a <span className="text-soft">PROTOBUF</span> schema for this
              topic. Loading schemas of that format from the registry is not supported yet —
              switch the format to <span className="text-soft">proto</span> and load the .proto
              file to decode these bodies.
            </div>
          )}
          </>
          )}

          {/* Action Buttons */}
          <div className="flex gap-3 pt-4">
            <Button
              onClick={() => onOpenChange(false)}
              variant="outline"
              className="flex-1 bg-transparent border-edge text-soft hover:bg-edge hover:text-strong font-mono"
            >
              Cancel
            </Button>
            {/* `disabled:pointer-events-auto` — ради подсказки: базовый класс
                кнопки глушит выключенной события, а вместе с ними и `title`.
                Нажатие от этого не появляется, выключенная кнопка событий
                click не отдаёт. */}
            {/* Единственная кнопка, которая что-то меняет. Пока схема не
                приехала, она выключена не для красоты: сохранять она будет то,
                что стоит в полях, а стоит там заготовка — `json` и пустой тип.
                Нажатие в эту секунду стёрло бы настройку топика вместо того,
                чтобы её показать.

                Причина, по которой она выключена, всегда названа в подсказке:
                выключенная кнопка без объяснения — худший из ответов, человек
                остаётся гадать, чего от него хотят. */}
            <Button
              onClick={handleSave}
              disabled={busy || !cluster || !ready || !!blocked}
              title={blocked ?? undefined}
              className="flex-1 bg-brand text-surface hover:bg-brand-hover font-mono disabled:pointer-events-auto disabled:cursor-not-allowed"
            >
              {busy ? 'Saving…' : 'Save'}
            </Button>
          </div>
        </div>
      </DialogContentNoClose>
    </Dialog>
  );
}
