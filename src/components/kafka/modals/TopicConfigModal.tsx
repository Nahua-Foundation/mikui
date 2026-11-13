import { useCallback, useEffect, useRef, useState } from 'react';
import { Button } from '../../ui/button';
import { ClipboardType, FileText, FolderOpen, Library, RotateCw, Upload, X } from 'lucide-react';
import { toast } from 'sonner';
import { open as openFileDialog } from '@tauri-apps/plugin-dialog';
import { Dialog, DialogHeader, DialogTitle } from '../../ui/dialog';
import { DialogContentNoClose } from '../DialogContentNoClose';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '../../ui/select';
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
import { BodyFormat, SchemaGuess, SourceBinding, SourceView, Topic, TopicSchema } from '../types';

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
  open: boolean;
  onOpenChange: (open: boolean) => void;
  /**
   * Схема топика изменилась на диске. Зовётся после КАЖДОЙ удавшейся правки, а
   * не только по Save: загрузка файла применяется сразу, и если этот топик
   * открыт, таблица обязана перерисоваться тут же, а не после закрытия окна.
   */
  onSchemaChanged?: (topic: string, schema: TopicSchema | null) => void;
}

/** Все форматы до единого — тот же список, что в шапке (`FormatSelect`).
 *  Формат, выбранный там, но отсутствующий здесь, оставлял бы этот селектор с
 *  пустым триггером: Radix показывает подпись выбранного пункта, а пункта нет. */
const FORMATS: { value: BodyFormat; label: string }[] = [
  { value: 'json', label: 'JSON' },
  { value: 'text', label: 'Text' },
  { value: 'proto', label: 'Proto' },
  { value: 'avro', label: 'Avro' },
  { value: 'jsonschema', label: 'JSON Schema' },
  { value: 'hex', label: 'Hex' },
];

/**
 * Какой тип message показать, когда схема приехала с бэкенда.
 *
 * Сохранённый выбор — как есть. Если его нет, а в загруженных .proto объявлен
 * ровно один тип, подставляется он: выбирать там не из чего, а без выбора тела
 * не декодируются вовсе. Когда типов несколько, угадать нельзя — поле остаётся
 * пустым, и тогда его требует `handleSave`.
 */
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
  if (schema?.source) return 'library';
  const files = schema?.files ?? [];
  if (files.some((file) => file.pasted)) return 'paste';
  if (files.length > 0) return 'files';
  return sources.some((source) => source.sets.some((set) => !set.error)) ? 'library' : 'paste';
}

/** Сколько ждать после последнего нажатия клавиши, прежде чем разбирать текст.
 *  Разбор идёт на бэкенде в отдельном потоке, но затевать его на каждый символ
 *  незачем: вставляют контракт целиком, а не набирают. */
const CHECK_DELAY_MS = 400;

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
  'min-w-0 font-mono text-xs rounded border border-transparent px-2 py-1.5 text-dim hover:text-soft ' +
  'data-[state=active]:bg-surface data-[state=active]:border-edge ' +
  'data-[state=active]:text-strong dark:data-[state=active]:text-strong';

/** Заготовка в пустом поле: из неё видно и что вставлять, и что ждут именно
 *  текст контракта, а не путь к нему. */
const PASTE_PLACEHOLDER = `syntax = "proto3";

package orders;

message OrderChanged {
  string id = 1;
  int64 at = 2;
}`;

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
  /** Поверх этой формы открыта сама библиотека. Окно источников живёт здесь, а
   *  не в шапке приложения: источники нужны ровно тогда, когда настраивают
   *  proto у топика, и других дверей к ним в приложении нет. */
  const [sourcesOpen, setSourcesOpen] = useState(false);
  /** Какой из трёх способов задать контракт сейчас открыт. */
  const [mode, setMode] = useState<ProtoMode>('paste');
  /** Текст контракта во вкладке «Paste text» — то, что в поле, а не то, что
   *  применено: применённое лежит в `schema.pasted_text`. */
  const [text, setText] = useState('');
  /** Чем плох текст в поле. null — разобрался либо ещё не проверяли. */
  const [textError, setTextError] = useState<string | null>(null);
  /** Проверка текста идёт прямо сейчас. */
  const [checking, setChecking] = useState(false);

  const topicName = topic?.name ?? null;

  /**
   * Перечитать библиотеку.
   *
   * Молча: без неё форма работает как работала — остаются свои файлы с диска, —
   * а красный тост поверх открывшихся настроек читался бы как поломка самих
   * настроек.
   */
  const reloadSources = useCallback(async () => {
    try {
      setSources(await api.listSchemaSources());
    } catch (e) {
      console.error('Failed to list schema sources', e);
    }
  }, []);

  // Состояние формы принадлежит паре кластер-топик, а модалка переиспользуется
  // для любого из них. Без перечитывания при открытии в ней остались бы файлы
  // и выбор от прошлого топика.
  useEffect(() => {
    if (!open || !cluster || !topicName) return;
    let cancelled = false;

    // Подбор относится к паре кластер-топик и к тому, что лежало в буфере в
    // момент нажатия: показывать его на следующем топике значило бы предлагать
    // ответ, посчитанный по чужим телам.
    setGuess(null);

    // Схема и библиотека читаются ВМЕСТЕ, а не двумя независимыми запросами:
    // от обеих зависит выбор открытой вкладки (`modeOf`), и реши его первый
    // пришедший ответ — у ненастроенного топика открывалось бы поле для текста
    // ровно до того мгновения, как приедет список из сорока контрактов.
    Promise.all([
      api.getTopicSchema(cluster, topicName),
      // Молча: без библиотеки форма работает как работала — остаются свои файлы
      // с диска и вставка текстом, — а красный тост поверх открывшихся настроек
      // читался бы как поломка самих настроек.
      api.listSchemaSources().catch((e): SourceView[] => {
        console.error('Failed to list schema sources', e);
        return [];
      }),
    ])
      .then(([loaded, library]) => {
        if (cancelled) return;
        setSchema(loaded);
        setSources(library);
        setFormat(loaded?.format ?? 'json');
        setMessage(chooseMessage(loaded));
        setMode(modeOf(loaded, library));
        // В поле — то, что применено. Пусто у топика, настроенного иначе: текст
        // от ПРОШЛОГО топика в нём был бы приглашением применить чужой контракт.
        setText(loaded?.pasted_text ?? '');
        setTextError(null);
        if (loaded?.error) {
          toast.error(`Schema of ${topicName} is broken: ${loaded.error}`);
        }
      })
      .catch((e) => {
        if (cancelled) return;
        console.error('Failed to load topic schema', e);
        toast.error(`Failed to load topic settings: ${describeError(e)}`);
      });

    return () => {
      cancelled = true;
    };
  }, [open, cluster, topicName]);

  /**
   * Разбор текста, пока он ещё в поле.
   *
   * Про ошибку в контракте человек должен узнать до того, как настройки топика
   * им заменены, — и до того, как он потянется к кнопке. Разбор тот же самый,
   * что и у применения (`check_proto_text` и `add_proto_text` зовут одно и то
   * же), поэтому ответ здесь не приближение, а именно то, что скажет Apply.
   *
   * Гонка ответов разрешается счётчиком: отпущенная клавиша отменяет ожидание,
   * но не уже улетевший запрос, и ответ на прежний текст обязан не перебить
   * ответ на нынешний.
   */
  const checkSeq = useRef(0);
  useEffect(() => {
    if (!open || mode !== 'paste') return;
    const trimmed = text.trim();
    if (!trimmed) {
      setTextError(null);
      setChecking(false);
      return;
    }
    // Применённый текст уже разобран — иначе он бы не применился. Проверять его
    // заново при каждом открытии окна значит ходить на бэкенд за ответом,
    // который и так известен.
    if (schema?.pasted_text === text) {
      setTextError(null);
      setChecking(false);
      return;
    }

    // Прежний ответ относился к прежнему тексту и с этого мгновения не значит
    // ничего. Оставить его на экране значило бы показывать ошибку в строке,
    // которую только что исправили, — и держать `Apply` выключенным из-за неё.
    setTextError(null);
    setChecking(true);
    const seq = ++checkSeq.current;
    const timer = setTimeout(() => {
      api
        .checkProtoText(text)
        .then(() => {
          if (seq === checkSeq.current) setTextError(null);
        })
        .catch((e) => {
          if (seq === checkSeq.current) setTextError(describeError(e));
        })
        .finally(() => {
          if (seq === checkSeq.current) setChecking(false);
        });
    }, CHECK_DELAY_MS);

    return () => clearTimeout(timer);
  }, [open, mode, text, schema?.pasted_text]);

  /**
   * Этому топику есть по чему подбирать: его тела и лежат в буфере воркера.
   *
   * Стоит здесь, до колбэков, а не рядом с разметкой: от него зависит
   * `handleBind` — подбор после привязки имеет смысл только у открытого топика.
   */
  const isOpenTopic = !!topicName && topicName === openTopic;

  /**
   * Общий хвост всех операций над файлами. Успех виден сразу: и в форме, и в
   * уже открытой таблице. Неудача не меняет ничего — на диске бэкенд её тоже
   * не применил.
   */
  const applySchema = useCallback(
    async (
      action: () => Promise<TopicSchema | null>,
      // Функцией, а не строкой, ради одного случая: загрузка .proto может
      // подтянуть файлы, которых пользователь не выбирал, и сказать об этом
      // можно только по результату.
      success: string | ((updated: TopicSchema | null) => string),
      // Что получилось — вызывающему: привязке набора надо знать, проставился
      // ли тип правилом корня, чтобы при отрицательном ответе добрать его
      // подбором по телам.
    ): Promise<{ ok: boolean; message: string | null }> => {
      if (!cluster || !topicName) return { ok: false, message: null };
      setBusy(true);
      try {
        const updated = await action();
        setSchema(updated);
        setFormat(updated?.format ?? 'json');
        // Файл только что загрузили — если тип в нём один, он и выбран.
        const chosen = chooseMessage(updated);
        setMessage(chosen);
        // В поле — ровно то, что применено. Любая операция здесь меняет ответ
        // на вопрос «чем декодировать», и если новый ответ — не вставленный
        // текст, то и поле обязано опустеть: текст в нём иначе выглядел бы
        // действующим контрактом, хотя топик читается уже не им.
        setText(updated?.pasted_text ?? '');
        setTextError(null);
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
   * Чего здесь намеренно НЕ делается — не трогается `format`. Его в этой форме
   * могли только что переключить и ещё не сохранить, и подставить сюда значение
   * с диска значило бы отменить несохранённый выбор в ответ на действие, к
   * формату не относящееся вовсе.
   */
  const handleSourcesChanged = useCallback(async () => {
    await reloadSources();
    if (!cluster || !topicName) return;
    try {
      const reloaded = await api.getTopicSchema(cluster, topicName);
      setSchema(reloaded);
      // С выбором типа так же, как с форматом: он может быть несохранённым, и
      // если он всё ещё есть среди объявленных, он и остаётся. `chooseMessage`
      // отвечает на это сам — надо только подсунуть ему не сохранённый выбор, а
      // тот, что стоит в форме.
      setMessage(chooseMessage(reloaded && { ...reloaded, message }));
      onSchemaChanged?.(topicName, reloaded);
    } catch (e) {
      console.error('Failed to re-read the topic schema', e);
    }
  }, [cluster, topicName, message, onSchemaChanged, reloadSources]);

  const handleLoadFiles = useCallback(async () => {
    if (!cluster || !topicName) return;
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

    await applySchema(
      () => api.addProtoFiles(cluster, topicName, paths),
      (updated) => {
        const loaded = `Loaded ${paths.length} .proto file${paths.length > 1 ? 's' : ''}`;
        // Подтянутое по импорту стоит назвать числом: пользователь выбрал один
        // файл, а в списке их стало шесть, и молчать об этом незачем.
        const pulled = (updated?.files ?? []).filter((f) => f.auto).length;
        return pulled > 0
          ? `${loaded}, ${pulled} more pulled in by imports`
          : loaded;
      },
    );
  }, [cluster, topicName, applySchema]);

  /**
   * Сделать контрактом топика то, что набрано в поле.
   *
   * Применяется сразу, как и загрузка файлов, а не по общей кнопке Save: Save
   * сохраняет ВЫБОР (формат и тип), а это — сама схема, и разбирается она на
   * бэкенде. Невалидный текст при этом не сохраняется вовсе: отказ приходит
   * оттуда же, откуда и предварительная проверка, и схема, которая работала,
   * остаётся работать.
   */
  const handleApplyText = useCallback(async () => {
    if (!cluster || !topicName) return;
    setBusy(true);
    try {
      const updated = await api.addProtoText(cluster, topicName, text);
      setSchema(updated);
      setFormat(updated.format);
      setMessage(chooseMessage(updated));
      setText(updated.pasted_text ?? text);
      setTextError(null);
      onSchemaChanged?.(topicName, updated);
      const count = updated.messages.length;
      toast.success(
        count === 1
          ? `Applied the pasted contract — ${updated.messages[0]}`
          : `Applied the pasted contract — ${count} message types`,
      );
    } catch (e) {
      // Своим путём, не через `applySchema`: тот сообщает об отказе тостом, а
      // тост уходит — текст же остаётся в поле, и править его будут здесь же.
      // Причина обязана остаться рядом с тем, к чему относится.
      console.error('Failed to apply the pasted contract', e);
      setTextError(describeError(e));
    } finally {
      setBusy(false);
    }
  }, [cluster, topicName, text, onSchemaChanged]);

  const handleRefresh = useCallback(
    (name?: string) => {
      if (!cluster || !topicName) return;
      void applySchema(
        () => api.refreshProtoFiles(cluster, topicName, name),
        name ? `Re-read ${name}` : 'Re-read all .proto files',
      );
    },
    [cluster, topicName, applySchema],
  );

  const handleRemove = useCallback(
    (name: string) => {
      if (!cluster || !topicName) return;
      void applySchema(() => api.removeProtoFile(cluster, topicName, name), `Removed ${name}`);
    },
    [cluster, topicName, applySchema],
  );

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
  const handleBind = useCallback(
    async (binding: SourceBinding | null) => {
      if (!cluster || !topicName) return;
      const applied = await applySchema(
        () => api.bindTopicSource(cluster, topicName, binding?.source ?? null, binding?.entry ?? null),
        binding ? `Bound to ${binding.entry}` : 'Unbound from the library',
      );

      // Корень набора тип уже проставил — больше спрашивать нечего, и ходить за
      // телами незачем. А вот когда корня НЕТ (их несколько или ни одного),
      // выбор остаётся висеть пустым, и единственный, кто может на него
      // ответить, — сами байты топика.
      //
      // Делается это сразу, а не ждёт нажатия: человек выбрал контракт затем,
      // чтобы топик читался, и отдельная кнопка между выбором и результатом —
      // это работа, которую приложение умеет сделать само. Тел оно при этом в
      // Kafka не запрашивает: подбор смотрит в буфер воркера, то есть в уже
      // прочитанное — на кластере с квотой подсказка не имеет права стоить
      // столько же, сколько сам топик. Оттуда же и условие `isOpenTopic`: у
      // закрытого топика буфера нет, и спрашивать не у чего.
      if (applied.ok && binding && !applied.message && isOpenTopic) {
        await runGuess(true);
      }
    },
    [cluster, topicName, applySchema, isOpenTopic, runGuess],
  );

  const handleSave = useCallback(async () => {
    if (!cluster || !topicName) return;
    setBusy(true);
    try {
      const saved = await api.saveTopicSchema(cluster, topicName, format, message);
      onSchemaChanged?.(topicName, saved);
      toast.success(`Settings saved for ${topicName}`);
      onOpenChange(false);
    } catch (e) {
      console.error('Failed to save topic settings', e);
      toast.error(describeError(e));
    } finally {
      setBusy(false);
    }
  }, [cluster, topicName, format, message, onSchemaChanged, onOpenChange]);

  if (!topic) return null;

  const files = schema?.files ?? [];
  const messages = schema?.messages ?? [];
  /**
   * Файлы, которые показывает вкладка «Local files».
   *
   * Вставленный текст из списка убран: файл он и есть файл, но строки «откуда
   * взяли» у него нет и быть не может, а кнопки ↻ и × относились бы к тому,
   * чего на диске не существует. Видно его во вкладке «Paste text», и видно
   * тем, чем он является, — текстом.
   */
  const picked = files.filter((file) => !file.pasted);

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
  const bound = schema?.source ?? null;
  /**
   * В библиотеке есть на что отвечать выпадающему списку.
   *
   * Сломанные наборы в счёт идут: выбрать их нельзя, но список при них нужен —
   * рядом с ним стоит объяснение, сколько их и почему их не видно, и показывать
   * это объяснение под надписью «источников нет» было бы противоречием.
   */
  const hasLibrary = usableSets > 0 || !!bound || brokenSets > 0;
  /** Привязка указывает на набор, который в библиотеке ЕСТЬ и разбирается. */
  const boundIsLive =
    !!bound &&
    sources.some(
      (source) =>
        source.id === bound.source &&
        source.sets.some((set) => set.entry === bound.entry && !set.error),
    );

  /**
   * Типов объявлено несколько, а выбор не сделан.
   *
   * Сохранять такую настройку незачем: без типа protobuf-тело не разобрать —
   * в .proto нет ничего, что сказало бы, каким из объявленных сообщений
   * записан топик. Раньше это сохранялось молча, и топик потом показывался
   * текстом без внятной причины.
   *
   * Про «несколько» — потому что единственный тип подставляет `chooseMessage`,
   * и пустым поле в этом случае не остаётся. Пустой список .proto тоже не
   * повод запрещать: файлы можно добавить и позже, а формат назначить сейчас.
   */
  const needsMessage = format === 'proto' && messages.length > 1 && !message;

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContentNoClose
        className="max-w-md bg-surface border-edge text-strong"
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
          {/* Message Type */}
          <div className="space-y-2">
            <Label className="font-mono text-sm text-soft">Message Type</Label>
            <Select value={format} onValueChange={(v) => setFormat(v as BodyFormat)}>
              <SelectTrigger className="bg-surface border-edge text-strong font-mono">
                <SelectValue />
              </SelectTrigger>
              <SelectContent className="bg-surface border-edge">
                {FORMATS.map(({ value, label }) => (
                  <SelectItem
                    key={value}
                    value={value}
                    className="text-strong font-mono focus:bg-edge"
                  >
                    {label}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>

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
                    вики или его только что написали — вставить и применить.
                    Файл при этом ровно один, и чужие импорты в нём не
                    разрешатся: рядом с вставленным текстом нет каталога, где
                    их искать. Об этом и говорит ошибка, когда до неё доходит. */}
                <TabsContent value="paste" className="space-y-2 pt-2">
                  <ProtoEditor
                    value={text}
                    onChange={setText}
                    disabled={busy}
                    placeholder={PASTE_PLACEHOLDER}
                  />
                  {/* Разбор — он же проверка: другого способа убедиться, что
                      текст является контрактом, не существует, а этот ещё и
                      тот же самый, каким контракт будет применён. Ошибка
                      остаётся под полем, пока её не исправят. */}
                  {textError ? (
                    <div className="font-mono text-xs text-danger break-words whitespace-pre-wrap">
                      {textError}
                    </div>
                  ) : checking ? (
                    <div className="font-mono text-xs text-dim">checking…</div>
                  ) : text && text === schema?.pasted_text ? (
                    <div className="font-mono text-xs text-dim">
                      This is the contract the topic is read by.
                    </div>
                  ) : null}
                  <Button
                    onClick={() => void handleApplyText()}
                    disabled={busy || !text.trim() || !!textError}
                    className="w-full bg-brand text-surface hover:bg-brand-hover font-mono disabled:pointer-events-auto disabled:cursor-not-allowed"
                    title={
                      textError
                        ? 'Fix the contract first — it does not parse'
                        : 'Make this text the contract of the topic'
                    }
                  >
                    {busy ? 'Working…' : 'Apply contract'}
                  </Button>
                </TabsContent>

                {/* --- Файлы с диска --------------------------------------- */}
                <TabsContent value="files" className="space-y-2 pt-2">
                  <div className="flex items-center justify-between">
                    <Label className="font-mono text-sm text-soft">Proto Files</Label>
                    {picked.length > 0 && (
                      <button
                        type="button"
                        onClick={() => handleRefresh()}
                        disabled={busy}
                        className="font-mono text-xs text-dim hover:text-brand disabled:opacity-50 bg-transparent border-none cursor-pointer"
                        title="Re-read every file from disk"
                      >
                        refresh all
                      </button>
                    )}
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
                          {/* У зависимости кнопок нет. Перечитывать её отдельно
                              незачем — она читается с диска заново при каждом
                              изменении набора, — а удалить её нельзя: она
                              держится импортом и уйдёт вместе с ним. */}
                          {!file.auto && (
                            <>
                              <button
                                type="button"
                                onClick={() => handleRefresh(file.name)}
                                disabled={busy}
                                className="p-1 text-dim hover:text-brand disabled:opacity-50 bg-transparent border-none cursor-pointer shrink-0"
                                title="Re-read this file from disk"
                              >
                                <RotateCw className="size-3.5" />
                              </button>
                              <button
                                type="button"
                                onClick={() => handleRemove(file.name)}
                                disabled={busy}
                                className="p-1 text-dim hover:text-strong disabled:opacity-50 bg-transparent border-none cursor-pointer shrink-0"
                                title="Remove this file"
                              >
                                <X className="size-3.5" />
                              </button>
                            </>
                          )}
                        </div>
                      ))}
                    </div>
                  ) : (
                    <div className="rounded border border-edge border-dashed p-3 text-center font-mono text-xs text-dim">
                      No .proto files loaded
                    </div>
                  )}

                  <Button
                    onClick={handleLoadFiles}
                    disabled={busy}
                    variant="outline"
                    className="w-full bg-transparent border-edge text-soft hover:bg-edge hover:text-strong font-mono"
                  >
                    <Upload className="size-4 mr-2" />
                    {busy ? 'Working…' : 'Load Proto Files'}
                  </Button>
                  {/* Выбрать достаточно корневой файл: импорты доищутся рядом с
                      ним на диске. Сказать об этом стоит здесь — иначе человек
                      выбирает весь каталог разом. */}
                  <div className="font-mono text-[11px] text-dim break-words">
                    Pick the contract itself — files it imports are found next to it on disk.
                  </div>
                </TabsContent>

                {/* --- Библиотека ------------------------------------------ */}
                {/* То, ради чего она и заведена: вместо диалога файлов, ожидания
                    резолва импортов и выбора типа — один выбор контракта. Дверь
                    в саму библиотеку тоже здесь: кнопка в шапке приложения
                    убрана, потому что источники касаются одного формата из
                    шести, а шапка общая на всё. */}
                <TabsContent value="library" className="space-y-2 pt-2">
                  <div className="flex items-center justify-between gap-2">
                    <Label className="font-mono text-sm text-soft">Schema source</Label>
                    {bound && (
                      <button
                        type="button"
                        onClick={() => void handleBind(null)}
                        disabled={busy}
                        className="font-mono text-xs text-dim hover:text-brand disabled:opacity-50 bg-transparent border-none cursor-pointer"
                        title="Stop reading this topic by a contract from the library"
                      >
                        unbind
                      </button>
                    )}
                  </div>
                  {hasLibrary ? (
                    <SchemaSetSelect
                      sources={sources}
                      value={boundIsLive ? bound : null}
                      onChange={(binding) => void handleBind(binding)}
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
                  {bound && !boundIsLive && (
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

              {/* Схема на диске есть, но не разбирается.
                  Одним блоком на все вкладки, а не внутри каждой: ошибка
                  относится к схеме ТОПИКА, а не к способу, которым её задали, —
                  и у вставленного текста её не бывает вовсе, он бы не
                  применился. */}
              {mode !== 'paste' && schema?.error && (
                <div className="font-mono text-xs text-brand break-words">{schema.error}</div>
              )}

              <div className="space-y-2">
                <div className="flex items-center justify-between gap-2">
                  <Label className="font-mono text-sm text-soft">Message</Label>
                  {/* Подбор по телам — единственный способ ответить на вопрос
                      «каким типом это записано», когда топик голый protobuf: в
                      теле нет ни имени типа, ни id схемы, а имя топика о типе
                      ничего не обещает. */}
                  <button
                    type="button"
                    onClick={() => void handleGuess()}
                    disabled={guessing || busy || !isOpenTopic}
                    title={
                      isOpenTopic
                        ? 'Try every loaded .proto type against the bodies already read'
                        : 'Open this topic first — the match runs on its own bodies'
                    }
                    className="font-mono text-xs text-dim hover:text-brand disabled:opacity-50 bg-transparent border-none cursor-pointer disabled:cursor-not-allowed"
                  >
                    {guessing ? 'matching…' : 'guess from bodies'}
                  </button>
                </div>

                {guess && <GuessResults guess={guess} chosen={message} onPick={setMessage} />}

                <ProtoMessageSelect
                  messages={messages}
                  value={message}
                  onChange={setMessage}
                  disabled={busy}
                />
                {/* Незаполненное обязательное поле, а не совет: без типа
                    декодировать нечем, и Save заблокирован. Сказать об этом
                    здесь, у самого поля, дешевле, чем дать гадать, почему
                    кнопка внизу не нажимается. */}
                {needsMessage && (
                  <div className="font-mono text-xs text-dim">
                    The loaded files declare {messages.length} messages — pick the one this topic
                    carries. Saving is blocked until you do.
                  </div>
                )}
                {/* Выбран не корень набора. Перебивать выбор нельзя — читать
                    топик вложенным типом законно, — но и промолчать нельзя:
                    именно так в настройках оказался внутренний тип, а топик
                    сыпал ошибками декодирования, и понять почему было нечем. */}
                {schema?.root && message && message !== schema.root && (
                  <div className="font-mono text-xs text-dim">
                    Nothing in this .proto contains{' '}
                    <button
                      type="button"
                      onClick={() => setMessage(schema.root)}
                      disabled={busy}
                      className="font-mono text-xs text-brand hover:underline disabled:opacity-50 bg-transparent border-none p-0 cursor-pointer"
                    >
                      {schema.root}
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
            <Button
              onClick={handleSave}
              disabled={busy || !cluster || needsMessage}
              title={needsMessage ? 'Pick the message type this topic carries first' : undefined}
              className="flex-1 bg-brand text-surface hover:bg-brand-hover font-mono disabled:pointer-events-auto disabled:cursor-not-allowed"
            >
              Save
            </Button>
          </div>
        </div>
      </DialogContentNoClose>
    </Dialog>
  );
}
