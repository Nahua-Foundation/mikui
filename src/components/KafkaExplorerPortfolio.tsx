import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { toast } from 'sonner';
import {
  BodyFormat,
  LensSetting,
  Topic,
  ClusterConnectPayload,
  ClusterUser,
  FavoriteInfo,
  FavoritesView,
  FavoriteTopics,
  FullMessage,
  MessageFilter,
  MessageLinkTarget,
  KafkaCluster,
  OpenTopicParams,
  OpenTopicResult,
  ReadMode,
  ReadRange,
  SavedMessage,
  SortSpec,
  TopicSchema,
  EMPTY_FAVORITES,
  EMPTY_FILTER,
  EMPTY_RANGE,
  clusterKey,
  hasQuery,
} from './kafka';
import * as api from './kafka/api';
import { HeaderDesktop } from './kafka';
import { TopicsPanel } from './kafka';
import { MessagesPanel } from './kafka';
import { StatusBar } from './kafka/StatusBar';
import { MessageDetailsModal } from './kafka';
import { TopicConfigModal } from './kafka';
import { TopicInfoModal } from './kafka';
import { ClusterConfigModal } from './kafka';
import { ClusterArchiveModal } from './kafka/modals/ClusterArchiveModal';
import { ClusterUsersModal } from './kafka/modals/ClusterUsersModal';
import { FavoritesModal } from './kafka';
import { SavedMessageModal } from './kafka';
import { ShareLinkModal } from './kafka';
import { OpenLinkModal } from './kafka';
import { ProduceMessageModal } from './kafka/modals/ProduceMessageModal';
import { useMessageWindow } from './kafka/useMessageWindow';
import { invoke } from '@tauri-apps/api/core';
import { listen } from '@tauri-apps/api/event';

type ClusterConfigMode = 'create' | 'edit';

/** Сколько сообщений вычитывать в буфер Rust на каждую партицию при открытии
 *  топика (и на сколько ещё дочитывать по кнопке «Загрузить ещё»). */
const DEFAULT_PARTITION_LIMIT = 1000;
/** Пауза между опросами хода загрузки, пока `open_topic` ещё не ответил. */
const PROGRESS_POLL_MS = 250;
/** Пауза перед отправкой фильтра. Фильтрация идёт по буферу в памяти и стоит
 *  единицы миллисекунд, но дёргать её на каждый символ всё равно незачем. */
const FILTER_DEBOUNCE_MS = 200;
/** Бэкенд отвечает так на чтение, отменённое сменой топика — см. worker.rs. */
const READ_SUPERSEDED = 'read superseded';
/** А так — на глубокий поиск, остановленный кнопкой. Отдельно от предыдущего:
 *  там чтение стало не нужно, здесь его отменили, и топик после этого
 *  перечитывается — см. `handleStopSearch`. */
const SEARCH_STOPPED = 'search stopped';

/** Не чаще одного тоста об ошибке декодирования за это время. Сообщения чужого
 *  формата идут полосой, и без паузы каждое окно выдачи заливало бы экран. */
const DECODE_ERROR_TOAST_MS = 5000;

/**
 * Сколько соседей вычитывать вокруг сообщения, открытого по ссылке.
 *
 * Не ноль: пришедший по ссылке почти всегда смотрит и на то, что было рядом, —
 * а перечитывать ради этого топик заново значило бы платить квотой за то, что
 * можно было взять сразу. И не больше: окно на одну партицию, брокер всё равно
 * присылает целый батч, так что сотня офсетов стоит примерно столько же,
 * сколько один.
 */
const LINK_CONTEXT = 50;

/** Звонок в дверь из бэкенда: система принесла ссылку — см. lib.rs. */
const LINK_EVENT = 'mikui://link';

/** Воркер упал и поднят заново: подключения больше нет — см. worker.rs. */
const WORKER_RESTARTED = 'mikui://worker-restarted';

const isSuperseded = (e: unknown) => String(e).includes(READ_SUPERSEDED);
const isStopped = (e: unknown) => String(e).includes(SEARCH_STOPPED);

const describeError = api.describeError;

export function KafkaExplorerPortfolio() {
  const [selectedTopic, setSelectedTopic] = useState<Topic | null>(null);
  /** Из каких партиций читаем. null — из всех. */
  const [selectedPartitions, setSelectedPartitions] = useState<number[] | null>(null);
  /** Что выбрано в селекторе порядка чтения: с какого конца читать топик либо
   *  какой его кусок. Определяет и порядок строк в таблице, и направление, в
   *  котором догружаются следующие порции. */
  const [readMode, setReadMode] = useState<ReadMode>('newest');
  /** Границы чтения для режимов `offset` и `timestamp`. */
  const [range, setRange] = useState<ReadRange>(EMPTY_RANGE);
  const [selectedMessage, setSelectedMessage] = useState<FullMessage | null>(null);
  /** Позиция открытого сообщения в таблице — по ней стрелки находят соседей. */
  const [selectedIndex, setSelectedIndex] = useState<number | null>(null);
  /** Открытое сообщение из архива. Отдельно от таблицы, а не флагом поверх неё:
   *  у него своё окно, свой формат тела и своё происхождение — топик, который
   *  сейчас может быть и не открыт. */
  const [savedMessage, setSavedMessage] = useState<SavedMessage | null>(null);
  const [savedFavorite, setSavedFavorite] = useState<FavoriteInfo | null>(null);
  const [isSavedModalOpen, setIsSavedModalOpen] = useState(false);
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [configTopic, setConfigTopic] = useState<Topic | null>(null);
  const [isConfigModalOpen, setIsConfigModalOpen] = useState(false);
  /** Топик, устройство которого показывает окно информации. Не обязательно
   *  открытый: посмотреть настройки соседнего — обычное дело, и читать его
   *  ради этого незачем. */
  const [infoTopic, setInfoTopic] = useState<Topic | null>(null);
  const [isTopicInfoOpen, setIsTopicInfoOpen] = useState(false);
  const [isClusterConfigModalOpen, setIsClusterConfigModalOpen] = useState(false);
  const [isClusterArchiveModalOpen, setIsClusterArchiveModalOpen] = useState(false);
  const [isClusterUsersModalOpen, setIsClusterUsersModalOpen] = useState(false);
  const [isFavoritesModalOpen, setIsFavoritesModalOpen] = useState(false);
  const [isProduceModalOpen, setIsProduceModalOpen] = useState(false);
  /** Собранная ссылка на открытое сообщение. null — окном не делились. */
  const [shareLink, setShareLink] = useState<string | null>(null);
  const [isShareOpen, setIsShareOpen] = useState(false);
  const [isOpenLinkOpen, setIsOpenLinkOpen] = useState(false);
  /** Ссылка, принесённая системой. null — окно открыли пунктом в шапке. */
  const [incomingLink, setIncomingLink] = useState<string | null>(null);
  /**
   * Сообщение, ради которого топик открыли по ссылке, — таблица его подсвечивает.
   *
   * Координатами, а не индексом: индекс живёт до ближайшей смены фильтра или
   * сортировки, а подсветка переживать их обязана. Гаснет, когда человек сам
   * доводит до строки курсор, и когда уходит с топика.
   */
  const [linkedMessage, setLinkedMessage] = useState<{
    partition: number;
    offset: number;
  } | null>(null);
  const [filters, setFilters] = useState<MessageFilter>(EMPTY_FILTER);
  /** Сортировка по клику на заголовок колонки. `null` — обычный порядок
   *  чтения из `readMode`. */
  const [sort, setSort] = useState<SortSpec | null>(null);
  /** Архив сохранённых сообщений вместе с занятым местом. Живёт на диске;
   *  здесь только последний отданный бэкендом снимок. */
  const [favorites, setFavorites] = useState<FavoritesView>(EMPTY_FAVORITES);
  /** Избранные топики ВСЕХ кластеров — так они и лежат в settings.json.
   *  Панели уходит только набор текущего. */
  const [favoriteTopics, setFavoriteTopics] = useState<FavoriteTopics>({});
  /** Путь к журналу паник, если приложение когда-нибудь падало. */
  const [crashLog, setCrashLog] = useState<string | null>(null);
  const [topics, setTopics] = useState<Topic[]>([]);

  const [total, setTotal] = useState(0);
  const [stats, setStats] = useState<OpenTopicResult | null>(null);
  const [isLoadingMessages, setIsLoadingMessages] = useState(false);
  const [isLoadingMore, setIsLoadingMore] = useState(false);
  /** Идёт глубокий поиск: топик читается до конца, в буфер попадают только
   *  совпадения. Отдельно от `isLoadingMessages` — у этих двух разный конец
   *  (у чтения свой лимит, у поиска только топик, буфер или кнопка «стоп») и
   *  разный вид в таблице. */
  const [isSearching, setIsSearching] = useState(false);
  /**
   * Счётчик перечитываний открытого топика теми же параметрами.
   *
   * Нужен ровно одному случаю: остановке глубокого поиска. Она оставляет буфер
   * пустым (в нём лежали одни находки), и топик надо открыть заново — теми же
   * топиком, партициями и границами, то есть без единого изменения в
   * зависимостях эффекта открытия. Без этого счётчика эффект бы не перезапустился.
   */
  const [readNonce, setReadNonce] = useState(0);
  /**
   * В буфере лежит добыча глубокого поиска, а не обычное чтение.
   *
   * Это состояние приходится знать снаружи, потому что буфер в нём НЕ
   * представляет топик: непопавшее под фильтр в него не кладут. Отсюда два
   * следствия, и оба важны.
   *
   * Первое: смена фильтра по такому буферу бессмысленна — расширенный запрос
   * искал бы в том, что уже просеяно прошлым, и молча не нашёл бы ничего
   * нового. Поэтому она перечитывает топик, а не пересевает буфер.
   *
   * Второе: «Load more» дописал бы в тот же буфер НЕпросеянное, и «loaded»
   * стало бы смесью двух разных величин. Поэтому не предлагается.
   */
  const [searchedBuffer, setSearchedBuffer] = useState(false);

  // Растёт, когда меняется САМ СПИСОК: другой топик, партиция, направление
  // чтения, фильтр. Сбрасывает кэш окон и обесценивает ответы на устаревшие
  // запросы. Фоновая догрузка сюда не относится — она только дописывает строки
  // в конец, и сбрасывать из-за неё кэш нельзя (см. useMessageWindow).
  const [generation, setGeneration] = useState(0);

  /** Схема ОТКРЫТОГО топика. Нужна модалке, чтобы знать, в каком виде приехало
   *  тело; всем остальным заведует бэкенд. */
  const [openSchema, setOpenSchema] = useState<TopicSchema | null>(null);

  /**
   * Чем открывать топик — ОДИН набор на обычное чтение и на глубокий поиск.
   *
   * Поиск читает тот же топик, с того же конца и в тех же границах, что и
   * таблица под ним; разъедься эти два набора — и он искал бы не в том, что
   * показано. Поэтому набор один, а не два похожих в двух местах.
   */
  const readParams = useCallback(
    (topic: Topic): OpenTopicParams => ({
      topic: topic.name,
      // Границы задают направление сами (см. `ReadRange::newest_first`),
      // и когда они есть, это поле бэкенду не указ.
      start_from: readMode === 'newest' ? 'newest' : 'oldest',
      limit: DEFAULT_PARTITION_LIMIT,
      partitions: selectedPartitions,
      filter: filters,
      range,
      sort,
    }),
    [readMode, selectedPartitions, filters, range, sort],
  );
  // Через ref: эффект открытия топика намеренно НЕ зависит от фильтра (его
  // применяет `set_filter`, без похода в Kafka), а `readParams` от фильтра
  // зависит. В зависимостях эффекта он перечитывал бы топик на каждый символ.
  const readParamsRef = useRef(readParams);
  readParamsRef.current = readParams;

  const lastDecodeToast = useRef(0);
  const reportDecodeError = useCallback((error: string) => {
    const now = Date.now();
    if (now - lastDecodeToast.current < DECODE_ERROR_TOAST_MS) return;
    lastDecodeToast.current = now;
    // Схему не сбрасываем: одно сообщение чужого формата — обычное дело в
    // топике, переживавшем смену контракта, и терять из-за него схему нельзя.
    toast.error(`Some messages don't match the schema: ${error}`);
  }, []);

  const { ensureRange, getRow, version } = useMessageWindow(generation, total, reportDecodeError);

  // Какой топик открыт ПРЯМО СЕЙЧАС — чтобы ответ на давно улетевший запрос
  // не приехал в чужую таблицу. Ref, а не state: нужно значение на момент
  // колбэка, а не на момент создания замыкания.
  const topicRef = useRef<string | null>(null);
  topicRef.current = selectedTopic?.name ?? null;

  /**
   * Переход по ссылке, который ещё не закончился.
   *
   * Ссылка называет партицию и офсет, а таблица работает по индексу строки —
   * значит показать сообщение можно только ПОСЛЕ того, как топик прочитан.
   * Держать это состоянием нельзя: открытие топика живёт в эффекте, и любая
   * лишняя зависимость перечитывала бы топик заново. Ref переживает рендеры и
   * читается ровно там, где чтение закончилось.
   */
  const pendingLinkRef = useRef<{
    topic: string;
    partition: number;
    offset: number;
    format: string | null;
    typeName: string | null;
  } | null>(null);

  const [clusters, setClusters] = useState<KafkaCluster[]>([]);
  const [selectedClusterId, setSelectedClusterId] = useState<string | null>(null);
  const [connectedClusterId, setConnectedClusterId] = useState<string | null>(null);
  /** Учётка, под которой держится текущее подключение. */
  const [connectedUserId, setConnectedUserId] = useState<string | null>(null);
  /**
   * Как называется то, к чему подключены. Отдельно от `connectedClusterId`
   * потому, что подключиться можно и из формы, не сохраняя кластер, — а шапка
   * обязана показывать имя и в этом случае.
   */
  const [connectedName, setConnectedName] = useState<string | null>(null);
  /** Идёт рукопожатие с кластером. Без этого признака переключение учётки
   *  выглядит как «ничего не произошло»: старая шапка и старый список топиков
   *  стоят на месте, пока бэкенд ходит в сеть. */
  const [isConnecting, setIsConnecting] = useState(false);
  const [clusterConfigMode, setClusterConfigMode] = useState<ClusterConfigMode>('create');
  const [usersModalClusterId, setUsersModalClusterId] = useState<string | null>(null);
  const [usersModalFromConfig, setUsersModalFromConfig] = useState(false);

  // Единственный источник правды по кластерам — список `clusters`: и шапка, и
  // модалка пользователей смотрят в него по id. Держать рядом ещё и копию
  // объекта означало бы, что добавленный пользователь виден в одном месте и
  // не виден в другом.
  const connectedCluster = clusters.find((c) => c.id === connectedClusterId) ?? null;
  const usersModalCluster = clusters.find((c) => c.id === usersModalClusterId) ?? null;
  // Тоже по id, а не снимком: из настроек кластера можно уйти в Manage users,
  // завести там пользователя и вернуться — снимок этого бы не показал.
  const selectedCluster = clusters.find((c) => c.id === selectedClusterId) ?? null;

  // Сохранённые подключения читаются с диска при старте. Раньше список жил
  // только в React state, поэтому добавленный кластер исчезал при перезапуске.
  useEffect(() => {
    api
      .listClusters()
      .then(setClusters)
      .catch((e) => {
        console.error('Failed to load clusters', e);
        toast.error(`Failed to load saved clusters: ${e}`);
      });
  }, []);

  /**
   * Перечитывает архив с диска.
   *
   * Зовётся не только при старте, но и на каждое открытие списка: сохранённое
   * тело лежит сырым, и его превью зависит от схемы топика, которую могли
   * привязать или снять уже после сохранения. Стоит это чтения килобайтного
   * индекса — пересчитывает бэкенд только то, что действительно разъехалось.
   */
  const loadFavorites = useCallback(() => {
    api
      .listFavorites()
      .then(setFavorites)
      .catch((e) => {
        console.error('Failed to load saved messages', e);
        toast.error(`Failed to load saved messages: ${describeError(e)}`);
      });
  }, []);

  // При старте — с диска: в этом весь смысл архива. Подключение для этого не
  // нужно, сообщения лежат у нас.
  useEffect(loadFavorites, [loadFavorites]);

  // Избранные топики — тоже с диска и тоже при старте: отметка, живущая до
  // закрытия окна, не стоила бы того, чтобы её ставить.
  useEffect(() => {
    api
      .loadFavoriteTopics()
      .then(setFavoriteTopics)
      .catch((e) => console.error('Failed to load favorite topics', e));
  }, []);

  // Падал ли бэкенд когда-нибудь. Спрашивается один раз при старте и НЕ
  // перечитывается: свежая паника и так приезжает своим сообщением, а вот
  // записанная в прошлый запуск иначе осталась бы незамеченной — именно её
  // обычно и просят прислать.
  useEffect(() => {
    api
      .panicLog()
      .then(setCrashLog)
      .catch((e) => console.error('Failed to check the panic log', e));
  }, []);


  const partitionsKey = selectedPartitions ? selectedPartitions.join(',') : 'all';
  const rangeKey = `${readMode}:${range.from_offset}:${range.to_offset}:${range.from_timestamp}:${range.to_timestamp}`;

  /** Под каким ключом искать схемы топиков этого подключения. Им же
   *  ключуется избранное: вопрос «тот ли это топик» у них общий. */
  const schemaCluster = clusterKey(connectedClusterId, connectedName);

  /** Избранное текущего подключения — только имена, панели больше не нужно. */
  const favoriteTopicNames = useMemo(
    () => new Set(schemaCluster ? (favoriteTopics[schemaCluster] ?? []) : []),
    [favoriteTopics, schemaCluster],
  );

  // Через ref, чтобы обработчик не пересоздавался на каждую поставленную
  // звезду: он висит на каждой строке списка, а строк тысячи.
  const favoriteTopicsRef = useRef(favoriteTopics);
  favoriteTopicsRef.current = favoriteTopics;

  /**
   * Поставить или снять звезду. Диск — сразу: отметка ставится одним кликом
   * между делом, и подтверждать её было бы не к месту, а терять при выходе
   * тем более.
   */
  const handleToggleFavoriteTopic = useCallback(
    (topic: Topic) => {
      // Ключа нет только без подключения, а тогда нет и списка топиков.
      if (!schemaCluster) return;
      const current = favoriteTopicsRef.current[schemaCluster] ?? [];
      const next = current.includes(topic.name)
        ? current.filter((name) => name !== topic.name)
        : [...current, topic.name];
      const updated = { ...favoriteTopicsRef.current, [schemaCluster]: next };
      // Пустой список — это отсутствие отметок, а не отметка «ничего»: иначе
      // файл копил бы по записи на каждый кластер, где звезду поставили и тут
      // же сняли.
      if (next.length === 0) delete updated[schemaCluster];

      favoriteTopicsRef.current = updated;
      setFavoriteTopics(updated);
      api.saveFavoriteTopics(updated).catch((e) => {
        console.error('Failed to save favorite topics', e);
        toast.error(`Failed to save favorites: ${describeError(e)}`);
      });
    },
    [schemaCluster],
  );

  // Открытие топика: вычитка в буфер Rust. Наружу приезжают только счётчики,
  // сами строки подтягиваются окнами по мере прокрутки.
  useEffect(() => {
    if (!selectedTopic) {
      setTotal(0);
      setStats(null);
      setOpenSchema(null);
      invoke('close_topic').catch(() => {});
      return;
    }

    let cancelled = false;
    // Схема, которой топик в итоге открылся. Нужна переходу по ссылке: он
    // сравнивает её с форматом отправителя (см. `finishPendingLink`), а
    // `openSchema` к тому моменту в этом замыкании ещё прежний.
    let applied: TopicSchema | null = null;
    setIsLoadingMessages(true);
    // Обычное чтение кладёт в буфер всё — то есть снимает признак «в буфере
    // одни находки», чем бы он ни был поставлен. В том числе и после
    // остановленного поиска: это перечитывание и есть его продолжение.
    setSearchedBuffer(false);
    // Список меняется целиком — обнуляем его ДО того, как приедут новые строки,
    // иначе Virtuoso успеет отрисовать чужие данные под новым топиком.
    setTotal(0);
    setGeneration((g) => g + 1);

    // Схема ставится ДО чтения: она решает, в каком виде уедут строки. Прислать
    // её после первого окна значило бы показать сырые байты и молча подменить
    // их разобранным телом секундой позже.
    //
    // Сломавшаяся схема топик не запирает: сообщаем и читаем как есть — иначе
    // одна испорченная схема лишала бы доступа к данным.
    const withSchema = schemaCluster
      ? api.applyTopicSchema(schemaCluster, selectedTopic.name).catch((e) => {
          if (!cancelled) {
            console.error('Failed to apply topic schema', e);
            toast.error(`Schema is not applied: ${describeError(e)}`);
          }
          return null;
        })
      : Promise.resolve(null);

    withSchema
      .then((schema) => {
        if (cancelled) return Promise.reject(new Error(READ_SUPERSEDED));
        applied = schema;
        setOpenSchema(schema);
        return api.openTopic(readParamsRef.current(selectedTopic));
      })
      .then((result) => {
        if (cancelled) return;
        setTotal(result.total);
        setStats(result);
        if (result.truncated) {
          toast.info(`Loaded ${result.loaded} messages; the topic has more`);
        }
        // Топик открывали ради одного сообщения — самое время его показать.
        finishPendingLink(applied);
      })
      .catch((e) => {
        if (cancelled || isSuperseded(e)) return;
        // Ссылка не сбылась: почему именно — уже сказано в ошибке ниже
        // («офсета больше нет», «нет доступа»), и держать переход в силе значило
        // бы выполнить его на следующем открытом топике.
        pendingLinkRef.current = null;
        console.error('Failed to open topic', e);
        toast.error(`Failed to read topic: ${describeError(e)}`);
        setTotal(0);
        setStats(null);
        // Заданные границы в топик не попали — оставлять их выбранными значит
        // оставлять пользователя перед пустой таблицей, из которой он выйдет
        // только вспомнив, что надо переключить селектор.
        if (readMode === 'offset' || readMode === 'timestamp') {
          setReadMode('newest');
          setRange(EMPTY_RANGE);
        }
      })
      .finally(() => {
        if (!cancelled) setIsLoadingMessages(false);
      });

    return () => {
      // Пользователь переключил топик, пока летел ответ — результат больше
      // не нужен. Раньше здесь была гонка: старый ответ перезаписывал новый.
      cancelled = true;
    };
    // filters здесь намеренно не в зависимостях: их применяет set_filter,
    // без повторного чтения из Kafka. А вот порядок и границы — в
    // зависимостях: и то, и другое требует перечитать топик заново.
    //
    // Партиции и границы — строкой, а не объектом: у объекта каждый рендер
    // новая идентичность, и топик перечитывался бы на ровном месте.
    //
    // `readNonce` — перечитать то же самое теми же параметрами. Единственный
    // его источник — остановка глубокого поиска: она оставляет буфер пустым,
    // и топик надо открыть заново, ничего в выборе не поменяв.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedTopic, partitionsKey, rangeKey, readNonce]);

  // Опрос хода чтения — общий для открытия топика и для "Load more".
  //
  // Раньше он жил внутри эффекта открытия, и во время "Load more" таблица
  // стояла мёртвой до самого конца чтения: кнопка светила «Loading…» полминуты
  // и выглядела как зависшая, хотя строки на бэкенде уже публиковались.
  //
  // Чтение публикует строки только в хвост, поэтому generation здесь НЕ
  // трогаем — кэш окон остаётся валидным (см. useMessageWindow).
  useEffect(() => {
    if (!isLoadingMessages && !isLoadingMore && !isSearching) return;

    const timer = window.setInterval(() => {
      api
        .getOpenTopicProgress()
        .then((p) => {
          if (p.done || p.topic !== topicRef.current) return;
          setTotal(p.total);
          setStats({
            total: p.total,
            loaded: p.loaded,
            buffer_bytes: p.buffer_bytes,
            memory_bytes: p.memory_bytes,
            memory_limit: p.memory_limit,
            truncated: p.truncated,
            read_bytes_per_sec: p.read_bytes_per_sec,
            peak_throttle_ms: p.peak_throttle_ms,
            active_brokers: p.active_brokers,
            known_brokers: p.known_brokers,
            scanned: p.scanned,
            approx_total: p.approx_total,
          });
        })
        .catch(() => {});
    }, PROGRESS_POLL_MS);

    return () => window.clearInterval(timer);
  }, [isLoadingMessages, isLoadingMore, isSearching]);

  /**
   * Глубокий поиск: прочитать топик до конца, оставив только совпадения.
   *
   * Топик открывается ЗАНОВО, поэтому таблица обнуляется прямо здесь, до
   * ответа: буфер на бэкенде уже сбрасывается, и показывать поверх него старые
   * строки — значит показывать то, чего там больше нет.
   */
  const handleDeepSearch = useCallback(() => {
    const topic = selectedTopic;
    if (!topic) return;
    const topicAtRequest = topicRef.current;
    const params = readParamsRef.current(topic);

    // Отложенная отправка фильтра, если она ещё в пути, уже не нужна и вредна:
    // поиск везёт этот же фильтр в своих параметрах, а `set_filter`, прилетев
    // следом, поменял бы его на бэкенде посреди чтения. Сито от этого не
    // поедет (оно снято на старте — см. `Sieve`), а вот таблица показала бы
    // буфер, просеянный одним запросом, через другой.
    //
    // «Применённым» отмечается ровно то, что уехало в поиск, — не значение из
    // замыкания: иначе следующая же смена фильтра могла бы счесть себя
    // ненужной, сравнив себя не с тем.
    if (filterTimer.current !== null) {
      window.clearTimeout(filterTimer.current);
      filterTimer.current = null;
    }
    appliedFilterRef.current = params.filter;

    setIsSearching(true);
    setSearchedBuffer(true);
    setTotal(0);
    setGeneration((g) => g + 1);
    // Счётчики предыдущего чтения обнуляются здесь же, а не с первым тиком
    // опроса: иначе четверть секунды шкала показывала бы «просмотрено 1000» от
    // того чтения, которое поиск только что стёр. Измеренная квота остаётся —
    // она про кластер, а не про это чтение.
    setStats((prev) =>
      prev ? { ...prev, total: 0, loaded: 0, scanned: 0, approx_total: null } : prev,
    );

    api
      .deepSearch(params)
      .then((result) => {
        if (topicRef.current !== topicAtRequest) return;
        setTotal(result.total);
        setStats(result);
        if (result.total === 0) {
          toast.info(
            result.truncated
              ? 'Search stopped before the end of the topic; nothing matched so far'
              : 'Nothing in this topic matches the filter',
          );
        }
      })
      .catch((e) => {
        // Остановлен пользователем или вытеснен уходом с топика — и то, и
        // другое нормальный ход событий, а не сбой. Перечитывание после
        // остановки заводит `handleStopSearch`, здесь делать нечего.
        if (isStopped(e) || isSuperseded(e) || topicRef.current !== topicAtRequest) return;
        console.error('deep_search failed', e);
        toast.error(`Search failed: ${describeError(e)}`);
      })
      .finally(() => setIsSearching(false));
  }, [selectedTopic]);

  /**
   * Остановить поиск и вернуть топик в обычный вид.
   *
   * Буфер после остановки пуст: в нём лежали ОДНИ находки, и оставить их в
   * таблице значило бы показать топик, из которого молча пропало всё
   * непопавшее. Поэтому топик перечитывается заново — тем же эффектом, что и
   * при обычном открытии, со схемой и сбросом кэша окон.
   */
  const handleStopSearch = useCallback(() => {
    api
      .stopSearch()
      .then((stopped) => {
        // Не успели: поиск закончился сам, пока летела команда. Буфер не
        // тронут, и перечитывать топик значило бы стереть только что найденное.
        if (stopped) setReadNonce((n) => n + 1);
      })
      .catch((e) => {
        console.error('stop_search failed', e);
        // Остановить не вышло, а состояние буфера неизвестно — перечитываем,
        // это единственный способ вернуть таблицу к чему-то определённому.
        setReadNonce((n) => n + 1);
      });
  }, []);

  // "Загрузить ещё": продолжает уже открытый топик с того окна, на котором
  // остановилось предыдущее чтение — без повторной вычитки уже показанного.
  // generation НЕ поднимаем: бэкенд публикует строки только в хвост,
  // позиции закэшированных строк не меняются.
  const handleLoadMore = useCallback(() => {
    const topicAtRequest = topicRef.current;
    setIsLoadingMore(true);
    api
      .loadMore({ additional: DEFAULT_PARTITION_LIMIT })
      .then((result) => {
        if (topicRef.current !== topicAtRequest) return;
        setTotal(result.total);
        setStats(result);
      })
      .catch((e) => {
        // Ушли с топика, пока дочитывалось — бэкенд отменил чтение намеренно.
        // Показывать это как сбой значило бы пугать пользователя на ровном
        // месте: ровно так раньше и вылезало "worker dropped the reply".
        if (isSuperseded(e) || topicRef.current !== topicAtRequest) return;
        console.error('load_more failed', e);
        toast.error(`Failed to load more messages: ${describeError(e)}`);
      })
      .finally(() => setIsLoadingMore(false));
  }, []);

  // Смена фильтра пересчитывается в Rust по уже загруженному буферу —
  // ни одного сетевого запроса.
  //
  // `selectedTopic` в зависимостях, но сам по себе перепосылки не стоит: топик
  // открывается уже со своим фильтром (`open_topic`), и второй `set_filter`
  // следом только затирал бы растущий `total` снимком, взятым посреди
  // подгрузки, и зря ронял кэш окон. Отсекаем тем же приёмом, что и для
  // сортировки ниже, — по последнему applied-значению.
  const filterTimer = useRef<number | null>(null);
  const appliedFilterRef = useRef(filters);
  useEffect(() => {
    if (!selectedTopic) return;
    if (appliedFilterRef.current === filters) return;
    if (filterTimer.current !== null) window.clearTimeout(filterTimer.current);

    filterTimer.current = window.setTimeout(() => {
      const topicAtRequest = topicRef.current;
      appliedFilterRef.current = filters;

      // В буфере лежит добыча поиска — пересевать её новым запросом нельзя.
      // Просеяно уже прошлым фильтром, и расширенный запрос честно не нашёл бы
      // ничего нового: искать негде. Перечитываем топик обычным чтением, с
      // новым фильтром — это ровно то состояние, в котором пользователь
      // оказался бы, введя этот запрос сразу.
      if (searchedBuffer) {
        setReadNonce((n) => n + 1);
        return;
      }

      api
        .setFilter(filters)
        .then((visible) => {
          // Ушли с топика, пока считалось — ответ относится к чужому буферу.
          if (topicRef.current !== topicAtRequest) return;
          setTotal(visible);
          setGeneration((g) => g + 1);
        })
        .catch((e) => console.error('set_filter failed', e));
    }, FILTER_DEBOUNCE_MS);

    return () => {
      if (filterTimer.current !== null) window.clearTimeout(filterTimer.current);
    };
  }, [filters, selectedTopic, searchedBuffer]);

  // Клик по заголовку колонки — как фильтр, пересчитывается в Rust по уже
  // загруженному буферу. В отличие от фильтра, который только выбрасывает
  // строки, сортировка может переставить их местами, поэтому кэш окон сбрасываем.
  const sortRef = useRef(sort);
  useEffect(() => {
    if (!selectedTopic) return;
    // На первом рендере после открытия топика сортировка уже применена
    // параметром `open_topic` — второй раз посылать её незачем.
    if (sortRef.current === sort) return;
    sortRef.current = sort;

    invoke<number>('set_sort', { sort })
      .then((visible) => {
        setTotal(visible);
        setGeneration((g) => g + 1);
      })
      .catch((e) => console.error('set_sort failed', e));
  }, [sort, selectedTopic]);

  // Пока сортировка по столбцу активна, фоновая догрузка может вклинить новые
  // строки куда угодно, а не только в хвост — обычная гарантия "хвост дописывается,
  // экран не едет" здесь не работает (см. `Worker::sort_view`). Приходится
  // сбрасывать кэш окон на каждый прирост `total`, а не только при явной смене
  // сортировки.
  const sortedTotalRef = useRef(total);
  useEffect(() => {
    if (!sort) return;
    if (sortedTotalRef.current === total) return;
    sortedTotalRef.current = total;
    setGeneration((g) => g + 1);
  }, [sort, total]);

  // Тело сообщения читается из арены в Rust — быстро, но не мгновенно, а
  // стрелками по таблице бегают быстрее, чем приходят ответы. Талон отсекает
  // опоздавшие: без него зажатая стрелка оставляла бы в модалке то тело,
  // которое приехало последним, а не то, на котором остановились.
  const bodyRequest = useRef(0);

  /** Тот же талон, но для окна сохранённого сообщения: окна два и живут они
   *  независимо. */
  const favoriteRequest = useRef(0);

  const showMessageAt = useCallback((index: number) => {
    const ticket = ++bodyRequest.current;
    const topicAtRequest = topicRef.current;
    invoke<FullMessage>('get_message_body', { index })
      .then((message) => {
        if (bodyRequest.current !== ticket || topicRef.current !== topicAtRequest) return;
        setSelectedMessage(message);
        setSelectedIndex(index);
        setIsModalOpen(true);
      })
      .catch((e) => {
        if (bodyRequest.current !== ticket) return;
        console.error('Failed to load message body', e);
        toast.error('Failed to load message');
      });
  }, []);

  /** Шаг по таблице из открытой модалки. За её краями — ничего не делаем:
   *  закрывать модалку или заворачивать список на другой конец пользователь
   *  не просил, а `total` меняется на ходу при догрузке хвоста. */
  const handleNavigateMessage = useCallback(
    (delta: -1 | 1) => {
      if (selectedIndex === null) return;
      const next = selectedIndex + delta;
      if (next < 0 || next >= total) return;
      showMessageAt(next);
    },
    [selectedIndex, total, showMessageAt],
  );

  /**
   * Последний шаг перехода по ссылке: топик прочитан — найти в нём то самое
   * сообщение и открыть.
   *
   * Зовётся из эффекта открытия, а не из отдельного эффекта по `isLoading`:
   * второй эффект в том же коммите увидел бы ещё старое значение флага и
   * бросился бы искать сообщение до того, как чтение началось.
   */
  const finishPendingLink = useCallback(
    (schema: TopicSchema | null) => {
      const pending = pendingLinkRef.current;
      if (!pending) return;
      pendingLinkRef.current = null;
      // Пока читалось, ушли на другой топик — переход опоздал.
      if (pending.topic !== topicRef.current) return;

      // Схема по ссылке не едет и ехать не может: локальные .proto лежат у
      // отправителя на диске. Сказать, чего не хватает, — единственное, что
      // здесь вообще можно сделать, и это заметно лучше двоичного мусора без
      // объяснений. Реестровые схемы (avro, jsonschema) подхватятся сами, и
      // тогда форматы совпадут и говорить будет не о чем.
      const mine = schema?.format ?? 'json';
      if (pending.format && pending.format !== mine) {
        const named = pending.typeName ? ` (${pending.typeName})` : '';
        toast.info(
          `The sender reads this topic as ${pending.format}${named}, you read it as ${mine}. ` +
            'Load the schema in topic settings to see the same body.',
        );
      }

      api
        .findMessage(pending.partition, pending.offset)
        .then((index) => {
          if (topicRef.current !== pending.topic) return;
          if (index === null) {
            // Диапазон вокруг офсета прочитался, а самого офсета в нём не
            // оказалось. Так выглядит дырка в компактированном топике и край
            // retention — обе причины называем, различить их нечем.
            toast.error(
              `No message at partition ${pending.partition}, offset ${pending.offset} — ` +
                'retention or compaction may have dropped it',
            );
            return;
          }
          // Подсветка ставится вместе с открытием окна, а не после его
          // закрытия: закрыть окно можно и мимо кнопки, а найти строку глазами
          // среди сотни соседей — это ровно то, чего ссылка избавляет.
          setLinkedMessage({ partition: pending.partition, offset: pending.offset });
          showMessageAt(index);
        })
        .catch((e) => {
          console.error('Failed to locate the linked message', e);
          toast.error(`Failed to open the linked message: ${describeError(e)}`);
        });
    },
    [showMessageAt],
  );

  /** Стабильный колбэк: иначе каждая перерисовка таблицы меняла бы пропс у
   *  всех видимых строк и обесценивала их `memo`. */
  const clearHighlight = useCallback(() => setLinkedMessage(null), []);

  const handleConfigClick = useCallback((topic: Topic) => {
    setConfigTopic(topic);
    setIsConfigModalOpen(true);
  }, []);

  const handleTopicInfo = useCallback((topic: Topic) => {
    setInfoTopic(topic);
    setIsTopicInfoOpen(true);
  }, []);

  /**
   * Схему топика поправили в настройках.
   *
   * Перечитывать топик из Kafka незачем: буфер в Rust хранит сырые байты, а
   * декодирование происходит на выдаче окна. Достаточно переставить декодер и
   * обесценить кэш окон — за это платится ноль трафика и ноль квоты.
   *
   * Настраивать можно и топик, который сейчас не открыт: тогда делать нечего,
   * его схема приедет при открытии.
   */
  const handleSchemaChanged = useCallback(
    (topic: string, schema: TopicSchema | null) => {
      if (!schemaCluster || topicRef.current !== topic) return;
      api
        .applyTopicSchema(schemaCluster, topic)
        .then((applied) => {
          if (topicRef.current !== topic) return;
          setOpenSchema(applied);
          setGeneration((g) => g + 1);
        })
        .catch((e) => {
          console.error('Failed to apply topic schema', e);
          toast.error(`Schema is not applied: ${describeError(e)}`);
          // Форма уже показывает новую схему, а строки остались прежними —
          // держать в состоянии картинку, которой нет в воркере, нельзя.
          setOpenSchema(schema);
        });
    },
    [schemaCluster],
  );

  /**
   * Переключение формата тела прямо из шапки.
   *
   * Пишется туда же, где живёт остальная схема топика, — в `proto.json` на
   * диске. Отдельного «где-то ещё запомнить выбранный формат» здесь нет и не
   * должно быть: он и так свойство пары кластер-топик, уже переживающее
   * перезапуск, и второе хранилище того же самого неизбежно разъехалось бы с
   * первым.
   *
   * `message` передаётся прежний: бэкенд принимает только тот, что есть в
   * схеме, а смена формата к выбору типа отношения не имеет.
   */
  const handleFormatChange = useCallback(
    (format: BodyFormat) => {
      const topic = topicRef.current;
      if (!schemaCluster || !topic || format === (openSchema?.format ?? 'json')) return;
      api
        .saveTopicSchema(schemaCluster, topic, format, openSchema?.message ?? null)
        .then((saved) => handleSchemaChanged(topic, saved))
        .catch((e) => {
          console.error('Failed to save the body format', e);
          toast.error(`Can't switch the format: ${describeError(e)}`);
        });
    },
    [schemaCluster, openSchema, handleSchemaChanged],
  );

  /**
   * Переключение линзы прямо из шапки.
   *
   * Хранится там же, где формат, и по той же причине: это свойство пары
   * кластер-топик, уже переживающее перезапуск. Применяется тем же способом —
   * `applyTopicSchema` переставляет линзу в воркере и обесценивает кэш окон, —
   * то есть без единого байта по сети: буфер держит сырые байты, а конверт
   * снимается на выдаче окна.
   */
  const handleLensChange = useCallback(
    (lens: LensSetting | null) => {
      const topic = topicRef.current;
      if (!schemaCluster || !topic || lens === (openSchema?.lens ?? null)) return;
      api
        .saveTopicLens(schemaCluster, topic, lens)
        .then((saved) => handleSchemaChanged(topic, saved))
        .catch((e) => {
          console.error('Failed to save the envelope lens', e);
          toast.error(`Can't switch the envelope: ${describeError(e)}`);
        });
    },
    [schemaCluster, openSchema, handleSchemaChanged],
  );

  /** Настройки схемы того топика, который открыт, — из шапки. */
  const handleOpenSchemaSettings = useCallback(() => {
    if (selectedTopic) handleConfigClick(selectedTopic);
  }, [selectedTopic, handleConfigClick]);

  const handleClusterClick = useCallback(() => setIsClusterArchiveModalOpen(true), []);

  const handleCreateNewCluster = useCallback(() => {
    setSelectedClusterId(null);
    setClusterConfigMode('create');
    setIsClusterConfigModalOpen(true);
  }, []);

  const handleEditCluster = useCallback((cluster: KafkaCluster) => {
    setSelectedClusterId(cluster.id);
    setClusterConfigMode('edit');
    setIsClusterConfigModalOpen(true);
  }, []);

  /**
   * Единственный путь подключения — им пользуются и кнопка Connect в форме,
   * и клик по строке в архиве, и смена Kafka-пользователя. Раньше клик по
   * архиву только красил строку и показывал «Connected», ничего не подключая.
   *
   * `keepTopic` — для смены учётки: читать ту же тему под другими ACL и есть
   * то, ради чего учётки переключают, и выбрасывать пользователя обратно к
   * списку топиков на каждое переключение незачем.
   *
   * `replace` — прежнее подключение заведомо устарело (у текущей учётки
   * поменяли креды), и откатываться на него нельзя ни при какой неудаче.
   *
   * Возвращает список топиков. Нужен переходу по ссылке: тому надо открыть
   * топик сразу после подключения, а `topics` в его замыкании к этому моменту
   * ещё прежний — состояние обновится только со следующим рендером.
   */
  const connect = useCallback(
    async (
      payload: ClusterConnectPayload,
      label: string,
      options?: { keepTopic?: boolean; replace?: boolean },
    ) => {
      setIsConnecting(true);
      // Соединение держится на СТАРОМ пароле: librdkafka аутентифицируется
      // один раз при подключении и после этого читает, ничего не переспрашивая.
      // Не разорвав его, мы бы оставили работающим сеанс по кредам, которых
      // больше нет, — и вся смена пароля выглядела бы фикцией.
      if (options?.replace) {
        await api.clusterDisconnect().catch(() => {});
      }
      try {
        // Бэкенд проверяет связь внутри `cluster_connect` и на неудаче
        // оставляет прежнее подключение нетронутым — поэтому здесь ничего не
        // трогаем до успеха.
        await api.clusterConnect(payload);
      } catch (e) {
        console.error('Connect failed', e);
        // Откатываться некуда: прежнее соединение мы разорвали сами.
        if (options?.replace) {
          setConnectedClusterId(null);
          setConnectedUserId(null);
          setConnectedName(null);
          setTopics([]);
          setSelectedTopic(null);
        }
        toast.error(`Failed to connect: ${describeError(e)}`);
        setIsConnecting(false);
        throw e;
      }

      // Соединение на бэкенде уже переставлено. Состояние UI обязано это
      // отразить даже если следующий шаг упадёт: иначе шапка показывала бы
      // одну учётку, пока воркер держит другую.
      setConnectedClusterId(payload.id ?? null);
      setConnectedUserId(payload.user_id ?? null);
      setConnectedName(label);
      // last_used и active_user_id обновил бэкенд — перечитываем список.
      api.listClusters().then(setClusters).catch(() => {});

      try {
        const loaded = await api.getTopics();
        setTopics(loaded);

        const previous = options?.keepTopic ? topicRef.current : null;
        const retained = previous ? loaded.find((t) => t.name === previous) : undefined;
        if (previous && !retained) {
          // Учётка сменилась на менее полномочную — топик просто исчез из
          // списка. Молча оставить его открытым значило бы показывать строки,
          // которых новому пользователю видеть не положено.
          toast.warning(`Topic ${previous} is not available to this user`);
        }
        // Новый объект, а не тот же самый: перечитать топик под новыми правами
        // можно только подняв поколение.
        setSelectedTopic(retained ? { ...retained } : null);
        setSelectedPartitions((prev) =>
          prev && retained && prev.some((p) => p >= retained.partitions) ? null : prev,
        );

        toast.success(`Connected to ${label} · ${loaded.length} topics`);
        return loaded;
      } catch (e) {
        console.error('Failed to list topics', e);
        setTopics([]);
        setSelectedTopic(null);
        toast.error(`Connected, but the topic list is unavailable: ${describeError(e)}`);
        throw e;
      } finally {
        setIsConnecting(false);
      }
    },
    [],
  );

  const handleConnectToCluster = useCallback(
    (cluster: KafkaCluster) => {
      const user = api.activeUser(cluster);
      connect(api.clusterToPayload(cluster, user), cluster.name).catch(() => {});
    },
    [connect],
  );

  /**
   * Переход по ссылке на сообщение — после того, как человек его подтвердил.
   *
   * Подключение у воркера одно и открытый топик один, поэтому переход именно
   * УВОДИТ: подключается к нужному кластеру, если это не текущий, и открывает
   * нужный кусок нужного топика. Предупреждение об этом показывает окно
   * подтверждения, здесь уже поздно спрашивать.
   *
   * Читается не одно сообщение, а окно вокруг него (`LINK_CONTEXT`): пришедший
   * по ссылке почти всегда смотрит и на соседей, а стоит это столько же —
   * брокер всё равно присылает целый батч на партицию.
   */
  const followLink = useCallback(
    async (target: MessageLinkTarget) => {
      // Список мог измениться, пока окно стояло открытым: кластер успели
      // удалить. Бэкенд его нашёл, а показывать нам уже нечего.
      const cluster = clusters.find((c) => c.id === target.cluster);
      if (!cluster) {
        toast.error('That connection is no longer in the list');
        return;
      }

      let available = topics;
      if (connectedClusterId !== target.cluster) {
        try {
          available = await connect(
            api.clusterToPayload(cluster, api.activeUser(cluster)),
            cluster.name,
          );
        } catch {
          // `connect` уже сказал, что именно не вышло.
          return;
        }
      }

      // Топика нет в списке — и это ровно та развилка, которую Kafka не
      // различает: неавторизованный топик не виден в метаданных так же, как
      // несуществующий. Формулировка та же, что и у воркера (см. worker.rs),
      // чтобы человек не гадал, почему на одно и то же две разные жалобы.
      const topic = available.find((t) => t.name === target.topic);
      if (!topic) {
        toast.error(
          `Topic ${target.topic} is not on ${cluster.name}, ` +
            'or your Kafka user has no access to it',
        );
        return;
      }
      if (target.partition >= topic.partitions) {
        toast.error(
          `Partition ${target.partition} does not exist: ${target.topic} has ${topic.partitions}`,
        );
        return;
      }

      pendingLinkRef.current = {
        topic: target.topic,
        partition: target.partition,
        offset: target.offset,
        format: target.format,
        typeName: target.type_name,
      };

      // Дальше работает обычный эффект открытия топика: ему всё равно, откуда
      // взялись партиция и границы — из шапки или из ссылки. Второго пути
      // чтения здесь нет и быть не должно.
      setSelectedPartitions([target.partition]);
      setReadMode('offset');
      setRange({
        from_offset: Math.max(0, target.offset - LINK_CONTEXT),
        to_offset: target.offset + LINK_CONTEXT,
        from_timestamp: null,
        to_timestamp: null,
      });
      setFilters(EMPTY_FILTER);
      // Пустой фильтр уедет вместе с `open_topic` — считаем применённым, иначе
      // следом улетит лишний `set_filter` (тот же приём, что в handleSelectTopic).
      appliedFilterRef.current = EMPTY_FILTER;
      // И сортировку: ссылка ведёт к конкретной строке, а не к чужому порядку,
      // оставшемуся от прошлого топика. Ref обновляем сами — по той же причине.
      sortRef.current = null;
      setSort(null);
      // Новый объект даже для того же топика: перечитать его надо в любом
      // случае, а эффект смотрит на идентичность.
      setSelectedTopic({ ...topic });
    },
    [clusters, connect, connectedClusterId, topics],
  );

  /**
   * Переподключение к тому же кластеру под указанной учёткой.
   *
   * `replace` — креды этой учётки только что изменились: прежнее соединение
   * держится на старом пароле, и сохранять его как запасной вариант нельзя.
   */
  const connectAsUser = useCallback(
    (user: ClusterUser, options?: { replace?: boolean }) => {
      if (!connectedCluster) return;
      connect(api.clusterToPayload(connectedCluster, user), connectedCluster.name, {
        keepTopic: true,
        replace: options?.replace,
      }).catch(() => {});
    },
    [connect, connectedCluster],
  );

  const handleSelectUser = useCallback(
    (user: ClusterUser) => {
      if (user.id === connectedUserId) return;
      connectAsUser(user);
    },
    [connectAsUser, connectedUserId],
  );

  /**
   * Применить только что сохранённые настройки подключения.
   *
   * Настройки СОСЕДНЕГО кластера правят, не бросая текущую сессию: увести с
   * неё по нажатию Save было бы самоуправством. Во всех остальных случаях —
   * подключаемся, ради этого настройки и правили.
   */
  const handleApplyClusterSettings = useCallback(
    async (cluster: KafkaCluster) => {
      if (connectedClusterId && connectedClusterId !== cluster.id) return;
      await connect(api.clusterToPayload(cluster, api.activeUser(cluster)), cluster.name, {
        keepTopic: true,
        // Параметры подключения изменились — прежний сеанс держится на
        // прежних и достоверным больше не является.
        replace: true,
      });
    },
    [connect, connectedClusterId],
  );

  /**
   * Полный разрыв подключения.
   *
   * Состояние UI обязано это отразить целиком: и шапка, и список топиков, и
   * открытая таблица — иначе останется картинка живого сеанса, которого нет.
   */
  /**
   * Забыть подключение. Бэкенду при этом ничего не говорится.
   *
   * Отдельно от `disconnect` ради перезапуска воркера: там отключаться уже не
   * от чего — подключение ушло вместе с упавшим потоком, — а состояние на
   * фронте осталось и показывает кластер живым.
   */
  const forgetConnection = useCallback(() => {
    setConnectedClusterId(null);
    setConnectedUserId(null);
    setConnectedName(null);
    setTopics([]);
    setSelectedTopic(null);
  }, []);

  const disconnect = useCallback(async () => {
    await api.clusterDisconnect().catch((e) => console.error('Disconnect failed', e));
    forgetConnection();
  }, [forgetConnection]);

  /**
   * Воркер упал и поднят заново.
   *
   * Сообщение об этом приезжает и ответом на команду, но ответ достаётся ровно
   * тому вызову, который заметил смерть, — а подключения нет уже ни у кого.
   * Поэтому состояние сбрасывается по событию: иначе шапка показывала бы
   * кластер подключённым, а список — топики, которых на новом воркере нет.
   *
   * Заодно перечитываем путь к журналу: до этой паники его могло не быть
   * вовсе, а кнопка «прислать трейс» нужна именно сейчас.
   */
  useEffect(() => {
    const listener = listen(WORKER_RESTARTED, () => {
      forgetConnection();
      api
        .panicLog()
        .then(setCrashLog)
        .catch((e) => console.error('Failed to check the panic log', e));
      // Дольше обычного: это не «не получилось», а «состояние потеряно, надо
      // переподключиться», и прочитать это человек обязан успеть.
      toast.error(
        'The Kafka worker crashed and was restarted. Reconnect to the cluster to continue — ' +
          'the crash log is in the header, please send it over.',
        { duration: 15000 },
      );
    });
    return () => {
      listener.then((stop) => stop()).catch(() => {});
    };
  }, [forgetConnection]);

  /** `fromConfig` — пришли из настроек кластера, значит есть куда вернуться. */
  const handleManageUsers = useCallback((cluster: KafkaCluster, fromConfig = false) => {
    setUsersModalClusterId(cluster.id);
    setUsersModalFromConfig(fromConfig);
    setIsClusterConfigModalOpen(false);
    setIsClusterUsersModalOpen(true);
  }, []);

  const handleBackToArchive = useCallback(() => {
    setIsClusterConfigModalOpen(false);
    setIsClusterArchiveModalOpen(true);
  }, []);

  /** Кластер уже записан на диск бэкендом — здесь только обновляем список. */
  const handleClusterSaved = useCallback((saved: KafkaCluster) => {
    setClusters((prev) => {
      const known = prev.some((c) => c.id === saved.id);
      return known ? prev.map((c) => (c.id === saved.id ? saved : c)) : [...prev, saved];
    });
  }, []);

  const handleDeleteCluster = useCallback(
    async (id: string) => {
      try {
        await api.deleteCluster(id);
        setClusters((prev) => prev.filter((c) => c.id !== id));
        if (connectedClusterId === id) {
          setConnectedClusterId(null);
          setConnectedUserId(null);
          setConnectedName(null);
        }
        toast.success('Cluster deleted');
      } catch (e) {
        console.error('Failed to delete cluster', e);
        toast.error(`Failed to delete cluster: ${e}`);
      }
    },
    [connectedClusterId],
  );

  /** И выбор партиций, и границы чтения, и поиск осмыслены только для того
   *  топика, на котором сделаны: у соседнего и партиций может быть меньше, и
   *  офсеты свои — иначе чтение упёрлось бы в «out of range» на ровном месте,
   *  а запрос из прошлого топика встретил бы пользователя пустой таблицей.
   *
   *  Сброс именно здесь, а не в эффекте открытия: React сложит его в один
   *  рендер с `setSelectedTopic`, и эффект увидит уже пустой фильтр — значит в
   *  `open_topic` уедет он же, без отдельной передачи значения. Смена партиций
   *  и границ через этот обработчик не идёт и поиск сохранит. */
  const handleSelectTopic = useCallback((topic: Topic | null) => {
    if (topicRef.current !== (topic?.name ?? null)) {
      // Подсветка принадлежит сообщению того топика, за которым пришли по
      // ссылке. В соседнем те же партиция с офсетом — другое сообщение.
      setLinkedMessage(null);
      setSelectedPartitions(null);
      setReadMode((mode) => (mode === 'offset' || mode === 'timestamp' ? 'newest' : mode));
      setRange(EMPTY_RANGE);
      setFilters(EMPTY_FILTER);
      // Пустой фильтр уедет вместе с `open_topic`, поэтому считаем его уже
      // применённым: иначе эффект ниже послал бы вдогонку второй `set_filter`.
      appliedFilterRef.current = EMPTY_FILTER;
    }
    setSelectedTopic(topic);
  }, []);

  const handleReadChange = useCallback((mode: ReadMode, next: ReadRange) => {
    setReadMode(mode);
    setRange(next);
  }, []);

  const handleRefresh = useCallback(() => {
    // Перечитываем топик с нуля, поднимая поколение.
    setSelectedTopic((t) => (t ? { ...t } : t));
  }, []);

  const handleOpenFavorites = useCallback(() => {
    // Перечитываем именно здесь: между прошлым открытием и этим могли поменять
    // схему топика, и превью сохранённого обязано её догнать — иначе список
    // показывает байты у сообщения, которое модалка уже разбирает по схеме.
    loadFavorites();
    setIsFavoritesModalOpen(true);
  }, [loadFavorites]);

  const handleOpenProduce = useCallback(() => setIsProduceModalOpen(true), []);

  /** Ссылку вставляют руками — принесённой системой здесь нет. */
  const handleOpenLink = useCallback(() => {
    setIncomingLink(null);
    setIsOpenLinkOpen(true);
  }, []);

  const handleRevealCrashLog = useCallback(() => {
    api.revealPanicLog().catch((e) => {
      console.error('Failed to reveal the panic log', e);
      toast.error(describeError(e));
    });
  }, []);

  /**
   * Ссылка, которую принесла система.
   *
   * Забирается ровно один раз и по двум поводам: при старте (приложение
   * ЗАПУСТИЛИ ссылкой — события тогда не было, некому было его слушать) и по
   * звонку в дверь, когда ссылку отдали уже работающему окну. Сам URL приезжает
   * не событием, а этим запросом: у него должен быть единственный потребитель,
   * иначе один и тот же переход выполнился бы дважды (см. `PendingLink`).
   */
  useEffect(() => {
    const take = () => {
      api
        .takePendingLink()
        .then((url) => {
          if (!url) return;
          setIncomingLink(url);
          setIsOpenLinkOpen(true);
        })
        .catch((e) => console.error('Failed to take the pending link', e));
    };

    take();
    const listener = listen(LINK_EVENT, take);
    return () => {
      listener.then((stop) => stop()).catch(() => {});
    };
  }, []);

  /**
   * Ссылка на открытое сообщение.
   *
   * Собирает её Rust: кластер в ссылке назван идентификатором, который выдал
   * брокер, а он известен только живому подключению. Схема тоже его — фронту
   * пришлось бы собирать подсказку о формате самому и разойтись с тем, чем
   * топик читается на самом деле.
   */
  const handleShare = useCallback(() => {
    const topic = selectedTopic?.name;
    if (!topic || !selectedMessage) return;
    api
      .buildMessageLink({
        cluster: schemaCluster,
        cluster_name: connectedName,
        topic,
        partition: selectedMessage.partition,
        offset: selectedMessage.offset,
        timestamp: selectedMessage.timestamp,
      })
      .then((url) => {
        setShareLink(url);
        setIsShareOpen(true);
      })
      .catch((e) => {
        console.error('Failed to build a message link', e);
        toast.error(`Can't build a link: ${describeError(e)}`);
      });
  }, [selectedTopic, selectedMessage, schemaCluster, connectedName]);

  /**
   * Кладёт открытое сообщение на диск.
   *
   * Наверх уезжает только индекс строки: тело бэкенд возьмёт у воркера сырыми
   * байтами, потому что здесь оно уже прошло через схему и `from_utf8_lossy` —
   * а схему к топику загружают когда угодно, в том числе после сохранения.
   * Сохранённое повторно не удваивается: бэкенд узнаёт его по паре
   * кластер-топик и координатам и обновляет запись на месте.
   */
  const handleAddToFavorite = useCallback(() => {
    const topic = selectedTopic?.name;
    if (!topic || !selectedMessage || selectedIndex === null) return;
    api
      .saveFavorite({
        cluster: schemaCluster ?? '',
        cluster_name: connectedName ?? '',
        topic,
        index: selectedIndex,
        partition: selectedMessage.partition,
        offset: selectedMessage.offset,
      })
      .then((result) => {
        setFavorites({ items: result.items, total_bytes: result.total_bytes });
        toast.success(result.updated ? 'Saved message updated' : 'Message saved to disk');
      })
      .catch((e) => {
        console.error('Failed to save message', e);
        toast.error(`Failed to save message: ${describeError(e)}`);
      });
  }, [selectedTopic, selectedMessage, selectedIndex, schemaCluster, connectedName]);

  const removeFavorite = useCallback((id: string) => {
    api
      .deleteFavorite(id)
      .then(setFavorites)
      .catch((e) => {
        console.error('Failed to delete saved message', e);
        toast.error(`Failed to delete saved message: ${describeError(e)}`);
      });
  }, []);

  const handleRemoveFavorite = useCallback(
    (favorite: FavoriteInfo) => removeFavorite(favorite.id),
    [removeFavorite],
  );

  /**
   * Идентификатор записи архива для открытого сообщения таблицы — `null`, если
   * оно ещё не сохранено. По нему кнопка в окне просмотра и решает, сохранять
   * ей или удалять.
   */
  const openMessageSavedId =
    selectedMessage && selectedTopic
      ? (favorites.items.find(
          (f) =>
            f.cluster === (schemaCluster ?? '') &&
            f.topic === selectedTopic.name &&
            f.partition === selectedMessage.partition &&
            f.offset === selectedMessage.offset,
        )?.id ?? null)
      : null;

  const handleClearFavorites = useCallback(() => {
    api
      .clearFavorites()
      .then((view) => {
        setFavorites(view);
        toast.success('All saved messages deleted');
      })
      .catch((e) => {
        console.error('Failed to delete saved messages', e);
        toast.error(`Failed to delete saved messages: ${describeError(e)}`);
      });
  }, []);

  /**
   * Открывает сохранённое сообщение в его собственном окне: тело лежит
   * отдельным файлом и читается по требованию, как и тело строки таблицы.
   *
   * Свой талон, а не общий с таблицей: окна два, и ответ на запрос из архива
   * не должен ни отменять открытие строки, ни отменяться им.
   */
  const handleOpenFavorite = useCallback((favorite: FavoriteInfo) => {
    const ticket = ++favoriteRequest.current;
    api
      .getFavorite(favorite.id)
      .then((message) => {
        if (favoriteRequest.current !== ticket) return;
        setSavedFavorite(favorite);
        setSavedMessage(message);
        setIsFavoritesModalOpen(false);
        setIsSavedModalOpen(true);
      })
      .catch((e) => {
        if (favoriteRequest.current !== ticket) return;
        console.error('Failed to load saved message', e);
        toast.error(`Failed to load saved message: ${describeError(e)}`);
      });
  }, []);

  return (
    <div
      className="bg-surface box-border content-stretch flex flex-col items-start justify-start p-0 relative w-full h-full"
      data-name="kafka-explorer-portfolio"
    >
      <HeaderDesktop
        selectedPartitions={selectedPartitions}
        onSelectPartitions={setSelectedPartitions}
        readMode={readMode}
        range={range}
        onReadChange={handleReadChange}
        topic={selectedTopic}
        clusterName={connectedName}
        cluster={connectedCluster}
        activeUserId={connectedUserId}
        isConnecting={isConnecting}
        onSelectUser={handleSelectUser}
        onManageUsers={() => connectedCluster && handleManageUsers(connectedCluster)}
        onClusterClick={handleClusterClick}
        filters={filters}
        onFiltersChange={setFilters}
        isSearching={isSearching}
        format={openSchema?.format ?? 'json'}
        onFormatChange={handleFormatChange}
        lens={openSchema?.lens ?? null}
        onLensChange={handleLensChange}
        onOpenSchema={handleOpenSchemaSettings}
        onRefresh={handleRefresh}
        onOpenFavorites={handleOpenFavorites}
        onProduce={handleOpenProduce}
        onOpenLink={handleOpenLink}
        crashLog={crashLog}
        onRevealCrashLog={handleRevealCrashLog}
      />

      <div className="box-border content-stretch flex flex-row items-start justify-start p-0 relative shrink-0 w-full flex-1 min-h-0 h-full">
        <TopicsPanel
          topics={topics}
          // Чьи это топики. Сменилась учётка — сменился и список: под другими
          // ACL видно другое, и прежний запрос в поле поиска относится уже не
          // к тому, что в нём лежит.
          scope={connectedClusterId && `${connectedClusterId}:${connectedUserId ?? ''}`}
          selectedTopic={selectedTopic}
          onTopicSelect={handleSelectTopic}
          onTopicInfo={handleTopicInfo}
          favoriteTopics={favoriteTopicNames}
          onToggleFavorite={handleToggleFavoriteTopic}
        />

        <div className="flex-1 min-h-0 flex flex-col h-full">
          {selectedTopic && (
            <>
              <MessagesPanel
                total={total}
                getRow={getRow}
                onRangeChanged={ensureRange}
                onSelectMessage={showMessageAt}
                isLoading={isLoadingMessages}
                version={version}
                // Обычная догрузка дописала бы в буфер НЕпросеянное, а там
                // после поиска лежат одни находки: «loaded» стало бы смесью
                // двух разных величин. Дочитать топик после поиска можно только
                // поиском же — или перечитав его с другим фильтром.
                canLoadMore={!!stats?.truncated && !searchedBuffer}
                truncated={!!stats?.truncated}
                // Пока идёт начальное чтение или поиск, воркер откажет ("a read
                // is already in progress") — кнопку не предлагаем вовсе.
                isLoadingMore={isLoadingMore || isLoadingMessages || isSearching}
                onLoadMore={handleLoadMore}
                hasFilter={hasQuery(filters)}
                isSearching={isSearching}
                searchedBuffer={searchedBuffer}
                scope={stats}
                onDeepSearch={handleDeepSearch}
                onStopSearch={handleStopSearch}
                sort={sort}
                onSortChange={setSort}
                highlight={linkedMessage}
                onHighlightSeen={clearHighlight}
              />
              <StatusBar stats={stats} />
            </>
          )}
        </div>
      </div>

      <MessageDetailsModal
        message={selectedMessage}
        open={isModalOpen}
        onOpenChange={setIsModalOpen}
        saved={openMessageSavedId !== null}
        onAddToFavorite={handleAddToFavorite}
        onRemoveFavorite={
          openMessageSavedId ? () => removeFavorite(openMessageSavedId) : undefined
        }
        onNavigate={handleNavigateMessage}
        onShare={handleShare}
        // Топик — единственное, чего в окне не видно нигде: партиция, офсет и
        // ключ у сообщения свои, а имя топика осталось в шапке под модалкой.
        description={
          selectedTopic ? (
            <>
              In topic <span className="text-brand">{selectedTopic.name}</span>
            </>
          ) : undefined
        }
        format={openSchema?.format}
      />

      {/* Ссылка на открытое сообщение — поверх его окна: делятся, не закрывая
          того, чем делятся. */}
      <ShareLinkModal
        url={shareLink}
        open={isShareOpen}
        onOpenChange={setIsShareOpen}
        description={
          selectedTopic && selectedMessage ? (
            <>
              <span className="text-dim">{connectedName ?? 'unnamed cluster'}</span>
              <span className="text-dim"> / </span>
              <span className="text-brand">{selectedTopic.name}</span>
              <span className="text-dim">
                {' '}
                · partition {selectedMessage.partition} · offset {selectedMessage.offset}
              </span>
            </>
          ) : undefined
        }
      />

      {/* Переход по ссылке. Не под условием подключения: по ссылке приходят с
          пустого приложения чаще, чем с открытого топика. */}
      <OpenLinkModal
        open={isOpenLinkOpen}
        onOpenChange={setIsOpenLinkOpen}
        incoming={incomingLink}
        connectedClusterId={connectedClusterId}
        connectedName={connectedName}
        openTopic={selectedTopic?.name ?? null}
        onFollow={followLink}
      />

      {/* Сохранённое сообщение — в своём окне: у него другое происхождение и
          другой формат тела, а топика, из которого оно взято, может не быть
          открытым вовсе. */}
      <SavedMessageModal
        favorite={savedFavorite}
        message={savedMessage}
        open={isSavedModalOpen}
        onOpenChange={setIsSavedModalOpen}
      />

      {/* Схема — та же, что применена к открытому топику: форма отправки
          предлагает те же типы, которыми таблица прямо сейчас читает, и
          второй раз ходить за ней на диск незачем. */}
      <ProduceMessageModal
        topic={selectedTopic}
        cluster={schemaCluster}
        schema={openSchema}
        open={isProduceModalOpen}
        onOpenChange={setIsProduceModalOpen}
      />

      <TopicConfigModal
        topic={configTopic}
        cluster={schemaCluster}
        open={isConfigModalOpen}
        onOpenChange={setIsConfigModalOpen}
        onSchemaChanged={handleSchemaChanged}
      />

      <TopicInfoModal
        topic={infoTopic}
        open={isTopicInfoOpen}
        onOpenChange={setIsTopicInfoOpen}
      />

      <ClusterArchiveModal
        open={isClusterArchiveModalOpen}
        onOpenChange={setIsClusterArchiveModalOpen}
        clusters={clusters}
        connectedClusterId={connectedClusterId}
        onDeleteCluster={handleDeleteCluster}
        onCreateNew={handleCreateNewCluster}
        onEditCluster={handleEditCluster}
        onConnectToCluster={handleConnectToCluster}
        onDisconnect={() => {
          disconnect().then(() => toast.info('Disconnected'));
        }}
      />

      <ClusterConfigModal
        open={isClusterConfigModalOpen}
        onOpenChange={setIsClusterConfigModalOpen}
        cluster={selectedCluster}
        mode={clusterConfigMode}
        onBack={handleBackToArchive}
        onSaved={handleClusterSaved}
        onConnect={connect}
        onManageUsers={(cluster) => handleManageUsers(cluster, true)}
        isConnected={!!selectedCluster && selectedCluster.id === connectedClusterId}
        onApply={handleApplyClusterSettings}
      />

      <ClusterUsersModal
        open={isClusterUsersModalOpen}
        onOpenChange={setIsClusterUsersModalOpen}
        cluster={usersModalCluster}
        activeUserId={usersModalClusterId === connectedClusterId ? connectedUserId : null}
        onChanged={handleClusterSaved}
        // Правка учётки, под которой мы сейчас подключены, — это смена
        // действующих кредов. Прежнее соединение при этом рвётся безусловно,
        // даже если новый пароль неверен: иначе чтение продолжало бы работать
        // по паролю, которого больше нет, и смена пароля выглядела бы фикцией.
        onReconnect={(user) => connectAsUser(user, { replace: true })}
        // Удалили учётку, на которой держалось подключение: сеанс аутентифицирован
        // паролем, которого больше нет, и оставлять его работающим — та же
        // фикция, что была со сменой пароля.
        onDisconnect={() => {
          disconnect().then(() => toast.info('Disconnected: the active user was deleted'));
        }}
        // Из настроек кластера сюда приходят через кнопку — значит есть куда
        // вернуться. Из шапки списком заведуют напрямую, и кнопки нет.
        onBack={
          usersModalFromConfig
            ? () => {
                setIsClusterUsersModalOpen(false);
                setIsClusterConfigModalOpen(true);
              }
            : undefined
        }
      />

      <FavoritesModal
        favorites={favorites}
        open={isFavoritesModalOpen}
        onOpenChange={setIsFavoritesModalOpen}
        onRemoveFavorite={handleRemoveFavorite}
        onClearFavorites={handleClearFavorites}
        onSelectMessage={handleOpenFavorite}
      />
    </div>
  );
}
