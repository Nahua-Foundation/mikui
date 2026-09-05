import { useCallback, useEffect, useRef, useState } from 'react';
import { toast } from 'sonner';
import {
  BodyFormat,
  LensSetting,
  Topic,
  ClusterConnectPayload,
  ClusterUser,
  FavoriteInfo,
  FavoritesView,
  FullMessage,
  MessageFilter,
  KafkaCluster,
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
import { ProduceMessageModal } from './kafka/modals/ProduceMessageModal';
import { useMessageWindow } from './kafka/useMessageWindow';
import { invoke } from '@tauri-apps/api/core';

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

/** Не чаще одного тоста об ошибке декодирования за это время. Сообщения чужого
 *  формата идут полосой, и без паузы каждое окно выдачи заливало бы экран. */
const DECODE_ERROR_TOAST_MS = 5000;

const isSuperseded = (e: unknown) => String(e).includes(READ_SUPERSEDED);

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
  const [filters, setFilters] = useState<MessageFilter>(EMPTY_FILTER);
  /** Сортировка по клику на заголовок колонки. `null` — обычный порядок
   *  чтения из `readMode`. */
  const [sort, setSort] = useState<SortSpec | null>(null);
  /** Архив сохранённых сообщений вместе с занятым местом. Живёт на диске;
   *  здесь только последний отданный бэкендом снимок. */
  const [favorites, setFavorites] = useState<FavoritesView>(EMPTY_FAVORITES);
  const [topics, setTopics] = useState<Topic[]>([]);

  const [total, setTotal] = useState(0);
  const [stats, setStats] = useState<OpenTopicResult | null>(null);
  const [isLoadingMessages, setIsLoadingMessages] = useState(false);
  const [isLoadingMore, setIsLoadingMore] = useState(false);

  // Растёт, когда меняется САМ СПИСОК: другой топик, партиция, направление
  // чтения, фильтр. Сбрасывает кэш окон и обесценивает ответы на устаревшие
  // запросы. Фоновая догрузка сюда не относится — она только дописывает строки
  // в конец, и сбрасывать из-за неё кэш нельзя (см. useMessageWindow).
  const [generation, setGeneration] = useState(0);

  /** Схема ОТКРЫТОГО топика. Нужна модалке, чтобы знать, в каком виде приехало
   *  тело; всем остальным заведует бэкенд. */
  const [openSchema, setOpenSchema] = useState<TopicSchema | null>(null);

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

  const partitionsKey = selectedPartitions ? selectedPartitions.join(',') : 'all';
  const rangeKey = `${readMode}:${range.from_offset}:${range.to_offset}:${range.from_timestamp}:${range.to_timestamp}`;

  /** Под каким ключом искать схемы топиков этого подключения. */
  const schemaCluster = clusterKey(connectedClusterId, connectedName);

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
    setIsLoadingMessages(true);
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
        setOpenSchema(schema);
        return invoke<OpenTopicResult>('open_topic', {
          params: {
            topic: selectedTopic.name,
            // Границы задают направление сами (см. `ReadRange::newest_first`),
            // и когда они есть, это поле бэкенду не указ.
            start_from: readMode === 'newest' ? 'newest' : 'oldest',
            limit: DEFAULT_PARTITION_LIMIT,
            partitions: selectedPartitions,
            filter: filters,
            range,
            sort,
          },
        });
      })
      .then((result) => {
        if (cancelled) return;
        setTotal(result.total);
        setStats(result);
        if (result.truncated) {
          toast.info(`Loaded ${result.loaded} messages; the topic has more`);
        }
      })
      .catch((e) => {
        if (cancelled || isSuperseded(e)) return;
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
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedTopic, partitionsKey, rangeKey]);

  // Опрос хода чтения — общий для открытия топика и для "Load more".
  //
  // Раньше он жил внутри эффекта открытия, и во время "Load more" таблица
  // стояла мёртвой до самого конца чтения: кнопка светила «Loading…» полминуты
  // и выглядела как зависшая, хотя строки на бэкенде уже публиковались.
  //
  // Чтение публикует строки только в хвост, поэтому generation здесь НЕ
  // трогаем — кэш окон остаётся валидным (см. useMessageWindow).
  useEffect(() => {
    if (!isLoadingMessages && !isLoadingMore) return;

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
          });
        })
        .catch(() => {});
    }, PROGRESS_POLL_MS);

    return () => window.clearInterval(timer);
  }, [isLoadingMessages, isLoadingMore]);

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
      invoke<number>('set_filter', { filter: filters })
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
  }, [filters, selectedTopic]);

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
  const disconnect = useCallback(async () => {
    await api.clusterDisconnect().catch((e) => console.error('Disconnect failed', e));
    setConnectedClusterId(null);
    setConnectedUserId(null);
    setConnectedName(null);
    setTopics([]);
    setSelectedTopic(null);
  }, []);

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
        format={openSchema?.format ?? 'json'}
        onFormatChange={handleFormatChange}
        lens={openSchema?.lens ?? null}
        onLensChange={handleLensChange}
        onOpenSchema={handleOpenSchemaSettings}
        onRefresh={handleRefresh}
        onOpenFavorites={handleOpenFavorites}
        onProduce={handleOpenProduce}
      />

      <div className="box-border content-stretch flex flex-row items-start justify-start p-0 relative shrink-0 w-full flex-1 min-h-0 h-full">
        <TopicsPanel
          topics={topics}
          selectedTopic={selectedTopic}
          onTopicSelect={handleSelectTopic}
          onTopicInfo={handleTopicInfo}
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
                canLoadMore={!!stats?.truncated}
                // Пока идёт начальное чтение, воркер откажет ("a read is already
                // in progress") — кнопку не предлагаем вовсе.
                isLoadingMore={isLoadingMore || isLoadingMessages}
                onLoadMore={handleLoadMore}
                sort={sort}
                onSortChange={setSort}
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
        format={openSchema?.format}
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
