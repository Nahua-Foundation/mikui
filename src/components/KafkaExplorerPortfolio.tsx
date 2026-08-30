import { useCallback, useEffect, useRef, useState } from 'react';
import { toast } from 'sonner';
import {
  Topic,
  ClusterConnectPayload,
  ClusterUser,
  FavoriteMessage,
  FullMessage,
  MessageFilter,
  KafkaCluster,
  OpenTopicResult,
  StartFrom,
  EMPTY_FILTER,
} from './kafka';
import * as api from './kafka/api';
import { HeaderDesktop } from './kafka';
import { TopicsPanel } from './kafka';
import { MessagesPanel } from './kafka';
import { MessageDetailsModal } from './kafka';
import { TopicConfigModal } from './kafka';
import { ClusterConfigModal } from './kafka';
import { ClusterArchiveModal } from './kafka/modals/ClusterArchiveModal';
import { ClusterUsersModal } from './kafka/modals/ClusterUsersModal';
import { FavoritesModal } from './kafka';
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

const isSuperseded = (e: unknown) => String(e).includes(READ_SUPERSEDED);

/** Пустое или невнятное `e` превратилось бы в тост «Failed to …: », который
 *  ничего не сообщает и выглядит как поломка самого приложения. */
function describeError(e: unknown): string {
  const text = e instanceof Error ? e.message : String(e ?? '');
  return text.trim() || 'unknown error';
}

export function KafkaExplorerPortfolio() {
  const [selectedTopic, setSelectedTopic] = useState<Topic | null>(null);
  /** Из каких партиций читаем. null — из всех. */
  const [selectedPartitions, setSelectedPartitions] = useState<number[] | null>(null);
  /** С какого конца топика читать. Определяет и порядок строк в таблице, и
   *  направление, в котором догружаются следующие порции. */
  const [startFrom, setStartFrom] = useState<StartFrom>('newest');
  const [selectedMessage, setSelectedMessage] = useState<FullMessage | null>(null);
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [configTopic, setConfigTopic] = useState<Topic | null>(null);
  const [isConfigModalOpen, setIsConfigModalOpen] = useState(false);
  const [isClusterConfigModalOpen, setIsClusterConfigModalOpen] = useState(false);
  const [isClusterArchiveModalOpen, setIsClusterArchiveModalOpen] = useState(false);
  const [isClusterUsersModalOpen, setIsClusterUsersModalOpen] = useState(false);
  const [isFavoritesModalOpen, setIsFavoritesModalOpen] = useState(false);
  const [filters, setFilters] = useState<MessageFilter>(EMPTY_FILTER);
  const [favorites, setFavorites] = useState<FavoriteMessage[]>([]);
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
  const { ensureRange, getRow, version } = useMessageWindow(generation, total);

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

  const partitionsKey = selectedPartitions ? selectedPartitions.join(',') : 'all';

  // Открытие топика: вычитка в буфер Rust. Наружу приезжают только счётчики,
  // сами строки подтягиваются окнами по мере прокрутки.
  useEffect(() => {
    if (!selectedTopic) {
      setTotal(0);
      setStats(null);
      invoke('close_topic').catch(() => {});
      return;
    }

    let cancelled = false;
    setIsLoadingMessages(true);
    // Список меняется целиком — обнуляем его ДО того, как приедут новые строки,
    // иначе Virtuoso успеет отрисовать чужие данные под новым топиком.
    setTotal(0);
    setGeneration((g) => g + 1);

    invoke<OpenTopicResult>('open_topic', {
      params: {
        topic: selectedTopic.name,
        start_from: startFrom,
        limit: DEFAULT_PARTITION_LIMIT,
        partitions: selectedPartitions,
        filter: filters,
      },
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
    // без повторного чтения из Kafka. А вот startFrom — в зависимостях:
    // сменить направление можно только перечитав топик с другого конца.
    //
    // Партиции — строкой, а не массивом: у массива каждый рендер новая
    // идентичность, и топик перечитывался бы на ровном месте.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedTopic, partitionsKey, startFrom]);

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
            truncated: p.truncated,
            read_bytes_per_sec: p.read_bytes_per_sec,
            peak_throttle_ms: p.peak_throttle_ms,
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
  const filterTimer = useRef<number | null>(null);
  useEffect(() => {
    if (!selectedTopic) return;
    if (filterTimer.current !== null) window.clearTimeout(filterTimer.current);

    filterTimer.current = window.setTimeout(() => {
      invoke<number>('set_filter', { filter: filters })
        .then((visible) => {
          setTotal(visible);
          setGeneration((g) => g + 1);
        })
        .catch((e) => console.error('set_filter failed', e));
    }, FILTER_DEBOUNCE_MS);

    return () => {
      if (filterTimer.current !== null) window.clearTimeout(filterTimer.current);
    };
  }, [filters, selectedTopic]);

  const handleSelectMessage = useCallback((index: number) => {
    invoke<FullMessage>('get_message_body', { index })
      .then((message) => {
        setSelectedMessage(message);
        setIsModalOpen(true);
      })
      .catch((e) => {
        console.error('Failed to load message body', e);
        toast.error('Failed to load message');
      });
  }, []);

  const handleConfigClick = useCallback((topic: Topic) => {
    setConfigTopic(topic);
    setIsConfigModalOpen(true);
  }, []);

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

  /** Выбор партиций осмыслен только для того топика, на котором сделан: у
   *  соседнего их может быть меньше, и чтение упёрлось бы в «out of range». */
  const handleSelectTopic = useCallback((topic: Topic | null) => {
    if (topicRef.current !== (topic?.name ?? null)) setSelectedPartitions(null);
    setSelectedTopic(topic);
  }, []);

  const handleRefresh = useCallback(() => {
    // Перечитываем топик с нуля, поднимая поколение.
    setSelectedTopic((t) => (t ? { ...t } : t));
  }, []);

  const handleOpenFavorites = useCallback(() => setIsFavoritesModalOpen(true), []);

  const handleAddToFavorite = useCallback(
    (message: FullMessage) => {
      setFavorites((prev) => [
        ...prev,
        { ...message, topicName: selectedTopic?.name || '', savedAt: new Date().toISOString() },
      ]);
      toast.success('Message added to favorites');
    },
    [selectedTopic],
  );

  const handleRemoveFavorite = useCallback((favorite: FavoriteMessage) => {
    setFavorites((prev) =>
      prev.filter((f) => f.partition !== favorite.partition || f.offset !== favorite.offset),
    );
    toast.success('Message removed from favorites');
  }, []);

  return (
    <div
      className="bg-surface box-border content-stretch flex flex-col items-start justify-start p-0 relative w-full h-full"
      data-name="kafka-explorer-portfolio"
    >
      <HeaderDesktop
        selectedPartitions={selectedPartitions}
        onSelectPartitions={setSelectedPartitions}
        startFrom={startFrom}
        onStartFromChange={setStartFrom}
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
        onRefresh={handleRefresh}
        onOpenFavorites={handleOpenFavorites}
        stats={stats}
      />

      <div className="box-border content-stretch flex flex-row items-start justify-start p-0 relative shrink-0 w-full flex-1 min-h-0 h-full">
        <TopicsPanel
          topics={topics}
          selectedTopic={selectedTopic}
          onTopicSelect={handleSelectTopic}
          onTopicConfig={handleConfigClick}
        />

        <div className="flex-1 min-h-0 flex flex-col h-full">
          {selectedTopic && (
            <MessagesPanel
              total={total}
              getRow={getRow}
              onRangeChanged={ensureRange}
              onSelectMessage={handleSelectMessage}
              isLoading={isLoadingMessages}
              version={version}
              canLoadMore={!!stats?.truncated}
              // Пока идёт начальное чтение, воркер откажет ("a read is already
              // in progress") — кнопку не предлагаем вовсе.
              isLoadingMore={isLoadingMore || isLoadingMessages}
              onLoadMore={handleLoadMore}
            />
          )}
        </div>
      </div>

      <MessageDetailsModal
        message={selectedMessage}
        open={isModalOpen}
        onOpenChange={setIsModalOpen}
        onAddToFavorite={handleAddToFavorite}
      />

      <TopicConfigModal
        topic={configTopic}
        open={isConfigModalOpen}
        onOpenChange={setIsConfigModalOpen}
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
        onSelectMessage={(message) => {
          setSelectedMessage(message);
          setIsFavoritesModalOpen(false);
          setIsModalOpen(true);
        }}
      />
    </div>
  );
}
