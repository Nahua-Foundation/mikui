import { useCallback, useEffect, useRef, useState } from 'react';
import { toast } from 'sonner';
import {
  Topic,
  ClusterConnectPayload,
  FavoriteMessage,
  FullMessage,
  MessageFilter,
  KafkaCluster,
  OpenTopicResult,
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

export function KafkaExplorerPortfolio() {
  const [selectedTopic, setSelectedTopic] = useState<Topic | null>(null);
  const [selectedPartition, setSelectedPartition] = useState<number | null>(null);
  const [selectedMessage, setSelectedMessage] = useState<FullMessage | null>(null);
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [configTopic, setConfigTopic] = useState<Topic | null>(null);
  const [isConfigModalOpen, setIsConfigModalOpen] = useState(false);
  const [isClusterConfigModalOpen, setIsClusterConfigModalOpen] = useState(false);
  const [isClusterArchiveModalOpen, setIsClusterArchiveModalOpen] = useState(false);
  const [isFavoritesModalOpen, setIsFavoritesModalOpen] = useState(false);
  const [filters, setFilters] = useState<MessageFilter>(EMPTY_FILTER);
  const [favorites, setFavorites] = useState<FavoriteMessage[]>([]);
  const [topics, setTopics] = useState<Topic[]>([]);

  const [total, setTotal] = useState(0);
  const [stats, setStats] = useState<OpenTopicResult | null>(null);
  const [isLoadingMessages, setIsLoadingMessages] = useState(false);
  const [isLoadingMore, setIsLoadingMore] = useState(false);

  // Растёт при любой смене содержимого списка. Сбрасывает кэш окон и
  // обесценивает ответы на устаревшие запросы.
  const [generation, setGeneration] = useState(0);
  const { ensureRange, getRow, version } = useMessageWindow(generation);

  const [clusters, setClusters] = useState<KafkaCluster[]>([]);
  const [selectedCluster, setSelectedCluster] = useState<KafkaCluster | null>(null);
  const [connectedClusterId, setConnectedClusterId] = useState<string | null>(null);
  const [clusterConfigMode, setClusterConfigMode] = useState<ClusterConfigMode>('create');

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

    // Пока open_topic не ответил финальным результатом, чтение уже может идти
    // прогрессивными шагами на бэкенде — опрашиваем прогресс, чтобы таблица
    // наполнялась по ходу дела, а не одним скачком в конце. Порядок строк
    // может ещё меняться до завершения (сортировка идёт по неполным данным),
    // поэтому каждый тик поднимает generation, сбрасывая кэш чанков.
    const progressTimer = window.setInterval(() => {
      api
        .getOpenTopicProgress()
        .then((p) => {
          if (cancelled || p.done) return;
          setTotal(p.total);
          setStats((prev) => ({
            total: p.total,
            loaded: p.loaded,
            buffer_bytes: prev?.buffer_bytes ?? 0,
            truncated: p.truncated,
          }));
          setGeneration((g) => g + 1);
        })
        .catch(() => {});
    }, PROGRESS_POLL_MS);

    invoke<OpenTopicResult>('open_topic', {
      params: {
        topic: selectedTopic.name,
        start_from: 'newest',
        limit: DEFAULT_PARTITION_LIMIT,
        partition: selectedPartition,
        filter: filters,
      },
    })
      .then((result) => {
        if (cancelled) return;
        setTotal(result.total);
        setStats(result);
        setGeneration((g) => g + 1);
        if (result.truncated) {
          toast.info(`Loaded ${result.loaded} messages; the topic has more`);
        }
      })
      .catch((e) => {
        if (cancelled) return;
        console.error('Failed to open topic', e);
        toast.error(`Failed to read topic: ${e}`);
        setTotal(0);
        setStats(null);
      })
      .finally(() => {
        window.clearInterval(progressTimer);
        if (!cancelled) setIsLoadingMessages(false);
      });

    return () => {
      // Пользователь переключил топик, пока летел ответ — результат больше
      // не нужен. Раньше здесь была гонка: старый ответ перезаписывал новый.
      cancelled = true;
      window.clearInterval(progressTimer);
    };
    // filters здесь намеренно не в зависимостях: их применяет set_filter,
    // без повторного чтения из Kafka.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedTopic, selectedPartition]);

  // "Загрузить ещё": продолжает уже открытый топик с того места, где
  // остановилось предыдущее чтение — без повторной вычитки уже показанного.
  // generation НЕ поднимаем: новые сообщения при сортировке всегда попадают
  // в хвост уже отображаемого порядка, позиции закэшированных строк не меняются.
  const handleLoadMore = useCallback(() => {
    setIsLoadingMore(true);
    api
      .loadMore({ additional: DEFAULT_PARTITION_LIMIT })
      .then((result) => {
        setTotal(result.total);
        setStats(result);
      })
      .catch((e) => {
        console.error('load_more failed', e);
        toast.error(`Failed to load more messages: ${e}`);
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
    setSelectedCluster(null);
    setClusterConfigMode('create');
    setIsClusterConfigModalOpen(true);
  }, []);

  const handleEditCluster = useCallback((cluster: KafkaCluster) => {
    setSelectedCluster(cluster);
    setClusterConfigMode('edit');
    setIsClusterConfigModalOpen(true);
  }, []);

  /**
   * Единственный путь подключения — им пользуются и кнопка Connect в форме,
   * и клик по строке в архиве. Раньше клик по архиву только красил строку и
   * показывал «Connected», ничего не подключая.
   */
  const connect = useCallback(async (payload: ClusterConnectPayload, label: string) => {
    try {
      await api.clusterConnect(payload);
      const loaded = await api.getTopics();
      setTopics(loaded);
      setSelectedTopic(null);
      setConnectedClusterId(payload.id ?? null);
      // last_used обновил бэкенд — перечитываем список, чтобы порядок совпадал.
      api.listClusters().then(setClusters).catch(() => {});
      toast.success(`Connected to ${label} · ${loaded.length} topics`);
    } catch (e) {
      console.error('Connect failed', e);
      toast.error(`Failed to connect: ${e}`);
      throw e;
    }
  }, []);

  const handleConnectToCluster = useCallback(
    (cluster: KafkaCluster) => {
      connect(api.clusterToPayload(cluster), cluster.name).catch(() => {});
    },
    [connect],
  );

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
        if (connectedClusterId === id) setConnectedClusterId(null);
        toast.success('Cluster deleted');
      } catch (e) {
        console.error('Failed to delete cluster', e);
        toast.error(`Failed to delete cluster: ${e}`);
      }
    },
    [connectedClusterId],
  );

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
        selectedPartition={selectedPartition}
        onSelectPartition={setSelectedPartition}
        topic={selectedTopic}
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
          onTopicSelect={setSelectedTopic}
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
              isLoadingMore={isLoadingMore}
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
