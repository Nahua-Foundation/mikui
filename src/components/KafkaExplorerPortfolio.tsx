import { useCallback, useEffect, useRef, useState } from 'react';
import { toast } from 'sonner';
import {
  Topic,
  FavoriteMessage,
  FullMessage,
  MessageFilter,
  KafkaCluster,
  OpenTopicResult,
  EMPTY_FILTER,
} from './kafka';
import { mockClusters } from './kafka';
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

/** Сколько сообщений вычитывать в буфер Rust при открытии топика. */
const READ_LIMIT = 50_000;
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

  // Растёт при любой смене содержимого списка. Сбрасывает кэш окон и
  // обесценивает ответы на устаревшие запросы.
  const [generation, setGeneration] = useState(0);
  const { ensureRange, getRow, version } = useMessageWindow(generation);

  const [clusters, setClusters] = useState<KafkaCluster[]>(mockClusters);
  const [selectedCluster, setSelectedCluster] = useState<KafkaCluster | null>(null);
  const [clusterConfigMode, setClusterConfigMode] = useState<ClusterConfigMode>('create');

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

    invoke<OpenTopicResult>('open_topic', {
      params: {
        topic: selectedTopic.name,
        start_from: 'newest',
        limit: READ_LIMIT,
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
          toast.info(`Loaded the ${result.loaded} most recent messages; the topic has more`);
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
        if (!cancelled) setIsLoadingMessages(false);
      });

    return () => {
      // Пользователь переключил топик, пока летел ответ — результат больше
      // не нужен. Раньше здесь была гонка: старый ответ перезаписывал новый.
      cancelled = true;
    };
    // filters здесь намеренно не в зависимостях: их применяет set_filter,
    // без повторного чтения из Kafka.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedTopic, selectedPartition]);

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

  const handleConnectToCluster = useCallback((cluster: KafkaCluster) => {
    setClusters((prev) =>
      prev.map((c) => ({
        ...c,
        isActive: c.id === cluster.id,
        lastUsed: c.id === cluster.id ? new Date().toISOString() : c.lastUsed,
      })),
    );
    toast.success(`Connected to ${cluster.name}`);
  }, []);

  const handleBackToArchive = useCallback(() => {
    setIsClusterConfigModalOpen(false);
    setIsClusterArchiveModalOpen(true);
  }, []);

  const handleSaveCluster = useCallback(
    (clusterData: Partial<KafkaCluster>) => {
      if (clusterConfigMode === 'create') {
        setClusters((prev) => [
          ...prev,
          {
            id: Date.now().toString(),
            name: clusterData.name || 'Untitled Cluster',
            brokers: clusterData.brokers || 'localhost:9092',
            securityProtocol: clusterData.securityProtocol || 'PLAINTEXT',
            saslMechanism: clusterData.saslMechanism,
            username: clusterData.username,
            password: clusterData.password,
            sslCaBundlePath: clusterData.sslCaBundlePath,
            createdAt: new Date().toISOString(),
            isActive: false,
          },
        ]);
      } else if (selectedCluster) {
        setClusters((prev) =>
          prev.map((c) => (c.id === selectedCluster.id ? { ...c, ...clusterData } : c)),
        );
      }
    },
    [clusterConfigMode, selectedCluster],
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
        onClustersChange={setClusters}
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
        onSave={handleSaveCluster}
        onConnected={(loaded: Topic[]) => {
          setTopics(loaded);
          setSelectedTopic(null);
        }}
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
