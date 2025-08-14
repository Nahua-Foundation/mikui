import { useState } from 'react';
import { toast } from 'sonner';
import { Topic, KafkaMessage, FavoriteMessage, MessageFilters, KafkaCluster } from './kafka';
import { mockTopics, mockMessages, mockFavorites, mockClusters } from './kafka';
import { HeaderDesktop } from './kafka';
import { TopicsPanel } from './kafka';
import { MessagesPanel } from './kafka';
import { MessageDetailsModal } from './kafka';
import { TopicConfigModal } from './kafka';
import { ClusterConfigModal } from './kafka';
import { ClusterArchiveModal } from './kafka/modals/ClusterArchiveModal';
import { FavoritesModal } from './kafka';

type ClusterConfigMode = 'create' | 'edit';

export function KafkaExplorerPortfolio() {
  const [selectedTopic, setSelectedTopic] = useState<Topic | null>(null);
  const [selectedPartition, setSelectedPartition] = useState<number | null>(null);
  const [selectedMessage, setSelectedMessage] = useState<KafkaMessage | null>(null);
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [configTopic, setConfigTopic] = useState<Topic | null>(null);
  const [isConfigModalOpen, setIsConfigModalOpen] = useState(false);
  const [isClusterConfigModalOpen, setIsClusterConfigModalOpen] = useState(false);
  const [isClusterArchiveModalOpen, setIsClusterArchiveModalOpen] = useState(false);
  const [isFavoritesModalOpen, setIsFavoritesModalOpen] = useState(false);
  const [filters, setFilters] = useState<MessageFilters>({ key: '', message: '' });
  const [isStreaming, setIsStreaming] = useState(false);
  const [favorites, setFavorites] = useState<FavoriteMessage[]>(mockFavorites);
  
  // Cluster management state
  const [clusters, setClusters] = useState<KafkaCluster[]>(mockClusters);
  const [selectedCluster, setSelectedCluster] = useState<KafkaCluster | null>(null);
  const [clusterConfigMode, setClusterConfigMode] = useState<ClusterConfigMode>('create');

  const filteredMessages = mockMessages.filter(msg => {
    // Filter by partition
    if (selectedPartition !== null && msg.partition !== selectedPartition) {
      return false;
    }
    
    // Filter by key
    if (filters.key.trim() !== '' && !msg.key.toLowerCase().includes(filters.key.toLowerCase())) {
      return false;
    }
    
    // Filter by message content
    if (filters.message.trim() !== '' && !msg.message.toLowerCase().includes(filters.message.toLowerCase())) {
      return false;
    }
    
    return true;
  });

  const handleSelectMessage = (message: KafkaMessage) => {
    setSelectedMessage(message);
    setIsModalOpen(true);
  };

  const handleConfigClick = (topic: Topic) => {
    setConfigTopic(topic);
    setIsConfigModalOpen(true);
  };

  const handleClusterClick = () => {
    setIsClusterArchiveModalOpen(true);
  };

  const handleCreateNewCluster = () => {
    setSelectedCluster(null);
    setClusterConfigMode('create');
    setIsClusterConfigModalOpen(true);
  };

  const handleEditCluster = (cluster: KafkaCluster) => {
    setSelectedCluster(cluster);
    setClusterConfigMode('edit');
    setIsClusterConfigModalOpen(true);
  };

  const handleConnectToCluster = (cluster: KafkaCluster) => {
    // Update cluster status
    setClusters(prev => prev.map(c => ({
      ...c,
      isActive: c.id === cluster.id,
      lastUsed: c.id === cluster.id ? new Date().toISOString() : c.lastUsed
    })));
    
    toast.success(`Connected to ${cluster.name}`);
  };

  const handleBackToArchive = () => {
    setIsClusterConfigModalOpen(false);
    setIsClusterArchiveModalOpen(true);
  };

  const handleSaveCluster = (clusterData: Partial<KafkaCluster>) => {
    if (clusterConfigMode === 'create') {
      const newCluster: KafkaCluster = {
        id: Date.now().toString(),
        name: clusterData.name || 'Untitled Cluster',
        brokers: clusterData.brokers || 'localhost:9092',
        securityProtocol: clusterData.securityProtocol || 'PLAINTEXT',
        saslMechanism: clusterData.saslMechanism,
        username: clusterData.username,
        password: clusterData.password,
        keystorePath: clusterData.keystorePath,
        keystorePassword: clusterData.keystorePassword,
        truststorePath: clusterData.truststorePath,
        truststorePassword: clusterData.truststorePassword,
        createdAt: new Date().toISOString(),
        isActive: false
      };
      setClusters(prev => [...prev, newCluster]);
    } else if (selectedCluster) {
      setClusters(prev => prev.map(cluster => 
        cluster.id === selectedCluster.id 
          ? { ...cluster, ...clusterData }
          : cluster
      ));
    }
  };

  const handleFiltersChange = (newFilters: MessageFilters) => {
    setFilters(newFilters);
  };

  const handleRefresh = () => {
    toast.success('Messages refreshed');
    // В реальном приложении здесь был бы вызов API для обновления сообщений
  };

  const handleToggleStream = () => {
    setIsStreaming(!isStreaming);
    if (!isStreaming) {
      toast.success('Streaming started');
      // В реальном приложении здесь был бы запуск потокового чтения
    } else {
      toast.success('Streaming stopped');
      // В реальном приложении здесь была бы остановка потокового чтения
    }
  };

  const handleOpenFavorites = () => {
    setIsFavoritesModalOpen(true);
  };

  const handleAddToFavorite = (message: KafkaMessage) => {
    const newFavorite: FavoriteMessage = {
      ...message,
      topicName: selectedTopic?.name || '',
      savedAt: new Date().toISOString()
    };
    setFavorites(prev => [...prev, newFavorite]);
    toast.success('Message added to favorites');
  };

  const handleRemoveFavorite = (favorite: FavoriteMessage) => {
    setFavorites(prev => prev.filter(f => f.partition !== favorite.partition || f.offset !== favorite.offset));
    toast.success('Message removed from favorites');
  };

  return (
    <div className="bg-[#0f172b] box-border content-stretch flex flex-col items-start justify-start p-0 relative w-full h-full" data-name="kafka-explorer-portfolio">
      
      {/* Header */}
      <HeaderDesktop 
        selectedTopic={selectedTopic?.name || null} 
        selectedPartition={selectedPartition}
        onSelectPartition={setSelectedPartition}
        topic={selectedTopic}
        onClusterClick={handleClusterClick}
        filters={filters}
        onFiltersChange={handleFiltersChange}
        onRefresh={handleRefresh}
        isStreaming={isStreaming}
        onToggleStream={handleToggleStream}
        onOpenFavorites={handleOpenFavorites}
      />
      
      {/* Main Content */}
      <div className="box-border content-stretch flex flex-row items-start justify-start p-0 relative shrink-0 w-full flex-1">
        {/* Left Panel - Topics */}
        <TopicsPanel
          topics={mockTopics}
          selectedTopic={selectedTopic}
          onTopicSelect={setSelectedTopic}
          onTopicConfig={handleConfigClick}
        />
        
        {/* Center Panel - Messages Table */}
        <div className="flex-1 flex flex-col">
          {selectedTopic && (
            <MessagesPanel
              messages={filteredMessages}
              onSelectMessage={handleSelectMessage}
            />
          )}
        </div>
      </div>
      
      {/* Message Details Modal */}
      <MessageDetailsModal
        message={selectedMessage}
        open={isModalOpen}
        onOpenChange={setIsModalOpen}
        onAddToFavorite={handleAddToFavorite}
      />
      
      {/* Topic Configuration Modal */}
      <TopicConfigModal
        topic={configTopic}
        open={isConfigModalOpen}
        onOpenChange={setIsConfigModalOpen}
      />

      {/* Cluster Archive Modal */}
      <ClusterArchiveModal
        open={isClusterArchiveModalOpen}
        onOpenChange={setIsClusterArchiveModalOpen}
        clusters={clusters}
        onClustersChange={setClusters}
        onCreateNew={handleCreateNewCluster}
        onEditCluster={handleEditCluster}
        onConnectToCluster={handleConnectToCluster}
      />
      
      {/* Cluster Configuration Modal */}
      <ClusterConfigModal
        open={isClusterConfigModalOpen}
        onOpenChange={setIsClusterConfigModalOpen}
        cluster={selectedCluster}
        mode={clusterConfigMode}
        onBack={handleBackToArchive}
        onSave={handleSaveCluster}
      />
      
      {/* Favorites Modal */}
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