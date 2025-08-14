import { useState } from 'react';
import { TopicsList } from './TopicsList';
import { MessageTable } from './MessageTable';
import { FilterPanel } from './FilterPanel';
import { ConfigPanel } from './ConfigPanel';

export interface KafkaMessage {
  partition: number;
  key: string;
  offset: number;
  message: string;
  timestamp: string;
}

export interface Topic {
  name: string;
  partitions: number;
}

const mockTopics: Topic[] = [
  { name: 'topic 1', partitions: 3 },
  { name: 'topic 2', partitions: 5 },
  { name: 'topic 3', partitions: 2 },
  { name: 'topic 4', partitions: 4 },
];

const mockMessages: KafkaMessage[] = [
  {
    partition: 0,
    key: 'user-123',
    offset: 1001,
    message: '{"userId": 123, "action": "login", "timestamp": "2025-08-14T10:30:00Z"}',
    timestamp: '2025-08-14T10:30:00Z'
  },
  {
    partition: 1,
    key: 'user-456',
    offset: 1002,
    message: '{"userId": 456, "action": "purchase", "productId": "prod-789", "amount": 99.99}',
    timestamp: '2025-08-14T10:31:15Z'
  },
  {
    partition: 0,
    key: 'user-789',
    offset: 1003,
    message: '{"userId": 789, "action": "logout", "sessionDuration": 1800}',
    timestamp: '2025-08-14T10:32:30Z'
  },
  {
    partition: 2,
    key: 'user-321',
    offset: 1004,
    message: '{"userId": 321, "action": "view_product", "productId": "prod-123"}',
    timestamp: '2025-08-14T10:33:45Z'
  },
];

export function KafkaExplorer() {
  const [selectedTopic, setSelectedTopic] = useState<string | null>(null);
  const [selectedPartition, setSelectedPartition] = useState<number | null>(null);
  const [messages, setMessages] = useState<KafkaMessage[]>(mockMessages);

  const filteredMessages = messages.filter(msg => {
    if (selectedPartition !== null && msg.partition !== selectedPartition) {
      return false;
    }
    return true;
  });

  return (
    <div className="h-screen flex bg-background">
      {/* Left Panel */}
      <div className="w-80 border-r border-border">
        <ConfigPanel />
        <TopicsList 
          topics={mockTopics}
          selectedTopic={selectedTopic}
          onSelectTopic={setSelectedTopic}
        />
      </div>
      
      {/* Right Panel */}
      <div className="flex-1 flex flex-col">
        <FilterPanel 
          selectedPartition={selectedPartition}
          onSelectPartition={setSelectedPartition}
          topic={selectedTopic ? mockTopics.find(t => t.name === selectedTopic) : null}
        />
        
        <div className="flex-1">
          <MessageTable 
            messages={filteredMessages}
            selectedTopic={selectedTopic}
          />
        </div>
      </div>
    </div>
  );
}