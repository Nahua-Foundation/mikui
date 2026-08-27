import { useState } from 'react';
import { Search, Edit } from 'lucide-react';
import { Input } from '../ui/input';
import { Topic } from './types';
import { Virtuoso } from 'react-virtuoso';

interface TopicItemProps {
  topic: Topic; 
  isSelected: boolean; 
  onClick: () => void;
  onConfigClick: (topic: Topic) => void;
}

function TopicItem({ topic, isSelected, onClick, onConfigClick }: TopicItemProps) {
  const handleEditClick = (e: React.MouseEvent) => {
    e.stopPropagation();
    onConfigClick(topic);
  };

  return (
    <div className="relative shrink-0 w-full cursor-pointer" data-name="folder" onClick={onClick}>
      <div className="flex flex-row items-center relative size-full">
        <div className="box-border content-stretch flex flex-row gap-1.5 items-center justify-start px-2 py-1 relative w-full">
          <button
            onClick={handleEditClick}
            className="size-4 text-dim hover:text-brand transition-colors cursor-pointer p-0 border-none bg-transparent"
          >
            <Edit className="size-4" />
          </button>
          <div className={`basis-0 font-mono font-[450] grow leading-[0] min-h-px min-w-px relative shrink-0 text-[14px] text-left ${
            isSelected ? 'text-slate-50' : 'text-soft hover:text-slate-50'
          }`}>
            <p className="block leading-[20px]" style={{whiteSpace: "nowrap"}}>{topic.name}</p>
          </div>
        </div>
      </div>
    </div>
  );
}

interface TopicsPanelProps {
  topics: Topic[];
  selectedTopic: Topic | null;
  onTopicSelect: (topic: Topic) => void;
  onTopicConfig: (topic: Topic) => void;
}

export function TopicsPanel({ topics, selectedTopic, onTopicSelect, onTopicConfig }: TopicsPanelProps) {
  const [topicFilter, setTopicFilter] = useState('');

  const filteredTopics = topicFilter ?
      topics.filter(topic =>
        topic.name.toLowerCase().includes(topicFilter.toLowerCase())
      ).
      sort((a, b) => a.name.length - b.name.length)
      :
      topics.sort((a, b) => a.name.localeCompare(b.name));

  return (
    <div className="w-[220px] flex flex-col h-full border-r border-edge">
      {/* Topics Filter */}
      <div className="p-2 border-b border-edge">
        <div className="relative">
          <Search className="absolute left-2 top-1/2 transform -translate-y-1/2 size-4 text-dim" />
          <Input
            value={topicFilter}
            onChange={(e) => setTopicFilter(e.target.value)}
            placeholder="Filter topics..."
            className="bg-surface border-edge text-slate-50 font-mono placeholder:text-dim pl-8"
          />
        </div>
      </div>
      
      {/* Topics List with Virtuoso */}
      <div className="flex-1 min-h-0">
        {filteredTopics.length === 0 && topicFilter ? (
          <div className="flex items-center justify-center py-4 text-dim font-mono text-sm">
            No topics match "{topicFilter}"
          </div>
        ) : (
          <Virtuoso
            className="h-full"
            totalCount={filteredTopics.length}
            itemContent={(index) => (
              <div
                key={filteredTopics[index].name}
                className="relative shrink-0 w-full p-2"
                data-name="topic item"
              >
                <TopicItem
                  topic={filteredTopics[index]}
                  isSelected={selectedTopic?.name === filteredTopics[index].name}
                  onClick={() => onTopicSelect(filteredTopics[index])}
                  onConfigClick={onTopicConfig}
                />
              </div>
            )}
          />
        )}
      </div>
    </div>
  );
}