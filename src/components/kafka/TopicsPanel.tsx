import { useState } from 'react';
import { Search, Edit } from 'lucide-react';
import { Input } from '../ui/input';
import { Topic } from './types';

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
            className="size-4 text-[#62748E] hover:text-[#ffb86a] transition-colors cursor-pointer p-0 border-none bg-transparent"
          >
            <Edit className="size-4" />
          </button>
          <div className={`basis-0 font-['Fira_Code:Retina',_sans-serif] font-[450] grow leading-[0] min-h-px min-w-px relative shrink-0 text-[14px] text-left ${
            isSelected ? 'text-slate-50' : 'text-[#90a1b9] hover:text-slate-50'
          }`}>
            <p className="block leading-[20px]">{topic.name}</p>
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

  const filteredTopics = topics.filter(topic => 
    topic.name.toLowerCase().includes(topicFilter.toLowerCase())
  );

  return (
    <div className="w-[220px] flex flex-col h-full border-r border-[#314158]">
      {/* Topics Filter */}
      <div className="p-2 border-b border-[#314158]">
        <div className="relative">
          <Search className="absolute left-2 top-1/2 transform -translate-y-1/2 size-4 text-[#62748E]" />
          <Input
            value={topicFilter}
            onChange={(e) => setTopicFilter(e.target.value)}
            placeholder="Filter topics..."
            className="bg-[#0f172b] border-[#314158] text-slate-50 font-['Fira_Code:Retina',_sans-serif] placeholder:text-[#62748E] pl-8"
          />
        </div>
      </div>
      
      {/* Topics List with scroll */}
      <div className="flex-1 overflow-auto">
        <div className="box-border content-stretch flex flex-col gap-1 items-start justify-start p-2 relative w-full">
          {filteredTopics.map((topic) => (
            <div key={topic.name} className="relative shrink-0 w-full" data-name="topic item">
              <TopicItem
                topic={topic}
                isSelected={selectedTopic?.name === topic.name}
                onClick={() => onTopicSelect(topic)}
                onConfigClick={onTopicConfig}
              />
            </div>
          ))}
          {filteredTopics.length === 0 && topicFilter && (
            <div className="flex items-center justify-center py-4 text-[#62748E] font-['Fira_Code:Retina',_sans-serif] text-sm">
              No topics match "{topicFilter}"
            </div>
          )}
        </div>
      </div>
    </div>
  );
}