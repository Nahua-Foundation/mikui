import { Topic } from './KafkaExplorer';
import { Button } from './ui/button';
import { ScrollArea } from './ui/scroll-area';

interface TopicsListProps {
  topics: Topic[];
  selectedTopic: string | null;
  onSelectTopic: (topic: string) => void;
}

export function TopicsList({ topics, selectedTopic, onSelectTopic }: TopicsListProps) {
  return (
    <div className="flex-1 p-4">
      <h3 className="mb-4">Topics</h3>
      
      <ScrollArea className="h-full">
        <div className="space-y-2">
          {topics.map((topic) => (
            <Button
              key={topic.name}
              variant={selectedTopic === topic.name ? "default" : "ghost"}
              className="w-full justify-start"
              onClick={() => onSelectTopic(topic.name)}
            >
              <div className="flex flex-col items-start">
                <span>{topic.name}</span>
                <span className="text-xs text-muted-foreground">
                  {topic.partitions} partitions
                </span>
              </div>
            </Button>
          ))}
        </div>
      </ScrollArea>
    </div>
  );
}