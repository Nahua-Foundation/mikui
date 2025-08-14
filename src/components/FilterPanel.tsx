import { Topic } from './KafkaExplorer';
import { Button } from './ui/button';
import { Card } from './ui/card';
import { Badge } from './ui/badge';
import { Filter } from 'lucide-react';

interface FilterPanelProps {
  selectedPartition: number | null;
  onSelectPartition: (partition: number | null) => void;
  topic: Topic | null;
}

export function FilterPanel({ selectedPartition, onSelectPartition, topic }: FilterPanelProps) {
  const partitions = topic ? Array.from({ length: topic.partitions }, (_, i) => i) : [];

  return (
    <Card className="m-4 p-4">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <Filter className="size-4" />
          <h3>Filters</h3>
        </div>
        
        <div className="flex items-center gap-2">
          <span className="text-sm text-muted-foreground">Partition:</span>
          
          <Button
            variant={selectedPartition === null ? "default" : "outline"}
            size="sm"
            onClick={() => onSelectPartition(null)}
          >
            All
          </Button>
          
          {partitions.map((partition) => (
            <Button
              key={partition}
              variant={selectedPartition === partition ? "default" : "outline"}
              size="sm"
              onClick={() => onSelectPartition(partition)}
            >
              {partition}
            </Button>
          ))}
        </div>
      </div>
      
      {topic && (
        <div className="mt-2 flex items-center gap-2">
          <span className="text-sm text-muted-foreground">Active topic:</span>
          <Badge variant="secondary">{topic.name}</Badge>
        </div>
      )}
    </Card>
  );
}