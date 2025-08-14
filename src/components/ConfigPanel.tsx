import { Button } from './ui/button';
import { Input } from './ui/input';
import { Label } from './ui/label';
import { Settings } from 'lucide-react';
import { Card } from './ui/card';

export function ConfigPanel() {
  return (
    <Card className="m-4 p-4">
      <div className="flex items-center gap-2 mb-4">
        <Settings className="size-4" />
        <h3>Configure</h3>
      </div>
      
      <div className="space-y-3">
        <div>
          <Label htmlFor="broker-url">Broker URL</Label>
          <Input 
            id="broker-url"
            placeholder="localhost:9092"
            defaultValue="localhost:9092"
          />
        </div>
        
        <div>
          <Label htmlFor="group-id">Consumer Group ID</Label>
          <Input 
            id="group-id"
            placeholder="kafka-explorer-group"
            defaultValue="kafka-explorer-group"
          />
        </div>
        
        <Button className="w-full" disabled>
          Connect to Kafka
        </Button>
        
        <p className="text-sm text-muted-foreground">
          Backend integration required for real Kafka connection
        </p>
      </div>
    </Card>
  );
}