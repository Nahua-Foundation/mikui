import { useState } from 'react';
import { KafkaMessage } from './KafkaExplorer';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from './ui/table';
import { Button } from './ui/button';
import { Card } from './ui/card';
import { ScrollArea } from './ui/scroll-area';
import { Badge } from './ui/badge';
import { Eye, Copy } from 'lucide-react';
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogTrigger } from './ui/dialog';
import { toast } from 'sonner';

interface MessageTableProps {
  messages: KafkaMessage[];
  selectedTopic: string | null;
}

export function MessageTable({ messages, selectedTopic }: MessageTableProps) {
  const [selectedMessage, setSelectedMessage] = useState<KafkaMessage | null>(null);

  const copyToClipboard = (text: string) => {
    navigator.clipboard.writeText(text);
    toast.success('Copied to clipboard');
  };

  const formatJson = (jsonString: string) => {
    try {
      return JSON.stringify(JSON.parse(jsonString), null, 2);
    } catch {
      return jsonString;
    }
  };

  if (!selectedTopic) {
    return (
      <div className="flex items-center justify-center h-full text-muted-foreground">
        <p>Select a topic to view messages</p>
      </div>
    );
  }

  return (
    <Card className="m-4 flex-1 flex flex-col">
      <div className="p-4 border-b">
        <h3>Messages</h3>
        <p className="text-sm text-muted-foreground">
          Showing {messages.length} messages from {selectedTopic}
        </p>
      </div>
      
      <ScrollArea className="flex-1">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Partition</TableHead>
              <TableHead>Key</TableHead>
              <TableHead>Offset</TableHead>
              <TableHead>Timestamp</TableHead>
              <TableHead className="w-2/5">Message</TableHead>
              <TableHead className="w-24">Actions</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {messages.map((message, index) => (
              <TableRow key={`${message.partition}-${message.offset}-${index}`}>
                <TableCell>
                  <Badge variant="outline">{message.partition}</Badge>
                </TableCell>
                <TableCell className="font-mono text-sm">{message.key}</TableCell>
                <TableCell className="font-mono">{message.offset}</TableCell>
                <TableCell className="text-sm">
                  {new Date(message.timestamp).toLocaleString()}
                </TableCell>
                <TableCell className="max-w-xs">
                  <div className="truncate font-mono text-sm bg-muted p-2 rounded">
                    {message.message}
                  </div>
                </TableCell>
                <TableCell>
                  <div className="flex gap-1">
                    <Dialog>
                      <DialogTrigger asChild>
                        <Button 
                          variant="ghost" 
                          size="sm"
                          onClick={() => setSelectedMessage(message)}
                        >
                          <Eye className="size-4" />
                        </Button>
                      </DialogTrigger>
                      <DialogContent className="max-w-4xl max-h-[80vh]">
                        <DialogHeader>
                          <DialogTitle>Message Details</DialogTitle>
                        </DialogHeader>
                        <div className="space-y-4">
                          <div className="grid grid-cols-2 gap-4">
                            <div>
                              <label className="text-sm">Partition</label>
                              <p className="font-mono">{message.partition}</p>
                            </div>
                            <div>
                              <label className="text-sm">Offset</label>
                              <p className="font-mono">{message.offset}</p>
                            </div>
                            <div>
                              <label className="text-sm">Key</label>
                              <p className="font-mono">{message.key}</p>
                            </div>
                            <div>
                              <label className="text-sm">Timestamp</label>
                              <p className="font-mono">{message.timestamp}</p>
                            </div>
                          </div>
                          
                          <div>
                            <div className="flex items-center justify-between mb-2">
                              <label className="text-sm">Message Content</label>
                              <Button 
                                variant="outline" 
                                size="sm"
                                onClick={() => copyToClipboard(message.message)}
                              >
                                <Copy className="size-4 mr-1" />
                                Copy
                              </Button>
                            </div>
                            <ScrollArea className="h-64 w-full">
                              <pre className="text-sm bg-muted p-4 rounded font-mono">
                                {formatJson(message.message)}
                              </pre>
                            </ScrollArea>
                          </div>
                        </div>
                      </DialogContent>
                    </Dialog>
                    
                    <Button 
                      variant="ghost" 
                      size="sm"
                      onClick={() => copyToClipboard(message.message)}
                    >
                      <Copy className="size-4" />
                    </Button>
                  </div>
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </ScrollArea>
    </Card>
  );
}