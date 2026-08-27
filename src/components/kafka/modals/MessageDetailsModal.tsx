import { useState } from 'react';
import { Button } from '../../ui/button';
import { Copy, Star } from 'lucide-react';
import { toast } from 'sonner';
import { Dialog, DialogHeader, DialogTitle, DialogDescription } from '../../ui/dialog';
import { DialogContentNoClose } from '../DialogContentNoClose';
import { KafkaMessage } from '../types';

interface MessageDetailsModalProps {
  message: KafkaMessage | null; 
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onAddToFavorite?: (message: KafkaMessage) => void;
}

export function MessageDetailsModal({ message, open, onOpenChange, onAddToFavorite }: MessageDetailsModalProps) {
  const [activeTab, setActiveTab] = useState<'payload' | 'headers'>('payload');

  const copyToClipboard = (text: string) => {
    navigator.clipboard.writeText(text);
    toast.success('Copied to clipboard');
  };

  const copyHeadersToClipboard = (headers: Record<string, string>) => {
    const headersText = Object.entries(headers)
      .map(([key, value]) => `${key}: ${value}`)
      .join('\n');
    copyToClipboard(headersText);
  };

  const formatJson = (jsonString: string) => {
    try {
      return JSON.stringify(JSON.parse(jsonString), null, 2);
    } catch {
      return jsonString;
    }
  };

  const handleCopy = () => {
    if (activeTab === 'payload') {
      copyToClipboard(message!.message);
    } else {
      copyHeadersToClipboard(message!.headers);
    }
  };

  const handleSave = () => {
    if (message && onAddToFavorite) {
      onAddToFavorite(message);
    }
  };

  if (!message) return null;

  const lines = formatJson(message.message).split('\n');

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContentNoClose className="max-w-4xl max-h-[90vh] bg-surface border-edge text-slate-50">
        <DialogHeader className="border-b border-edge pb-4">
          <DialogTitle className="font-mono text-soft text-lg">
            Message Details
          </DialogTitle>
          <DialogDescription className="font-mono text-soft text-sm">
            View details of the selected message
          </DialogDescription>
        </DialogHeader>
        
        <div className="space-y-6 overflow-auto">
          {/* Message Metadata */}
          <div className="grid grid-cols-2 gap-6">
            <div>
              <div className="font-mono text-sm text-soft mb-1">Partition</div>
              <div className="font-mono text-brand">{message.partition}</div>
            </div>
            <div>
              <div className="font-mono text-sm text-soft mb-1">Offset</div>
              <div className="font-mono text-brand">{message.offset}</div>
            </div>
            <div>
              <div className="font-mono text-sm text-soft mb-1">Key</div>
              <div className="font-mono text-slate-50">{message.key}</div>
            </div>
            <div>
              <div className="font-mono text-sm text-soft mb-1">Timestamp</div>
              <div className="font-mono text-slate-50">{message.timestamp}</div>
            </div>
          </div>
          
          {/* Content Section with Tab Buttons */}
          <div>
            <div className="flex items-center justify-between mb-4">
              <div className="flex gap-1">
                <Button
                  variant="ghost"
                  size="sm"
                  className={`font-mono px-3 py-1 h-auto ${
                    activeTab === 'payload' 
                      ? 'bg-brand text-surface hover:bg-brand-hover' 
                      : 'bg-transparent text-soft hover:bg-edge hover:text-slate-50'
                  }`}
                  onClick={() => setActiveTab('payload')}
                >
                  payload
                </Button>
                <Button
                  variant="ghost"
                  size="sm"
                  className={`font-mono px-3 py-1 h-auto ${
                    activeTab === 'headers' 
                      ? 'bg-brand text-surface hover:bg-brand-hover' 
                      : 'bg-transparent text-soft hover:bg-edge hover:text-slate-50'
                  }`}
                  onClick={() => setActiveTab('headers')}
                >
                  headers
                </Button>
              </div>
              <div className="flex gap-2">
                {onAddToFavorite && (
                  <Button 
                    variant="outline" 
                    size="sm"
                    className="bg-transparent border-edge text-soft hover:bg-edge hover:text-slate-50"
                    onClick={handleSave}
                  >
                    <Star className="size-4 mr-1" />
                    Save
                  </Button>
                )}
                <Button 
                  variant="outline" 
                  size="sm"
                  className="bg-transparent border-edge text-soft hover:bg-edge hover:text-slate-50"
                  onClick={handleCopy}
                >
                  <Copy className="size-4 mr-1" />
                  Copy
                </Button>
              </div>
            </div>
            
            <div className="bg-sunken border border-edge rounded-lg p-4 max-h-96 overflow-auto">
              {activeTab === 'payload' ? (
                <div className="flex gap-4">
                  {/* Line numbers */}
                  <div className="font-mono text-soft text-right leading-6 select-none">
                    {lines.map((_, index) => (
                      <div key={index}>{index + 1}</div>
                    ))}
                  </div>
                  
                  {/* JSON content */}
                  <div className="font-mono leading-6 flex-1">
                    {lines.map((line, index) => (
                      <div key={index}>
                        {line.split('').map((char, charIndex) => {
                          if (char === '"' && line.includes(':')) {
                            return <span key={charIndex} className="text-brand">{char}</span>;
                          }
                          if (char === '{' || char === '}' || char === '[' || char === ']' || char === ',' || char === ':') {
                            return <span key={charIndex} className="text-syntax-brace">{char}</span>;
                          }
                          if (/\d/.test(char) && !line.includes('"')) {
                            return <span key={charIndex} className="text-syntax-bracket">{char}</span>;
                          }
                          return <span key={charIndex} className="text-soft">{char}</span>;
                        })}
                      </div>
                    ))}
                  </div>
                </div>
              ) : (
                <div>
                  {Object.keys(message.headers).length > 0 ? (
                    <div className="space-y-2">
                      {Object.entries(message.headers).map(([key, value], index) => (
                        <div key={index} className="flex gap-4 font-mono leading-6">
                          <div className="text-brand min-w-0 flex-shrink-0">
                            {key}:
                          </div>
                          <div className="text-soft break-all">
                            {value}
                          </div>
                        </div>
                      ))}
                    </div>
                  ) : (
                    <div className="font-mono text-soft text-center py-8">
                      No headers found
                    </div>
                  )}
                </div>
              )}
            </div>
          </div>
        </div>
      </DialogContentNoClose>
    </Dialog>
  );
}