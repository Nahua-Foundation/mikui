import { useState } from 'react';
import { Button } from '../../ui/button';
import { Copy, Star } from 'lucide-react';
import { toast } from 'sonner';
import { Dialog, DialogHeader, DialogTitle, DialogDescription } from '../../ui/dialog';
import { DialogContentNoClose } from '../DialogContentNoClose';
import { FullMessage, MessageHeader } from '../types';

/** Потолок отрисовки. Тело на 10 МБ иначе положило бы вкладку на лопатки
 *  ещё до того, как пользователь что-то увидит. */
const MAX_RENDERED_LINES = 2000;

/** Токены JSON: ключ, строка, число, пунктуация. */
const JSON_TOKEN = /("(?:\\.|[^"\\])*")\s*:|("(?:\\.|[^"\\])*")|(-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)|([{}[\],:])/g;

/**
 * Раскрашивает строку JSON несколькими span-ами.
 *
 * Раньше здесь был `line.split('')` с одним `<span>` на КАЖДЫЙ символ: тело
 * в 100 КБ превращалось в сотню тысяч DOM-узлов и намертво вешало окно.
 * Теперь на строку приходится несколько узлов вместо сотни.
 */
function highlightLine(line: string) {
  const parts: React.ReactNode[] = [];
  let last = 0;
  let match: RegExpExecArray | null;

  JSON_TOKEN.lastIndex = 0;
  while ((match = JSON_TOKEN.exec(line)) !== null) {
    if (match.index > last) {
      parts.push(<span key={`t${last}`} className="text-soft">{line.slice(last, match.index)}</span>);
    }
    const [full, propertyKey, str, num, punct] = match;
    const key = `m${match.index}`;
    if (propertyKey !== undefined) {
      parts.push(<span key={key} className="text-brand">{propertyKey}</span>);
      parts.push(<span key={`${key}c`} className="text-syntax-brace">{full.slice(propertyKey.length)}</span>);
    } else if (str !== undefined) {
      parts.push(<span key={key} className="text-soft">{str}</span>);
    } else if (num !== undefined) {
      parts.push(<span key={key} className="text-syntax-bracket">{num}</span>);
    } else if (punct !== undefined) {
      parts.push(<span key={key} className="text-syntax-brace">{punct}</span>);
    }
    last = match.index + full.length;
  }

  if (last < line.length) {
    parts.push(<span key={`t${last}`} className="text-soft">{line.slice(last)}</span>);
  }
  return parts.length > 0 ? parts : <span className="text-soft">{line}</span>;
}

interface MessageDetailsModalProps {
  message: FullMessage | null;
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onAddToFavorite?: (message: FullMessage) => void;
}

export function MessageDetailsModal({ message, open, onOpenChange, onAddToFavorite }: MessageDetailsModalProps) {
  const [activeTab, setActiveTab] = useState<'payload' | 'headers'>('payload');

  const copyToClipboard = (text: string) => {
    navigator.clipboard.writeText(text);
    toast.success('Copied to clipboard');
  };

  const copyHeadersToClipboard = (headers: MessageHeader[]) => {
    copyToClipboard(headers.map((h) => `${h.key}: ${h.value}`).join('\n'));
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
      copyToClipboard(message!.value);
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

  const allLines = formatJson(message.value).split('\n');
  const lines = allLines.slice(0, MAX_RENDERED_LINES);
  const hiddenLines = allLines.length - lines.length;

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
              <div className="font-mono text-slate-50">{message.timestamp ? new Date(message.timestamp).toLocaleString() : '—'}</div>
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
                      <div key={index}>{highlightLine(line)}</div>
                    ))}
                    {hiddenLines > 0 && (
                      <div className="text-dim pt-2">
                        … {hiddenLines.toLocaleString()} more lines not rendered ({message.value_size.toLocaleString()} bytes total).
                        Use Copy to get the full payload.
                      </div>
                    )}
                  </div>
                </div>
              ) : (
                <div>
                  {message.headers.length > 0 ? (
                    <div className="space-y-2">
                      {message.headers.map(({ key, value }, index) => (
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