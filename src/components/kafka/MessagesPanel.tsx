import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { KafkaMessage } from './types';
import { Virtuoso } from 'react-virtuoso';

interface MessageRowProps {
  message: KafkaMessage; 
  onClick: () => void;
  gridTemplate: string;
}

function MessageRow({ message, onClick, gridTemplate }: MessageRowProps) {
  return (
    <div 
      className="border-b border-[#314158] cursor-pointer hover:bg-[#1e293b]"
      onClick={onClick}
    >
      <div
        className="grid gap-4 p-3 text-sm"
        style={{ gridTemplateColumns: gridTemplate }}
      >
        <div className="font-['Fira_Code:Retina',_sans-serif] text-[#ffb86a]">
          {message.partition}
        </div>
        <div className="font-['Fira_Code:Retina',_sans-serif] text-[#90a1b9] truncate">
          {message.key}
        </div>
        <div className="font-['Fira_Code:Retina',_sans-serif] text-[#90a1b9]">
          {message.offset}
        </div>
        <div className="font-['Fira_Code:Retina',_sans-serif] text-[#90a1b9] truncate">
          {message.timestamp}
        </div>
        <div className="font-['Fira_Code:Retina',_sans-serif] text-[#90a1b9] truncate">
          {message.message}
        </div>
      </div>
    </div>
  );
}

interface MessagesPanelProps {
  messages: KafkaMessage[];
  onSelectMessage: (message: KafkaMessage) => void;
  isLoading?: boolean;
}

export function MessagesPanel({ messages, onSelectMessage, isLoading }: MessagesPanelProps) {
  // column widths: [partition, key, offset, timestamp, message]
  const [colWidths, setColWidths] = useState<(string)[]>([
    '80px', '120px', '100px', '150px', '1fr'
  ]);

  const minPx = 60; // minimal width in px for resizable columns

  const gridTemplate = useMemo(() => colWidths.join(' '), [colWidths]);

  const dragging = useRef<{ index: number; startX: number; startW: number } | null>(null);

  const onMouseMove = useCallback((e: MouseEvent) => {
    if (!dragging.current) return;
    const { index, startX, startW } = dragging.current;
    const dx = e.clientX - startX;
    const newW = Math.max(minPx, startW + dx);
    setColWidths(prev => {
      const next = [...prev];
      next[index] = `${newW}px`;
      return next;
    });
  }, []);

  const stopDragging = useCallback(() => {
    if (!dragging.current) return;
    dragging.current = null;
    window.removeEventListener('mousemove', onMouseMove);
    window.removeEventListener('mouseup', stopDragging);
  }, [onMouseMove]);

  const startDragging = useCallback((index: number, e: React.MouseEvent) => {
    // do not allow dragging for the last column (message) which is 1fr by default
    if (index >= colWidths.length - 1) return;
    const target = e.currentTarget as HTMLDivElement;
    const rect = target.parentElement?.getBoundingClientRect();
    // compute current pixel width of the column
    // We trust colWidths[index] if it's in px; otherwise read from DOM
    let startW = 0;
    const current = colWidths[index];
    if (current.endsWith('px')) {
      startW = parseInt(current, 10) || 0;
    }
    if (!startW && rect) {
      // fallback: approximate by dividing header cell offsetWidth
      const headerCell = target.parentElement as HTMLElement;
      startW = headerCell.offsetWidth;
    }
    dragging.current = { index, startX: e.clientX, startW };
    window.addEventListener('mousemove', onMouseMove);
    window.addEventListener('mouseup', stopDragging);
    e.preventDefault();
    e.stopPropagation();
  }, [colWidths, onMouseMove, stopDragging]);

  useEffect(() => {
    return () => {
      window.removeEventListener('mousemove', onMouseMove);
      window.removeEventListener('mouseup', stopDragging);
    };
  }, [onMouseMove, stopDragging]);

  return (
    <div className="flex-1 min-h-0 flex flex-col h-full">
      {/* Header */}
      <div className="border-b border-[#314158] bg-[#0f172b]">
        <div
          className="grid gap-4 p-3 text-sm font-['Fira_Code:Retina',_sans-serif] text-[#90a1b9] select-none"
          style={{ gridTemplateColumns: gridTemplate }}
        >
          {["partition", "key", "offset", "timestamp", "message"].map((label, i) => (
            <div key={label} className="relative">
              <div>{label}</div>
              {/* Resizer handle for all except the last column */}
              {i < colWidths.length - 1 && (
                <div
                  onMouseDown={(e) => startDragging(i, e)}
                  className="absolute top-0 right-[-8px] h-full w-4 cursor-col-resize"
                  style={{
                    // create a visible thin line centered in the 4px handle
                    // using a pseudo-line via background gradient
                    backgroundImage:
                      'linear-gradient(to right, transparent 7px, #314158 7px, #314158 8px, transparent 8px)'
                  }}
                  title="Drag to resize"
                />
              )}
            </div>
          ))}
        </div>
      </div>
      
      {/* Messages */}
      <div className="flex-1 min-h-0 overflow-hidden h-full">
        {isLoading && (
          <div className="p-4 text-center text-[#90a1b9] font-['Fira_Code:Retina',_sans-serif]">Loading messages…</div>
        )}
        <Virtuoso
          style={{ height: '100%' }}
          data={messages}
          itemContent={(index, message) => (
            <MessageRow
              key={`${message.partition}-${message.offset}-${index}`}
              message={message}
              onClick={() => onSelectMessage(message)}
              gridTemplate={gridTemplate}
            />
          )}
          components={{
            EmptyPlaceholder: () => (!isLoading ? (
              <div className="p-4 text-center text-[#62748E] font-['Fira_Code:Retina',_sans-serif]">No messages</div>
            ) : null)
          }}
        />
      </div>
    </div>
  );
}