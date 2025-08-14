import { KafkaMessage } from './types';

interface MessageRowProps {
  message: KafkaMessage; 
  onClick: () => void;
}

function MessageRow({ message, onClick }: MessageRowProps) {
  return (
    <div 
      className="border-b border-[#314158] cursor-pointer hover:bg-[#1e293b]"
      onClick={onClick}
    >
      <div className="grid grid-cols-[80px_120px_100px_150px_1fr] gap-4 p-3 text-sm">
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
}

export function MessagesPanel({ messages, onSelectMessage }: MessagesPanelProps) {
  return (
    <div className="flex-1 flex flex-col">
      {/* Header */}
      <div className="border-b border-[#314158] bg-[#0f172b]">
        <div className="grid grid-cols-[80px_120px_100px_150px_1fr] gap-4 p-3 text-sm font-['Fira_Code:Retina',_sans-serif] text-[#90a1b9]">
          <div>partition</div>
          <div>key</div>
          <div>offset</div>
          <div>timestamp</div>
          <div>message</div>
        </div>
      </div>
      
      {/* Messages */}
      <div className="flex-1 overflow-auto">
        {messages.map((message, index) => (
          <MessageRow
            key={`${message.partition}-${message.offset}-${index}`}
            message={message}
            onClick={() => onSelectMessage(message)}
          />
        ))}
      </div>
    </div>
  );
}