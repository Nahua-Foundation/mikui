import { useState } from 'react';
import { ChevronDown, Filter, RefreshCw, Play, Pause, Folder } from 'lucide-react';
import { Button } from '../ui/button';
import { DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuTrigger } from '../ui/dropdown-menu';
import { Input } from '../ui/input';
import { Label } from '../ui/label';
import { Topic, MessageFilters } from './types';
import { Tab } from './components/Tab';
import { MenuItem } from './components/MenuItem';

interface HeaderDesktopProps {
  selectedTopic: string | null; 
  selectedPartition: number | null;
  onSelectPartition: (partition: number | null) => void;
  topic: Topic | null;
  onClusterClick: () => void;
  filters: MessageFilters;
  onFiltersChange: (filters: MessageFilters) => void;
  onRefresh: () => void;
  isStreaming: boolean;
  onToggleStream: () => void;
  onOpenFavorites: () => void;
}

export function HeaderDesktop({ 
  selectedTopic, 
  selectedPartition, 
  onSelectPartition, 
  topic, 
  onClusterClick, 
  filters, 
  onFiltersChange, 
  onRefresh, 
  isStreaming, 
  onToggleStream, 
  onOpenFavorites 
}: HeaderDesktopProps) {
  const partitions = topic ? Array.from({ length: topic.partitions }, (_, i) => i) : [];
  const [localFilters, setLocalFilters] = useState<MessageFilters>(filters);

  const handleApplyFilters = () => {
    onFiltersChange(localFilters);
  };

  const handleClearFilters = () => {
    const emptyFilters = { key: '', message: '' };
    setLocalFilters(emptyFilters);
    onFiltersChange(emptyFilters);
  };

  const hasActiveFilters = filters.key.trim() !== '' || filters.message.trim() !== '';

  return (
    <div
      className="box-border content-stretch flex flex-row items-center justify-between p-0 relative shrink-0 w-full border-b border-[#314158]"
      data-name="header desktop"
    >
      <div className="box-border content-stretch flex flex-row gap-32 items-center justify-start p-0 relative shrink-0">
        <div className="box-border content-stretch flex flex-row items-center justify-start p-0 relative shrink-0">
          {/* Connection Status Indicator */}
          <div className="flex items-center justify-center px-4 py-4">
            <div className="relative">
              <div className="w-2 h-2 bg-green-500 rounded-full"></div>
              <div className="absolute inset-0 w-2 h-2 bg-green-500 rounded-full animate-pulse opacity-75"></div>
              <div className="absolute inset-0 w-2 h-2 bg-green-500 rounded-full shadow-[0_0_6px_rgba(34,197,94,0.6)]"></div>
            </div>
          </div>
          <MenuItem><Tab title="cluster" onClick={onClusterClick} /></MenuItem>
        </div>
      </div>
      
      <div className="box-border content-stretch flex flex-row items-center justify-start p-0 relative shrink-0">
        {/* Partitions Dropdown */}
        {topic && (
          <MenuItem borderSide="none">
            <div className="box-border content-stretch flex flex-row gap-2.5 items-center justify-center px-8 py-4 relative shrink-0">
              <DropdownMenu>
                <DropdownMenuTrigger className="font-['Fira_Code:Retina',_sans-serif] font-[450] text-[16px] text-[#90a1b9] hover:text-slate-50 bg-transparent hover:bg-transparent p-0 h-auto gap-2 flex items-center border-none outline-none cursor-pointer">
                  <span>partitions: {selectedPartition !== null ? selectedPartition : 'all'}</span>
                  <ChevronDown className="size-4" />
                </DropdownMenuTrigger>
                <DropdownMenuContent 
                  className="bg-[#0f172b] border-[#314158] min-w-32"
                  align="end"
                >
                  <DropdownMenuItem 
                    className={`font-['Fira_Code:Retina',_sans-serif] cursor-pointer ${
                      selectedPartition === null 
                        ? 'bg-[#314158] text-slate-50' 
                        : 'text-[#90a1b9] hover:bg-[#314158] hover:text-slate-50'
                    }`}
                    onClick={() => onSelectPartition(null)}
                  >
                    all
                  </DropdownMenuItem>
                  {partitions.map((partition) => (
                    <DropdownMenuItem 
                      key={partition}
                      className={`font-['Fira_Code:Retina',_sans-serif] cursor-pointer ${
                        selectedPartition === partition 
                          ? 'bg-[#314158] text-slate-50' 
                          : 'text-[#90a1b9] hover:bg-[#314158] hover:text-slate-50'
                      }`}
                      onClick={() => onSelectPartition(partition)}
                    >
                      {partition}
                    </DropdownMenuItem>
                  ))}
                </DropdownMenuContent>
              </DropdownMenu>
            </div>
          </MenuItem>
        )}

        {/* Filters Dropdown */}
        {topic && (
          <MenuItem borderSide="none">
            <div className="box-border content-stretch flex flex-row gap-2.5 items-center justify-center px-8 py-4 relative shrink-0">
              <DropdownMenu>
                <DropdownMenuTrigger className={`font-['Fira_Code:Retina',_sans-serif] font-[450] text-[16px] hover:text-slate-50 bg-transparent hover:bg-transparent p-0 h-auto gap-2 flex items-center border-none outline-none cursor-pointer ${
                  hasActiveFilters ? 'text-[#ffb86a]' : 'text-[#90a1b9]'
                }`}>
                  <Filter className="size-4" />
                  <span>filters</span>
                  <ChevronDown className="size-4" />
                </DropdownMenuTrigger>
                <DropdownMenuContent 
                  className="bg-[#0f172b] border-[#314158] min-w-80 p-4"
                  align="end"
                >
                  <div className="space-y-4">
                    <div className="space-y-2">
                      <Label className="font-['Fira_Code:Retina',_sans-serif] text-sm text-[#90a1b9]">
                        Key
                      </Label>
                      <Input
                        value={localFilters.key}
                        onChange={(e) => setLocalFilters(prev => ({ ...prev, key: e.target.value }))}
                        placeholder="Filter by key..."
                        className="bg-[#0f172b] border-[#314158] text-slate-50 font-['Fira_Code:Retina',_sans-serif] placeholder:text-[#62748E]"
                      />
                    </div>
                    <div className="space-y-2">
                      <Label className="font-['Fira_Code:Retina',_sans-serif] text-sm text-[#90a1b9]">
                        Message
                      </Label>
                      <Input
                        value={localFilters.message}
                        onChange={(e) => setLocalFilters(prev => ({ ...prev, message: e.target.value }))}
                        placeholder="Filter by message content..."
                        className="bg-[#0f172b] border-[#314158] text-slate-50 font-['Fira_Code:Retina',_sans-serif] placeholder:text-[#62748E]"
                      />
                    </div>
                    <div className="flex gap-2 pt-2">
                      <Button
                        onClick={handleClearFilters}
                        variant="outline"
                        size="sm"
                        className="flex-1 bg-transparent border-[#314158] text-[#90a1b9] hover:bg-[#314158] hover:text-slate-50 font-['Fira_Code:Retina',_sans-serif]"
                      >
                        Clear
                      </Button>
                      <Button
                        onClick={handleApplyFilters}
                        size="sm"
                        className="flex-1 bg-[#ffb86a] text-[#0f172b] hover:bg-[#e5a860] font-['Fira_Code:Retina',_sans-serif]"
                      >
                        Apply
                      </Button>
                    </div>
                  </div>
                </DropdownMenuContent>
              </DropdownMenu>
            </div>
          </MenuItem>
        )}

        {/* Favorites Button */}
        {topic && (
          <MenuItem borderSide="none">
            <div className="box-border content-stretch flex flex-row gap-2.5 items-center justify-center px-4 py-4 relative shrink-0">
              <button
                onClick={onOpenFavorites}
                className="text-[#90a1b9] hover:text-[#ffb86a] bg-transparent border-none outline-none cursor-pointer p-0 transition-colors"
                title="Favorites"
              >
                <Folder className="size-4" />
              </button>
            </div>
          </MenuItem>
        )}

        {/* Stream Button */}
        {topic && (
          <MenuItem borderSide="none">
            <div className="box-border content-stretch flex flex-row gap-2.5 items-center justify-center px-4 py-4 relative shrink-0">
              <button
                onClick={onToggleStream}
                className={`bg-transparent border-none outline-none cursor-pointer p-0 transition-colors ${
                  isStreaming 
                    ? 'text-[#ffb86a]' 
                    : 'text-[#90a1b9] hover:text-[#ffb86a]'
                }`}
                title={isStreaming ? "Stop streaming" : "Start streaming"}
              >
                {isStreaming ? <Pause className="size-4" /> : <Play className="size-4" />}
              </button>
            </div>
          </MenuItem>
        )}

        {/* Refresh Button */}
        {topic && (
          <MenuItem borderSide="none">
            <div className="box-border content-stretch flex flex-row gap-2.5 items-center justify-center px-4 py-4 relative shrink-0">
              <button
                onClick={onRefresh}
                className="text-[#90a1b9] hover:text-[#ffb86a] bg-transparent border-none outline-none cursor-pointer p-0 transition-colors"
                title="Refresh messages"
              >
                <RefreshCw className="size-4" />
              </button>
            </div>
          </MenuItem>
        )}
      </div>
    </div>
  );
}