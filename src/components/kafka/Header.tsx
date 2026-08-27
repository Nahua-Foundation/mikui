import { useState } from 'react';
import { ChevronDown, Filter, RefreshCw, Play, Pause, Folder, Search } from 'lucide-react';
import { Button } from '../ui/button';
import { DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuTrigger } from '../ui/dropdown-menu';
import { Input } from '../ui/input';
import { Label } from '../ui/label';
import { Topic, MessageFilters } from './types';
import { Tab } from './components/Tab';
import { MenuItem } from './components/MenuItem';

interface HeaderDesktopProps {
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
  const [searchQuery, setSearchQuery] = useState('');

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
      className="box-border content-stretch flex flex-row items-center justify-between p-0 relative shrink-0 w-full border-b border-edge"
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
        {/* Search field (left of partitions) */}
        {topic && (
          <MenuItem>
            <div className="box-border content-stretch flex flex-row gap-2.5 items-center justify-center px-4 py-4 relative shrink-0">
              <div className="relative">
                <Search className="absolute left-2 top-1/2 transform -translate-y-1/2 size-4 text-dim" />
                <Input
                  value={searchQuery}
                  onChange={(e) => setSearchQuery(e.target.value)}
                  placeholder="Search"
                  className="bg-surface border-edge text-slate-50 font-mono placeholder:text-dim pl-8 w-64"
                />
              </div>
            </div>
          </MenuItem>
        )}
        {/* Partitions Dropdown */}
        {topic && (
          <MenuItem>
            <div className="box-border content-stretch flex flex-row gap-2.5 items-center justify-center px-8 py-4 relative shrink-0">
              <DropdownMenu>
                <DropdownMenuTrigger className="font-mono font-[450] text-[16px] text-soft hover:text-slate-50 bg-transparent hover:bg-transparent p-0 h-auto gap-2 flex items-center border-none outline-none cursor-pointer">
                  <span>partitions: {selectedPartition !== null ? selectedPartition : 'all'}</span>
                  <ChevronDown className="size-4" />
                </DropdownMenuTrigger>
                <DropdownMenuContent 
                  className="bg-surface border-edge min-w-32"
                  align="end"
                >
                  <DropdownMenuItem 
                    className={`font-mono cursor-pointer ${
                      selectedPartition === null 
                        ? 'bg-edge text-slate-50' 
                        : 'text-soft hover:bg-edge hover:text-slate-50'
                    }`}
                    onClick={() => onSelectPartition(null)}
                  >
                    all
                  </DropdownMenuItem>
                  {partitions.map((partition) => (
                    <DropdownMenuItem 
                      key={partition}
                      className={`font-mono cursor-pointer ${
                        selectedPartition === partition 
                          ? 'bg-edge text-slate-50' 
                          : 'text-soft hover:bg-edge hover:text-slate-50'
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
          <MenuItem>
            <div className="box-border content-stretch flex flex-row gap-2.5 items-center justify-center px-8 py-4 relative shrink-0">
              <DropdownMenu>
                <DropdownMenuTrigger className={`font-mono font-[450] text-[16px] hover:text-slate-50 bg-transparent hover:bg-transparent p-0 h-auto gap-2 flex items-center border-none outline-none cursor-pointer ${
                  hasActiveFilters ? 'text-brand' : 'text-soft'
                }`}>
                  <Filter className="size-4" />
                  <span>filters</span>
                  <ChevronDown className="size-4" />
                </DropdownMenuTrigger>
                <DropdownMenuContent 
                  className="bg-surface border-edge min-w-80 p-4"
                  align="end"
                >
                  <div className="space-y-4">
                    <div className="space-y-2">
                      <Label className="font-mono text-sm text-soft">
                        Key
                      </Label>
                      <Input
                        value={localFilters.key}
                        onChange={(e) => setLocalFilters(prev => ({ ...prev, key: e.target.value }))}
                        placeholder="Filter by key..."
                        className="bg-surface border-edge text-slate-50 font-mono placeholder:text-dim"
                      />
                    </div>
                    <div className="space-y-2">
                      <Label className="font-mono text-sm text-soft">
                        Message
                      </Label>
                      <Input
                        value={localFilters.message}
                        onChange={(e) => setLocalFilters(prev => ({ ...prev, message: e.target.value }))}
                        placeholder="Filter by message content..."
                        className="bg-surface border-edge text-slate-50 font-mono placeholder:text-dim"
                      />
                    </div>
                    <div className="flex gap-2 pt-2">
                      <Button
                        onClick={handleClearFilters}
                        variant="outline"
                        size="sm"
                        className="flex-1 bg-transparent border-edge text-soft hover:bg-edge hover:text-slate-50 font-mono"
                      >
                        Clear
                      </Button>
                      <Button
                        onClick={handleApplyFilters}
                        size="sm"
                        className="flex-1 bg-brand text-surface hover:bg-brand-hover font-mono"
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
          <MenuItem>
            <div className="box-border content-stretch flex flex-row gap-2.5 items-center justify-center px-4 py-4 relative shrink-0">
              <button
                onClick={onOpenFavorites}
                className="text-soft hover:text-brand bg-transparent border-none outline-none cursor-pointer p-0 transition-colors"
                title="Favorites"
              >
                <Folder className="size-4" />
              </button>
            </div>
          </MenuItem>
        )}

        {/* Stream Button */}
        {topic && (
          <MenuItem>
            <div className="box-border content-stretch flex flex-row gap-2.5 items-center justify-center px-4 py-4 relative shrink-0">
              <button
                onClick={onToggleStream}
                className={`bg-transparent border-none outline-none cursor-pointer p-0 transition-colors ${
                  isStreaming 
                    ? 'text-brand' 
                    : 'text-soft hover:text-brand'
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
          <MenuItem>
            <div className="box-border content-stretch flex flex-row gap-2.5 items-center justify-center px-4 py-4 relative shrink-0">
              <button
                onClick={onRefresh}
                className="text-soft hover:text-brand bg-transparent border-none outline-none cursor-pointer p-0 transition-colors"
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