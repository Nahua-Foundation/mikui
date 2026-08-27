import { ChevronDown, Filter, RefreshCw, Play, Folder, Search } from 'lucide-react';
import { Button } from '../ui/button';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from '../ui/dropdown-menu';
import { Input } from '../ui/input';
import { Label } from '../ui/label';
import { Checkbox } from '../ui/checkbox';
import { Topic, MessageFilter, OpenTopicResult, EMPTY_FILTER } from './types';
import { Tab } from './components/Tab';
import { MenuItem } from './components/MenuItem';

interface HeaderDesktopProps {
  selectedPartition: number | null;
  onSelectPartition: (partition: number | null) => void;
  topic: Topic | null;
  onClusterClick: () => void;
  filters: MessageFilter;
  onFiltersChange: (filters: MessageFilter) => void;
  onRefresh: () => void;
  onOpenFavorites: () => void;
  stats: OpenTopicResult | null;
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(0)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

export function HeaderDesktop({
  selectedPartition,
  onSelectPartition,
  topic,
  onClusterClick,
  filters,
  onFiltersChange,
  onRefresh,
  onOpenFavorites,
  stats,
}: HeaderDesktopProps) {
  const partitions = topic ? Array.from({ length: topic.partitions }, (_, i) => i) : [];
  const hasActiveFilters = filters.key.trim() !== '' || filters.value.trim() !== '';

  return (
    <div
      className="box-border content-stretch flex flex-row items-center justify-between p-0 relative shrink-0 w-full border-b border-edge"
      data-name="header desktop"
    >
      <div className="box-border content-stretch flex flex-row gap-32 items-center justify-start p-0 relative shrink-0">
        <div className="box-border content-stretch flex flex-row items-center justify-start p-0 relative shrink-0">
          <div className="flex items-center justify-center px-4 py-4">
            <div className="relative">
              <div className="w-2 h-2 bg-green-500 rounded-full"></div>
              <div className="absolute inset-0 w-2 h-2 bg-green-500 rounded-full animate-pulse opacity-75"></div>
              <div className="absolute inset-0 w-2 h-2 bg-green-500 rounded-full shadow-[0_0_6px_rgba(34,197,94,0.6)]"></div>
            </div>
          </div>
          <MenuItem>
            <Tab title="cluster" onClick={onClusterClick} />
          </MenuItem>

          {/* Сколько лежит в буфере Rust. Приложение обещает быть экономным —
              пусть цифра будет на виду, а не на словах. */}
          {stats && (
            <div className="px-4 font-mono text-xs text-dim whitespace-nowrap">
              {stats.loaded.toLocaleString()} msg · {formatBytes(stats.buffer_bytes)}
              {stats.truncated && <span className="text-brand"> · truncated</span>}
            </div>
          )}
        </div>
      </div>

      <div className="box-border content-stretch flex flex-row items-center justify-start p-0 relative shrink-0">
        {topic && (
          <MenuItem>
            <div className="box-border content-stretch flex flex-row gap-2.5 items-center justify-center px-4 py-4 relative shrink-0">
              <div className="relative">
                <Search className="absolute left-2 top-1/2 transform -translate-y-1/2 size-4 text-dim" />
                {/* Поле поиска раньше было декоративным. Теперь это фильтр по
                    телу сообщения; применяется в Rust по сырым байтам. */}
                <Input
                  value={filters.value}
                  onChange={(e) => onFiltersChange({ ...filters, value: e.target.value })}
                  placeholder="Search in messages"
                  className="bg-surface border-edge text-slate-50 font-mono placeholder:text-dim pl-8 w-64"
                />
              </div>
            </div>
          </MenuItem>
        )}

        {topic && (
          <MenuItem>
            <div className="box-border content-stretch flex flex-row gap-2.5 items-center justify-center px-8 py-4 relative shrink-0">
              <DropdownMenu>
                <DropdownMenuTrigger className="font-mono font-[450] text-[16px] text-soft hover:text-slate-50 bg-transparent hover:bg-transparent p-0 h-auto gap-2 flex items-center border-none outline-none cursor-pointer">
                  <span>partitions: {selectedPartition !== null ? selectedPartition : 'all'}</span>
                  <ChevronDown className="size-4" />
                </DropdownMenuTrigger>
                <DropdownMenuContent className="bg-surface border-edge min-w-32 max-h-80 overflow-auto" align="end">
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

        {topic && (
          <MenuItem>
            <div className="box-border content-stretch flex flex-row gap-2.5 items-center justify-center px-8 py-4 relative shrink-0">
              <DropdownMenu>
                <DropdownMenuTrigger
                  className={`font-mono font-[450] text-[16px] hover:text-slate-50 bg-transparent hover:bg-transparent p-0 h-auto gap-2 flex items-center border-none outline-none cursor-pointer ${
                    hasActiveFilters ? 'text-brand' : 'text-soft'
                  }`}
                >
                  <Filter className="size-4" />
                  <span>filters</span>
                  <ChevronDown className="size-4" />
                </DropdownMenuTrigger>
                <DropdownMenuContent className="bg-surface border-edge min-w-80 p-4" align="end">
                  <div className="space-y-4">
                    <div className="space-y-2">
                      <Label className="font-mono text-sm text-soft">Key</Label>
                      <Input
                        value={filters.key}
                        onChange={(e) => onFiltersChange({ ...filters, key: e.target.value })}
                        placeholder="Filter by key..."
                        className="bg-surface border-edge text-slate-50 font-mono placeholder:text-dim"
                      />
                    </div>
                    <div className="space-y-2">
                      <Label className="font-mono text-sm text-soft">Message</Label>
                      <Input
                        value={filters.value}
                        onChange={(e) => onFiltersChange({ ...filters, value: e.target.value })}
                        placeholder="Filter by message content..."
                        className="bg-surface border-edge text-slate-50 font-mono placeholder:text-dim"
                      />
                    </div>
                    <div className="flex items-center gap-2">
                      <Checkbox
                        id="case-sensitive"
                        checked={filters.case_sensitive}
                        onCheckedChange={(checked) =>
                          onFiltersChange({ ...filters, case_sensitive: checked === true })
                        }
                      />
                      <Label htmlFor="case-sensitive" className="font-mono text-sm text-soft cursor-pointer">
                        Case sensitive
                      </Label>
                    </div>
                    <div className="flex gap-2 pt-2">
                      <Button
                        onClick={() => onFiltersChange(EMPTY_FILTER)}
                        variant="outline"
                        size="sm"
                        className="flex-1 bg-transparent border-edge text-soft hover:bg-edge hover:text-slate-50 font-mono"
                      >
                        Clear
                      </Button>
                    </div>
                  </div>
                </DropdownMenuContent>
              </DropdownMenu>
            </div>
          </MenuItem>
        )}

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

        {topic && (
          <MenuItem>
            <div className="box-border content-stretch flex flex-row gap-2.5 items-center justify-center px-4 py-4 relative shrink-0">
              {/* Живой хвост — Фаза 3. Кнопка на месте, чтобы не менять раскладку
                  шапки, но выключена: раньше она показывала тост и не делала
                  ничего, что вводило в заблуждение. */}
              <button
                disabled
                className="text-dim bg-transparent border-none outline-none p-0 cursor-not-allowed opacity-50"
                title="Live tail — not implemented yet"
              >
                <Play className="size-4" />
              </button>
            </div>
          </MenuItem>
        )}

        {topic && (
          <MenuItem>
            <div className="box-border content-stretch flex flex-row gap-2.5 items-center justify-center px-4 py-4 relative shrink-0">
              <button
                onClick={onRefresh}
                className="text-soft hover:text-brand bg-transparent border-none outline-none cursor-pointer p-0 transition-colors"
                title="Re-read the topic"
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
