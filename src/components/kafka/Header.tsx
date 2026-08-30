import {
  ArrowDown,
  ArrowUp,
  ChevronDown,
  Filter,
  RefreshCw,
  Play,
  Folder,
  Search,
  User,
  Users,
} from 'lucide-react';
import { Button } from '../ui/button';
import {
  DropdownMenu,
  DropdownMenuCheckboxItem,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from '../ui/dropdown-menu';
import { Input } from '../ui/input';
import { Label } from '../ui/label';
import { Checkbox } from '../ui/checkbox';
import {
  Topic,
  ClusterUser,
  KafkaCluster,
  MessageFilter,
  OpenTopicResult,
  StartFrom,
  EMPTY_FILTER,
  needsSasl,
} from './types';
import { Tab } from './components/Tab';
import { MenuItem } from './components/MenuItem';

/** Направление чтения — оно же порядок строк в таблице. Подпись объясняет
 *  не только «с какого конца», но и что будет догружаться дальше: именно это
 *  и определяет, куда поедет список во время фоновой подгрузки. */
const START_FROM_OPTIONS: { value: StartFrom; label: string; hint: string }[] = [
  { value: 'newest', label: 'newest first', hint: 'самые новые сверху, старые догружаются вниз' },
  { value: 'oldest', label: 'oldest first', hint: 'самые старые сверху, новые догружаются вниз' },
];

/** Со скольки выбранных партиций перечисление перестаёт помещаться в шапку. */
const PARTITIONS_SHOWN_INLINE = 3;

interface HeaderDesktopProps {
  /** null — читаем все партиции топика. */
  selectedPartitions: number[] | null;
  onSelectPartitions: (partitions: number[] | null) => void;
  startFrom: StartFrom;
  onStartFromChange: (startFrom: StartFrom) => void;
  topic: Topic | null;
  /**
   * Как называется то, к чему подключены. null — ни к чему. Отдельно от
   * `cluster` потому, что подключиться можно и из формы, не сохраняя кластер:
   * имя в шапке есть, а записи на диске за ним нет.
   */
  clusterName: string | null;
  /** Сохранённая запись кластера, если подключались к такой. */
  cluster: KafkaCluster | null;
  /** Идёт рукопожатие: ни «подключены», ни «нет» — про это надо сказать. */
  isConnecting: boolean;
  /** Учётка, под которой держится текущее подключение. */
  activeUserId: string | null;
  onClusterClick: () => void;
  onSelectUser: (user: ClusterUser) => void;
  onManageUsers: () => void;
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

/**
 * Что делает клик по партиции в списке.
 *
 * `null` значит «все» — и это не то же самое, что «выбраны все по одной»:
 * пустой выбор и полный выбор сводятся к нему же, иначе в списке остались бы
 * два состояния с одинаковым результатом чтения и разными галочками.
 */
export function togglePartition(
  selected: number[] | null,
  partition: number,
  total: number,
): number[] | null {
  const current = selected ?? [];
  const next = current.includes(partition)
    ? current.filter((p) => p !== partition)
    : [...current, partition].sort((a, b) => a - b);
  return next.length === 0 || next.length === total ? null : next;
}

function partitionsLabel(selected: number[] | null, total: number): string {
  if (!selected || selected.length === 0 || selected.length === total) return 'all';
  if (selected.length <= PARTITIONS_SHOWN_INLINE) return selected.join(', ');
  return `${selected.length} of ${total}`;
}

export function HeaderDesktop({
  selectedPartitions,
  onSelectPartitions,
  startFrom,
  onStartFromChange,
  topic,
  clusterName,
  cluster,
  isConnecting,
  activeUserId,
  onClusterClick,
  onSelectUser,
  onManageUsers,
  filters,
  onFiltersChange,
  onRefresh,
  onOpenFavorites,
  stats,
}: HeaderDesktopProps) {
  const partitions = topic ? Array.from({ length: topic.partitions }, (_, i) => i) : [];
  const hasActiveFilters = filters.key.trim() !== '' || filters.value.trim() !== '';
  const allPartitions = selectedPartitions === null;

  const connected = clusterName !== null;
  const users = cluster?.users ?? [];
  const activeUser = users.find((u) => u.id === activeUserId) ?? null;
  // На PLAINTEXT-кластере логина нет и не будет — селектор там только мешал бы.
  const showUsers =
    connected && !!cluster && (users.length > 0 || needsSasl(cluster.security_protocol));

  return (
    <div
      className="box-border content-stretch flex flex-row items-center justify-between p-0 relative shrink-0 w-full border-b border-edge"
      data-name="header desktop"
    >
      <div className="box-border content-stretch flex flex-row gap-32 items-center justify-start p-0 relative shrink-0">
        <div className="box-border content-stretch flex flex-row items-center justify-start p-0 relative shrink-0">
          {/* Индикатор различает три состояния, а не два. Раньше он светился
              зелёным всегда — включая пустое приложение, к которому не
              подключено ничего, и рукопожатие, которое ещё может не удаться. */}
          <div className="flex items-center justify-center px-4 py-4">
            {isConnecting ? (
              <div
                className="w-2 h-2 bg-brand rounded-full animate-pulse"
                title="Connecting…"
              ></div>
            ) : connected ? (
              <div className="relative">
                <div className="w-2 h-2 bg-green-500 rounded-full"></div>
                <div className="absolute inset-0 w-2 h-2 bg-green-500 rounded-full animate-pulse opacity-75"></div>
                <div className="absolute inset-0 w-2 h-2 bg-green-500 rounded-full shadow-[0_0_6px_rgba(34,197,94,0.6)]"></div>
              </div>
            ) : (
              <div className="w-2 h-2 bg-edge rounded-full" title="Not connected"></div>
            )}
          </div>
          {/* Имя кластера вместо слова «cluster»: из шапки должно быть видно,
              на что смотришь, — с двумя похожими стендами это единственное
              место, где ошибка заметна до чтения чужого топика. */}
          <MenuItem>
            <Tab
              title={clusterName ?? 'Select cluster'}
              active={connected}
              onClick={onClusterClick}
            />
          </MenuItem>

          {/* Kafka-пользователь. Одну и ту же тему на кластере смотрят из-под
              разных учёток с разными ACL, поэтому переключение живёт в шапке
              рядом с кластером, а не в настройках подключения. */}
          {showUsers && cluster && (
            <MenuItem>
              <div className="box-border content-stretch flex flex-row gap-2.5 items-center justify-center px-4 py-4 relative shrink-0">
                <DropdownMenu>
                  <DropdownMenuTrigger className="font-mono font-[450] text-[16px] text-soft hover:text-slate-50 bg-transparent hover:bg-transparent p-0 h-auto gap-2 flex items-center border-none outline-none cursor-pointer">
                    <User className="size-4" />
                    <span className={activeUser ? '' : 'text-dim'}>
                      {activeUser ? activeUser.username : 'no user'}
                    </span>
                    <ChevronDown className="size-4" />
                  </DropdownMenuTrigger>
                  <DropdownMenuContent
                    className="bg-surface border-edge min-w-56 max-h-80 overflow-auto"
                    align="start"
                  >
                    {users.map((user) => (
                      <DropdownMenuItem
                        key={user.id}
                        className={`font-mono cursor-pointer justify-between ${
                          user.id === activeUserId
                            ? 'bg-edge text-slate-50'
                            : 'text-soft hover:bg-edge hover:text-slate-50'
                        }`}
                        onClick={() => onSelectUser(user)}
                      >
                        <span>{user.username}</span>
                        {/* Учётка без пароля подключится анонимно и упрётся в
                            ACL — честнее сказать об этом до попытки. */}
                        {!user.has_password && (
                          <span className="text-xs text-brand ml-3">no password</span>
                        )}
                      </DropdownMenuItem>
                    ))}
                    {users.length === 0 && (
                      <div className="font-mono text-xs text-dim px-2 py-1.5">
                        No users yet
                      </div>
                    )}
                    <DropdownMenuSeparator className="bg-edge" />
                    <DropdownMenuItem
                      className="font-mono cursor-pointer text-soft hover:bg-edge hover:text-slate-50"
                      onClick={onManageUsers}
                    >
                      <Users className="size-4" />
                      Manage users…
                    </DropdownMenuItem>
                  </DropdownMenuContent>
                </DropdownMenu>
              </div>
            </MenuItem>
          )}

          {/* Сколько лежит в буфере Rust. Приложение обещает быть экономным —
              пусть цифра будет на виду, а не на словах. */}
          {stats && (
            <div className="px-4 font-mono text-xs text-dim whitespace-nowrap">
              {stats.loaded.toLocaleString()} msg · {formatBytes(stats.buffer_bytes)}
              {stats.truncated && <span className="text-brand"> · truncated</span>}
              {/* Скорость, которую даёт кластер. Без неё медленная загрузка
                  неотличима от зависшего приложения — а на кластере с квотой
                  на чтение она медленная всегда. */}
              {stats.read_bytes_per_sec !== null && (
                <span title="Read throughput the cluster actually allows">
                  {' '}
                  · {formatBytes(stats.read_bytes_per_sec)}/s
                </span>
              )}
              {stats.peak_throttle_ms > 0 && (
                <span
                  className="text-brand"
                  title={`The broker held responses back for up to ${stats.peak_throttle_ms} ms — a read quota is in effect`}
                >
                  {' '}
                  · throttled
                </span>
              )}
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

        {/* Направление чтения. Не косметика: топик перечитывается с другого
            конца, поэтому селектор стоит рядом с партициями, а не в фильтрах. */}
        {topic && (
          <MenuItem>
            <div className="box-border content-stretch flex flex-row gap-2.5 items-center justify-center px-8 py-4 relative shrink-0">
              <DropdownMenu>
                <DropdownMenuTrigger className="font-mono font-[450] text-[16px] text-soft hover:text-slate-50 bg-transparent hover:bg-transparent p-0 h-auto gap-2 flex items-center border-none outline-none cursor-pointer">
                  {startFrom === 'newest' ? (
                    <ArrowDown className="size-4" />
                  ) : (
                    <ArrowUp className="size-4" />
                  )}
                  <span>{startFrom === 'newest' ? 'newest first' : 'oldest first'}</span>
                  <ChevronDown className="size-4" />
                </DropdownMenuTrigger>
                <DropdownMenuContent className="bg-surface border-edge min-w-72" align="end">
                  {START_FROM_OPTIONS.map((option) => (
                    <DropdownMenuItem
                      key={option.value}
                      className={`font-mono cursor-pointer flex-col items-start gap-0.5 ${
                        startFrom === option.value
                          ? 'bg-edge text-slate-50'
                          : 'text-soft hover:bg-edge hover:text-slate-50'
                      }`}
                      onClick={() => onStartFromChange(option.value)}
                    >
                      <span>{option.label}</span>
                      <span className="text-xs text-dim">{option.hint}</span>
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
                <DropdownMenuTrigger className="font-mono font-[450] text-[16px] text-soft hover:text-slate-50 bg-transparent hover:bg-transparent p-0 h-auto gap-2 flex items-center border-none outline-none cursor-pointer">
                  <span>partitions: {partitionsLabel(selectedPartitions, partitions.length)}</span>
                  <ChevronDown className="size-4" />
                </DropdownMenuTrigger>
                {/* Меню не закрывается по клику (`onSelect` гасится): выбрать
                    три партиции из двадцати, открывая список заново на каждую,
                    — это не выбор, а перебор. */}
                <DropdownMenuContent className="bg-surface border-edge min-w-40 max-h-80 overflow-auto" align="end">
                  <DropdownMenuCheckboxItem
                    checked={allPartitions}
                    onSelect={(e) => e.preventDefault()}
                    // Снять чек с «all» некуда: пустой выбор — это и есть «all».
                    onCheckedChange={() => onSelectPartitions(null)}
                    className={`font-mono cursor-pointer ${
                      allPartitions ? 'text-slate-50' : 'text-soft hover:bg-edge hover:text-slate-50'
                    }`}
                  >
                    all
                  </DropdownMenuCheckboxItem>
                  {partitions.map((partition) => {
                    const checked = selectedPartitions?.includes(partition) ?? false;
                    return (
                      <DropdownMenuCheckboxItem
                        key={partition}
                        checked={checked}
                        onSelect={(e) => e.preventDefault()}
                        onCheckedChange={() =>
                          onSelectPartitions(
                            togglePartition(selectedPartitions, partition, partitions.length),
                          )
                        }
                        className={`font-mono cursor-pointer ${
                          checked ? 'text-slate-50' : 'text-soft hover:bg-edge hover:text-slate-50'
                        }`}
                      >
                        {partition}
                      </DropdownMenuCheckboxItem>
                    );
                  })}
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
