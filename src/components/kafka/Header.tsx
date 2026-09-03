import { ChevronDown, Filter, RefreshCw, Play, Folder, Search, Send, User, Users } from 'lucide-react';
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
  BodyFormat,
  Topic,
  ClusterUser,
  KafkaCluster,
  MessageFilter,
  ReadMode,
  ReadRange,
  EMPTY_FILTER,
  needsSasl,
} from './types';
import { Tab } from './components/Tab';
import { MenuItem } from './components/MenuItem';
import { IconAction } from './components/IconAction';
import { FormatSelect } from './components/FormatSelect';
import { ReadOrder } from './components/ReadOrder';

/** Со скольки выбранных партиций перечисление перестаёт помещаться в шапку. */
const PARTITIONS_SHOWN_INLINE = 3;

interface HeaderDesktopProps {
  /** null — читаем все партиции топика. */
  selectedPartitions: number[] | null;
  onSelectPartitions: (partitions: number[] | null) => void;
  readMode: ReadMode;
  range: ReadRange;
  onReadChange: (mode: ReadMode, range: ReadRange) => void;
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
  /** Чем показывать тела открытого топика. */
  format: BodyFormat;
  onFormatChange: (format: BodyFormat) => void;
  /** Открыть настройки схемы текущего топика. */
  onOpenSchema: () => void;
  onRefresh: () => void;
  onOpenFavorites: () => void;
  onProduce: () => void;
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

/** Полный перечень — в подсказке, когда в подпись он не уложился. */
function partitionsTitle(selected: number[] | null, total: number): string {
  if (!selected || selected.length === 0 || selected.length === total) {
    return `All ${total} partitions`;
  }
  return `Partitions ${selected.join(', ')}`;
}

export function HeaderDesktop({
  selectedPartitions,
  onSelectPartitions,
  readMode,
  range,
  onReadChange,
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
  format,
  onFormatChange,
  onOpenSchema,
  onRefresh,
  onOpenFavorites,
  onProduce,
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
                {/* Мельче названия кластера: кластер — это «где я», а учётка
                    — уточнение к нему, и спорить с ним за внимание ей незачем. */}
                <DropdownMenu>
                  <DropdownMenuTrigger className="font-mono text-xs text-soft hover:text-slate-50 bg-transparent hover:bg-transparent p-0 h-auto gap-1.5 flex items-center border-none outline-none cursor-pointer">
                    <User className="size-3.5" />
                    <span className={activeUser ? '' : 'text-dim'}>
                      {activeUser ? activeUser.username : 'no user'}
                    </span>
                    <ChevronDown className="size-3.5" />
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

          {/* Счётчиков буфера, скорости и throttled здесь больше нет — они
              переехали в полоску под таблицей (`StatusBar`). Это фоновые
              сведения: смотреть на них постоянно не нужно, а места крупным
              шрифтом они занимали столько, что шапка перестала вмещать
              собственные селекторы. */}
        </div>
      </div>

      <div className="box-border content-stretch flex flex-row items-center justify-start p-0 relative shrink-0">
        {/* Фильтры живут кнопкой ВНУТРИ поля поиска, а не отдельным пунктом
            рядом: поиск по телу сообщения — это уже фильтр, и разносить их по
            двум местам значило бы занимать шапку дважды под одно и то же. */}
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
                  className="bg-surface border-edge text-slate-50 font-mono placeholder:text-dim pl-8 pr-9 w-64"
                />
                <DropdownMenu>
                  <DropdownMenuTrigger
                    title="Filters"
                    className={`absolute right-2 top-1/2 -translate-y-1/2 bg-transparent border-none outline-none cursor-pointer p-0 transition-colors ${
                      hasActiveFilters ? 'text-brand' : 'text-dim hover:text-slate-50'
                    }`}
                  >
                    <Filter className="size-4" />
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
                        <Label
                          htmlFor="case-sensitive"
                          className="font-mono text-sm text-soft cursor-pointer"
                        >
                          Case sensitive
                        </Label>
                      </div>
                      {/* Гасим, а не прячем: включённый флажок, которого не
                          видно, пользователь не смог бы ни заметить, ни снять.
                          Бэкенд без схемы его тоже игнорирует — сходятся. */}
                      <div className="flex items-center gap-2">
                        <Checkbox
                          id="search-decoded"
                          checked={filters.search_decoded}
                          disabled={format !== 'proto'}
                          onCheckedChange={(checked) =>
                            onFiltersChange({ ...filters, search_decoded: checked === true })
                          }
                        />
                        <Label
                          htmlFor="search-decoded"
                          title={
                            format === 'proto'
                              ? 'Also search field names, numbers and enum labels. Decodes every message, so it is slower.'
                              : 'Needs a protobuf schema for this topic'
                          }
                          className={`font-mono text-sm ${
                            format === 'proto' ? 'text-soft cursor-pointer' : 'text-dim'
                          }`}
                        >
                          Search decoded body
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
            </div>
          </MenuItem>
        )}

        {/* Формат тела. Стоит рядом с чтением, а не в настройках топика,
            потому что это выбор ПОКАЗА: одно и то же тело смотрят то
            разобранным по схеме, то сырым текстом, и открывать ради
            переключения модальное окно незачем. Сам выбор при этом хранится
            там же, где и раньше, — в схеме топика на диске. */}
        {topic && (
          <MenuItem>
            <div className="box-border content-stretch flex flex-row gap-2.5 items-center justify-center px-4 py-4 relative shrink-0">
              <FormatSelect
                format={format}
                onChange={onFormatChange}
                onOpenSchema={onOpenSchema}
              />
            </div>
          </MenuItem>
        )}

        {/* Порядок и границы чтения. Не косметика: топик перечитывается с
            другого конца или по другому куску, поэтому селектор стоит рядом с
            партициями, а не в фильтрах. */}
        {topic && (
          <MenuItem>
            <div className="box-border content-stretch flex flex-row gap-2.5 items-center justify-center px-8 py-4 relative shrink-0">
              <ReadOrder
                mode={readMode}
                range={range}
                singlePartition={selectedPartitions?.length === 1}
                onChange={onReadChange}
              />
            </div>
          </MenuItem>
        )}

        {topic && (
          <MenuItem>
            <div className="box-border content-stretch flex flex-row gap-2.5 items-center justify-center px-6 py-4 relative shrink-0">
              {/* Два уровня, а не «partitions: 0, 2, 5» в строку: перечисление
                  растёт с числом выбранных, и одной строкой оно раздувало шапку
                  до того, что переставало в неё влезать. Подпись сверху
                  постоянной ширины, значение под ней. */}
              <DropdownMenu>
                <DropdownMenuTrigger className="font-mono text-xs text-soft hover:text-slate-50 bg-transparent hover:bg-transparent p-0 h-auto gap-1.5 flex items-start border-none outline-none cursor-pointer">
                  <div className="flex flex-col items-start leading-tight">
                    <span className="text-dim">partitions</span>
                    <span
                      className="text-soft max-w-40 truncate"
                      title={partitionsTitle(selectedPartitions, partitions.length)}
                    >
                      {partitionsLabel(selectedPartitions, partitions.length)}
                    </span>
                  </div>
                  <ChevronDown className="size-3.5 mt-0.5" />
                </DropdownMenuTrigger>
                {/* Меню не закрывается по клику (`onSelect` гасится): выбрать
                    три партиции из двадцати, открывая список заново на каждую,
                    — это не выбор, а перебор. */}
                <DropdownMenuContent className="bg-surface border-edge min-w-24 max-h-80 overflow-auto" align="end">
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

        {/* Не под условием `topic`, в отличие от соседей: сохранённые сообщения
            лежат на диске и переживают и топик, и подключение — добраться до
            них, ничего не открывая, обычное дело. */}
        <IconAction onClick={onOpenFavorites} title="Saved messages">
          <Folder className="size-4" />
        </IconAction>

        {/* Живой хвост — Фаза 3. Кнопка на месте, чтобы не менять раскладку
            шапки, но выключена: раньше она показывала тост и не делала ничего,
            что вводило в заблуждение. */}
        {topic && (
          <IconAction disabled title="Live tail — not implemented yet">
            <Play className="size-4" />
          </IconAction>
        )}

        {/* Отправка стоит рядом с чтением, а не в настройках топика: это
            действие над теми же данными, что и в таблице, и делают его в том же
            заходе — посмотреть, что лежит, и положить рядом своё. */}
        {topic && (
          <IconAction onClick={onProduce} title="Produce a message">
            <Send className="size-4" />
          </IconAction>
        )}

        {topic && (
          <IconAction onClick={onRefresh} title="Re-read the topic">
            <RefreshCw className="size-4" />
          </IconAction>
        )}
      </div>
    </div>
  );
}
