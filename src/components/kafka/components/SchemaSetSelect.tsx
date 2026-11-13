import { useEffect, useState } from 'react';
import { Check, ChevronLeft, ChevronRight, ChevronsUpDown } from 'lucide-react';
import { Button } from '../../ui/button';
import { Popover, PopoverContent, PopoverTrigger } from '../../ui/popover';
import { Command, CommandEmpty, CommandInput, CommandItem, CommandList } from '../../ui/command';
import { SourceBinding, SourceView } from '../types';

/**
 * Выбор контракта из библиотеки — в два уровня, с поиском поверх обоих.
 *
 * Плоским списком это не живёт. Два репозитория дали сорок с лишним контрактов,
 * а у человека, который работает с mikui постоянно, их будут сотни: имя
 * репозитория повторялось в каждой строке, а путь — то, по чему и выбирают, —
 * обрезался справа. Поэтому сперва ПРОЕКТЫ, а контракты уже внутри выбранного.
 *
 * Поиск при этом работает по ВСЕМ контрактам всех источников, даже когда проект
 * ещё не выбран. Иначе двухуровневый список стал бы хуже плоского: человек,
 * который помнит имя файла, но не помнит репозиторий, был бы вынужден обойти их
 * по одному — а именно за этим в список и приходят чаще всего.
 */

/** Отбор по подстроке. Нечёткий на путях находит почти всё: буквы запроса
 *  рассыпаются по сегментам. Тот же довод, что у фильтра топиков. */
const hit = (value: string, search: string) =>
  value.toLowerCase().includes(search.trim().toLowerCase());

/** Имя файла — то, что человек помнит и ищет. */
export const fileOf = (entry: string) => entry.slice(entry.lastIndexOf('/') + 1);

/** Каталог без имени файла. Пусто — контракт лежит в корне дерева. */
export const dirOf = (entry: string) => {
  const cut = entry.lastIndexOf('/');
  return cut < 0 ? '' : entry.slice(0, cut);
};

/** Контракт вместе с источником, из которого он взят. */
export interface SetOption {
  sourceId: string;
  sourceLabel: string;
  entry: string;
}

/**
 * Контракты, которые можно выбрать: все разбирающиеся наборы всех источников.
 *
 * Сломанные сюда не попадают — привязать к ним нечего, а почему они сломаны,
 * написано в окне библиотеки. Сколько их, форма настроек говорит отдельной
 * строкой: молча укороченный список читался бы как полный.
 */
export function usableSets(sources: SourceView[]): SetOption[] {
  return sources.flatMap((source) =>
    source.sets
      .filter((set) => !set.error)
      .map((set) => ({ sourceId: source.id, sourceLabel: source.label, entry: set.entry })),
  );
}

/**
 * Что показать на запрос — по ВСЕМ источникам сразу, независимо от того, зашли
 * уже в проект или нет.
 *
 * В этом весь смысл правки: двухуровневый список без сквозного поиска был бы
 * хуже плоского. Человек, который помнит имя файла, но не помнит репозиторий,
 * обходил бы проекты по одному — а приходят сюда чаще всего как раз за
 * конкретным файлом.
 *
 * Ищем и по пути, и по имени проекта: «internalizer» — такой же законный
 * запрос, как «orderbook», и отвечать на него надо тем же полем.
 */
export function searchSets(sources: SourceView[], search: string): SetOption[] {
  if (!search.trim()) return [];
  return usableSets(sources).filter(
    (option) => hit(option.entry, search) || hit(option.sourceLabel, search),
  );
}

/**
 * Разложить найденное на «в этом проекте» и «в остальных».
 *
 * Поиск внутри проекта НАМЕРЕННО не ограничен им: человек помнит имя файла
 * примерно, а в каком репозитории он лежит — часто не помнит вовсе, и список,
 * который на такой запрос отвечает «ничего нет», врёт. Но и валить всё в одну
 * кучу нельзя: раз уж в проект зашли, его контракты — это ответ на вопрос, а
 * чужие — догадка о том, что спросить хотели не совсем это. Отсюда две группы, а
 * не отбор.
 *
 * `sourceId` пуст — в проект не заходили, делить не на что: всё найденное
 * одинаково своё.
 */
export function splitBySource(
  options: SetOption[],
  sourceId: string | null,
): { inside: SetOption[]; elsewhere: SetOption[] } {
  if (!sourceId) return { inside: options, elsewhere: [] };
  return {
    inside: options.filter((option) => option.sourceId === sourceId),
    elsewhere: options.filter((option) => option.sourceId !== sourceId),
  };
}

/**
 * Подпись выбранного контракта — то, что написано на самой кнопке.
 *
 * Привязка ищется ПО ОБОИМ её полям. Сверка по одному лишь источнику была
 * ошибкой, и обидной: кнопка показывала первый контракт репозитория независимо
 * от того, какой выбрали. Привязка при этом была верной, и тип подставлялся
 * верный — расходилась одна только подпись, то есть ровно то единственное
 * место, по которому выбор и проверяют глазами.
 *
 * `null` — выбора нет либо контракт под ним в списке не нашёлся. Второе бывает,
 * и бывает законно: пропавший из библиотеки контракт форма настроек сюда не
 * передаёт (`boundIsLive === false`), но пока список ещё не прочитан, передаёт
 * привязку как есть — не дожидаться же ради подписи. Честный ответ здесь
 * «нечего показать», а не имя чужого файла.
 */
export function labelOf(options: SetOption[], value: SourceBinding | null): string | null {
  if (!value) return null;
  const chosen = options.find(
    (option) => option.sourceId === value.source && option.entry === value.entry,
  );
  return chosen ? `${chosen.sourceLabel} › ${chosen.entry}` : null;
}

/** Ключ строки для cmdk. С разделителем, которого в путях не бывает. */
const rowKey = (source: string, entry: string) => `${source}\u0000${entry}`;

/**
 * Ключ строки «назад» — тем же нулевым байтом и по той же причине: cmdk
 * различает строки по `value`, и совпасть с настоящим контрактом эта строка не
 * должна ни при каком имени файла.
 */
const BACK_ROW = '\u0000back';

interface SchemaSetSelectProps {
  sources: SourceView[];
  value: SourceBinding | null;
  onChange: (binding: SourceBinding) => void;
  disabled?: boolean;
}

export function SchemaSetSelect({
  sources,
  value,
  onChange,
  disabled = false,
}: SchemaSetSelectProps) {
  const [open, setOpen] = useState(false);
  const [search, setSearch] = useState('');
  /** В какой проект зашли. null — показываем список проектов. */
  const [drilled, setDrilled] = useState<string | null>(null);

  const options = usableSets(sources);
  /** Проекты, в которых есть что выбрать, в порядке их появления. */
  const projects = sources
    .map((source) => ({
      source,
      count: options.filter((option) => option.sourceId === source.id).length,
    }))
    .filter((project) => project.count > 0);

  const label = labelOf(options, value);

  // Открыли с уже выбранным контрактом — заходим сразу в его проект: «вернуться
  // к предыдущему выбору» должно означать именно это, а не начать с нуля.
  useEffect(() => {
    if (!open) return;
    setSearch('');
    setDrilled(value?.source ?? null);
  }, [open, value?.source]);

  const searching = search.trim().length > 0;
  const inside = drilled ? projects.find(({ source }) => source.id === drilled) : undefined;

  /** Что показываем: плоские совпадения по всей библиотеке или контракты
   *  выбранного проекта. Список самих проектов рисуется отдельно. */
  const matches = searching
    ? searchSets(sources, search)
    : inside
      ? options.filter((option) => option.sourceId === inside.source.id)
      : [];

  /**
   * Найденное, разложенное на «в этом проекте» и «в остальных».
   *
   * Делится только когда ищут ИЗНУТРИ проекта: вне его все совпадения
   * одинаково свои, и разделитель там нечему отделять.
   */
  const { inside: here, elsewhere } = splitBySource(
    matches,
    searching && inside ? inside.source.id : null,
  );

  const pick = (source: string, entry: string) => {
    setOpen(false);
    if (!(value && value.source === source && value.entry === entry)) {
      onChange({ source, entry });
    }
  };

  /**
   * Строка контракта. Одна на обе группы — различаются они только тем, надо ли
   * называть репозиторий.
   *
   * `withSource` — назвать. В своей группе он не нужен: проект назван и в
   * подписи поиска, и в шапке уровня, а повторённый в каждой строке он отнимает
   * место у пути, по которому как раз и выбирают. В чужой, наоборот,
   * единственное, что объясняет, откуда взялась строка.
   */
  const row = ({ sourceId, sourceLabel, entry }: SetOption, withSource: boolean) => {
    const selected = value?.source === sourceId && value?.entry === entry;
    const dir = dirOf(entry);
    const under = withSource ? [sourceLabel, dir].filter(Boolean).join(' › ') : dir;
    return (
      <CommandItem
        key={rowKey(sourceId, entry)}
        value={rowKey(sourceId, entry)}
        title={`${sourceLabel} › ${entry}`}
        onSelect={() => pick(sourceId, entry)}
        className="font-mono text-xs text-strong data-[selected=true]:bg-edge"
      >
        <Check className={`size-3.5 shrink-0 ${selected ? 'opacity-100' : 'opacity-0'}`} />
        {/* Имя файла отдельной строкой, путь под ним и помельче: различаются
            контракты как раз хвостом, а в одну строку обрезается именно он. */}
        <div className="min-w-0 flex-1">
          <div className="truncate">{fileOf(entry)}</div>
          {under && <div className="text-[11px] text-dim truncate">{under}</div>}
        </div>
      </CommandItem>
    );
  };

  return (
    // `modal` — из-за колеса мыши; то же, что и у `ProtoMessageSelect`:
    // окно держит `react-remove-scroll`, попоувер живёт в портале снаружи, и
    // без собственного замка прокрутка списка не работает.
    <Popover modal open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <Button
          variant="outline"
          role="combobox"
          disabled={disabled || options.length === 0}
          title={label ?? undefined}
          className="justify-between min-w-0 w-full bg-surface border-edge text-strong font-mono disabled:opacity-50"
        >
          <span className={`truncate ${label ? '' : 'text-dim'}`}>
            {label ?? (options.length === 0 ? 'No schema sources yet' : 'Pick a contract')}
          </span>
          <ChevronsUpDown className="size-4 shrink-0 opacity-50" />
        </Button>
      </PopoverTrigger>
      <PopoverContent
        align="start"
        className="w-auto min-w-(--radix-popover-trigger-width) max-w-[36rem] p-0 bg-surface border-edge"
      >
        {/* Отбор свой, а не cmdk-шный: от строки поиска зависит не только
            состав списка, но и УРОВЕНЬ — пустая показывает проекты, непустая
            ищет по всем контрактам сразу. Оставь фильтрацию библиотеке, и она
            отобрала бы ещё и строки проектов, которых в поиске быть не должно. */}
        <Command shouldFilter={false} className="bg-surface">
          <CommandInput
            value={search}
            onValueChange={setSearch}
            placeholder={inside && !searching ? `Search in ${inside.source.label}…` : 'Search all contracts…'}
            className="font-mono text-strong"
          />
          <CommandList>
            <CommandEmpty className="p-3 font-mono text-xs text-dim">Nothing found</CommandEmpty>

            {/* Шапка уровня: куда зашли и чем выйти. Строкой списка, а не
                кнопкой над ним, — чтобы до неё доставали стрелки, как и до
                всего остального. */}
            {inside && !searching && (
              <CommandItem
                value={BACK_ROW}
                onSelect={() => setDrilled(null)}
                className="font-mono text-xs text-dim data-[selected=true]:bg-edge"
              >
                <ChevronLeft className="size-3.5 shrink-0" />
                <span className="truncate">all projects</span>
              </CommandItem>
            )}

            {!inside &&
              !searching &&
              projects.map(({ source, count }) => (
                <CommandItem
                  key={source.id}
                  value={source.id}
                  onSelect={() => setDrilled(source.id)}
                  className="font-mono text-xs text-strong data-[selected=true]:bg-edge"
                >
                  <div className="min-w-0 flex-1">
                    <div className="truncate" title={source.location}>
                      {source.label}
                    </div>
                    <div className="text-[11px] text-dim">
                      {count} {count === 1 ? 'contract' : 'contracts'}
                    </div>
                  </div>
                  <ChevronRight className="size-3.5 shrink-0 opacity-50" />
                </CommandItem>
              ))}

            {/* Сперва то, что нашлось в ЭТОМ проекте. Когда в проект не
                заходили, здесь и лежит всё найденное: делить не на что. */}
            {here.map((option) => row(option, !inside))}

            {/* Чужие совпадения — за разделителем, который их и называет.
                Без него список выглядел бы так, будто поиск по проекту не
                работает: подпись над ним обещает «search in dealer/exchange», а
                в строках попадаются файлы других репозиториев. Обещание при
                этом верное — свои идут первыми, — и сказать об этом надо ровно
                там, где свои кончились.

                Строкой-не-пунктом: стрелки её проскакивают, как и положено
                заголовку, — cmdk считает пунктами только `CommandItem`. */}
            {elsewhere.length > 0 && (
              <div className="flex items-center gap-2 px-2 py-1.5" aria-hidden>
                <span className="shrink-0 font-mono text-[11px] text-dim">
                  {here.length > 0 ? 'in other projects' : 'nothing here — in other projects'}
                </span>
                <span className="h-px flex-1 bg-edge" />
              </div>
            )}
            {elsewhere.map((option) => row(option, true))}
          </CommandList>
        </Command>
      </PopoverContent>
    </Popover>
  );
}
