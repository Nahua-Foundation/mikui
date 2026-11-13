import { useCallback, useEffect, useState } from 'react';
import {
  ChevronDown,
  ChevronRight,
  FolderOpen,
  Github,
  Gitlab,
  RotateCw,
  Trash2,
} from 'lucide-react';
import { toast } from 'sonner';
import { open as openDialog } from '@tauri-apps/plugin-dialog';
import { Dialog, DialogHeader, DialogTitle } from '../../ui/dialog';
import { DialogContentNoClose } from '../DialogContentNoClose';
import { Button } from '../../ui/button';
import { Input } from '../../ui/input';
import { Label } from '../../ui/label';
import * as api from '../api';
import { describeError } from '../api';
import { REPO_LABEL, RepoDefaults, RepoKind, SourceView } from '../types';

/** Пустая форма репозитория — она же начальное состояние, она же то, к чему
 *  форма возвращается после удачной загрузки и при следующем открытии окна. */
const EMPTY_FORM = { url: '', gitRef: '', subdir: '', token: '', ca: '' };

/** Заготовки ещё не спрашивали. Хост и признак токена зависят от провайдера, а
 *  он неизвестен, пока форму не открыли. */
const NO_DEFAULTS: RepoDefaults = { url_prefix: null, host: null, has_token: false };

/** Что писать в поле токена у каждого провайдера. Права называются теми
 *  словами, какими они названы у него самого: искать `read_repository` в
 *  интерфейсе GitHub негде, как и `Contents: read` в GitLab. */
const TOKEN_HINT: Record<RepoKind, string> = {
  gitlab: 'personal access token (read_repository)',
  github: 'personal access token (repo, or Contents: read)',
};

/** Образец URL проекта — по нему видно, что ждут адрес репозитория целиком. */
const URL_HINT: Record<RepoKind, string> = {
  gitlab: 'https://gitlab.example.com/group/contracts',
  github: 'https://github.com/acme/contracts',
};

interface SchemaSourcesModalProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  /** Библиотека изменилась: топик, привязанный к набору, мог начать или
   *  перестать декодироваться, и открытая таблица обязана это заметить. */
  onChanged?: () => void;
}

/**
 * Библиотека наборов .proto.
 *
 * Заведена затем, чтобы файлы перестали копироваться на каждый топик: раньше
 * единицей загрузки был ТОПИК, и положить .proto до выбора топика было некуда,
 * а на ферме без чекаутов — неоткуда взять вовсе.
 *
 * Источник — это дерево (репозиторий или папка), а набор — входной файл в нём,
 * то есть контракт одного топика. Репозиторий набором не является: в нём лежат
 * контракты нескольких топиков, и разобранный целиком он не даёт единственного
 * корня — а корень и есть то, чем приложение определяет тип топика.
 *
 * Окно глобальное, к открытому топику отношения не имеет: один
 * контракт-репозиторий обслуживает и dev, и prod.
 */
export function SchemaSourcesModal({ open, onOpenChange, onChanged }: SchemaSourcesModalProps) {
  const [sources, setSources] = useState<SourceView[]>([]);
  const [busy, setBusy] = useState(false);
  const [expanded, setExpanded] = useState<string | null>(null);
  /** Какого провайдера форма сейчас раскрыта. null — показаны кнопки выбора. */
  const [adding, setAdding] = useState<RepoKind | null>(null);
  const [form, setForm] = useState(EMPTY_FORM);
  /** Хост последнего источника ТОГО провайдера, чья форма открыта, и признак
   *  сохранённого на него токена. */
  const [defaults, setDefaults] = useState<RepoDefaults>(NO_DEFAULTS);

  const reload = useCallback(async () => {
    try {
      setSources(await api.listSchemaSources());
    } catch (e) {
      console.error('Failed to list schema sources', e);
      toast.error(`Failed to load schema sources: ${describeError(e)}`);
    }
  }, []);

  // Окно закрывают, а компонент остаётся смонтированным — Radix убирает только
  // содержимое, состояние переживает закрытие целиком. Поэтому раскрытая форма
  // репозитория встречала на следующем открытии вместо кнопок «добавить»,
  // причём с набранным в прошлый раз URL. Открытие — это начало работы, и окно
  // должно выглядеть так же, как в первый раз.
  useEffect(() => {
    if (!open) return;
    setAdding(null);
    setForm(EMPTY_FORM);
    setDefaults(NO_DEFAULTS);
    setExpanded(null);
    void reload();
  }, [open, reload]);

  /**
   * Общий хвост всех операций: успех перечитывает список и сообщает наружу.
   *
   * Возвращает, удалось ли. Нужно форме репозитория: очищать поля на неудаче
   * значит заставить набрать URL, ветку и подкаталог заново — ровно тогда,
   * когда их как раз и надо поправить.
   */
  const run = useCallback(
    async (action: () => Promise<unknown>, success: string): Promise<boolean> => {
      setBusy(true);
      try {
        await action();
        await reload();
        onChanged?.();
        toast.success(success);
        return true;
      } catch (e) {
        console.error('Schema source operation failed', e);
        toast.error(describeError(e));
        return false;
      } finally {
        setBusy(false);
      }
    },
    [reload, onChanged],
  );

  const handleAddFolder = useCallback(async () => {
    let picked: string | string[] | null;
    try {
      picked = await openDialog({ directory: true, multiple: false });
    } catch (e) {
      console.error('Failed to open folder dialog', e);
      toast.error(`Failed to open the folder dialog: ${describeError(e)}`);
      return;
    }
    const path = Array.isArray(picked) ? picked[0] : picked;
    if (!path) return;
    await run(() => api.addFolderSource(path), `Added ${path}`);
  }, [run]);

  /**
   * Раскрывает форму провайдера, подставив хост, с которым у НЕГО работали в
   * прошлый раз.
   *
   * Заготовка спрашивается здесь, а не вместе со списком источников: до выбора
   * провайдера ответа на вопрос «какой хост» не существует — у GitLab он свой,
   * у GitHub свой, и подставить чужой значило бы предложить приставку, которую
   * первым делом надо стереть.
   */
  const handleOpenRepo = useCallback(async (kind: RepoKind) => {
    setForm(EMPTY_FORM);
    setDefaults(NO_DEFAULTS);
    setAdding(kind);
    try {
      const loaded = await api.repoSourceDefaults(kind);
      setDefaults(loaded);
      // Только в пустое поле: пока ходили за заготовкой, человек мог уже
      // вставить адрес, и затирать его ответом на вопрос, которого он не
      // задавал, нельзя.
      setForm((current) =>
        current.url ? current : { ...current, url: loaded.url_prefix ?? '' },
      );
    } catch (e) {
      // Не повод тревожить: без заготовки форма просто пустая, как и была.
      console.error('Failed to read the repository defaults', e);
    }
  }, []);

  const handleAddRepo = useCallback(async () => {
    const url = form.url.trim();
    if (!url || !adding) return;
    const ok = await run(
      () =>
        api.addRepoSource({
          kind: adding,
          url,
          gitRef: form.gitRef.trim() || null,
          subdir: form.subdir.trim() || null,
          caBundlePath: form.ca.trim() || null,
          // Пусто — не «идти без токена», а «взять сохранённый на этот хост»;
          // решает это бэкенд, он же и хранит секрет.
          token: form.token.trim() || null,
        }),
      `Added ${url}`,
    );
    if (!ok) return;
    setForm(EMPTY_FORM);
    setAdding(null);
  }, [adding, form, run]);

  const handleForgetToken = useCallback(async () => {
    if (!defaults.host || !adding) return;
    await run(
      () => api.forgetHostToken(adding, defaults.host!),
      `Forgot the token for ${defaults.host}`,
    );
  }, [adding, defaults.host, run]);

  /**
   * Сохранённый токен подойдёт тому, что набрано в поле URL.
   *
   * Сверка по хосту обязательна: токен лежит на хост, и пообещать его проекту
   * ДРУГОГО сервера значило бы соврать подписью под пустым полем — а ответ
   * сервера на запрос без токена не отличается от ответа на несуществующий
   * проект. Пока хост ещё не набран, обещание считается верным: в поле стоит
   * ровно та приставка, которую мы сами и подставили.
   */
  const typedHost = hostOf(form.url);
  const savedToken =
    defaults.has_token && !!defaults.host && (!typedHost || typedHost === defaults.host);

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContentNoClose
        className="max-w-2xl bg-surface border-edge text-strong"
        aria-describedby={undefined}
      >
        <DialogHeader className="border-b border-edge pb-4">
          <DialogTitle className="font-mono text-soft text-lg">Schema sources</DialogTitle>
        </DialogHeader>

        <div className="space-y-4 pt-4 min-w-0">
          {sources.length > 0 ? (
            <div className="max-h-80 overflow-auto rounded border border-edge divide-y divide-edge">
              {sources.map((source) => (
                <SourceRow
                  key={source.id}
                  source={source}
                  busy={busy}
                  open={expanded === source.id}
                  onToggle={() => setExpanded(expanded === source.id ? null : source.id)}
                  onRefresh={() =>
                    void run(() => api.refreshSchemaSource(source.id), `Re-read ${source.label}`)
                  }
                  onDelete={() =>
                    void run(() => api.deleteSchemaSource(source.id), `Removed ${source.label}`)
                  }
                />
              ))}
            </div>
          ) : (
            <div className="rounded border border-edge border-dashed p-3 text-center font-mono text-xs text-dim">
              No sources yet
            </div>
          )}

          {adding ? (
            <div className="space-y-2 rounded border border-edge p-3">
              <Label className="font-mono text-xs text-soft">
                {REPO_LABEL[adding]} project URL
              </Label>
              <Input
                value={form.url}
                onChange={(e) => setForm({ ...form, url: e.target.value })}
                placeholder={URL_HINT[adding]}
                className="bg-surface border-edge text-strong font-mono text-xs"
              />
              <div className="grid grid-cols-2 gap-2">
                <div className="space-y-1">
                  {/* Пусто — ветка проекта по умолчанию: её спросят у API, а не
                      заставят выяснять, `master` там или `main`. */}
                  <Label className="font-mono text-xs text-soft">Branch or tag</Label>
                  <Input
                    value={form.gitRef}
                    onChange={(e) => setForm({ ...form, gitRef: e.target.value })}
                    placeholder="default branch"
                    className="bg-surface border-edge text-strong font-mono text-xs"
                  />
                </div>
                <div className="space-y-1">
                  {/* «in dir», а не просто «in»: без слова про каталог поле
                      читается как «контракты такие-то», а не «ищи их там». А
                      сама подпись — не «что скачивать»: указанный `internal`
                      однажды оставил без импортов всё, что тянет общие типы из
                      соседнего `pkg`. */}
                  <Label className="font-mono text-xs text-soft">Contracts in dir</Label>
                  <Input
                    value={form.subdir}
                    onChange={(e) => setForm({ ...form, subdir: e.target.value })}
                    placeholder="whole repository"
                    className="bg-surface border-edge text-strong font-mono text-xs"
                  />
                </div>
              </div>
              <div className="flex items-center justify-between gap-2">
                <Label className="font-mono text-xs text-soft">Access token</Label>
                {/* Сохранённый токен — на ХОСТ, а не на проект: PAT выдают на
                    человека и на инсталляцию, и заводить его заново на каждый
                    из десятков тысяч репозиториев сервера незачем. Забыть —
                    тут же, иначе секрет, переживающий все источники, нечем
                    убрать из приложения. */}
                {savedToken && (
                  <button
                    type="button"
                    onClick={() => void handleForgetToken()}
                    disabled={busy}
                    className="font-mono text-[11px] text-dim hover:text-brand disabled:opacity-50 bg-transparent border-none cursor-pointer"
                    title={`Remove the saved token for ${defaults.host} from the system keychain`}
                  >
                    forget saved token
                  </button>
                )}
              </div>
              <Input
                type="password"
                value={form.token}
                onChange={(e) => setForm({ ...form, token: e.target.value })}
                placeholder={
                  savedToken
                    ? `saved for ${defaults.host} — type to replace`
                    : TOKEN_HINT[adding]
                }
                className="bg-surface border-edge text-strong font-mono text-xs"
              />
              <Label className="font-mono text-xs text-soft">CA bundle</Label>
              <Input
                value={form.ca}
                onChange={(e) => setForm({ ...form, ca: e.target.value })}
                placeholder="system trust store"
                className="bg-surface border-edge text-strong font-mono text-xs"
              />
              <div className="flex gap-2 pt-1">
                <Button
                  onClick={() => setAdding(null)}
                  variant="outline"
                  className="flex-1 bg-transparent border-edge text-soft hover:bg-edge hover:text-strong font-mono"
                >
                  Cancel
                </Button>
                <Button
                  onClick={() => void handleAddRepo()}
                  disabled={busy || !form.url.trim()}
                  className="flex-1 bg-brand text-surface hover:bg-brand-hover font-mono"
                >
                  {busy ? 'Fetching…' : 'Fetch'}
                </Button>
              </div>
            </div>
          ) : (
            // Провайдер выбирается кнопкой, а не угадывается по имени хоста:
            // своя инсталляция живёт на `github.company.com` или
            // `git.company.com`, и догадка по имени ошибалась бы ровно там, где
            // поправить её человеку нечем.
            //
            // «Add a …» переехало из подписей кнопок в заголовок над ними: трёх
            // полных подписей строка не вмещала — третья кнопка вылезала за
            // подложку окна. Повторять глагол на каждой и незачем: он один и
            // тот же, а различаются они ровно одним словом, которое и осталось.
            <div className="space-y-2">
              <Label className="font-mono text-xs text-soft">Add a source</Label>
              {/* `grid`, а не `flex`: колонки равны по построению, и ширина
                  кнопки не зависит от длины подписи. `min-w-0` — чтобы в худшем
                  случае (другой шрифт, другой масштаб) подпись ужалась внутри
                  своей трети, а не раздвинула строку шире окна. */}
              <div className="grid grid-cols-3 gap-2">
                <Button
                  onClick={() => void handleAddFolder()}
                  disabled={busy}
                  variant="outline"
                  className="min-w-0 bg-transparent border-edge text-soft hover:bg-edge hover:text-strong font-mono"
                  title="A directory on this machine — its .proto files are copied into the app"
                >
                  <FolderOpen className="size-4 mr-2" />
                  Folder
                </Button>
                <Button
                  onClick={() => void handleOpenRepo('gitlab')}
                  disabled={busy}
                  variant="outline"
                  className="min-w-0 bg-transparent border-edge text-soft hover:bg-edge hover:text-strong font-mono"
                  title="A GitLab project — its tree is fetched as an archive"
                >
                  <Gitlab className="size-4 mr-2" />
                  GitLab
                </Button>
                <Button
                  onClick={() => void handleOpenRepo('github')}
                  disabled={busy}
                  variant="outline"
                  className="min-w-0 bg-transparent border-edge text-soft hover:bg-edge hover:text-strong font-mono"
                  title="A GitHub project, github.com or an Enterprise Server — its tree is fetched as an archive"
                >
                  <Github className="size-4 mr-2" />
                  GitHub
                </Button>
              </div>
            </div>
          )}

          {/* Бегущая полоска под кнопками — то есть ровно под `Fetch`, когда
              форма раскрыта, и под ↻, когда дерево перезабирают.
              Неопределённая намеренно: заполнять её по процентам значило бы их
              выдумать — архив отдаётся потоком, длины в ответе нет.
              Отвечает она на единственный вопрос, который возникает на тяжёлом
              репозитории: идёт работа или повисло. */}
          <div className="h-0.5 overflow-hidden rounded-full bg-edge" aria-hidden>
            {busy && <div className="h-full w-1/3 bg-brand animate-crawl" />}
          </div>

          <div className="flex pt-2">
            <Button
              onClick={() => onOpenChange(false)}
              variant="outline"
              className="flex-1 bg-transparent border-edge text-soft hover:bg-edge hover:text-strong font-mono"
            >
              Close
            </Button>
          </div>
        </div>
      </DialogContentNoClose>
    </Dialog>
  );
}

/**
 * Хост из URL проекта — тем же правилом, что и на бэкенде (`schema::host_of`).
 *
 * Здесь он нужен ровно на одно: решить, подойдёт ли сохранённый токен тому, что
 * набрано в поле. Копия правила, а не вызов команды, — потому что вопрос
 * возникает на каждое нажатие клавиши.
 */
function hostOf(url: string): string | null {
  const rest = url.trim().split('://').pop() ?? '';
  const host = rest.split(/[/?#]/)[0]?.split(':')[0]?.trim().toLowerCase();
  return host ? host : null;
}

/**
 * Путь контракта с выброшенной серединой: `internal/…/api_events.proto`.
 *
 * Обрезать с краю (`truncate`) здесь нечем помочь: пути в репозитории длиной в
 * шесть сегментов, и в ширину окна влезает только начало — то самое, которое у
 * всех контрактов одинаковое, а имя файла, единственное, что их различает,
 * уезжает за край. Полный путь остаётся в подсказке.
 */
function shortPath(path: string): string {
  const parts = path.split('/');
  if (parts.length < 3) return path;
  return `${parts[0]}/…/${parts[parts.length - 1]}`;
}

interface SourceRowProps {
  source: SourceView;
  busy: boolean;
  open: boolean;
  onToggle: () => void;
  onRefresh: () => void;
  onDelete: () => void;
}

function SourceRow({ source, busy, open, onToggle, onRefresh, onDelete }: SourceRowProps) {
  const Chevron = open ? ChevronDown : ChevronRight;
  const broken = source.sets.filter((set) => set.error).length;
  return (
    <div className="p-2 min-w-0">
      <div className="flex items-center gap-2 min-w-0">
        <button
          type="button"
          onClick={onToggle}
          className="p-0.5 text-dim hover:text-strong bg-transparent border-none cursor-pointer shrink-0"
          title={open ? 'Hide the contracts' : 'Show the contracts'}
        >
          <Chevron className="size-3.5" />
        </button>
        <div className="min-w-0 flex-1">
          <div className="flex items-center gap-1.5 min-w-0">
            <span className="font-mono text-xs text-strong truncate">{source.label}</span>
            <span className="shrink-0 font-mono text-[10px] text-dim border border-edge rounded px-1">
              {source.kind}
            </span>
            {source.git_ref && (
              <span className="shrink-0 font-mono text-[10px] text-dim">{source.git_ref}</span>
            )}
          </div>
          <div className="font-mono text-[11px] text-dim truncate" title={source.location}>
            {source.location}
          </div>
          <div className="font-mono text-[11px] text-dim">
            {source.files} files · {source.sets.length} contracts
            {/* Сломанные — отдельным числом и цветом. Без него заголовок врёт
                самым обидным образом: «19 contracts» читается как «девятнадцать
                готовых к работе», а в выпадающем списке настроек топика их
                оказывается один. */}
            {broken > 0 && <span className="text-brand"> · {broken} broken</span>}
            {source.fetched_at && ` · fetched ${source.fetched_at.slice(0, 19).replace('T', ' ')}`}
          </div>
        </div>
        <button
          type="button"
          onClick={onRefresh}
          disabled={busy}
          className="p-1 text-dim hover:text-brand disabled:opacity-50 bg-transparent border-none cursor-pointer shrink-0"
          title="Fetch this source again"
        >
          <RotateCw className="size-3.5" />
        </button>
        <button
          type="button"
          onClick={onDelete}
          disabled={busy}
          className="p-1 text-dim hover:text-strong disabled:opacity-50 bg-transparent border-none cursor-pointer shrink-0"
          title="Remove this source"
        >
          <Trash2 className="size-3.5" />
        </button>
      </div>

      {source.error && (
        <div className="font-mono text-[11px] text-brand break-words pl-6">{source.error}</div>
      )}

      {open && source.sets.length > 0 && (
        <div className="mt-2 pl-6 space-y-0.5">
          {source.sets.map((set) => (
            // Подсказка живёт на КАЖДОЙ строке своя, а не одна на всю запись.
            // Висела она на этой обёртке — и накрывала собой обе строки: на
            // обрезанном тексте ошибки всплывал полный путь к файлу, то есть
            // ровно то, что и так написано строкой выше. А сама ошибка, которая
            // в окно не влезает, не показывалась целиком нигде.
            <div key={set.entry} className="min-w-0 rounded px-1 py-0.5 hover:bg-edge">
              <div className="font-mono text-[11px] text-soft truncate" title={set.entry}>
                {shortPath(set.entry)}
              </div>
              {/* Что это за типы внутри — здесь не разбирают: сюда приходят за
                  списком контрактов, а тип выбирают уже на топике. А вот
                  ОШИБКУ разбора молчать нельзя — ради неё сломанные наборы и
                  показываются вообще: иначе человеку нечего чинить, а в списке
                  настроек топика их просто нет. */}
              {set.error && (
                // В строке — первая строка ошибки, в подсказке — ошибка
                // целиком: у незарезолвленного импорта за первой строкой идёт
                // перечисление каталогов, где искали, и оно-то и объясняет, что
                // делать.
                <div className="font-mono text-[11px] text-brand truncate" title={set.error}>
                  {set.error.split('\n')[0]}
                </div>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
