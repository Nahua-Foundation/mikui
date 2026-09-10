import { useCallback, useEffect, useState } from 'react';
import { Check, ChevronsUpDown, FileJson, RotateCw, Upload, X } from 'lucide-react';
import { open as openFileDialog } from '@tauri-apps/plugin-dialog';
import { toast } from 'sonner';
import { Button } from '../../ui/button';
import { Label } from '../../ui/label';
import { Popover, PopoverContent, PopoverTrigger } from '../../ui/popover';
import { Command, CommandEmpty, CommandInput, CommandItem, CommandList } from '../../ui/command';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '../../ui/select';
import * as api from '../api';
import { describeError } from '../api';
import { AvroView, TopicSchema } from '../types';

/** Значение селектора версии, означающее «последняя». Строка, потому что
 *  `Select` работает со строками, а пустая у него занята под «не выбрано». */
const LATEST = 'latest';

interface AvroSchemaProps {
  /** Ключ кластера — см. `clusterKey`. */
  cluster: string;
  topic: string;
  /** Avro-половина схемы. null — у топика её ещё нет. */
  avro: AvroView | null;
  busy: boolean;
  /**
   * Применить изменение и показать результат. Тот же хвост, что у
   * protobuf-операций в `TopicConfigModal`: успех виден и в форме, и в уже
   * открытой таблице, неудача не меняет ничего.
   */
  onApply: (action: () => Promise<TopicSchema | null>, success: string) => void;
}

/**
 * Avro-часть настроек топика.
 *
 * Источника схемы два, и они взаимоисключающи: либо subject реестра, либо
 * локальные .avsc. Выбор одного очищает другой — иначе непонятно, чем именно
 * приложение декодирует, а «чем-то из двух, смотря что проверилось раньше» —
 * это не ответ.
 *
 * Отдельный случай, которого нет у protobuf: РЕЕСТР НАСТРОЕН, А СХЕМА НЕ
 * ВЫБРАНА. Это рабочее состояние, а не недонастроенное: в confluent-формате id
 * схемы едет в каждом сообщении, и топику достаточно самого реестра. Поэтому
 * форма не требует выбрать subject и говорит об этом прямо.
 */
export function AvroSchema({ cluster, topic, avro, busy, onApply }: AvroSchemaProps) {
  const files = avro?.files ?? [];
  const records = avro?.records ?? [];
  const hasRegistry = avro?.registry ?? false;
  const subject = avro?.subject ?? null;
  /** Subject, найденный в реестре без участия пользователя. Показывается
   *  только пока свой выбор не сделан: сделанный выбор сильнее. */
  const detected = subject === null && files.length === 0 ? (avro?.detected ?? null) : null;

  const handleLoadFiles = useCallback(async () => {
    let picked: string | string[] | null;
    try {
      picked = await openFileDialog({
        multiple: true,
        filters: [{ name: 'Avro schema', extensions: ['avsc', 'json'] }],
      });
    } catch (e) {
      console.error('Failed to open file dialog', e);
      toast.error(`Failed to open file dialog: ${describeError(e)}`);
      return;
    }

    const paths = Array.isArray(picked) ? picked : picked ? [picked] : [];
    if (paths.length === 0) return;

    onApply(
      () => api.addAvroFiles(cluster, topic, paths),
      `Loaded ${paths.length} .avsc file${paths.length > 1 ? 's' : ''}`,
    );
  }, [cluster, topic, onApply]);

  return (
    <div className="space-y-4">
      {/* Реестр */}
      <div className="space-y-2">
        <div className="flex items-center justify-between">
          <Label className="font-mono text-sm text-soft">Schema Registry</Label>
          {subject && (
            <button
              type="button"
              onClick={() => onApply(() => api.saveTopicAvroSubject(cluster, topic, null, null), 'Unbound the subject')}
              disabled={busy}
              className="font-mono text-xs text-dim hover:text-brand disabled:opacity-50 bg-transparent border-none cursor-pointer"
            >
              unbind
            </button>
          )}
        </div>

        {hasRegistry ? (
          <div className="space-y-2">
            <SubjectPicker
              cluster={cluster}
              subject={subject}
              placeholder={detected ? `${detected} — found automatically` : undefined}
              disabled={busy || files.length > 0}
              onChange={(picked) =>
                onApply(
                  () => api.saveTopicAvroSubject(cluster, topic, picked, null),
                  picked ? `Bound to ${picked}` : 'Unbound the subject',
                )
              }
            />
            {subject && (
              <VersionSelect
                cluster={cluster}
                subject={subject}
                version={avro?.version ?? null}
                disabled={busy}
                onChange={(version) =>
                  onApply(
                    () => api.saveTopicAvroSubject(cluster, topic, subject, version),
                    version === null ? 'Following the latest version' : `Pinned to version ${version}`,
                  )
                }
              />
            )}
            {/* Ровно тот случай, ради которого форма не требует выбора: тело
                в confluent-формате само называет свою схему. А если реестр
                держит и subject этого топика, то и голый datum есть чем
                разобрать — сказать об этом надо прямо, иначе непонятно, откуда
                у топика взялся avro-формат, которого никто не выбирал. */}
            {!subject && files.length === 0 && (
              <div className="font-mono text-xs text-dim">
                {detected ? (
                  <>
                    Using <span className="text-soft">{detected}</span>, found in the registry
                    by this topic&apos;s name. Nothing to pick unless you want a different
                    subject or a pinned version.
                  </>
                ) : (
                  <>
                    Nothing to pick unless you want to: messages written by a standard Avro
                    serializer carry their schema id, and it is resolved through the registry
                    on its own. A subject is needed only for bodies without that header.
                  </>
                )}
              </div>
            )}
          </div>
        ) : (
          <div className="rounded border border-edge border-dashed p-3 font-mono text-xs text-dim">
            No schema registry for this cluster. Add one in the connection settings, or load
            .avsc files below.
          </div>
        )}
      </div>

      {/* Локальные файлы */}
      <div className="space-y-2">
        <div className="flex items-center justify-between">
          <Label className="font-mono text-sm text-soft">Local .avsc</Label>
          {files.length > 0 && (
            <button
              type="button"
              onClick={() => onApply(() => api.refreshAvroFiles(cluster, topic), 'Re-read all .avsc files')}
              disabled={busy}
              className="font-mono text-xs text-dim hover:text-brand disabled:opacity-50 bg-transparent border-none cursor-pointer"
              title="Re-read every file from disk"
            >
              refresh all
            </button>
          )}
        </div>

        {files.length > 0 ? (
          <div className="max-h-40 overflow-auto rounded border border-edge divide-y divide-edge">
            {files.map((file) => (
              <div key={file.name} className="flex items-center gap-2 p-2">
                <FileJson className="size-3.5 shrink-0 text-dim" />
                <div className="min-w-0 flex-1">
                  <div className="font-mono text-xs text-strong truncate">{file.name}</div>
                  <div className="font-mono text-[11px] text-dim truncate" title={file.source}>
                    {file.source}
                  </div>
                </div>
                <button
                  type="button"
                  onClick={() => onApply(() => api.refreshAvroFiles(cluster, topic, file.name), `Re-read ${file.name}`)}
                  disabled={busy}
                  className="p-1 text-dim hover:text-brand disabled:opacity-50 bg-transparent border-none cursor-pointer shrink-0"
                  title="Re-read this file from disk"
                >
                  <RotateCw className="size-3.5" />
                </button>
                <button
                  type="button"
                  onClick={() => onApply(() => api.removeAvroFile(cluster, topic, file.name), `Removed ${file.name}`)}
                  disabled={busy}
                  className="p-1 text-dim hover:text-strong disabled:opacity-50 bg-transparent border-none cursor-pointer shrink-0"
                  title="Remove this file"
                >
                  <X className="size-3.5" />
                </button>
              </div>
            ))}
          </div>
        ) : (
          <div className="rounded border border-edge border-dashed p-3 text-center font-mono text-xs text-dim">
            No .avsc files loaded
          </div>
        )}

        {avro?.error && (
          <div className="font-mono text-xs text-brand break-words">{avro.error}</div>
        )}

        <Button
          onClick={handleLoadFiles}
          disabled={busy}
          variant="outline"
          className="w-full bg-transparent border-edge text-soft hover:bg-edge hover:text-strong font-mono"
        >
          <Upload className="size-4 mr-2" />
          {busy ? 'Working…' : 'Load .avsc Files'}
        </Button>
        {/* Сказать заранее честнее, чем удивить: subject снимется сам. */}
        {subject && (
          <div className="font-mono text-xs text-dim">
            Loading files will unbind {subject}: a topic decodes by one schema, not two.
          </div>
        )}
      </div>

      {/* Запись — только у файлов: у subject корень задан им самим. */}
      {files.length > 0 && (
        <div className="space-y-2">
          <Label className="font-mono text-sm text-soft">Record</Label>
          <Select
            value={avro?.record ?? ''}
            onValueChange={(value) =>
              onApply(() => api.saveTopicAvroRecord(cluster, topic, value), `Decoding as ${value}`)
            }
            disabled={busy || records.length === 0}
          >
            <SelectTrigger className="bg-surface border-edge text-strong font-mono">
              <SelectValue
                placeholder={records.length === 0 ? 'Load an .avsc file first' : 'Choose a record'}
              />
            </SelectTrigger>
            <SelectContent className="bg-surface border-edge">
              {records.map((name) => (
                <SelectItem key={name} value={name} className="text-strong font-mono focus:bg-edge">
                  {name}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          {records.length > 1 && !avro?.record && (
            <div className="font-mono text-xs text-dim">
              Pick the record this topic carries — bodies stay undecoded until you do.
            </div>
          )}
        </div>
      )}
    </div>
  );
}

/**
 * Выбор subject.
 *
 * С поиском, а не обычным списком: в корпоративном реестре subject'ов сотни,
 * и пролистывать их до нужного — не работа. Список тянется ЛЕНИВО, при первом
 * открытии: это сетевой запрос, и делать его на каждое открытие настроек
 * топика незачем.
 */
export function SubjectPicker({
  cluster,
  subject,
  placeholder,
  disabled,
  onChange,
}: {
  cluster: string;
  subject: string | null;
  /** Что стоит в поле, пока выбора нет. Не «Choose a subject», когда subject
   *  уже нашёлся сам: пустое поле над работающей схемой сбивает с толку. */
  placeholder?: string;
  disabled: boolean;
  onChange: (subject: string) => void;
}) {
  const [open, setOpen] = useState(false);
  const [subjects, setSubjects] = useState<string[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    if (!open || subjects !== null || loading) return;
    let cancelled = false;
    setLoading(true);
    api
      .listRegistrySubjects(cluster)
      .then((found) => {
        if (cancelled) return;
        setSubjects(found.slice().sort((a, b) => a.localeCompare(b)));
        setError(null);
      })
      .catch((e) => {
        console.error('Failed to list registry subjects', e);
        // В самом списке, а не тостом: пользователь смотрит сюда, и причина
        // нужна ему здесь же — иначе непонятно, почему список пуст.
        if (!cancelled) setError(describeError(e));
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [open, cluster, subjects, loading]);

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <Button
          variant="outline"
          role="combobox"
          disabled={disabled}
          title={disabled && !subject ? 'Remove the loaded .avsc files to bind a subject' : undefined}
          className="w-full justify-between bg-surface border-edge text-strong font-mono disabled:opacity-50"
        >
          <span className={`truncate ${subject ? '' : 'text-dim'}`}>
            {subject ?? placeholder ?? 'Choose a subject'}
          </span>
          <ChevronsUpDown className="size-4 shrink-0 opacity-50" />
        </Button>
      </PopoverTrigger>
      <PopoverContent className="w-(--radix-popover-trigger-width) p-0 bg-surface border-edge">
        <Command className="bg-surface">
          <CommandInput placeholder="Search subjects…" className="font-mono text-strong" />
          <CommandList>
            <CommandEmpty className="p-3 font-mono text-xs text-dim">
              {loading ? 'Asking the registry…' : (error ?? 'Nothing found')}
            </CommandEmpty>
            {(subjects ?? []).map((name) => (
              <CommandItem
                key={name}
                value={name}
                onSelect={() => {
                  setOpen(false);
                  if (name !== subject) onChange(name);
                }}
                className="font-mono text-xs text-strong data-[selected=true]:bg-edge"
              >
                <Check className={`size-3.5 ${name === subject ? 'opacity-100' : 'opacity-0'}`} />
                <span className="truncate">{name}</span>
              </CommandItem>
            ))}
          </CommandList>
        </Command>
      </PopoverContent>
    </Popover>
  );
}

/**
 * Версия subject.
 *
 * «Latest» — не то же самое, что номер последней: она перечитывается при каждом
 * открытии топика, поэтому топик сам подхватывает новый контракт. Закреплённый
 * номер, наоборот, переживает выкладку новой версии — это и нужно, когда
 * смотришь старые сообщения.
 *
 * Экспортируется ради JSON-Schema-половины настроек: реестр у обеих один, и
 * версия в нём выбирается одинаково.
 */
export function VersionSelect({
  cluster,
  subject,
  version,
  disabled,
  onChange,
}: {
  cluster: string;
  subject: string;
  version: number | null;
  disabled: boolean;
  onChange: (version: number | null) => void;
}) {
  const [versions, setVersions] = useState<number[]>([]);

  useEffect(() => {
    let cancelled = false;
    api
      .listSubjectVersions(cluster, subject)
      .then((found) => {
        if (!cancelled) setVersions(found);
      })
      .catch((e) => {
        // Без тоста: сам subject уже выбран и работает, а без списка версий
        // остаётся «latest» — это ровно то, что было бы по умолчанию.
        console.error('Failed to list subject versions', e);
        if (!cancelled) setVersions([]);
      });
    return () => {
      cancelled = true;
    };
  }, [cluster, subject]);

  return (
    <Select
      value={version === null ? LATEST : String(version)}
      onValueChange={(value) => onChange(value === LATEST ? null : Number(value))}
      disabled={disabled}
    >
      <SelectTrigger className="bg-surface border-edge text-strong font-mono">
        <SelectValue />
      </SelectTrigger>
      <SelectContent className="bg-surface border-edge max-h-80">
        <SelectItem value={LATEST} className="text-strong font-mono focus:bg-edge">
          latest
        </SelectItem>
        {versions.map((v) => (
          <SelectItem key={v} value={String(v)} className="text-strong font-mono focus:bg-edge">
            version {v}
          </SelectItem>
        ))}
      </SelectContent>
    </Select>
  );
}
