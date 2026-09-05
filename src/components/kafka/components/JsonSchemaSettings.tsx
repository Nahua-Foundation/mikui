import { useCallback } from 'react';
import { FileJson, RotateCw, Upload, X } from 'lucide-react';
import { open as openFileDialog } from '@tauri-apps/plugin-dialog';
import { toast } from 'sonner';
import { Button } from '../../ui/button';
import { Label } from '../../ui/label';
import * as api from '../api';
import { describeError } from '../api';
import { JsonView, TopicSchema } from '../types';
import { SubjectPicker, VersionSelect } from './AvroSchema';

interface JsonSchemaSettingsProps {
  /** Ключ кластера — см. `clusterKey`. */
  cluster: string;
  topic: string;
  /** JSON-Schema-половина схемы. null — у топика её ещё нет. */
  json: JsonView | null;
  busy: boolean;
  /** Применить изменение и показать результат — тот же хвост, что у остальных
   *  операций формы настроек. */
  onApply: (action: () => Promise<TopicSchema | null>, success: string) => void;
}

/**
 * JSON-Schema-часть настроек топика.
 *
 * Устроена как аврошная — subject реестра либо локальные файлы, взаимоисключающе
 * — но говорит другое, и разницу стоит держать в голове: ПОКАЗУ тела схема здесь
 * не нужна вообще. За confluent-заголовком лежит обычный JSON, и Rust читает его
 * без всякого контракта. Схема нужна только отправке: она отличает валидное
 * сообщение от текста, который потребитель топика не прочитает.
 *
 * Поэтому здесь нет ни выбора корневой записи (корень у JSON Schema один), ни
 * предупреждений «тела останутся неразобранными»: они не останутся.
 */
export function JsonSchemaSettings({
  cluster,
  topic,
  json,
  busy,
  onApply,
}: JsonSchemaSettingsProps) {
  const files = json?.files ?? [];
  const hasRegistry = json?.registry ?? false;
  const subject = json?.subject ?? null;
  /** Subject, найденный в реестре без участия пользователя. Показывается
   *  только пока свой выбор не сделан: сделанный выбор сильнее. */
  const detected = subject === null && files.length === 0 ? (json?.detected ?? null) : null;

  const handleLoadFiles = useCallback(async () => {
    let picked: string | string[] | null;
    try {
      picked = await openFileDialog({
        multiple: true,
        filters: [{ name: 'JSON schema', extensions: ['json', 'schema.json'] }],
      });
    } catch (e) {
      console.error('Failed to open file dialog', e);
      toast.error(`Failed to open file dialog: ${describeError(e)}`);
      return;
    }

    const paths = Array.isArray(picked) ? picked : picked ? [picked] : [];
    if (paths.length === 0) return;

    onApply(
      () => api.addJsonFiles(cluster, topic, paths),
      `Loaded ${paths.length} schema file${paths.length > 1 ? 's' : ''}`,
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
              onClick={() =>
                onApply(
                  () => api.saveTopicJsonSubject(cluster, topic, null, null),
                  'Unbound the subject',
                )
              }
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
                  () => api.saveTopicJsonSubject(cluster, topic, picked, null),
                  picked ? `Bound to ${picked}` : 'Unbound the subject',
                )
              }
            />
            {subject && (
              <VersionSelect
                cluster={cluster}
                subject={subject}
                version={json?.version ?? null}
                disabled={busy}
                onChange={(version) =>
                  onApply(
                    () => api.saveTopicJsonSubject(cluster, topic, subject, version),
                    version === null
                      ? 'Following the latest version'
                      : `Pinned to version ${version}`,
                  )
                }
              />
            )}
            {/* Ключевое отличие от avro, и сказать его надо прямо: без схемы
                топик всё равно ЧИТАЕТСЯ. Иначе пустое поле выглядит как
                недонастроенное состояние, которым оно не является. */}
            {!subject && files.length === 0 && (
              <div className="font-mono text-xs text-dim">
                {detected ? (
                  <>
                    Using <span className="text-soft">{detected}</span>, found in the registry by
                    this topic&apos;s name. Bodies read without it either way — the schema is
                    what checks messages you send.
                  </>
                ) : (
                  <>
                    Optional: bodies of this format are plain JSON and are shown without a
                    schema. Pick a subject to have messages you send checked against the
                    topic&apos;s contract before they leave.
                  </>
                )}
              </div>
            )}
          </div>
        ) : (
          <div className="rounded border border-edge border-dashed p-3 font-mono text-xs text-dim">
            No schema registry for this cluster. Add one in the connection settings, or load
            schema files below.
          </div>
        )}
      </div>

      {/* Локальные файлы */}
      <div className="space-y-2">
        <div className="flex items-center justify-between">
          <Label className="font-mono text-sm text-soft">Local schema files</Label>
          {files.length > 0 && (
            <button
              type="button"
              onClick={() =>
                onApply(() => api.refreshJsonFiles(cluster, topic), 'Re-read all schema files')
              }
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
            {files.map((file, index) => (
              <div key={file.name} className="flex items-center gap-2 p-2">
                <FileJson className="size-3.5 shrink-0 text-dim" />
                <div className="min-w-0 flex-1">
                  <div className="font-mono text-xs text-slate-50 truncate">
                    {file.name}
                    {/* Первый файл — основная схема, остальные разрешают её
                        `$ref`. Порядок здесь значащий, и молчать о нём нельзя. */}
                    {index === 0 && files.length > 1 && (
                      <span className="text-dim"> · root</span>
                    )}
                  </div>
                  <div className="font-mono text-[11px] text-dim truncate" title={file.source}>
                    {file.source}
                  </div>
                </div>
                <button
                  type="button"
                  onClick={() =>
                    onApply(
                      () => api.refreshJsonFiles(cluster, topic, file.name),
                      `Re-read ${file.name}`,
                    )
                  }
                  disabled={busy}
                  className="p-1 text-dim hover:text-brand disabled:opacity-50 bg-transparent border-none cursor-pointer shrink-0"
                  title="Re-read this file from disk"
                >
                  <RotateCw className="size-3.5" />
                </button>
                <button
                  type="button"
                  onClick={() =>
                    onApply(
                      () => api.removeJsonFile(cluster, topic, file.name),
                      `Removed ${file.name}`,
                    )
                  }
                  disabled={busy}
                  className="p-1 text-dim hover:text-slate-50 disabled:opacity-50 bg-transparent border-none cursor-pointer shrink-0"
                  title="Remove this file"
                >
                  <X className="size-3.5" />
                </button>
              </div>
            ))}
          </div>
        ) : (
          <div className="rounded border border-edge border-dashed p-3 text-center font-mono text-xs text-dim">
            No schema files loaded
          </div>
        )}

        {json?.error && (
          <div className="font-mono text-xs text-brand break-words">{json.error}</div>
        )}

        <Button
          onClick={handleLoadFiles}
          disabled={busy}
          variant="outline"
          className="w-full bg-transparent border-edge text-soft hover:bg-edge hover:text-slate-50 font-mono"
        >
          <Upload className="size-4 mr-2" />
          {busy ? 'Working…' : 'Load Schema Files'}
        </Button>
        {/* Сказать заранее честнее, чем удивить: subject снимется сам. */}
        {subject && (
          <div className="font-mono text-xs text-dim">
            Loading files will unbind {subject}: a topic is checked by one schema, not two.
          </div>
        )}
      </div>
    </div>
  );
}
