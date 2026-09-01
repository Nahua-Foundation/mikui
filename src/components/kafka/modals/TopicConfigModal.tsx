import { useCallback, useEffect, useState } from 'react';
import { Button } from '../../ui/button';
import { Bookmark, FileText, RotateCw, Upload, X } from 'lucide-react';
import { toast } from 'sonner';
import { open as openFileDialog } from '@tauri-apps/plugin-dialog';
import { Dialog, DialogHeader, DialogTitle } from '../../ui/dialog';
import { DialogContentNoClose } from '../DialogContentNoClose';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '../../ui/select';
import { Input } from '../../ui/input';
import { Label } from '../../ui/label';
import * as api from '../api';
import { describeError } from '../api';
import { BodyFormat, Topic, TopicSchema } from '../types';

interface TopicConfigModalProps {
  topic: Topic | null;
  /** Ключ кластера — см. `clusterKey`. null, когда подключения нет. */
  cluster: string | null;
  open: boolean;
  onOpenChange: (open: boolean) => void;
  /**
   * Схема топика изменилась на диске. Зовётся после КАЖДОЙ удавшейся правки, а
   * не только по Save: загрузка файла применяется сразу, и если этот топик
   * открыт, таблица обязана перерисоваться тут же, а не после закрытия окна.
   */
  onSchemaChanged?: (topic: string, schema: TopicSchema | null) => void;
}

const FORMATS: { value: BodyFormat; label: string }[] = [
  { value: 'json', label: 'JSON' },
  { value: 'text', label: 'Text' },
  { value: 'proto', label: 'Proto' },
  { value: 'avro', label: 'Avro' },
];

export function TopicConfigModal({
  topic,
  cluster,
  open,
  onOpenChange,
  onSchemaChanged,
}: TopicConfigModalProps) {
  const [format, setFormat] = useState<BodyFormat>('json');
  const [message, setMessage] = useState<string | null>(null);
  const [schema, setSchema] = useState<TopicSchema | null>(null);
  /** Идёт разбор .proto на бэкенде: кнопки, которые тоже его затронут,
   *  на это время выключены. */
  const [busy, setBusy] = useState(false);
  const [schemaRegistry, setSchemaRegistry] = useState<string>('');
  const [password, setPassword] = useState<string>('');

  const topicName = topic?.name ?? null;

  // Состояние формы принадлежит паре кластер-топик, а модалка переиспользуется
  // для любого из них. Без перечитывания при открытии в ней остались бы файлы
  // и выбор от прошлого топика.
  useEffect(() => {
    if (!open || !cluster || !topicName) return;
    let cancelled = false;

    api
      .getTopicSchema(cluster, topicName)
      .then((loaded) => {
        if (cancelled) return;
        setSchema(loaded);
        setFormat(loaded?.format ?? 'json');
        setMessage(loaded?.message ?? null);
        if (loaded?.error) {
          toast.error(`Proto schema of ${topicName} is broken: ${loaded.error}`);
        }
      })
      .catch((e) => {
        if (cancelled) return;
        console.error('Failed to load topic schema', e);
        toast.error(`Failed to load topic settings: ${describeError(e)}`);
      });

    return () => {
      cancelled = true;
    };
  }, [open, cluster, topicName]);

  /**
   * Общий хвост всех операций над файлами. Успех виден сразу: и в форме, и в
   * уже открытой таблице. Неудача не меняет ничего — на диске бэкенд её тоже
   * не применил.
   */
  const applySchema = useCallback(
    async (action: () => Promise<TopicSchema | null>, success: string) => {
      if (!cluster || !topicName) return;
      setBusy(true);
      try {
        const updated = await action();
        setSchema(updated);
        setFormat(updated?.format ?? 'json');
        setMessage(updated?.message ?? null);
        onSchemaChanged?.(topicName, updated);
        toast.success(success);
      } catch (e) {
        console.error('Proto schema operation failed', e);
        toast.error(describeError(e));
      } finally {
        setBusy(false);
      }
    },
    [cluster, topicName, onSchemaChanged],
  );

  const handleLoadFiles = useCallback(async () => {
    if (!cluster || !topicName) return;
    let picked: string | string[] | null;
    try {
      picked = await openFileDialog({
        multiple: true,
        filters: [{ name: 'Proto', extensions: ['proto'] }],
      });
    } catch (e) {
      console.error('Failed to open file dialog', e);
      toast.error(`Failed to open file dialog: ${describeError(e)}`);
      return;
    }

    const paths = Array.isArray(picked) ? picked : picked ? [picked] : [];
    if (paths.length === 0) return;

    await applySchema(
      () => api.addProtoFiles(cluster, topicName, paths),
      `Loaded ${paths.length} .proto file${paths.length > 1 ? 's' : ''}`,
    );
  }, [cluster, topicName, applySchema]);

  const handleRefresh = useCallback(
    (name?: string) => {
      if (!cluster || !topicName) return;
      void applySchema(
        () => api.refreshProtoFiles(cluster, topicName, name),
        name ? `Re-read ${name}` : 'Re-read all .proto files',
      );
    },
    [cluster, topicName, applySchema],
  );

  const handleRemove = useCallback(
    (name: string) => {
      if (!cluster || !topicName) return;
      void applySchema(() => api.removeProtoFile(cluster, topicName, name), `Removed ${name}`);
    },
    [cluster, topicName, applySchema],
  );

  const handleSave = useCallback(async () => {
    if (!cluster || !topicName) return;
    setBusy(true);
    try {
      const saved = await api.saveTopicSchema(cluster, topicName, format, message);
      onSchemaChanged?.(topicName, saved);
      toast.success(`Settings saved for ${topicName}`);
      onOpenChange(false);
    } catch (e) {
      console.error('Failed to save topic settings', e);
      toast.error(describeError(e));
    } finally {
      setBusy(false);
    }
  }, [cluster, topicName, format, message, onSchemaChanged, onOpenChange]);

  const handleAddToFavorites = () => {
    toast.success(`Added "${topicName}" to favorites`);
  };

  if (!topic) return null;

  const files = schema?.files ?? [];
  const messages = schema?.messages ?? [];

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContentNoClose
        className="max-w-md bg-surface border-edge text-slate-50"
        aria-describedby={undefined}
      >
        <DialogHeader className="border-b border-edge pb-4 min-w-0">
          <div className="flex items-center justify-between min-w-0">
            <DialogTitle className="font-mono text-soft text-lg truncate min-w-0" title={topic.name}>
              {topic.name}
            </DialogTitle>
            <button
              onClick={handleAddToFavorites}
              className="p-1 text-dim hover:text-brand transition-colors duration-200 cursor-pointer border-none bg-transparent outline-none shrink-0"
              title="Add to favorites"
            >
              <Bookmark className="size-4" />
            </button>
          </div>
        </DialogHeader>

        {/* min-w-0: содержимое окна — ячейка грида, а она по умолчанию не уже
            своего min-content. Без этого длинный путь к .proto растягивает
            колонку, и форма вылезает за подложку модалки. */}
        <div className="space-y-6 pt-4 min-w-0">
          {/* Message Type */}
          <div className="space-y-2">
            <Label className="font-mono text-sm text-soft">Message Type</Label>
            <Select value={format} onValueChange={(v) => setFormat(v as BodyFormat)}>
              <SelectTrigger className="bg-surface border-edge text-slate-50 font-mono">
                <SelectValue />
              </SelectTrigger>
              <SelectContent className="bg-surface border-edge">
                {FORMATS.map(({ value, label }) => (
                  <SelectItem
                    key={value}
                    value={value}
                    className="text-slate-50 font-mono focus:bg-edge"
                  >
                    {label}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>

          {/* Proto specific fields */}
          {format === 'proto' && (
            <div className="space-y-4">
              <div className="space-y-2">
                <div className="flex items-center justify-between">
                  <Label className="font-mono text-sm text-soft">Proto Files</Label>
                  {files.length > 0 && (
                    <button
                      type="button"
                      onClick={() => handleRefresh()}
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
                        <FileText className="size-3.5 shrink-0 text-dim" />
                        <div className="min-w-0 flex-1">
                          <div className="font-mono text-xs text-slate-50 truncate">
                            {file.name}
                          </div>
                          {/* Исходный путь: по нему работает «обновить», и
                              именно он отличает два одноимённых контракта. */}
                          <div className="font-mono text-[11px] text-dim truncate" title={file.source}>
                            {file.source}
                          </div>
                        </div>
                        <button
                          type="button"
                          onClick={() => handleRefresh(file.name)}
                          disabled={busy}
                          className="p-1 text-dim hover:text-brand disabled:opacity-50 bg-transparent border-none cursor-pointer shrink-0"
                          title="Re-read this file from disk"
                        >
                          <RotateCw className="size-3.5" />
                        </button>
                        <button
                          type="button"
                          onClick={() => handleRemove(file.name)}
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
                    No .proto files loaded
                  </div>
                )}

                {schema?.error && (
                  <div className="font-mono text-xs text-brand break-words">{schema.error}</div>
                )}

                <Button
                  onClick={handleLoadFiles}
                  disabled={busy}
                  variant="outline"
                  className="w-full bg-transparent border-edge text-soft hover:bg-edge hover:text-slate-50 font-mono"
                >
                  <Upload className="size-4 mr-2" />
                  {busy ? 'Working…' : 'Load Proto Files'}
                </Button>
              </div>

              <div className="space-y-2">
                <Label className="font-mono text-sm text-soft">Message</Label>
                <Select
                  value={message ?? ''}
                  onValueChange={setMessage}
                  disabled={busy || messages.length === 0}
                >
                  <SelectTrigger className="bg-surface border-edge text-slate-50 font-mono">
                    <SelectValue
                      placeholder={
                        messages.length === 0 ? 'Load a .proto file first' : 'Choose a message'
                      }
                    />
                  </SelectTrigger>
                  <SelectContent className="bg-surface border-edge">
                    {messages.map((name) => (
                      <SelectItem
                        key={name}
                        value={name}
                        className="text-slate-50 font-mono focus:bg-edge"
                      >
                        {name}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
                {/* Без выбранного типа декодировать нечем, и тела поедут
                    текстом — сказать об этом дешевле, чем дать гадать. */}
                {messages.length > 1 && !message && (
                  <div className="font-mono text-xs text-dim">
                    Pick the message this topic carries — bodies stay undecoded until you do.
                  </div>
                )}
              </div>
            </div>
          )}

          {/* Avro specific fields */}
          {format === 'avro' && (
            <div className="space-y-4">
              <div className="space-y-2">
                <Label className="font-mono text-sm text-soft">Schema Registry URL</Label>
                <Input
                  value={schemaRegistry}
                  onChange={(e) => setSchemaRegistry(e.target.value)}
                  placeholder="http://localhost:8081"
                  className="bg-surface border-edge text-slate-50 font-mono placeholder:text-dim"
                />
              </div>
              <div className="space-y-2">
                <Label className="font-mono text-sm text-soft">Password</Label>
                <Input
                  type="password"
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  placeholder="Enter password"
                  className="bg-surface border-edge text-slate-50 font-mono placeholder:text-dim"
                />
              </div>
            </div>
          )}

          {/* Action Buttons */}
          <div className="flex gap-3 pt-4">
            <Button
              onClick={() => onOpenChange(false)}
              variant="outline"
              className="flex-1 bg-transparent border-edge text-soft hover:bg-edge hover:text-slate-50 font-mono"
            >
              Cancel
            </Button>
            <Button
              onClick={handleSave}
              disabled={busy || !cluster}
              className="flex-1 bg-brand text-surface hover:bg-brand-hover font-mono"
            >
              Save
            </Button>
          </div>
        </div>
      </DialogContentNoClose>
    </Dialog>
  );
}
