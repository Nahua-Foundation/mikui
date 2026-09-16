import { useCallback, useEffect, useState } from 'react';
import { Button } from '../../ui/button';
import { FileText, RotateCw, Upload, X } from 'lucide-react';
import { toast } from 'sonner';
import { open as openFileDialog } from '@tauri-apps/plugin-dialog';
import { Dialog, DialogHeader, DialogTitle } from '../../ui/dialog';
import { DialogContentNoClose } from '../DialogContentNoClose';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '../../ui/select';
import { Label } from '../../ui/label';
import { AvroSchema } from '../components/AvroSchema';
import { JsonSchemaSettings } from '../components/JsonSchemaSettings';
import { ProtoMessageSelect } from '../components/ProtoMessageSelect';
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

/** Все форматы до единого — тот же список, что в шапке (`FormatSelect`).
 *  Формат, выбранный там, но отсутствующий здесь, оставлял бы этот селектор с
 *  пустым триггером: Radix показывает подпись выбранного пункта, а пункта нет. */
const FORMATS: { value: BodyFormat; label: string }[] = [
  { value: 'json', label: 'JSON' },
  { value: 'text', label: 'Text' },
  { value: 'proto', label: 'Proto' },
  { value: 'avro', label: 'Avro' },
  { value: 'jsonschema', label: 'JSON Schema' },
  { value: 'hex', label: 'Hex' },
];

/**
 * Какой тип message показать, когда схема приехала с бэкенда.
 *
 * Сохранённый выбор — как есть. Если его нет, а в загруженных .proto объявлен
 * ровно один тип, подставляется он: выбирать там не из чего, а без выбора тела
 * не декодируются вовсе. Когда типов несколько, угадать нельзя — поле остаётся
 * пустым, и тогда его требует `handleSave`.
 */
function chooseMessage(schema: TopicSchema | null): string | null {
  const messages = schema?.messages ?? [];
  if (schema?.message) {
    // Пустой список — это «файлов нет вовсе», и судить по нему не о чем:
    // сохранённый выбор остаётся, иначе открытие окна у топика с временно
    // недоступными .proto стирало бы привязку. А вот если список есть и
    // выбора в нём НЕТ, то тип из файла уехал — и держать имя, которого
    // больше не существует, значит обещать декодирование, которого не будет.
    if (messages.length === 0 || messages.includes(schema.message)) return schema.message;
  }
  return messages.length === 1 ? messages[0] : null;
}

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
        setMessage(chooseMessage(loaded));
        if (loaded?.error) {
          toast.error(`Schema of ${topicName} is broken: ${loaded.error}`);
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
    async (
      action: () => Promise<TopicSchema | null>,
      // Функцией, а не строкой, ради одного случая: загрузка .proto может
      // подтянуть файлы, которых пользователь не выбирал, и сказать об этом
      // можно только по результату.
      success: string | ((updated: TopicSchema | null) => string),
    ) => {
      if (!cluster || !topicName) return;
      setBusy(true);
      try {
        const updated = await action();
        setSchema(updated);
        setFormat(updated?.format ?? 'json');
        // Файл только что загрузили — если тип в нём один, он и выбран.
        setMessage(chooseMessage(updated));
        onSchemaChanged?.(topicName, updated);
        toast.success(typeof success === 'string' ? success : success(updated));
      } catch (e) {
        console.error('Schema operation failed', e);
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
      (updated) => {
        const loaded = `Loaded ${paths.length} .proto file${paths.length > 1 ? 's' : ''}`;
        // Подтянутое по импорту стоит назвать числом: пользователь выбрал один
        // файл, а в списке их стало шесть, и молчать об этом незачем.
        const pulled = (updated?.files ?? []).filter((f) => f.auto).length;
        return pulled > 0
          ? `${loaded}, ${pulled} more pulled in by imports`
          : loaded;
      },
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

  if (!topic) return null;

  const files = schema?.files ?? [];
  const messages = schema?.messages ?? [];

  /**
   * Типов объявлено несколько, а выбор не сделан.
   *
   * Сохранять такую настройку незачем: без типа protobuf-тело не разобрать —
   * в .proto нет ничего, что сказало бы, каким из объявленных сообщений
   * записан топик. Раньше это сохранялось молча, и топик потом показывался
   * текстом без внятной причины.
   *
   * Про «несколько» — потому что единственный тип подставляет `chooseMessage`,
   * и пустым поле в этом случае не остаётся. Пустой список .proto тоже не
   * повод запрещать: файлы можно добавить и позже, а формат назначить сейчас.
   */
  const needsMessage = format === 'proto' && messages.length > 1 && !message;

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContentNoClose
        className="max-w-md bg-surface border-edge text-strong"
        aria-describedby={undefined}
      >
        {/* Кнопки «в избранное» здесь больше нет: она показывала тост и не
            делала ничего — избранного у топиков в приложении не существует. */}
        <DialogHeader className="border-b border-edge pb-4 min-w-0">
          <DialogTitle className="font-mono text-soft text-lg truncate min-w-0" title={topic.name}>
            {topic.name}
          </DialogTitle>
        </DialogHeader>

        {/* min-w-0: содержимое окна — ячейка грида, а она по умолчанию не уже
            своего min-content. Без этого длинный путь к .proto растягивает
            колонку, и форма вылезает за подложку модалки. */}
        <div className="space-y-6 pt-4 min-w-0">
          {/* Message Type */}
          <div className="space-y-2">
            <Label className="font-mono text-sm text-soft">Message Type</Label>
            <Select value={format} onValueChange={(v) => setFormat(v as BodyFormat)}>
              <SelectTrigger className="bg-surface border-edge text-strong font-mono">
                <SelectValue />
              </SelectTrigger>
              <SelectContent className="bg-surface border-edge">
                {FORMATS.map(({ value, label }) => (
                  <SelectItem
                    key={value}
                    value={value}
                    className="text-strong font-mono focus:bg-edge"
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
                        <FileText
                          className={`size-3.5 shrink-0 ${file.auto ? 'text-dim/60' : 'text-dim'}`}
                        />
                        <div className="min-w-0 flex-1">
                          <div className="flex items-center gap-1.5 min-w-0">
                            <div
                              className={`font-mono text-xs truncate ${file.auto ? 'text-soft' : 'text-strong'}`}
                            >
                              {file.name}
                            </div>
                            {/* Файл не выбирали — его нашли по import.
                                Сказать об этом надо: иначе в списке появляются
                                файлы, которых пользователь туда не кладл, и
                                непонятно, откуда они и почему без кнопок. */}
                            {file.auto && (
                              <span
                                className="shrink-0 font-mono text-[10px] text-dim border border-edge rounded px-1"
                                title="Found by an import in the selected files"
                              >
                                import
                              </span>
                            )}
                          </div>
                          {/* Исходный путь: по нему работает «обновить», и
                              именно он отличает два одноимённых контракта. */}
                          <div className="font-mono text-[11px] text-dim truncate" title={file.source}>
                            {file.source}
                          </div>
                        </div>
                        {/* У зависимости кнопок нет. Перечитывать её отдельно
                            незачем — она читается с диска заново при каждом
                            изменении набора, — а удалить её нельзя: она
                            держится импортом и уйдёт вместе с ним. */}
                        {!file.auto && (
                          <>
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
                              className="p-1 text-dim hover:text-strong disabled:opacity-50 bg-transparent border-none cursor-pointer shrink-0"
                              title="Remove this file"
                            >
                              <X className="size-3.5" />
                            </button>
                          </>
                        )}
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
                  className="w-full bg-transparent border-edge text-soft hover:bg-edge hover:text-strong font-mono"
                >
                  <Upload className="size-4 mr-2" />
                  {busy ? 'Working…' : 'Load Proto Files'}
                </Button>
              </div>

              <div className="space-y-2">
                <Label className="font-mono text-sm text-soft">Message</Label>
                <ProtoMessageSelect
                  messages={messages}
                  value={message}
                  onChange={setMessage}
                  disabled={busy}
                />
                {/* Незаполненное обязательное поле, а не совет: без типа
                    декодировать нечем, и Save заблокирован. Сказать об этом
                    здесь, у самого поля, дешевле, чем дать гадать, почему
                    кнопка внизу не нажимается. */}
                {needsMessage && (
                  <div className="font-mono text-xs text-dim">
                    The loaded files declare {messages.length} messages — pick the one this topic
                    carries. Saving is blocked until you do.
                  </div>
                )}
              </div>
            </div>
          )}

          {/* Avro specific fields */}
          {format === 'avro' && cluster && topicName && (
            <AvroSchema
              cluster={cluster}
              topic={topicName}
              avro={schema?.avro ?? null}
              busy={busy}
              onApply={(action, success) => void applySchema(action, success)}
            />
          )}

          {/* JSON Schema specific fields */}
          {format === 'jsonschema' && cluster && topicName && (
            <JsonSchemaSettings
              cluster={cluster}
              topic={topicName}
              json={schema?.json ?? null}
              busy={busy}
              onApply={(action, success) => void applySchema(action, success)}
            />
          )}

          {/* Реестр держит для топика protobuf-схему, а тянуть её оттуда
              приложение пока не умеет — только из локальных .proto. Промолчать
              значило бы оставить пользователя гадать, почему топик со схемой
              показывается текстом. */}
          {schema?.detected_kind === 'PROTOBUF' && format !== 'proto' && (
            <div className="rounded border border-edge bg-sunken p-3 font-mono text-xs text-dim">
              The registry holds a <span className="text-soft">PROTOBUF</span> schema for this
              topic. Loading schemas of that format from the registry is not supported yet —
              switch the format to <span className="text-soft">proto</span> and load the .proto
              file to decode these bodies.
            </div>
          )}

          {/* Action Buttons */}
          <div className="flex gap-3 pt-4">
            <Button
              onClick={() => onOpenChange(false)}
              variant="outline"
              className="flex-1 bg-transparent border-edge text-soft hover:bg-edge hover:text-strong font-mono"
            >
              Cancel
            </Button>
            {/* `disabled:pointer-events-auto` — ради подсказки: базовый класс
                кнопки глушит выключенной события, а вместе с ними и `title`.
                Нажатие от этого не появляется, выключенная кнопка событий
                click не отдаёт. */}
            <Button
              onClick={handleSave}
              disabled={busy || !cluster || needsMessage}
              title={needsMessage ? 'Pick the message type this topic carries first' : undefined}
              className="flex-1 bg-brand text-surface hover:bg-brand-hover font-mono disabled:pointer-events-auto disabled:cursor-not-allowed"
            >
              Save
            </Button>
          </div>
        </div>
      </DialogContentNoClose>
    </Dialog>
  );
}
