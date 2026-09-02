import { useEffect, useMemo, useState } from 'react';
import { ChevronDown, ChevronRight, Search } from 'lucide-react';
import { Button } from '../../ui/button';
import { Input } from '../../ui/input';
import { Dialog, DialogHeader, DialogTitle } from '../../ui/dialog';
import { DialogContentNoClose } from '../DialogContentNoClose';
import * as api from '../api';
import { describeError } from '../api';
import { formatBytes } from '../format';
import { PartitionShare, partitionColors } from '../components/PartitionShare';
import { PartitionDetails, Topic, TopicDetails } from '../types';

interface TopicInfoModalProps {
  topic: Topic | null;
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

/** Значения, которого нет. Пустая строка и «не сообщили» — не одно и то же,
 *  и рисовать их одинаково пустым местом значило бы врать про второе. */
const NO_VALUE = '—';

/** Почему число сообщений — оценка, а не счёт. Вопрос возникает первым же. */
const MESSAGES_HINT =
  'Sum of high − low across partitions. An upper bound, not a count: transaction ' +
  'markers take offsets too, and compaction leaves gaps. Read from ListOffsets, ' +
  'which carries no records and costs no read quota.';

/** Одна цифра из устройства топика. */
function Stat({
  label,
  value,
  alarming,
  hint,
}: {
  label: string;
  value: string;
  alarming?: boolean;
  hint?: string;
}) {
  return (
    <div className="flex flex-col gap-1 rounded border border-edge px-3 py-2" title={hint}>
      <span className="font-mono text-[11px] text-dim">{label}</span>
      <span className={`font-mono text-sm ${alarming ? 'text-danger' : 'text-slate-50'}`}>
        {value}
      </span>
    </div>
  );
}

/** Сколько сообщений лежит в топике — с оговоркой, когда границы сняты не со
 *  всех партиций: там это уже не оценка топика, а нижняя граница. */
function messagesLabel(details: TopicDetails): string {
  if (details.messages === null) return NO_VALUE;
  const prefix = details.offsets_partial ? '≥ ' : '~';
  return `${prefix}${details.messages.toLocaleString()}`;
}

/** Границы партиции строкой. Пустая партиция видна по совпавшим офсетам, и
 *  показать это честнее, чем нарисовать «0». */
function offsetsLabel(partition: PartitionDetails): string {
  if (partition.low === null || partition.high === null) return NO_VALUE;
  return `${partition.low.toLocaleString()} … ${partition.high.toLocaleString()}`;
}

function messageCount(partition: PartitionDetails): string {
  if (partition.low === null || partition.high === null) return NO_VALUE;
  return (partition.high - partition.low).toLocaleString();
}

/**
 * Сколько топик занимает на диске брокера.
 *
 * Точного числа взять негде: размер лога Kafka отдаёт запросом
 * `DescribeLogDirs`, которого librdkafka не реализует. Но нужное измеримо
 * косвенно и бесплатно. Байты, пришедшие по сети, — это данные в том же виде,
 * в каком они лежат у брокера: сжатыми, вместе с накладными расходами формата
 * записи. Поделив их на число распаковавшихся сообщений, получаем цену одного
 * сообщения НА ДИСКЕ — со сжатием ровно тем, каким сжат этот топик, а не
 * угаданным по `compression.type`.
 *
 * Знаменатель считает всё, что распаковалось, включая выброшенное нами за
 * границей окна, — иначе в оценку вошла бы наша же предвыборка.
 *
 * `null` — оценивать не по чему: топик не открыт, прочитано слишком мало либо
 * не известно число сообщений.
 */
function diskEstimate(details: TopicDetails): { text: string; hint: string } | null {
  const {
    sample_wire_bytes: wire,
    sample_messages: sampled,
    sample_bytes: raw,
    messages,
    replication_factor: replicas,
  } = details;
  if (!wire || !sampled || raw === null || messages === null) return null;

  const perMessage = wire / sampled;
  const log = Math.round(perMessage * messages);
  const ratio = raw / wire;
  const compression =
    ratio >= 1.2 ? `about ${ratio.toFixed(1)}× compressed` : 'stored essentially uncompressed';
  const cluster =
    replicas && replicas > 1
      ? ` Across ${replicas} replicas the cluster holds about ${formatBytes(log * replicas)}.`
      : '';

  return {
    text: `${details.offsets_partial ? '≥' : '≈'} ${formatBytes(log)} on server disk`,
    hint:
      `Measured, not guessed: ${formatBytes(wire)} came off the wire for the ` +
      `${sampled.toLocaleString()} messages read from this topic (${compression}), which is ` +
      `~${formatBytes(Math.round(perMessage))} per message as the broker stores it — times the ` +
      'estimated message count. Order of magnitude only: the sample comes from one end of the ' +
      `topic, so a topic whose message sizes drifted over time will be off.${cluster}`,
  };
}

/**
 * Что за топик открыт: из чего он состоит и как настроен.
 *
 * Только чтение. Правка настроек топика — операция над кластером, а не над
 * просмотром, и делать её из просмотрщика между делом не следует: сюда
 * приходят посмотреть, чем объясняется то, что видно в таблице (сколько
 * хранится, чем сжато, во сколько реплик пишется, где кончились сообщения).
 */
export function TopicInfoModal({ topic, open, onOpenChange }: TopicInfoModalProps) {
  const [details, setDetails] = useState<TopicDetails | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [needle, setNeedle] = useState('');
  /** Разбивка по партициям нужна не всегда, а места занимает много — на
   *  топике в сотню партиций она вытеснила бы собой всё остальное. */
  const [showPartitions, setShowPartitions] = useState(false);

  const topicName = topic?.name ?? null;

  // Спрашивается у кластера при каждом открытии, а не кэшируется: и настройки,
  // и границы партиций меняются вне приложения, и показать вчерашний retention
  // опаснее, чем подождать ответа.
  useEffect(() => {
    if (!open || !topicName) return;
    let cancelled = false;

    setLoading(true);
    setError(null);
    setDetails(null);
    setNeedle('');
    setShowPartitions(false);

    api
      .describeTopic(topicName)
      .then((loaded) => {
        if (!cancelled) setDetails(loaded);
      })
      .catch((e) => {
        if (cancelled) return;
        console.error('Failed to describe topic', e);
        setError(describeError(e));
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });

    return () => {
      cancelled = true;
    };
  }, [open, topicName]);

  // Параметров у топика под сотню, а ищут в них обычно один известный по
  // имени — без поиска список приходится проглядывать целиком.
  const shown = useMemo(() => {
    const text = needle.trim().toLowerCase();
    const config = details?.config ?? [];
    if (!text) return config;
    return config.filter(
      (entry) =>
        entry.name.toLowerCase().includes(text) ||
        (entry.value ?? '').toLowerCase().includes(text),
    );
  }, [details, needle]);

  const volume = details && diskEstimate(details);
  /** Та же раскладка, что и у полосы: таблица служит ей легендой, и
   *  разъехаться этим двум местам нельзя. */
  const colors = useMemo(() => partitionColors(details?.partitions ?? []), [details]);

  if (!topic) return null;

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContentNoClose
        className="max-w-2xl bg-surface border-edge text-slate-50"
        aria-describedby={undefined}
      >
        <DialogHeader className="border-b border-edge pb-4 min-w-0">
          <DialogTitle className="font-mono text-soft text-lg truncate min-w-0" title={topic.name}>
            {topic.name}
          </DialogTitle>
        </DialogHeader>

        <div className="space-y-4 pt-2 min-w-0">
          {loading && (
            <div className="font-mono text-sm text-dim py-8 text-center">Reading the topic…</div>
          )}

          {error && <div className="font-mono text-sm text-danger break-words">{error}</div>}

          {details && (
            <>
              <div className="grid grid-cols-4 gap-2">
                <Stat label="partitions" value={String(details.partitions.length)} />
                <Stat
                  label="replicas"
                  value={
                    details.replication_factor === null
                      ? NO_VALUE
                      : String(details.replication_factor)
                  }
                  hint="Replicas per partition, the largest across the topic."
                />
                <Stat
                  label="messages"
                  value={messagesLabel(details)}
                  hint={
                    details.offsets_partial
                      ? `${MESSAGES_HINT} Offsets of some partitions could not be read in time, ` +
                        'so this counts only part of the topic.'
                      : MESSAGES_HINT
                  }
                />
                <Stat
                  label="under-replicated"
                  value={String(details.under_replicated)}
                  alarming={details.under_replicated > 0}
                  hint="Partitions whose in-sync replica set is smaller than their replica set."
                />
              </div>

              {/* Размер топика кластер не сообщает (`DescribeLogDirs`
                  librdkafka не реализует), но у ОТКРЫТОГО топика он измерим по
                  уже перекачанному, и это ничего не стоит. Строкой, а не
                  плиткой: цифра без объяснения, откуда она взялась, вводила бы
                  в заблуждение сильнее, чем её отсутствие. */}
              {volume && (
                <p className="font-mono text-xs text-soft" title={volume.hint}>
                  {volume.text}
                  <span className="text-dim">
                    {' '}
                    · measured on {details.sample_messages?.toLocaleString()} messages read
                  </span>
                </p>
              )}

              {/* Разбивка по партициям: где сообщения кончились, какая пустая,
                  у какой отвалился лидер. Свёрнута — на сотне партиций она
                  вытеснила бы собой настройки, за которыми сюда и приходят. */}
              {details.partitions.length > 0 && (
                <div className="space-y-2">
                  <PartitionShare partitions={details.partitions} />

                  <button
                    type="button"
                    onClick={() => setShowPartitions((v) => !v)}
                    className="flex items-center gap-1 font-mono text-xs text-dim hover:text-slate-50 bg-transparent border-none p-0 cursor-pointer transition-colors"
                  >
                    {showPartitions ? (
                      <ChevronDown className="size-3.5" />
                    ) : (
                      <ChevronRight className="size-3.5" />
                    )}
                    {showPartitions
                      ? 'hide partitions'
                      : `show ${details.partitions.length} partitions`}
                  </button>

                  {showPartitions && (
                    <div className="max-h-48 overflow-auto rounded border border-edge divide-y divide-edge">
                      <div className="grid grid-cols-[3rem_4rem_4rem_minmax(0,1fr)_6rem] gap-2 px-3 py-1.5 font-mono text-[11px] text-dim sticky top-0 bg-surface">
                        <span>part</span>
                        <span>leader</span>
                        <span>isr</span>
                        <span>offsets</span>
                        <span className="text-right">messages</span>
                      </div>
                      {details.partitions.map((partition) => {
                        const lagging = partition.in_sync < partition.replicas;
                        const swatch = colors.get(partition.id);
                        return (
                          <div
                            key={partition.id}
                            className="grid grid-cols-[3rem_4rem_4rem_minmax(0,1fr)_6rem] gap-2 px-3 py-1.5 font-mono text-xs"
                          >
                            {/* Точка того же цвета, что и сектор полосы: эта
                                таблица и есть легенда к ней, отдельная
                                повторяла бы её целиком. У партиции без сектора
                                (пустой либо не измеренной) точка полая — иначе
                                легенда обещала бы цвет, которого в полосе
                                нет. */}
                            <span className="flex items-center gap-1.5 text-soft">
                              <span
                                className={`size-2 shrink-0 rounded-full ${
                                  swatch ? '' : 'border border-edge'
                                }`}
                                style={swatch ? { backgroundColor: swatch.fill } : undefined}
                              />
                              {partition.id}
                            </span>
                            {/* -1 — лидера нет: писать и читать из партиции
                                сейчас нельзя, и это не деталь оформления. */}
                            <span className={partition.leader < 0 ? 'text-danger' : 'text-soft'}>
                              {partition.leader < 0 ? 'none' : partition.leader}
                            </span>
                            <span className={lagging ? 'text-danger' : 'text-soft'}>
                              {partition.in_sync}/{partition.replicas}
                            </span>
                            <span className="text-slate-50 truncate" title={offsetsLabel(partition)}>
                              {offsetsLabel(partition)}
                            </span>
                            <span className="text-slate-50 text-right">
                              {messageCount(partition)}
                            </span>
                          </div>
                        );
                      })}
                    </div>
                  )}
                </div>
              )}

              {/* Настройки закрыты отдельным правом (DescribeConfigs), и его
                  вполне может не быть. Устройство топика выше при этом
                  показано — молчать про причину было бы хуже всего. */}
              {details.config_error && (
                <div className="rounded border border-edge border-dashed p-3 font-mono text-xs text-dim break-words">
                  Settings are unavailable: {details.config_error}
                </div>
              )}

              {details.config.length > 0 && (
                <>
                  <div className="relative">
                    <Search className="absolute left-2 top-1/2 transform -translate-y-1/2 size-4 text-dim" />
                    <Input
                      value={needle}
                      onChange={(e) => setNeedle(e.target.value)}
                      placeholder="Filter settings..."
                      className="bg-surface border-edge text-slate-50 font-mono placeholder:text-dim pl-8"
                    />
                  </div>

                  <div className="max-h-72 overflow-auto rounded border border-edge divide-y divide-edge">
                    {shown.map((entry) => (
                      <div
                        key={entry.name}
                        className="grid grid-cols-[minmax(0,1fr)_minmax(0,1fr)] gap-3 px-3 py-1.5"
                      >
                        <span className="font-mono text-xs text-soft break-all">{entry.name}</span>
                        {/* Унаследованное от кластера значение приглушено:
                            в ответе приезжают все параметры подряд, и без
                            этой разницы не видно, что топику задали руками. */}
                        <span
                          className={`font-mono text-xs break-all ${
                            entry.is_default ? 'text-dim' : 'text-slate-50'
                          }`}
                        >
                          {entry.value === null || entry.value === '' ? NO_VALUE : entry.value}
                        </span>
                      </div>
                    ))}
                    {shown.length === 0 && (
                      <div className="px-3 py-4 text-center font-mono text-xs text-dim">
                        No settings match "{needle}"
                      </div>
                    )}
                  </div>

                  <p className="font-mono text-[11px] text-dim">
                    Dimmed values are inherited from the cluster; the rest are set on this topic.
                  </p>
                </>
              )}
            </>
          )}

          <div className="flex pt-2">
            <Button
              onClick={() => onOpenChange(false)}
              variant="outline"
              className="flex-1 bg-transparent border-edge text-soft hover:bg-edge hover:text-slate-50 font-mono"
            >
              Close
            </Button>
          </div>
        </div>
      </DialogContentNoClose>
    </Dialog>
  );
}
