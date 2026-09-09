import { useEffect, useState } from 'react';
import { AlertTriangle, Link2 } from 'lucide-react';
import { Button } from '../../ui/button';
import { Input } from '../../ui/input';
import { Dialog, DialogHeader, DialogTitle, DialogDescription } from '../../ui/dialog';
import { DialogContentNoClose } from '../DialogContentNoClose';
import { MessageLinkTarget } from '../types';
import { formatTimestamp } from '../format';
import * as api from '../api';

/** Строка «подпись — значение». Подпись фиксированной ширины: значения тогда
 *  стоят одной колонкой и сравниваются взглядом, а не поиском по строке. */
function Field({ label, value, accent }: { label: string; value: string; accent?: boolean }) {
  return (
    <div className="flex items-baseline gap-4 font-mono">
      <div className="w-24 shrink-0 text-xs text-dim">{label}</div>
      <div className={`min-w-0 truncate ${accent ? 'text-brand' : 'text-slate-50'}`} title={value}>
        {value}
      </div>
    </div>
  );
}

/**
 * Переход по ссылке на сообщение.
 *
 * Одно окно на два входа. Ссылку либо приносит система (`incoming` — клик по
 * `mikui://…` в переписке), либо её вставляют руками. Второй вход не запасной:
 * `mikui://` регистрирует ОС, то есть работает он только у установленной
 * сборки, а половина почтовых клиентов и мессенджеров незнакомую схему
 * кликабельной не делает вовсе. Отправить в такой ситуации человека
 * переустанавливать приложение — не ответ.
 *
 * Подтверждение спрашивается ВСЕГДА, даже когда ничего не закрывается. Переход
 * по ссылке — это подключение к кластеру по данным, приехавшим извне, и
 * выполнять его молча приложение не должно. Заодно это единственное место, где
 * человеку показывают, куда он идёт, до того как туда пойти.
 */
export function OpenLinkModal({
  open,
  onOpenChange,
  incoming,
  connectedClusterId,
  connectedName,
  openTopic,
  onFollow,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  /** Ссылка, принесённая системой. null — открыли пунктом меню. */
  incoming: string | null;
  /**
   * К чему подключены сейчас — чтобы предупредить, что это закроется.
   *
   * Идентификатор, а не имя: два сохранённых подключения вполне могут
   * называться одинаково, и по имени переход на соседний кластер выглядел бы
   * как переход на тот же самый — то есть без предупреждения.
   */
  connectedClusterId: string | null;
  /** Как оно называется — для самого текста предупреждения. */
  connectedName: string | null;
  /** Что сейчас открыто в таблице. */
  openTopic: string | null;
  onFollow: (target: MessageLinkTarget) => void;
}) {
  const [text, setText] = useState('');
  const [target, setTarget] = useState<MessageLinkTarget | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  // Каждое открытие — с чистого листа: разобранная в прошлый раз ссылка к
  // нынешней отношения не имеет, а показанная ошибка тем более.
  useEffect(() => {
    if (!open) return;
    setText(incoming ?? '');
    setTarget(null);
    setError(null);
    setBusy(false);
  }, [open, incoming]);

  const resolve = (url: string) => {
    setBusy(true);
    setError(null);
    api
      .resolveMessageLink(url)
      .then(setTarget)
      .catch((e) => setError(api.describeError(e)))
      .finally(() => setBusy(false));
  };

  // Ссылку, принесённую системой, разбираем сами: человек уже нажал на неё
  // один раз, и просить его нажать ещё раз «Open» здесь было бы издевательством.
  // Подтверждение он всё равно увидит — просто следующим шагом.
  useEffect(() => {
    if (open && incoming) resolve(incoming);
    // resolve пересоздаётся каждый рендер, но зависит только от аргумента.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open, incoming]);

  const follow = () => {
    if (!target) return;
    onFollow(target);
    onOpenChange(false);
  };

  // Предупреждаем ровно о том, что и правда закроется. Подключение у воркера
  // одно, открытый топик тоже один: переход по ссылке на соседний кластер
  // уводит с того, на что человек сейчас смотрит.
  // `connectedName` — признак «подключены хоть к чему-то»: у подключения из
  // формы идентификатора нет вовсе, и такой сеанс переход тоже разорвёт.
  const leavingCluster =
    !!target && !!connectedName && connectedClusterId !== target.cluster ? connectedName : null;
  const leavingTopic = !!target && !!openTopic && openTopic !== target.topic ? openTopic : null;

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      {/* 50rem вместо 42rem: в окне четыре поля с длинными значениями — имя
          топика и имя кластера, — и в прежнюю ширину они укладывались только
          усечением. */}
      <DialogContentNoClose className="max-w-[50rem] sm:max-w-[50rem] bg-surface border-edge text-slate-50">
        <DialogHeader className="border-b border-edge pb-4">
          <DialogTitle className="font-mono text-soft text-lg">Open a message link</DialogTitle>
          <DialogDescription className="font-mono text-soft text-sm">
            {target
              ? 'This is where the link leads'
              : 'Paste a mikui:// link someone shared with you'}
          </DialogDescription>
        </DialogHeader>

        {target ? (
          <div className="space-y-4">
            {/* Поля строго друг под другом, подпись слева.
                Раньше они стояли сеткой 2×2, и читать приходилось зигзагом —
                а это ровно то место, где человек СВЕРЯЕТ, туда ли он идёт. */}
            <div className="bg-sunken border border-edge rounded-lg p-4 space-y-2">
              {/* Имя кластера МЕСТНОЕ: у отправителя тот же кластер может
                  называться иначе, и показать его имя значило бы указать на
                  строку, которой в списке подключений получателя нет. */}
              <Field label="Cluster" value={target.cluster_name} />
              <Field label="Topic" value={target.topic} accent />
              <Field label="Partition" value={String(target.partition)} />
              <Field label="Offset" value={String(target.offset)} />
              <Field
                label="Timestamp"
                value={target.timestamp ? formatTimestamp(target.timestamp) : '—'}
              />
            </div>

            {/* У иконки нет сдвига: у `text-xs` строка ровно 16px, столько же
                в `size-4`, и любой `mt` роняет её ниже текста. */}
            {(leavingCluster || leavingTopic) && (
              <div className="flex items-start gap-2 font-mono text-xs text-brand">
                <AlertTriangle className="size-4 shrink-0" />
                <span>
                  {leavingCluster
                    ? `Opening it disconnects from ${leavingCluster}`
                    : 'Opening it closes the current topic'}
                  {leavingCluster && leavingTopic ? ` and closes ${leavingTopic}` : ''}.
                </span>
              </div>
            )}

            <div className="flex justify-end gap-2">
              <Button
                variant="outline"
                size="sm"
                className="bg-transparent border-edge text-soft hover:bg-edge hover:text-slate-50"
                onClick={() => onOpenChange(false)}
              >
                Cancel
              </Button>
              <Button
                size="sm"
                className="bg-brand text-surface hover:bg-brand-hover font-mono"
                onClick={follow}
              >
                <Link2 className="size-4 mr-1" />
                Open message
              </Button>
            </div>
          </div>
        ) : (
          <div className="space-y-3">
            <div className="flex items-center gap-2">
              <Input
                autoFocus
                value={text}
                onChange={(e) => setText(e.target.value)}
                onKeyDown={(e) => e.key === 'Enter' && text.trim() && resolve(text)}
                placeholder="mikui://message/v1?…"
                className="bg-sunken border-edge text-slate-50 font-mono text-xs placeholder:text-dim"
              />
              <Button
                size="sm"
                disabled={busy || !text.trim()}
                className="bg-brand text-surface hover:bg-brand-hover font-mono shrink-0"
                onClick={() => resolve(text)}
              >
                {busy ? 'Checking…' : 'Open'}
              </Button>
            </div>

            {/* Ошибка разбора и ошибка «нет такого подключения» показываются
                одинаково и целиком: обе объясняют, что делать, и сокращать их
                до «invalid link» — значит выбросить ровно ту часть, ради
                которой они написаны. */}
            {error && (
              <div className="flex items-start gap-2 font-mono text-xs text-danger break-words">
                <AlertTriangle className="size-4 shrink-0" />
                <span>{error}</span>
              </div>
            )}
          </div>
        )}
      </DialogContentNoClose>
    </Dialog>
  );
}
