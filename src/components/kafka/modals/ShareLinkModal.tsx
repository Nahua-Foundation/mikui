import { useEffect, useRef } from 'react';
import { Copy, Link2 } from 'lucide-react';
import { toast } from 'sonner';
import { Button } from '../../ui/button';
import { Input } from '../../ui/input';
import { Dialog, DialogHeader, DialogTitle, DialogDescription } from '../../ui/dialog';
import { DialogContentNoClose } from '../DialogContentNoClose';

/**
 * Ссылка на сообщение — показанная и уже положенная в буфер обмена.
 *
 * Копирование происходит при открытии окна, а не по кнопке: за «поделиться»
 * нажали один раз, и требовать второго нажатия ради того же самого незачем.
 * Кнопка всё равно есть — для повторного копирования, если буфер успели занять
 * чем-то другим.
 *
 * Саму ссылку окно ПОКАЗЫВАЕТ, а не ограничивается тостом «скопировано».
 * Ссылка отправляется людям, и видеть, что именно уедет в переписку, надо до
 * отправки, а не после: в ней имя топика и имя кластера.
 */
export function ShareLinkModal({
  url,
  description,
  open,
  onOpenChange,
}: {
  /** null — ссылку ещё не собрали. */
  url: string | null;
  /** Откуда сообщение: кластер, топик и координаты. */
  description?: React.ReactNode;
  open: boolean;
  onOpenChange: (open: boolean) => void;
}) {
  const field = useRef<HTMLInputElement>(null);

  const copy = (text: string) => {
    navigator.clipboard.writeText(text);
    toast.success('Link copied to clipboard');
  };

  // Копируем ровно один раз на открытие, а не на каждый рендер.
  useEffect(() => {
    if (!open || !url) return;
    copy(url);
    // Выделяем текст: так видно, что скопировалось, и так же ссылку можно
    // забрать руками, если в системе с буфером что-то не так.
    field.current?.select();
  }, [open, url]);

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContentNoClose className="max-w-2xl bg-surface border-edge text-slate-50">
        <DialogHeader className="border-b border-edge pb-4">
          <DialogTitle className="font-mono text-soft text-lg">Share this message</DialogTitle>
          <DialogDescription className="font-mono text-soft text-sm">
            {description}
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-3">
          <div className="flex items-center gap-2">
            <Input
              ref={field}
              value={url ?? ''}
              readOnly
              onFocus={(e) => e.currentTarget.select()}
              className="bg-sunken border-edge text-slate-50 font-mono text-xs"
            />
            <Button
              variant="outline"
              size="sm"
              className="bg-transparent border-edge text-soft hover:bg-edge hover:text-slate-50 shrink-0"
              onClick={() => url && copy(url)}
            >
              <Copy className="size-4 mr-1" />
              Copy
            </Button>
          </div>

          <div className="flex items-start gap-2 font-mono text-xs text-dim">
            <Link2 className="size-4 shrink-0 text-brand" />
            {/* Что именно уехало в буфер и что произойдёт у получателя. Второе
                важнее первого: ссылка НЕ несёт само сообщение — она указывает
                на него в кластере, и открыть её сможет только тот, у кого есть
                и подключение к этому кластеру, и права на топик. */}
            <span>
              Copied to clipboard. It opens in mikui on the recipient's machine — they need a
              connection to this cluster and access to the topic. The message itself is not in
              the link.
            </span>
          </div>
        </div>
      </DialogContentNoClose>
    </Dialog>
  );
}
