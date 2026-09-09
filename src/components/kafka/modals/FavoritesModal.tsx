import { ReactNode, useEffect, useState } from 'react';
import { Button } from '../../ui/button';
import { AlertTriangle, Star, Trash2 } from 'lucide-react';
import { Dialog, DialogHeader, DialogTitle, DialogDescription } from '../../ui/dialog';
import { DialogContentNoClose } from '../DialogContentNoClose';
import { FavoriteInfo, FavoritesView } from '../types';
import { formatBytes, formatTimestamp } from '../format';

/**
 * После этого объёма цифра занятого места краснеет.
 *
 * Ничего не запрещает и ничему не мешает: сохранённое сообщение тем и ценно,
 * что лежит у нас, и отказываться его хранить приложение не вправе. Но архив
 * растёт молча — каждое сохранение это чужие мегабайты на своём диске, — и
 * сказать об этом надо раньше, чем место кончится.
 */
const WARN_BYTES = 256 * 1024 * 1024;

/** Сколько «delete all» ждёт подтверждения, прежде чем снова стать обычной
 *  кнопкой. */
const ARM_MS = 4000;

/** Как упорядочен список. По размеру — единственный способ найти те записи,
 *  ради удаления которых сюда и приходят. */
type Order = 'saved' | 'size';

/** Кнопка-переключатель. Та же, что у вкладок payload/headers в модалке
 *  сообщения: два способа рисовать один и тот же выбор ни к чему. */
function OrderButton({
  active,
  onClick,
  children,
}: {
  active: boolean;
  onClick: () => void;
  children: ReactNode;
}) {
  return (
    <Button
      variant="ghost"
      size="sm"
      className={`font-mono px-3 py-1 h-auto ${
        active
          ? 'bg-brand text-surface hover:bg-brand-hover'
          : 'bg-transparent text-soft hover:bg-edge hover:text-strong'
      }`}
      onClick={onClick}
    >
      {children}
    </Button>
  );
}

interface FavoritesModalProps {
  favorites: FavoritesView;
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onRemoveFavorite: (favorite: FavoriteInfo) => void;
  onClearFavorites: () => void;
  onSelectMessage: (favorite: FavoriteInfo) => void;
}

export function FavoritesModal({
  favorites,
  open,
  onOpenChange,
  onRemoveFavorite,
  onClearFavorites,
  onSelectMessage,
}: FavoritesModalProps) {
  const [order, setOrder] = useState<Order>('saved');
  /** Первый клик по «delete all» только взводит кнопку. Диалогов подтверждения
   *  в приложении нет, а необратимая кнопка рядом с обычными нужна не одна. */
  const [armed, setArmed] = useState(false);

  // Взведённая кнопка сама разряжается: подтверждение относится к тому клику,
  // ради которого его спросили, а не к следующему заходу в это окно. По таймеру,
  // а не по потере фокуса, — кнопка в WebKit фокус по клику и не получает.
  useEffect(() => {
    if (!armed) return;
    const timer = setTimeout(() => setArmed(false), ARM_MS);
    return () => clearTimeout(timer);
  }, [armed]);

  useEffect(() => {
    if (!open) setArmed(false);
  }, [open]);

  const handleClear = () => {
    if (!armed) {
      setArmed(true);
      return;
    }
    setArmed(false);
    onClearFavorites();
  };

  const { items, total_bytes: totalBytes } = favorites;
  const heavy = totalBytes >= WARN_BYTES;

  // Копия, а не сортировка на месте: `items` приезжают из состояния выше.
  const shown = [...items].sort((a, b) =>
    order === 'size' ? b.bytes - a.bytes : b.saved_at.localeCompare(a.saved_at),
  );

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      {/* `flex flex-col` вместо сетки по умолчанию: у списка неизвестной длины
          строка сетки растягивается под содержимое, и шестая сохранённая запись
          уезжала за нижний край экрана вместо того, чтобы прокручиваться. */}
      {/* `sm:max-w-5xl` не для порядка: у диалога в базовом классе лежит
          `sm:max-w-lg`, и без парного sm-варианта ширина схлопывалась до 32rem
          на любом окне шире 640 px. Тем же приёмом задана ширина окна
          сообщения. */}
      <DialogContentNoClose className="flex flex-col max-w-5xl sm:max-w-5xl max-h-[90vh] overflow-hidden bg-surface border-edge text-strong">
        <DialogHeader className="shrink-0 border-b border-edge pb-4">
          <DialogTitle className="font-mono text-soft text-lg">
            Saved Messages ({items.length})
            {items.length > 0 && (
              <>
                <span className="text-dim"> · </span>
                <span
                  className={heavy ? 'text-danger' : 'text-brand'}
                  title={
                    `Everything saved takes ${formatBytes(totalBytes)} on disk` +
                    (heavy
                      ? `, past the ${formatBytes(WARN_BYTES)} mark. Nothing is capped —` +
                        ' delete what you no longer need, heaviest first.'
                      : '. Saved messages are kept until you delete them.')
                  }
                >
                  {formatBytes(totalBytes)} on disk
                </span>
              </>
            )}
          </DialogTitle>
          <DialogDescription className="font-mono text-dim text-sm">
            Kept on this machine: a saved message stays readable after the topic's
            retention has dropped it.
          </DialogDescription>
        </DialogHeader>

        {items.length > 0 && (
          <div className="flex shrink-0 items-center justify-between gap-4 pt-2">
            <div className="flex items-center gap-1">
              <span className="font-mono text-dim text-sm mr-1">order</span>
              <OrderButton active={order === 'saved'} onClick={() => setOrder('saved')}>
                newest
              </OrderButton>
              <OrderButton active={order === 'size'} onClick={() => setOrder('size')}>
                heaviest
              </OrderButton>
            </div>

            <Button
              onClick={handleClear}
              variant="outline"
              size="sm"
              className={`font-mono bg-transparent border-edge ${
                armed
                  ? 'text-danger border-danger hover:bg-edge hover:text-danger-hover'
                  : 'text-dim hover:bg-edge hover:text-danger'
              }`}
            >
              <Trash2 className="size-4 mr-1" />
              {armed ? `delete all ${items.length}?` : 'delete all'}
            </Button>
          </div>
        )}

        <div className="flex-1 min-h-0 overflow-auto">
          {items.length === 0 ? (
            <div className="flex items-center justify-center py-16">
              <div className="text-center">
                <Star className="size-12 text-dim mx-auto mb-4" />
                <div className="font-mono text-soft text-lg mb-2">
                  No saved messages yet
                </div>
                <div className="font-mono text-dim text-sm">
                  Save a message with the star icon in message details — it will be kept
                  on disk and survive both a restart and the cluster's retention.
                </div>
              </div>
            </div>
          ) : (
            <div className="space-y-4 pt-4">
              {shown.map((favorite) => (
                <div
                  key={favorite.id}
                  className={`border border-edge rounded-lg p-4 transition-colors ${
                    favorite.missing
                      ? 'opacity-60'
                      : 'hover:bg-elevated cursor-pointer'
                  }`}
                  onClick={() => !favorite.missing && onSelectMessage(favorite)}
                >
                  {/* Кластер и топик: список общий на все подключения, и без
                      кластера одноимённые топики dev и prod неразличимы. */}
                  <div className="flex items-center justify-between gap-4 mb-2">
                    <div className="font-mono truncate">
                      <span className="text-dim">{favorite.cluster_name || '—'}</span>
                      <span className="text-dim"> / </span>
                      <span className="text-brand">{favorite.topic}</span>
                    </div>
                    <div className="flex items-center gap-3 shrink-0">
                      <span
                        className="font-mono text-sm text-soft"
                        title={`This message takes ${formatBytes(
                          favorite.bytes,
                        )} on disk; in Kafka its body was ${favorite.value_size.toLocaleString()} bytes on the wire.`}
                      >
                        {formatBytes(favorite.bytes)}
                      </span>
                      <Button
                        onClick={(e: any) => {
                          e.stopPropagation();
                          onRemoveFavorite(favorite);
                        }}
                        variant="outline"
                        size="sm"
                        className="bg-transparent border-edge text-danger hover:bg-edge hover:text-danger-hover"
                      >
                        <Trash2 className="size-4" />
                      </Button>
                    </div>
                  </div>

                  {/* Время САМОГО сообщения — рядом с координатами, а не рядом
                      с датой сохранения ниже: и то, и другое отвечает на вопрос
                      «что это за сообщение», тогда как «Saved» — на вопрос
                      «когда мы его забрали». */}
                  <div className="flex flex-wrap items-center gap-4 mb-2">
                    <div className="font-mono text-soft text-sm">
                      partition: {favorite.partition}
                    </div>
                    <div className="font-mono text-soft text-sm">
                      offset: {favorite.offset}
                    </div>
                    <div className="font-mono text-soft text-sm">
                      time: {formatTimestamp(favorite.timestamp)}
                    </div>
                  </div>

                  <div className="font-mono text-dim text-sm mb-3">
                    Saved: {new Date(favorite.saved_at).toLocaleString()}
                  </div>

                  {/* Файл тела исчез — каталог настроек могли почистить руками.
                      Запись всё равно показана: иначе её нечем удалить. */}
                  {favorite.missing ? (
                    <div className="flex items-center gap-2 rounded-lg border border-edge bg-sunken p-3 font-mono text-sm text-danger">
                      <AlertTriangle className="size-4 shrink-0" />
                      The saved body is gone from disk — only this entry is left.
                    </div>
                  ) : (
                    <div className="bg-sunken border border-edge rounded-lg p-3">
                      <div className="font-mono text-sm text-soft truncate">
                        {favorite.preview}
                      </div>
                    </div>
                  )}
                </div>
              ))}
            </div>
          )}
        </div>
      </DialogContentNoClose>
    </Dialog>
  );
}
