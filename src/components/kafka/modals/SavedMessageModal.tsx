import { FavoriteInfo, SavedMessage } from '../types';
import { MessageDetailsModal } from './MessageDetailsModal';

/**
 * Окно сохранённого сообщения.
 *
 * Отдельное от окна строки таблицы, хотя показывает то же самое, потому что
 * отвечает на другой вопрос. У строки таблицы «откуда это» видно по шапке:
 * топик открыт, кластер подключён. У сохранённого не видно ничего — архив общий
 * на все кластеры, и открыть его можно вообще без подключения, — поэтому вместо
 * подписи про «выбранную строку» здесь стоят кластер и топик, откуда сообщение
 * взято.
 *
 * Всё остальное — тело, заголовки, копирование — то же самое и тем же кодом:
 * второе окно просмотра сообщения рано или поздно разошлось бы с первым.
 */
export function SavedMessageModal({
  favorite,
  message,
  open,
  onOpenChange,
}: {
  /** Строка архива, из которой окно открыли: она знает, откуда сообщение. */
  favorite: FavoriteInfo | null;
  /** null — тело ещё не приехало. */
  message: SavedMessage | null;
  open: boolean;
  onOpenChange: (open: boolean) => void;
}) {
  return (
    <MessageDetailsModal
      message={message}
      open={open}
      onOpenChange={onOpenChange}
      title="Saved Message"
      description={
        favorite && (
          <>
            <span className="text-dim">{favorite.cluster_name || 'unnamed cluster'}</span>
            <span className="text-dim"> / </span>
            <span className="text-brand">{favorite.topic}</span>
            <span className="text-dim"> · saved {new Date(favorite.saved_at).toLocaleString()}</span>
          </>
        )
      }
      // Формат приезжает вместе с телом: он взят у ТОГО топика, откуда
      // сообщение, а не у того, что открыт в таблице прямо сейчас.
      format={message?.format}
      // Стрелок нет намеренно: у записи архива нет соседей по таблице.
      // Сохранять тоже нечего — она уже сохранена, а удаляют из списка.
    />
  );
}
