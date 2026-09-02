/** Форматирование величин, которые показываются больше чем в одном месте. */

/** Один экземпляр на приложение: пересоздавать форматтер на каждую строку
 *  заметно дороже самого форматирования. `Intl.DateTimeFormat` секунд точнее
 *  не берёт, поэтому миллисекунды дописываются отдельно. */
const TIME_FORMAT = new Intl.DateTimeFormat(undefined, {
  year: 'numeric',
  month: '2-digit',
  day: '2-digit',
  hour: '2-digit',
  minute: '2-digit',
  second: '2-digit',
  hour12: false,
});

/**
 * Время сообщения, Unix millis.
 *
 * Одно на всех, кто его показывает: таблица, окно сообщения и список
 * сохранённых. Раньше форматов было два — с миллисекундами и без, — и одно и то
 * же сообщение выглядело в таблице иначе, чем в открывшей его модалке.
 */
export function formatTimestamp(millis: number): string {
  if (!millis) return '—';
  const ms = String(((millis % 1000) + 1000) % 1000).padStart(3, '0');
  return `${TIME_FORMAT.format(new Date(millis))}.${ms}`;
}

export function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(0)} KB`;
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)} GB`;
}
