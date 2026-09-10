/**
 * Подсветка JSON. Общая для модалки чтения и модалки отправки.
 *
 * Жила в `MessageDetailsModal`, пока читателем была одна. Скопировать её во
 * вторую значило бы завести два места, где цвета расходятся: тело, которое
 * пользователь отправляет, обязано выглядеть ровно так же, как то, которое он
 * потом прочитает.
 */

/** Токены JSON: ключ, строка, число, boolean, null, пунктуация. */
const JSON_TOKEN =
  /("(?:\\.|[^"\\])*")\s*:|("(?:\\.|[^"\\])*")|(-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)|(true|false)|(null)|([{}[\],:])/g;

/**
 * Раскрашивает строку JSON несколькими span-ами.
 *
 * Раньше здесь был `line.split('')` с одним `<span>` на КАЖДЫЙ символ: тело
 * в 100 КБ превращалось в сотню тысяч DOM-узлов и намертво вешало окно.
 * Теперь на строку приходится несколько узлов вместо сотни.
 *
 * `enumValues` — имена значений из СХЕМЫ (`FullMessage.enum_values`), а не
 * догадка по виду строки: decoder.rs печатает enum тем же JSON-string, что и
 * обычную строку, и обычные значения вроде `"USDT"` или `"RUB"` выглядят
 * ровно как КАПС-имя enum. Раньше здесь была именно такая догадка по
 * регулярке — и красила подобные строки как enum, хотя по контракту это
 * просто строки.
 */
export function highlightLine(line: string, enumValues: ReadonlySet<string>) {
  const parts: React.ReactNode[] = [];
  let last = 0;
  let match: RegExpExecArray | null;

  JSON_TOKEN.lastIndex = 0;
  while ((match = JSON_TOKEN.exec(line)) !== null) {
    if (match.index > last) {
      parts.push(<span key={`t${last}`} className="text-soft">{line.slice(last, match.index)}</span>);
    }
    const [full, propertyKey, str, num, bool, nul, punct] = match;
    const key = `m${match.index}`;
    if (propertyKey !== undefined) {
      parts.push(<span key={key} className="text-brand">{propertyKey}</span>);
      parts.push(<span key={`${key}c`} className="text-syntax-brace">{full.slice(propertyKey.length)}</span>);
    } else if (str !== undefined) {
      const cls = enumValues.has(str.slice(1, -1)) ? 'text-syntax-enum' : 'text-syntax-string';
      parts.push(<span key={key} className={cls}>{str}</span>);
    } else if (num !== undefined) {
      parts.push(<span key={key} className="text-syntax-bracket">{num}</span>);
    } else if (bool !== undefined) {
      parts.push(<span key={key} className="text-syntax-boolean">{bool}</span>);
    } else if (nul !== undefined) {
      parts.push(<span key={key} className="text-dim">{nul}</span>);
    } else if (punct !== undefined) {
      parts.push(<span key={key} className="text-syntax-brace">{punct}</span>);
    }
    last = match.index + full.length;
  }

  if (last < line.length) {
    parts.push(<span key={`t${last}`} className="text-soft">{line.slice(last)}</span>);
  }
  return parts.length > 0 ? parts : <span className="text-soft">{line}</span>;
}

/** Ни одного enum-значения. Общий пустой набор, чтобы не плодить его на рендер. */
export const NO_ENUM_VALUES: ReadonlySet<string> = new Set<string>();
