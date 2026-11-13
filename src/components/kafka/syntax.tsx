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

/**
 * Ключевые слова .proto, которые стоит выделять.
 *
 * Не весь словарь языка: `optional`, `repeated`, `reserved` и прочее к
 * пониманию текста добавляют мало, а раскрашенным в контракте оказывается
 * половина строк — и подсветка перестаёт что-либо подсказывать. Выделено то,
 * по чему текст ПРОСМАТРИВАЮТ: объявления типов и служебные строки шапки.
 */
const PROTO_KEYWORDS = new Set([
  'syntax',
  'package',
  'import',
  'option',
  'message',
  'enum',
  'service',
  'rpc',
  'oneof',
  'extend',
  'returns',
  'public',
  'weak',
]);

/** Встроенные типы полей: их видно в каждой строке, и они же отделяют тип от имени. */
const PROTO_BUILTINS = new Set([
  'double',
  'float',
  'int32',
  'int64',
  'uint32',
  'uint64',
  'sint32',
  'sint64',
  'fixed32',
  'fixed64',
  'sfixed32',
  'sfixed64',
  'bool',
  'string',
  'bytes',
  'map',
]);

/** Токены .proto: комментарий, строка, число, слово, пунктуация. */
const PROTO_TOKEN =
  /(\/\/[^\n]*|\/\*[\s\S]*?\*\/)|("(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*')|(\b\d+\b)|([A-Za-z_][A-Za-z0-9_.]*)|([{}[\]()<>;,=])/g;

/**
 * Раскрашивает строку .proto.
 *
 * Устроена как `highlightLine` и по той же причине — несколько `span` на
 * строку, а не один на символ: контракт в полтораста строк иначе стоил бы
 * десятков тысяч узлов, а поле с ним ещё и перерисовывается на каждое нажатие
 * клавиши.
 *
 * Строчный разбор — осознанное упрощение: многострочный комментарий `/* … *\/`
 * здесь закрашивается только в тех строках, где виден его край. Поле с
 * контрактом — это поле ВВОДА, а не просмотрщик: текст в нём живёт ровно до
 * нажатия «Apply», и держать ради точной раскраски состояние между строками
 * значило бы платить за неё пересчётом всего текста на каждый символ.
 */
export function highlightProto(line: string) {
  const parts: React.ReactNode[] = [];
  let last = 0;
  let match: RegExpExecArray | null;

  PROTO_TOKEN.lastIndex = 0;
  while ((match = PROTO_TOKEN.exec(line)) !== null) {
    if (match.index > last) {
      parts.push(
        <span key={`t${last}`} className="text-soft">
          {line.slice(last, match.index)}
        </span>,
      );
    }
    const [full, comment, str, num, word, punct] = match;
    const key = `m${match.index}`;
    if (comment !== undefined) {
      parts.push(<span key={key} className="text-dim">{comment}</span>);
    } else if (str !== undefined) {
      parts.push(<span key={key} className="text-syntax-string">{str}</span>);
    } else if (num !== undefined) {
      parts.push(<span key={key} className="text-syntax-bracket">{num}</span>);
    } else if (word !== undefined) {
      // Три исхода: служебное слово, встроенный тип и всё остальное — имена
      // пакетов, типов и полей. Последние и есть содержание контракта, поэтому
      // цвет у них обычный: подсвечивать надо то, что их обрамляет.
      const cls = PROTO_KEYWORDS.has(word)
        ? 'text-brand'
        : PROTO_BUILTINS.has(word)
          ? 'text-syntax-boolean'
          : 'text-soft';
      parts.push(<span key={key} className={cls}>{word}</span>);
    } else if (punct !== undefined) {
      parts.push(<span key={key} className="text-syntax-brace">{punct}</span>);
    }
    last = match.index + full.length;
  }

  if (last < line.length) {
    parts.push(
      <span key={`t${last}`} className="text-soft">
        {line.slice(last)}
      </span>,
    );
  }
  return parts.length > 0 ? parts : <span className="text-soft">{line}</span>;
}
