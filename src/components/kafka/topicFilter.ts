/**
 * Фильтр списка топиков: разбор строки и проверка имени.
 *
 * Синтаксис нарочно без грамматики. Условия разделяются пробелом, каждое
 * разбирается само по себе, скобок и приоритетов нет. Отсюда главное его
 * свойство: НЕВАЛИДНОГО ВВОДА НЕ СУЩЕСТВУЕТ. Любая строка что-то да значит,
 * поэтому не нужны ни разбор ошибок, ни подсветка синтаксиса, ни состояние
 * «пока не дописал — список пуст».
 *
 *   qa      имя содержит qa
 *   ^qa     начинается с qa
 *   qa$     заканчивается на qa
 *   !qa     НЕ содержит qa
 *   a|b     a или b
 *   пробел  все условия сразу
 *
 * Экранирования нет, и оно не нужно: Kafka разрешает в имени топика только
 * `[a-zA-Z0-9._-]`, так что `!`, `^`, `$` и `|` в настоящее имя попасть не
 * могут и ни с чем не столкнутся.
 *
 * Регистр не учитывается нигде: имена топиков читают и набирают глазами, а не
 * сверяют побайтово.
 */

/**
 * Одно условие — та часть, которую нельзя разделить дальше.
 *
 * Оба якоря сразу (`^qa$`) — это «имя равно qa» целиком, а не «начинается и
 * заканчивается»: второе для одного и того же куска текста значило бы то же
 * самое лишь случайно.
 */
interface Term {
  /** Уже в нижнем регистре — иначе сравнение пришлось бы приводить на каждое имя. */
  needle: string;
  negated: boolean;
  anchorStart: boolean;
  anchorEnd: boolean;
}

/**
 * Разобранный фильтр: группы соединены через И, условия внутри группы — через ИЛИ.
 *
 * Пустой список групп — это «фильтра нет», и такой разбор даёт не только пустая
 * строка, но и огрызок вроде `!` или `^`, набранный по дороге к настоящему
 * условию. Отсюда и правило: недописанное условие НЕ сужает выдачу. Иначе
 * список моргал бы пустотой на каждом первом символе — `!` в одиночку означал
 * бы «не содержит ничего», то есть «не показывать ничего».
 */
export interface TopicQuery {
  groups: Term[][];
}

/** Фильтр, который ничего не отсеивает. */
export const EMPTY_QUERY: TopicQuery = { groups: [] };

/** Разбирает одно условие. `null` — условия в нём не осталось (одни операторы). */
function parseTerm(raw: string): Term | null {
  let rest = raw;

  const negated = rest.startsWith('!');
  if (negated) rest = rest.slice(1);

  // Якоря снимаются ТОЛЬКО с краёв: `^` после `!` — часть отрицаемого условия,
  // а `$` в середине — обычный символ. Найти его в имени всё равно не выйдет,
  // но это забота Kafka, а не разбора: он не выдумывает смысла там, где его
  // не написали.
  const anchorStart = rest.startsWith('^');
  if (anchorStart) rest = rest.slice(1);

  const anchorEnd = rest.length > 0 && rest.endsWith('$');
  if (anchorEnd) rest = rest.slice(0, -1);

  if (!rest) return null;
  return { needle: rest, negated, anchorStart, anchorEnd };
}

/**
 * Разбирает строку фильтра.
 *
 * Пробелы вокруг `|` схлопываются ПЕРЕД делением на условия, поэтому `a|b`,
 * `a | b`, `a| b` и `a |b` означают одно и то же. Без этого шага половина
 * написаний тихо превращалась бы в И вместо ИЛИ — разница не видна на глаз, а
 * выдача отличается радикально.
 */
export function parseTopicQuery(input: string): TopicQuery {
  const normalized = input.trim().toLowerCase().replace(/\s*\|\s*/g, '|');
  if (!normalized) return EMPTY_QUERY;

  const groups: Term[][] = [];
  for (const token of normalized.split(/\s+/)) {
    const terms = token
      .split('|')
      .map(parseTerm)
      .filter((term): term is Term => term !== null);
    if (terms.length > 0) groups.push(terms);
  }
  return { groups };
}

function matchesTerm(lowered: string, term: Term): boolean {
  let hit: boolean;
  if (term.anchorStart && term.anchorEnd) {
    hit = lowered === term.needle;
  } else if (term.anchorStart) {
    hit = lowered.startsWith(term.needle);
  } else if (term.anchorEnd) {
    hit = lowered.endsWith(term.needle);
  } else {
    hit = lowered.includes(term.needle);
  }
  return term.negated ? !hit : hit;
}

/** Проходит ли имя топика через фильтр. */
export function matchesQuery(name: string, query: TopicQuery): boolean {
  if (query.groups.length === 0) return true;
  const lowered = name.toLowerCase();
  return query.groups.every((group) => group.some((term) => matchesTerm(lowered, term)));
}

/**
 * Есть ли в фильтре хоть одно условие «найди», а не «убери».
 *
 * По этому вопросу список выбирает порядок. Когда что-то ищут, короткие
 * совпадения обычно релевантнее, и они идут вперёд. А `!qa` ничего не ищет —
 * это тот же полный список, из которого убрали лишнее, и пересобирать его по
 * длине значило бы перетасовать привычный алфавитный порядок ни за чем.
 */
export function narrowsToMatch(query: TopicQuery): boolean {
  return query.groups.some((group) => group.some((term) => !term.negated));
}
