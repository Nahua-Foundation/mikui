import { describe, expect, it } from 'vitest';
import { matchesQuery, narrowsToMatch, parseTopicQuery } from './topicFilter';

/**
 * Набор из настоящей жизни: длинный общий префикс, а всё различие — в хвосте.
 * Ради него фильтр и затевался, поэтому проверяем на нём, а не на abc/def.
 */
const PREFIX = 'some.very.large.large.large.big.big.big.topic.';
const TAILS = [
  'name',
  'name.qa',
  'name.qa3',
  'name.qa4',
  'name-2',
  'name-2.qa',
  'name-2.qa2',
  'name-2.qa3',
  'name-2.qa4',
];
const TOPICS = TAILS.map((tail) => PREFIX + tail);

/** Отбор по фильтру. Префикс из ответа убран — иначе в ожиданиях не разглядеть
 *  того самого хвоста, ради которого всё и написано. */
function select(query: string, topics: string[] = TOPICS): string[] {
  const parsed = parseTopicQuery(query);
  return topics.filter((name) => matchesQuery(name, parsed)).map((name) => name.slice(PREFIX.length));
}

describe('фильтра нет', () => {
  it('пустая строка пропускает всё', () => {
    expect(select('')).toEqual(TAILS);
    expect(parseTopicQuery('').groups).toHaveLength(0);
  });

  it('одни пробелы — тоже', () => {
    expect(select('   \t ')).toEqual(TAILS);
  });

  // Набранное по дороге к настоящему условию не должно опустошать список: иначе
  // он моргает на каждом первом символе.
  it.each(['!', '^', '$', '^$', '!$', '!^', '|', '||', ' | '])(
    'огрызок %j ничего не отсеивает',
    (query) => {
      expect(parseTopicQuery(query).groups).toHaveLength(0);
      expect(select(query)).toEqual(TAILS);
    },
  );
});

describe('одно условие', () => {
  it('без операторов ищет подстроку', () => {
    expect(select('qa')).toEqual([
      'name.qa',
      'name.qa3',
      'name.qa4',
      'name-2.qa',
      'name-2.qa2',
      'name-2.qa3',
      'name-2.qa4',
    ]);
  });

  // Тот самый случай, ради которого всё затевалось: подстрока `qa` тащит за
  // собой qa2/qa3/qa4, а якорь конца — нет.
  it('$ находит только точный хвост', () => {
    expect(select('qa$')).toEqual(['name.qa', 'name-2.qa']);
  });

  it('^ привязывает к началу', () => {
    expect(select('^some')).toEqual(TAILS);
    expect(select('^qa')).toEqual([]);
  });

  it('оба якоря — это равенство целиком', () => {
    const list = ['orders', 'orders.qa', 'x.orders'];
    const exact = list.filter((name) => matchesQuery(name, parseTopicQuery('^orders$')));
    expect(exact).toEqual(['orders']);
  });

  it('! исключает', () => {
    expect(select('!qa')).toEqual(['name', 'name-2']);
  });

  it('! складывается с якорем', () => {
    expect(select('!qa$')).toEqual([
      'name',
      'name.qa3',
      'name.qa4',
      'name-2',
      'name-2.qa2',
      'name-2.qa3',
      'name-2.qa4',
    ]);
    // Убирает ровно одно имя — то, что на `name-2` и кончается.
    expect(select('!name-2$')).toEqual(TAILS.filter((tail) => tail !== 'name-2'));
    expect(select('!^some')).toEqual([]);
  });
});

describe('несколько условий', () => {
  it('пробел — это И', () => {
    expect(select('qa !name-2')).toEqual(['name.qa', 'name.qa3', 'name.qa4']);
    expect(select('qa$ !name-2')).toEqual(['name.qa']);
  });

  it('И работает и на трёх условиях', () => {
    expect(select('^some !qa2 qa$')).toEqual(['name.qa', 'name-2.qa']);
  });

  it('лишние пробелы между условиями не мешают', () => {
    expect(select('  qa$   !name-2  ')).toEqual(['name.qa']);
  });

  it('| — это ИЛИ', () => {
    expect(select('qa$|qa3$')).toEqual(['name.qa', 'name.qa3', 'name-2.qa', 'name-2.qa3']);
  });

  // Пробелы вокруг `|` не должны менять смысл: разница не видна на глаз, а
  // выдача у И и ИЛИ отличается радикально.
  it.each(['qa$|qa3$', 'qa$ | qa3$', 'qa$| qa3$', 'qa$ |qa3$'])(
    'написание %j значит одно и то же',
    (query) => {
      expect(select(query)).toEqual(['name.qa', 'name.qa3', 'name-2.qa', 'name-2.qa3']);
    },
  );

  it('| по краям ничего не ломает', () => {
    expect(select('qa$|')).toEqual(['name.qa', 'name-2.qa']);
    expect(select('|qa$')).toEqual(['name.qa', 'name-2.qa']);
  });

  it('ИЛИ связывает сильнее, чем И', () => {
    // `qa2$|qa4$` — одна группа, `^some` — другая, между ними И.
    expect(select('^some qa2$|qa4$')).toEqual(['name.qa4', 'name-2.qa2', 'name-2.qa4']);
  });

  it('отрицание внутри ИЛИ остаётся отрицанием', () => {
    expect(select('!qa|name-2')).toEqual([
      'name',
      'name-2',
      'name-2.qa',
      'name-2.qa2',
      'name-2.qa3',
      'name-2.qa4',
    ]);
  });

  // Ровно то, что пришлось бы писать без якоря конца. Работает — но тащит qa4,
  // про который в момент набора забыли, и сломается на первом же qa5.
  it('перечисление исключений даёт не то же самое, что qa$', () => {
    expect(select('qa !qa2 !qa3')).toEqual(['name.qa', 'name.qa4', 'name-2.qa', 'name-2.qa4']);
  });
});

describe('мелочи разбора', () => {
  it('регистр не важен ни с той, ни с другой стороны', () => {
    const list = ['Orders.QA', 'orders.qa3'];
    const hit = (query: string) => list.filter((n) => matchesQuery(n, parseTopicQuery(query)));
    expect(hit('qa$')).toEqual(['Orders.QA']);
    expect(hit('^ORDERS')).toEqual(list);
    expect(hit('!Qa3')).toEqual(['Orders.QA']);
  });

  it('$ в середине — обычный символ, а не якорь', () => {
    // В имени топика Kafka такого символа быть не может, так что совпадений
    // нет — но и падать разбору не с чего.
    expect(select('a$b')).toEqual([]);
    expect(parseTopicQuery('a$b').groups).toHaveLength(1);
  });

  it('^ после ! — якорь, а не текст', () => {
    expect(select('!^some.very')).toEqual([]);
  });

  it('каждое условие — своя группа', () => {
    expect(parseTopicQuery('qa$ !name-2').groups).toHaveLength(2);
    expect(parseTopicQuery('qa$|name-2').groups).toHaveLength(1);
  });
});

describe('narrowsToMatch', () => {
  it.each(['qa', 'qa$', '^some', 'qa$|qa3$', '!name-2 qa'])('%j ищет', (query) => {
    expect(narrowsToMatch(parseTopicQuery(query))).toBe(true);
  });

  it.each(['', '!qa', '!qa !name-2', '!qa|!name-2'])('%j только убирает', (query) => {
    expect(narrowsToMatch(parseTopicQuery(query))).toBe(false);
  });
});
