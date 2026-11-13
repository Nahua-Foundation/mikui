import { describe, expect, it } from 'vitest';
import { dirOf, fileOf, labelOf, searchSets, splitBySource, usableSets } from './SchemaSetSelect';
import { SourceView } from '../types';

/** Источник с набором контрактов. Всё, кроме `sets`, списку безразлично. */
function source(id: string, label: string, sets: [string, string | null][]): SourceView {
  return {
    id,
    label,
    kind: 'gitlab',
    location: `https://gitlab.example.com/group/${label}`,
    files: sets.length,
    sets: sets.map(([entry, error]) => ({ entry, root: null, messages: 0, error })),
    error: null,
  } as SourceView;
}

const LIBRARY: SourceView[] = [
  source('s1', 'internalizer', [
    ['internal/adapters/kafka/types/md/orderbook_v2.proto', null],
    ['internal/adapters/kafka/types/pubev/pubev.proto', null],
    ['internal/ports/types/api/api.proto', 'not found in import path'],
  ]),
  source('s2', 'contracts', [
    ['proto/exec/exec.proto', null],
    ['proto/middle/middle.proto', null],
  ]),
];

describe('usableSets', () => {
  it('отдаёт только разбирающиеся наборы, со своим источником у каждого', () => {
    expect(usableSets(LIBRARY)).toEqual([
      {
        sourceId: 's1',
        sourceLabel: 'internalizer',
        entry: 'internal/adapters/kafka/types/md/orderbook_v2.proto',
      },
      {
        sourceId: 's1',
        sourceLabel: 'internalizer',
        entry: 'internal/adapters/kafka/types/pubev/pubev.proto',
      },
      { sourceId: 's2', sourceLabel: 'contracts', entry: 'proto/exec/exec.proto' },
      { sourceId: 's2', sourceLabel: 'contracts', entry: 'proto/middle/middle.proto' },
    ]);
  });

  it('сломанный набор выбрать нельзя — привязать к нему нечего', () => {
    expect(usableSets(LIBRARY).map((o) => o.entry)).not.toContain(
      'internal/ports/types/api/api.proto',
    );
  });
});

describe('searchSets', () => {
  /** Главное свойство всей затеи: поиск идёт по библиотеке целиком, а не по
   *  проекту, в который зашли. Иначе два уровня были бы хуже одного. */
  it('находит контракт в любом проекте, не заходя ни в один', () => {
    expect(searchSets(LIBRARY, 'orderbook')).toEqual([
      {
        sourceId: 's1',
        sourceLabel: 'internalizer',
        entry: 'internal/adapters/kafka/types/md/orderbook_v2.proto',
      },
    ]);
  });

  it('совпадения из разных проектов приезжают вместе', () => {
    expect(searchSets(LIBRARY, 'proto/').map((o) => o.sourceId)).toEqual(['s2', 's2']);
    expect(searchSets(LIBRARY, 'types').map((o) => o.sourceId)).toEqual(['s1', 's1']);
  });

  it('имя проекта — такой же запрос, как имя файла', () => {
    expect(searchSets(LIBRARY, 'contracts').map((o) => o.entry)).toEqual([
      'proto/exec/exec.proto',
      'proto/middle/middle.proto',
    ]);
  });

  it('регистр не важен, пробелы по краям не считаются', () => {
    expect(searchSets(LIBRARY, '  PUBEV  ').map((o) => o.entry)).toEqual([
      'internal/adapters/kafka/types/pubev/pubev.proto',
    ]);
  });

  it('пустой запрос — не «всё», а «не ищем»: список показывает проекты', () => {
    expect(searchSets(LIBRARY, '')).toEqual([]);
    expect(searchSets(LIBRARY, '   ')).toEqual([]);
  });

  it('сломанные наборы не находятся и поиском', () => {
    expect(searchSets(LIBRARY, 'api.proto')).toEqual([]);
  });
});

describe('splitBySource', () => {
  /** Поиск изнутри проекта остаётся сквозным — это свойство, а не промах: имя
   *  файла человек помнит примерно, а репозиторий часто не помнит вовсе. Но
   *  свои контракты обязаны идти первыми, иначе подпись «search in …» врёт. */
  it('свои контракты впереди, чужие отдельно', () => {
    const found = searchSets(LIBRARY, 'proto');
    const { inside, elsewhere } = splitBySource(found, 's2');

    expect(inside.map((o) => o.entry)).toEqual([
      'proto/exec/exec.proto',
      'proto/middle/middle.proto',
    ]);
    expect(elsewhere.map((o) => o.sourceId)).toEqual(['s1', 's1']);
  });

  /** В проект не заходили — делить не на что: всё найденное одинаково своё, и
   *  разделителю нечего отделять. */
  it('без выбранного проекта всё найденное идёт одной группой', () => {
    const found = searchSets(LIBRARY, 'proto');
    expect(splitBySource(found, null)).toEqual({ inside: found, elsewhere: [] });
  });

  /** Ни одного совпадения в своём проекте — группа пустая, но чужие показать
   *  всё равно надо: именно они и есть ответ на вопрос «а где же он лежит». */
  it('пустая своя группа не мешает показать чужие', () => {
    const { inside, elsewhere } = splitBySource(searchSets(LIBRARY, 'orderbook'), 's2');
    expect(inside).toEqual([]);
    expect(elsewhere.map((o) => o.entry)).toEqual([
      'internal/adapters/kafka/types/md/orderbook_v2.proto',
    ]);
  });

  /** Разбиение ничего не теряет: сумма групп — это ровно то, что нашлось. */
  it('сохраняет всё найденное', () => {
    const found = searchSets(LIBRARY, 'proto');
    const { inside, elsewhere } = splitBySource(found, 's1');
    expect([...inside, ...elsewhere]).toHaveLength(found.length);
  });
});

describe('labelOf', () => {
  /** Та самая ошибка, ради которой подпись вынесена в отдельную функцию: сверка
   *  шла по одному лишь источнику, и кнопка показывала ПЕРВЫЙ контракт
   *  репозитория, какой бы из них ни выбрали. Привязка при этом была верной —
   *  расходилась только подпись, то есть единственное, по чему выбор проверяют. */
  it('называет выбранный контракт, а не первый контракт его репозитория', () => {
    const options = usableSets(LIBRARY);
    expect(labelOf(options, { source: 's1', entry: 'internal/ports/types/api/api.proto' })).toBe(
      null,
    );
    expect(
      labelOf(options, {
        source: 's1',
        entry: 'internal/adapters/kafka/types/pubev/pubev.proto',
      }),
    ).toBe('internalizer › internal/adapters/kafka/types/pubev/pubev.proto');
    expect(labelOf(options, { source: 's2', entry: 'proto/middle/middle.proto' })).toBe(
      'contracts › proto/middle/middle.proto',
    );
  });

  it('без выбора подписи нет', () => {
    expect(labelOf(usableSets(LIBRARY), null)).toBe(null);
  });

  /** Один и тот же путь в двух репозиториях — это два разных контракта, и
   *  источник в сверке участвует наравне с путём. */
  it('различает одинаковые пути в разных источниках', () => {
    const options = [
      { sourceId: 's1', sourceLabel: 'one', entry: 'api/orders.proto' },
      { sourceId: 's2', sourceLabel: 'two', entry: 'api/orders.proto' },
    ];
    expect(labelOf(options, { source: 's2', entry: 'api/orders.proto' })).toBe(
      'two › api/orders.proto',
    );
  });

  /** Контракт из привязки в библиотеке не найден: показывать нечего, и чужое
   *  имя вместо него было бы худшим из ответов. */
  it('молчит, когда контракта под привязкой больше нет', () => {
    expect(labelOf(usableSets(LIBRARY), { source: 's1', entry: 'gone.proto' })).toBe(null);
    expect(labelOf(usableSets(LIBRARY), { source: 'removed', entry: 'api/orders.proto' })).toBe(
      null,
    );
  });
});

describe('разбор пути', () => {
  it('имя файла и каталог — то, из чего собраны две строки записи', () => {
    const entry = 'internal/adapters/kafka/types/md/orderbook_v2.proto';
    expect(fileOf(entry)).toBe('orderbook_v2.proto');
    expect(dirOf(entry)).toBe('internal/adapters/kafka/types/md');
  });

  it('контракт в корне дерева каталога не имеет', () => {
    expect(fileOf('user.proto')).toBe('user.proto');
    expect(dirOf('user.proto')).toBe('');
  });
});
