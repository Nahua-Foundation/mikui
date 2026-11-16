import { describe, expect, it } from 'vitest';
import {
  Draft,
  draftFilled,
  draftFromSchema,
  draftMatchesSchema,
  messageFor,
  modeFromSchema,
  modeOf,
  pickedPathsOf,
  previewFromSchema,
  settledWithoutLibrary,
} from './TopicConfigModal';
import { BodyFormat, SchemaFile, SourceView, TopicSchema } from '../types';

/** Схема топика: всё, кроме способа задать контракт, выбору вкладки безразлично. */
function schema(parts: {
  files?: SchemaFile[];
  source?: { source: string; entry: string } | null;
  format?: BodyFormat;
  pasted_text?: string | null;
  messages?: string[];
  root?: string | null;
}): TopicSchema {
  return {
    cluster: 'c',
    topic: 't',
    format: parts.format ?? 'proto',
    files: parts.files ?? [],
    source: parts.source ?? null,
    message: null,
    messages: parts.messages ?? [],
    root: parts.root ?? null,
    produce_choices: [],
    error: null,
    avro: null,
    json: null,
    lens: null,
    detected_kind: null,
    pasted_text: parts.pasted_text ?? null,
  } as TopicSchema;
}

const EMPTY: Draft = { text: '', paths: [], binding: null };

/** Источник с одним набором: разбирается он или нет — единственное, что важно. */
function library(broken: boolean): SourceView[] {
  return [
    {
      id: 's1',
      label: 'contracts',
      kind: 'gitlab',
      location: 'https://gitlab.example.com/group/contracts',
      files: 1,
      sets: [
        {
          entry: 'orders.proto',
          root: null,
          messages: 1,
          error: broken ? 'not found in import path' : null,
        },
      ],
      error: null,
    } as SourceView,
  ];
}

const DISK: SchemaFile = { name: 'orders.proto', source: '/home/u/orders.proto' };
const PASTED: SchemaFile = { name: 'pasted.proto', source: '', pasted: true };

describe('modeOf', () => {
  /** Настроенный топик открывается тем способом, которым он настроен: видеть
   *  надо то, чем он читается, а не предложение настроить его заново. */
  it('shows the way the topic is already configured', () => {
    expect(modeOf(schema({ source: { source: 's1', entry: 'orders.proto' } }), [])).toBe('library');
    expect(modeOf(schema({ files: [PASTED] }), library(false))).toBe('paste');
    expect(modeOf(schema({ files: [DISK] }), library(false))).toBe('files');
  });

  /** Привязка сильнее файлов: сосуществовать они не могут, но если в записи
   *  почему-то оказалось и то и другое, читается топик именно набором. */
  it('puts the library binding above leftover files', () => {
    expect(
      modeOf(
        schema({ files: [DISK], source: { source: 's1', entry: 'orders.proto' } }),
        library(false),
      ),
    ).toBe('library');
  });

  /** У ненастроенного топика открывается библиотека — но только если в ней
   *  правда есть что выбрать: один выбор контракта дешевле любого другого
   *  способа, ради этого она и заведена. */
  it('offers the library to an unconfigured topic when it has contracts', () => {
    expect(modeOf(schema({}), library(false))).toBe('library');
    expect(modeOf(null, library(false))).toBe('library');
  });

  /** Пустая библиотека — не ответ: из вкладки с пустым списком работать
   *  нечем, а поле для текста требует только самого контракта. */
  it('falls back to pasting when there is nothing to pick', () => {
    expect(modeOf(schema({}), [])).toBe('paste');
    expect(modeOf(null, [])).toBe('paste');
  });

  /** Сломанные наборы выбрать нельзя — значит библиотека на вопрос «есть ли что
   *  выбрать» отвечает «нет», даже когда в ней девятнадцать контрактов. */
  it('does not count contracts that do not parse', () => {
    expect(modeOf(schema({}), library(true))).toBe('paste');
  });
});

/** Та часть ответа, которую даёт сама схема: по ней и видно, кому библиотека
 *  нужна, а кому нет. */
describe('modeFromSchema', () => {
  it('answers for a topic that is configured somehow', () => {
    expect(modeFromSchema(schema({ source: { source: 's1', entry: 'orders.proto' } }))).toBe(
      'library',
    );
    expect(modeFromSchema(schema({ files: [PASTED] }))).toBe('paste');
    expect(modeFromSchema(schema({ files: [DISK] }))).toBe('files');
  });

  /** Ненастроенный топик — единственный, за кого отвечает библиотека. */
  it('leaves the unconfigured topic to the library', () => {
    expect(modeFromSchema(schema({}))).toBeNull();
    expect(modeFromSchema(null)).toBeNull();
  });
});

describe('settledWithoutLibrary', () => {
  /**
   * Главное, ради чего это заведено: библиотеку ждут только там, где без неё
   * нечего показать.
   *
   * У avro-топика вкладок с контрактами нет вовсе, а стоил поход за библиотекой
   * полторы секунды — и платились они на каждое открытие настроек, какого бы
   * формата топик ни был.
   */
  it('does not make other formats wait for the .proto library', () => {
    for (const format of ['json', 'text', 'avro', 'jsonschema', 'hex'] as BodyFormat[]) {
      expect(settledWithoutLibrary(schema({ format }))).toBe(true);
    }
  });

  /** Настроенный protobuf-топик знает свою вкладку сам. */
  it('shows a configured proto topic right away', () => {
    expect(
      settledWithoutLibrary(schema({ source: { source: 's1', entry: 'orders.proto' } })),
    ).toBe(true);
    expect(settledWithoutLibrary(schema({ files: [DISK] }))).toBe(true);
    expect(settledWithoutLibrary(schema({ files: [PASTED] }))).toBe(true);
  });

  /** А ненастроенный — не знает, и показывать ему наугад выбранную вкладку
   *  значит моргнуть ею, как только приедет библиотека. */
  it('waits for the library when the proto topic is configured by nothing', () => {
    expect(settledWithoutLibrary(schema({}))).toBe(false);
  });
});

describe('draftFilled', () => {
  /** Пустая вкладка — это не «ничего не выбрано по ошибке», а законное
   *  состояние: на неё зашли посмотреть. Сохранять с неё нечего. */
  it('tells an empty tab from one that carries a contract', () => {
    expect(draftFilled('paste', EMPTY)).toBe(false);
    expect(draftFilled('files', EMPTY)).toBe(false);
    expect(draftFilled('library', EMPTY)).toBe(false);

    expect(draftFilled('paste', { ...EMPTY, text: 'message A {}' })).toBe(true);
    expect(draftFilled('files', { ...EMPTY, paths: ['/a.proto'] })).toBe(true);
    expect(
      draftFilled('library', { ...EMPTY, binding: { source: 's1', entry: 'a.proto' } }),
    ).toBe(true);
  });

  /** Пробелы контрактом не являются: иначе Save оживал бы от случайного
   *  переноса строки, а разбор падал бы на пустом файле. */
  it('does not count whitespace as a pasted contract', () => {
    expect(draftFilled('paste', { ...EMPTY, text: '   \n\t ' })).toBe(false);
  });

  /** Заполнены могут быть все три сразу — переключаться между вкладками, не
   *  теряя набранного, это и позволяет. Отвечает `draftFilled` при этом только
   *  про ту, которая открыта. */
  it('answers about the open tab only', () => {
    const everything: Draft = {
      text: 'message A {}',
      paths: ['/a.proto'],
      binding: { source: 's1', entry: 'a.proto' },
    };
    expect(draftFilled('paste', everything)).toBe(true);
    expect(draftFilled('files', { ...everything, paths: [] })).toBe(false);
  });
});

describe('draftFromSchema / previewFromSchema', () => {
  /** Открыть настройки — значит увидеть действующий контракт, а не чистый лист.
   *  И заполнена при этом ровно одна вкладка: контракт у топика один. */
  it('fills the tab the topic is configured by, and only it', () => {
    const pasted = draftFromSchema(
      schema({ files: [PASTED], pasted_text: 'syntax = "proto3";' }),
    );
    expect(pasted).toEqual({ text: 'syntax = "proto3";', paths: [], binding: null });

    const files = draftFromSchema(schema({ files: [DISK] }));
    expect(files).toEqual({ text: '', paths: ['/home/u/orders.proto'], binding: null });

    const bound = draftFromSchema(schema({ source: { source: 's1', entry: 'orders.proto' } }));
    expect(bound).toEqual({
      text: '',
      paths: [],
      binding: { source: 's1', entry: 'orders.proto' },
    });
  });

  it('leaves every tab empty for a topic configured by nothing', () => {
    expect(draftFromSchema(schema({}))).toEqual(EMPTY);
    expect(draftFromSchema(null)).toEqual(EMPTY);
  });

  /**
   * Главное свойство: разбор сохранённого контракта УЖЕ известен.
   *
   * На нём держится то, что открытие настроек настроенного топика не стоит ни
   * одного запроса: типы, корень и список файлов приезжают вместе со схемой, и
   * спрашивать их заново незачем.
   */
  it('reuses the parse that arrived with the schema', () => {
    const stored = schema({
      files: [DISK],
      messages: ['orders.Order', 'orders.Line'],
      root: 'orders.Order',
    });
    expect(previewFromSchema(stored)).toEqual({
      files: [DISK],
      messages: ['orders.Order', 'orders.Line'],
      root: 'orders.Order',
    });
    // А у ненастроенного разбирать нечего, и выдумывать пустой разбор нельзя:
    // пустой список типов и отсутствие разбора — разные вещи для селектора.
    expect(previewFromSchema(schema({}))).toBeNull();
  });
});

describe('draftMatchesSchema', () => {
  /**
   * Совпадение значит две вещи сразу: разбор спрашивать не надо, и сохранять
   * контракт тоже не надо — пришли поменять один тип.
   */
  it('recognises the contract the topic already carries', () => {
    const pasted = schema({ files: [PASTED], pasted_text: 'syntax = "proto3";' });
    expect(draftMatchesSchema('paste', draftFromSchema(pasted), pasted)).toBe(true);

    const files = schema({ files: [DISK] });
    expect(draftMatchesSchema('files', draftFromSchema(files), files)).toBe(true);

    const bound = schema({ source: { source: 's1', entry: 'orders.proto' } });
    expect(draftMatchesSchema('library', draftFromSchema(bound), bound)).toBe(true);
  });

  /** Правка текста — уже не то, что сохранено, даже на один символ. */
  it('sees an edited contract as changed', () => {
    const pasted = schema({ files: [PASTED], pasted_text: 'syntax = "proto3";' });
    expect(draftMatchesSchema('paste', { ...EMPTY, text: 'syntax = "proto3"; ' }, pasted)).toBe(
      false,
    );
  });

  /**
   * Вкладка решает. Тот же черновик на вкладке, которой топик НЕ настроен, —
   * это изменение, а не совпадение: сохранение сменит способ.
   */
  it('answers per tab, not per content', () => {
    const bound = schema({ source: { source: 's1', entry: 'orders.proto' } });
    const draft = draftFromSchema(bound);
    expect(draftMatchesSchema('library', draft, bound)).toBe(true);
    expect(draftMatchesSchema('paste', draft, bound)).toBe(false);
    expect(draftMatchesSchema('files', draft, bound)).toBe(false);
  });

  /** Доисканные импортом файлы в сверку не идут: пользователь их не выбирал, и
   *  сохранение отдаёт обратно только выбранное — импорты бэкенд доищет сам. */
  it('compares the picked files, not the ones imports pulled in', () => {
    const pulled: SchemaFile = { name: 'common/types.proto', source: '/home/u/common/types.proto', auto: true };
    const withImport = schema({ files: [DISK, pulled] });
    expect(pickedPathsOf(withImport)).toEqual(['/home/u/orders.proto']);
    expect(draftMatchesSchema('files', { ...EMPTY, paths: ['/home/u/orders.proto'] }, withImport)).toBe(
      true,
    );
  });
});

describe('messageFor', () => {
  /** Корень набора — ответ по умолчанию, и этого обычно хватает: на настоящих
   *  контрактах правило корня совпало с назначенным вручную 14 раз из 14. */
  it('takes the root of the set when nothing is chosen yet', () => {
    expect(messageFor({ files: [], messages: ['a.A', 'a.B'], root: 'a.A' }, null)).toBe('a.A');
  });

  /** Выбор руками сильнее правила корня: читать топик вложенным типом законно. */
  it('keeps a choice the new set still declares', () => {
    expect(messageFor({ files: [], messages: ['a.A', 'a.B'], root: 'a.A' }, 'a.B')).toBe('a.B');
  });

  /** А вот выбор, которого в новом наборе НЕТ, держать нельзя: это обещание
   *  декодирования, которого не будет. */
  it('drops a choice the new set does not declare', () => {
    expect(messageFor({ files: [], messages: ['a.A'], root: null }, 'gone.Type')).toBe('a.A');
  });

  /** Единственный тип подставляется и без корня: выбирать не из чего. */
  it('takes the only type when there is no root', () => {
    expect(messageFor({ files: [], messages: ['a.A'], root: null }, null)).toBe('a.A');
  });

  /** Ни корня, ни единственного — выбор за человеком, и выдумывать его нельзя:
   *  два структурно разных типа по имени набора не различить. */
  it('leaves the choice open when the set has several roots', () => {
    expect(messageFor({ files: [], messages: ['a.A', 'a.B'], root: null }, null)).toBeNull();
    expect(messageFor(null, 'a.A')).toBeNull();
  });
});
