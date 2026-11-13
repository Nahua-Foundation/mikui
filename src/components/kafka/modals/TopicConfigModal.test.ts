import { describe, expect, it } from 'vitest';
import { modeFromSchema, modeOf, settledWithoutLibrary } from './TopicConfigModal';
import { BodyFormat, SchemaFile, SourceView, TopicSchema } from '../types';

/** Схема топика: всё, кроме способа задать контракт, выбору вкладки безразлично. */
function schema(parts: {
  files?: SchemaFile[];
  source?: { source: string; entry: string } | null;
  format?: BodyFormat;
}): TopicSchema {
  return {
    cluster: 'c',
    topic: 't',
    format: parts.format ?? 'proto',
    files: parts.files ?? [],
    source: parts.source ?? null,
    message: null,
    messages: [],
    root: null,
    produce_choices: [],
    error: null,
    avro: null,
    json: null,
    lens: null,
    detected_kind: null,
    pasted_text: null,
  } as TopicSchema;
}

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
