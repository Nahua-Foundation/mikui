import { describe, expect, it } from 'vitest';
import { hasEnvelope } from './LensSelect';
import { BodyFormat } from '../types';

const ALL: BodyFormat[] = ['json', 'text', 'proto', 'avro', 'jsonschema', 'hex'];

describe('hasEnvelope', () => {
  /**
   * Главное, что здесь закреплено, — это НЕ список исключений, а его
   * однострочность.
   *
   * Соблазн спрятать селектор конверта у «неподходящих» форматов возникает
   * естественно: proto и avro не похожи на JSON, и кажется, что снимать у них
   * нечего. Но линза работает поверх декодера, по уже разобранному телу, а
   * конверты Debezium и Kafka Connect ездят через все три сериализатора
   * Confluent — Avro, Protobuf и JSON Schema. У protobuf-топика `before`,
   * `after` и `op` оказываются ровно там же, где у джейсонного.
   */
  it('keeps the envelope selector for every format that decodes to JSON', () => {
    for (const format of ALL.filter((f) => f !== 'hex')) {
      expect(hasEnvelope(format), format).toBe(true);
    }
  });

  /**
   * И единственное настоящее исключение: `hex` печатает байты парами заглавных
   * цифр через пробел (`0A 15 08 C8`). JSON из этого не выйдет ни при каком
   * теле, и ключи конверта в такой строке не встретятся — там нет ни строчных
   * букв, ни двоеточий.
   */
  it('drops it for hex, where there is nothing an envelope could be made of', () => {
    expect(hasEnvelope('hex')).toBe(false);
  });
});
