//! В какой обёртке приехало тело со схемой.
//!
//! Жило в `avro::wire`, пока формат со схемой был один. Обёртка confluent —
//! маркер плюс id схемы в реестре — у всех трёх форматов реестра ОДНА И ТА ЖЕ,
//! и держать её разбор в аврошном модуле значило бы, что protobuf и JSON Schema
//! импортируют себе `avro::` за тем, в чём аврошного нет ничего.
//!
//! Что здесь есть:
//!   * `framing` — обёртки Avro. Их несколько, потому что тело Avro не несёт
//!     схемы вообще и класть её рядом придумали по-разному;
//!   * `proto_framing` — та же confluent-обёртка, но с хвостом из
//!     message-indexes, который есть только у protobuf;
//!   * `frame` — сборка заголовка для отправки.
//!
//! У JSON Schema своего варианта нет: там за заголовком лежит обычный JSON, и
//! хватает `framing`.

/// Заголовок confluent-формата: маркер плюс четырёхбайтовый id.
pub const CONFLUENT_HEADER: usize = 5;

const CONFLUENT_MAGIC: u8 = 0x00;
const CONTAINER_MAGIC: &[u8] = b"Obj\x01";
const SINGLE_OBJECT_MAGIC: &[u8] = &[0xc3, 0x01];
/// Маркер плюс 8 байт отпечатка CRC-64-AVRO.
const SINGLE_OBJECT_HEADER: usize = 10;

/// Собирает confluent-заголовок перед готовым телом.
///
/// Ровно то, чего ждёт от топика штатный потребитель со своим
/// `Kafka*Deserializer`. Общая на Avro и JSON Schema: разницы в заголовке между
/// ними нет никакой — она вся в том, что лежит за ним.
pub fn frame(id: u32, body: &[u8]) -> Vec<u8> {
    let mut framed = Vec::with_capacity(body.len() + CONFLUENT_HEADER);
    framed.push(CONFLUENT_MAGIC);
    framed.extend_from_slice(&id.to_be_bytes());
    framed.extend_from_slice(body);
    framed
}

/// | Что | Признак | Где схема |
/// |---|---|---|
/// | confluent | `0x00` и 4 байта big-endian | в реестре, по id; своя на каждое сообщение |
/// | контейнер (OCF) | `Obj\x01` | внутри самих байтов |
/// | single-object | `0xC3 0x01` и 8 байт отпечатка | нигде: отпечаток мы разрешить не умеем |
/// | голый datum | ничего из перечисленного | закреплена настройками топика |
///
/// Порядок проверок важен: голый datum — это ОТСУТСТВИЕ признаков, и потому
/// он последний. Ошибиться в другую сторону тоже нельзя — тело, начинающееся с
/// нулевого байта, вполне может быть законным голым datum'ом (нулём кодируется,
/// например, пустая строка), поэтому за confluent его принимают только вместе с
/// настроенным реестром; решает это `decoder`, а не разбор заголовка.
#[derive(Debug, PartialEq, Eq)]
pub enum Framing<'a> {
    /// Схему надо взять в реестре по этому id.
    Confluent { id: u32, datum: &'a [u8] },
    /// Контейнерный файл — схема лежит в его заголовке.
    Container,
    /// Схема названа отпечатком, а не идентификатором. Разрешить его можно
    /// только имея на руках все схемы разом и считая по ним CRC-64-AVRO;
    /// реестр по отпечатку искать не умеет, поэтому честнее сказать об этом,
    /// чем разобрать хвост чужой схемой и показать правдоподобный мусор.
    SingleObject,
    /// Ни одного признака — тело как есть.
    Bare(&'a [u8]),
}

pub fn framing(body: &[u8]) -> Framing<'_> {
    if body.starts_with(CONTAINER_MAGIC) {
        return Framing::Container;
    }
    if body.starts_with(SINGLE_OBJECT_MAGIC) && body.len() >= SINGLE_OBJECT_HEADER {
        return Framing::SingleObject;
    }
    // Ровно пять байт — это заголовок без тела, что законно: запись со всеми
    // полями по умолчанию кодируется в ноль байт.
    if body.first() == Some(&CONFLUENT_MAGIC) && body.len() >= CONFLUENT_HEADER {
        let id = u32::from_be_bytes([body[1], body[2], body[3], body[4]]);
        return Framing::Confluent {
            id,
            datum: &body[CONFLUENT_HEADER..],
        };
    }
    Framing::Bare(body)
}

// --- Protobuf -----------------------------------------------------------------

/// Обёртка тела protobuf.
///
/// Отличие от Avro одно, но существенное: между заголовком и телом стоит ещё
/// МАССИВ ИНДЕКСОВ, адресующий конкретный message внутри файла схемы. Пока
/// его не снять, тело не разберётся ничем — первый же varint индексов
/// притворится тегом поля. Именно поэтому топик от `KafkaProtobufSerializer`
/// не читался даже с правильным .proto на руках.
#[derive(Debug, PartialEq, Eq)]
pub enum ProtoFraming<'a> {
    Confluent {
        id: u32,
        /// Путь до message внутри файла схемы: индекс типа верхнего уровня,
        /// затем индексы вложенных. Пустым не бывает — см. `message_indexes`.
        indexes: Vec<i64>,
        datum: &'a [u8],
    },
    /// Ни маркера, ни индексов — тело как его записал обычный сериализатор.
    Bare(&'a [u8]),
}

pub fn proto_framing(body: &[u8]) -> ProtoFraming<'_> {
    if body.first() != Some(&CONFLUENT_MAGIC) || body.len() < CONFLUENT_HEADER {
        return ProtoFraming::Bare(body);
    }
    let id = u32::from_be_bytes([body[1], body[2], body[3], body[4]]);
    let rest = &body[CONFLUENT_HEADER..];

    // Индексы не разобрались — значит, это и не confluent-протобаф, что бы ни
    // стояло в первом байте: нулём начинается и вполне законное тело, у
    // которого поле 0 отсутствует, а первым идёт что-то другое. Отдаём как
    // есть, и пусть его пробует разобрать выбранный message.
    let Some((indexes, datum)) = message_indexes(rest) else {
        return ProtoFraming::Bare(body);
    };
    ProtoFraming::Confluent { id, indexes, datum }
}

/// Снимает массив message-indexes.
///
/// Формат: зигзаг-varint длины, затем столько же зигзаг-varint'ов. Отдельным
/// правилом стоит СОКРАЩЕНИЕ: одиночный нулевой байт означает не пустой массив,
/// а `[0]` — самый частый случай, первый message файла. Пустым массив не бывает
/// вовсе: индексы адресуют тип, а «никакой тип» смысла не имеет.
///
/// `None` — байты на массив индексов не похожи.
fn message_indexes(bytes: &[u8]) -> Option<(Vec<i64>, &[u8])> {
    let (len, mut rest) = zigzag(bytes)?;
    if len == 0 {
        return Some((vec![0], rest));
    }
    // Потолок на длину: без него битый первый varint попросил бы у нас массив
    // на миллиарды элементов ещё до того, как мы заметим, что байты кончились.
    if !(0..=MAX_MESSAGE_INDEXES).contains(&len) {
        return None;
    }

    let mut indexes = Vec::with_capacity(len as usize);
    for _ in 0..len {
        let (index, tail) = zigzag(rest)?;
        // Индекс — это позиция в списке типов, отрицательной она не бывает.
        if index < 0 {
            return None;
        }
        indexes.push(index);
        rest = tail;
    }
    Some((indexes, rest))
}

/// Сколько уровней вложенности мы готовы принять за правду. Глубже реальные
/// схемы не забираются, а число нужно, чтобы битый varint не просил память.
const MAX_MESSAGE_INDEXES: i64 = 64;

/// Читает один зигзаг-varint. `None` — байты кончились или число не влезло.
fn zigzag(bytes: &[u8]) -> Option<(i64, &[u8])> {
    let mut value: u64 = 0;
    let mut shift = 0;
    for (position, byte) in bytes.iter().enumerate() {
        // Десять групп по семь бит — потолок для 64-битного числа. Дальше
        // varint не читаем: продолжение за ним означает битые данные.
        if shift >= 64 {
            return None;
        }
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            // Зигзаг: младший бит — знак, остальное — модуль.
            let decoded = ((value >> 1) as i64) ^ -((value & 1) as i64);
            return Some((decoded, &bytes[position + 1..]));
        }
        shift += 7;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn confluent_header_gives_the_id_and_the_rest() {
        let body = [0x00, 0x00, 0x00, 0x00, 0x2a, 0xde, 0xad];
        assert_eq!(
            framing(&body),
            Framing::Confluent {
                id: 42,
                datum: &[0xde, 0xad]
            }
        );
    }

    /// Идентификаторы в живых реестрах давно за пределами одного байта —
    /// порядок байтов обязан быть big-endian, как в спецификации.
    #[test]
    fn the_id_is_big_endian() {
        let body = [0x00, 0x00, 0x01, 0x00, 0x00];
        assert_eq!(framing(&body), Framing::Confluent { id: 65536, datum: &[] });
    }

    #[test]
    fn a_container_file_is_recognised_by_its_magic() {
        assert_eq!(framing(b"Obj\x01\x04\x16avro.schema"), Framing::Container);
    }

    #[test]
    fn single_object_encoding_is_recognised_and_not_mistaken_for_a_datum() {
        let body = [0xc3, 0x01, 1, 2, 3, 4, 5, 6, 7, 8, 0x0a];
        assert_eq!(framing(&body), Framing::SingleObject);
    }

    #[test]
    fn anything_else_is_a_bare_datum() {
        assert_eq!(framing(&[0x0a, 0x61]), Framing::Bare(&[0x0a, 0x61]));
        assert_eq!(framing(&[]), Framing::Bare(&[]));
    }

    /// Тело короче заголовка — не confluent, что бы ни стояло в первом байте:
    /// id из него не собрать. Одиночный нулевой байт — это законный datum
    /// (пустая строка, ноль, false), и принять его за обрубленный заголовок
    /// значило бы не показать вполне читаемое сообщение.
    #[test]
    fn a_lone_zero_byte_stays_a_datum() {
        assert_eq!(framing(&[0x00]), Framing::Bare(&[0x00]));
        assert_eq!(framing(&[0x00, 0x00, 0x00]), Framing::Bare(&[0x00, 0x00, 0x00]));
    }

    #[test]
    fn a_frame_is_the_magic_the_id_and_the_body() {
        assert_eq!(frame(42, &[0xde, 0xad]), vec![0x00, 0x00, 0x00, 0x00, 0x2a, 0xde, 0xad]);
    }

    /// Собранное `frame` обязано разбираться `framing` — иначе мы кладём в
    /// топик то, чего сами прочитать не сможем.
    #[test]
    fn framing_undoes_frame() {
        let framed = frame(65536, b"body");
        assert_eq!(
            framing(&framed),
            Framing::Confluent {
                id: 65536,
                datum: b"body"
            }
        );
    }

    // --- Protobuf ----------------------------------------------------------

    /// Самый частый случай и единственное исключение из правила: нулевой байт
    /// на месте длины — это не пустой массив, а `[0]`.
    #[test]
    fn a_zero_length_is_the_shorthand_for_the_first_message() {
        // 0x00 magic, id = 1, 0x00 индексы, дальше тело.
        let body = [0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x0a, 0x02, b'h', b'i'];
        assert_eq!(
            proto_framing(&body),
            ProtoFraming::Confluent {
                id: 1,
                indexes: vec![0],
                datum: &[0x0a, 0x02, b'h', b'i'],
            }
        );
    }

    /// Массив из одного элемента, записанный полным способом: длина 1, затем
    /// сам индекс. Зигзаг: 1 → 0x02, 2 → 0x04.
    #[test]
    fn a_single_index_is_read_with_its_length() {
        let body = [0x00, 0x00, 0x00, 0x00, 0x07, 0x02, 0x04, 0xde];
        assert_eq!(
            proto_framing(&body),
            ProtoFraming::Confluent {
                id: 7,
                indexes: vec![2],
                datum: &[0xde],
            }
        );
    }

    /// Вложенный тип: путь длиной больше единицы.
    #[test]
    fn a_nested_path_keeps_every_step() {
        // длина 3 (зигзаг 0x06), индексы 1, 0, 2 (зигзаг 0x02, 0x00, 0x04).
        let body = [0x00, 0x00, 0x00, 0x00, 0x09, 0x06, 0x02, 0x00, 0x04, 0xbe, 0xef];
        assert_eq!(
            proto_framing(&body),
            ProtoFraming::Confluent {
                id: 9,
                indexes: vec![1, 0, 2],
                datum: &[0xbe, 0xef],
            }
        );
    }

    /// Индексы обещают больше, чем есть байт. Это не confluent-тело, и
    /// притворяться, что мы его разобрали, нельзя.
    #[test]
    fn a_truncated_index_array_is_not_confluent() {
        let body = [0x00, 0x00, 0x00, 0x00, 0x01, 0x06, 0x02];
        assert_eq!(proto_framing(&body), ProtoFraming::Bare(&body));
    }

    /// Обычное тело protobuf без всякой обёртки.
    #[test]
    fn a_bare_protobuf_body_is_left_alone() {
        let body = [0x0a, 0x02, b'h', b'i'];
        assert_eq!(proto_framing(&body), ProtoFraming::Bare(&body));
    }

    /// Тело короче заголовка — не confluent, как и у Avro.
    #[test]
    fn a_short_protobuf_body_is_bare() {
        assert_eq!(proto_framing(&[0x00]), ProtoFraming::Bare(&[0x00]));
        assert_eq!(proto_framing(&[]), ProtoFraming::Bare(&[]));
    }

    /// Длина индексов, не влезающая ни в какую реальную схему, — признак того,
    /// что мы читаем не индексы. Важно, что это НЕ паника и не запрос памяти.
    #[test]
    fn an_absurd_index_count_is_refused_without_allocating() {
        // Зигзаг-varint на 0x7fffffff: заведомо больше потолка.
        let body = [0x00, 0x00, 0x00, 0x00, 0x01, 0xfe, 0xff, 0xff, 0xff, 0x0f, 0xaa];
        assert_eq!(proto_framing(&body), ProtoFraming::Bare(&body));
    }

    #[test]
    fn zigzag_reads_the_encoding_protobuf_actually_writes() {
        assert_eq!(zigzag(&[0x00]).unwrap().0, 0);
        assert_eq!(zigzag(&[0x02]).unwrap().0, 1);
        assert_eq!(zigzag(&[0x04]).unwrap().0, 2);
        // Многобайтовый: 300 → зигзаг 600 → 0xd8 0x04.
        assert_eq!(zigzag(&[0xd8, 0x04]).unwrap().0, 300);
        // Продолжение обещано, а байтов нет.
        assert!(zigzag(&[0x80]).is_none());
        assert!(zigzag(&[]).is_none());
    }
}
