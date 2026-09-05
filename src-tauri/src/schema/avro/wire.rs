//! В какой обёртке приехало тело Avro.
//!
//! Само тело схемы не несёт, поэтому её кладут рядом — и в экосистеме Kafka
//! это делают несколькими способами. Различаются они первыми байтами, так что
//! спрашивать пользователя не о чем: обёртку видно.
//!
//! | Что | Признак | Где схема |
//! |---|---|---|
//! | confluent | `0x00` и 4 байта big-endian | в реестре, по id; своя на каждое сообщение |
//! | контейнер (OCF) | `Obj\x01` | внутри самих байтов |
//! | single-object | `0xC3 0x01` и 8 байт отпечатка | нигде: отпечаток мы разрешить не умеем |
//! | голый datum | ничего из перечисленного | закреплена настройками топика |
//!
//! Порядок проверок важен: голый datum — это ОТСУТСТВИЕ признаков, и потому
//! он последний. Ошибиться в другую сторону тоже нельзя — тело, начинающееся с
//! нулевого байта, вполне может быть законным голым datum'ом (нулём кодируется,
//! например, пустая строка), поэтому за confluent его принимают только вместе с
//! настроенным реестром; решает это `decoder`, а не разбор заголовка.

/// Заголовок confluent-формата: маркер плюс четырёхбайтовый id.
pub const CONFLUENT_HEADER: usize = 5;

const CONFLUENT_MAGIC: u8 = 0x00;
const CONTAINER_MAGIC: &[u8] = b"Obj\x01";
const SINGLE_OBJECT_MAGIC: &[u8] = &[0xc3, 0x01];
/// Маркер плюс 8 байт отпечатка CRC-64-AVRO.
const SINGLE_OBJECT_HEADER: usize = 10;

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
}
