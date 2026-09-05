//! Чем превращают тело сообщения в то, что можно показать.
//!
//! Один тип на оба формата со схемой. Всё, что стоит по пути тела наружу —
//! `kafka::text`, буфер воркера, архив сохранённых, — держит именно его и про
//! protobuf с Avro не знает ничего: разница между ними кончается здесь.
//!
//! Enum, а не `dyn`: вариантов два, оба известны в крейте, и виртуальный вызов
//! на каждое тело в таблице не окупается ничем.

use super::avro::AvroDecoder;
use super::proto::ProtoDecoder;

pub enum Decoder {
    Proto(ProtoDecoder),
    Avro(AvroDecoder),
}

impl Decoder {
    /// Разбирает тело и печатает его компактным JSON — без переносов строк.
    ///
    /// Компактным намеренно: таблице нужна ровно одна строка, а модалке —
    /// отступы, которые она и так расставит сама тем же кодом, каким давно
    /// печатает JSON-топики.
    pub fn decode(&self, payload: &[u8]) -> Result<String, String> {
        match self {
            Decoder::Proto(d) => d.decode(payload),
            Decoder::Avro(d) => d.decode(payload),
        }
    }

    /// Тело вместе с именами enum ИМЕННО ТОЙ схемы, которой оно разобрано.
    ///
    /// У protobuf это всегда один и тот же список — тип топика выбран заранее.
    /// У Avro в confluent-формате схема адресуется id из заголовка КАЖДОГО
    /// сообщения, поэтому список приходится отдавать вместе с телом. Отдельным
    /// методом, а не всегда: `Vec<String>` на каждую строку таблицы не нужен
    /// никому, а зовут это только для сообщения, которое открыли.
    pub fn decode_with_enums(&self, payload: &[u8]) -> Result<(String, Vec<String>), String> {
        match self {
            Decoder::Proto(d) => d.decode(payload).map(|json| (json, d.enum_values().to_vec())),
            Decoder::Avro(d) => d.decode_with_enums(payload),
        }
    }
}
