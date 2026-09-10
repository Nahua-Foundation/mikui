mod filter;
mod hex;
mod quota;
// Видно всему крейту по той же причине, что и `text`: сохранённое сообщение
// показывается тем же способом, что и прочитанное.
pub(crate) mod lens;
mod raw_consumer;
mod store;
mod types;
mod worker;

// Видно всему крейту ради `crate::favorites`: сохранённое сообщение и
// показывается, и превьюится ровно так же, как прочитанное из топика, и второй
// способ решать, что такое превью и что такое «двоичное тело», рано или поздно
// разошёлся бы с первым.
pub(crate) mod text;

// Разбор нужен отправке (`lib.rs`), печать — показу тела (`schema::Decoder`).
// Обе поимённо, а не модулем целиком: больше в нём ничего и нет.
pub use hex::{decode as decode_hex, encode as encode_hex};
pub use types::*;
pub use worker::{Command, WorkerHandle};
