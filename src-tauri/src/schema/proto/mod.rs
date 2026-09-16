//! Protobuf-половина схем топиков: разбор .proto, декодирование тел по
//! выбранному message и заготовка для отправки.
//!
//! Общее с Avro — привязка к топику, файл настроек и раскладка файлов — живёт
//! этажом выше, в `schema`. Здесь только то, что знает про protobuf.

pub mod decoder;
pub mod imports;
pub mod linked;
pub mod resolve;
pub mod template;

pub use decoder::ProtoDecoder;
