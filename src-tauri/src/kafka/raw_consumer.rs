//! Тонкая unsafe-обвязка над legacy simple consumer API librdkafka
//! (`rd_kafka_consume_start_queue` / `rd_kafka_consume_queue` /
//! `rd_kafka_consume_stop`).
//!
//! В отличие от высокоуровневого `assign()`/`subscribe()`, эти функции не
//! создают объект консьюмер-группы (`cgrp`) и не требуют `group.id` ни при
//! каких условиях, кроме офсета `RD_KAFKA_OFFSET_STORED` (см. rdkafka.c,
//! `rd_kafka_consume_start0`) — который здесь не используется. Поэтому
//! чтение через них не задевает ACL на консьюмер-группы вообще, в отличие
//! от `BaseConsumer::assign()`.
//!
//! Безопасный крейт `rdkafka` этот API не оборачивает, поэтому здесь прямой
//! FFI через `rdkafka::bindings`/`rdkafka::types` (реэкспорт `rdkafka-sys`,
//! уже транзитивная зависимость). Весь `unsafe` в приложении собран в этом
//! модуле — вызывающий код (`worker.rs`) работает с безопасными типами.

use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_void};
use std::slice;

use rdkafka::bindings::{
    rd_kafka_consume_batch_queue, rd_kafka_consume_start_queue, rd_kafka_consume_stop,
    rd_kafka_err2str, rd_kafka_header_get_all, rd_kafka_last_error, rd_kafka_message_destroy,
    rd_kafka_message_headers, rd_kafka_message_timestamp, rd_kafka_queue_destroy,
    rd_kafka_queue_new, rd_kafka_timestamp_type_t, rd_kafka_topic_destroy, rd_kafka_topic_new,
};
use rdkafka::types::{RDKafka, RDKafkaMessage, RDKafkaQueue, RDKafkaRespErr, RDKafkaTopic};

/// `RD_KAFKA_OFFSET_BEGINNING` — читать с начала партиции.
pub const OFFSET_BEGINNING: i64 = -2;
/// Базовое значение для макроса `RD_KAFKA_OFFSET_TAIL(CNT)` из `rdkafka.h`:
/// `TAIL(CNT) = TAIL_BASE - CNT`. Готовой константы в безопасном крейте нет,
/// т.к. `Offset::OffsetTail` там реализован как обёртка над этим же макросом.
const OFFSET_TAIL_BASE: i64 = -2000;

/// Офсет «последние `count` сообщений», как `Offset::OffsetTail(count)` в
/// высокоуровневом API — только не требует group.id.
pub fn offset_tail(count: i64) -> i64 {
    OFFSET_TAIL_BASE - count
}

fn last_error_str() -> String {
    unsafe {
        let err = rd_kafka_last_error();
        let msg = rd_kafka_err2str(err);
        if msg.is_null() {
            format!("{err:?}")
        } else {
            CStr::from_ptr(msg).to_string_lossy().into_owned()
        }
    }
}

pub fn err_str(err: RDKafkaRespErr) -> String {
    unsafe {
        let msg = rd_kafka_err2str(err);
        if msg.is_null() {
            format!("{err:?}")
        } else {
            CStr::from_ptr(msg).to_string_lossy().into_owned()
        }
    }
}

fn ptr_to_opt_slice<'a>(ptr: *mut c_void, len: usize) -> Option<&'a [u8]> {
    if ptr.is_null() {
        None
    } else {
        Some(unsafe { slice::from_raw_parts(ptr as *const u8, len) })
    }
}

/// Очередь, в которую legacy-консьюмер складывает вычитанные сообщения со
/// всех запущенных на ней партиций — так один цикл опроса мультиплексирует
/// несколько партиций, как раньше делал `BaseConsumer::poll`.
pub struct RawQueue {
    ptr: *mut RDKafkaQueue,
}

impl RawQueue {
    pub fn new(client: *mut RDKafka) -> Result<Self, String> {
        let ptr = unsafe { rd_kafka_queue_new(client) };
        if ptr.is_null() {
            return Err("rd_kafka_queue_new returned null".to_string());
        }
        Ok(Self { ptr })
    }
}

impl Drop for RawQueue {
    fn drop(&mut self) {
        unsafe { rd_kafka_queue_destroy(self.ptr) };
    }
}

/// Хендл топика для legacy API. Создание не затрагивает никакую группу.
pub struct RawTopic {
    ptr: *mut RDKafkaTopic,
}

impl RawTopic {
    /// `client` — сырой `rd_kafka_t*`, взятый у уже существующего
    /// `BaseConsumer` через `Consumer::client().native_ptr()`. Топик
    /// наследует конфиг клиента (NULL topic conf), как и раньше через
    /// `assign()`.
    pub fn new(client: *mut RDKafka, topic: &str) -> Result<Self, String> {
        let name =
            CString::new(topic).map_err(|_| "topic name contains a NUL byte".to_string())?;
        let ptr = unsafe { rd_kafka_topic_new(client, name.as_ptr(), std::ptr::null_mut()) };
        if ptr.is_null() {
            return Err(format!(
                "rd_kafka_topic_new('{topic}') failed: {}",
                last_error_str()
            ));
        }
        Ok(Self { ptr })
    }

    /// Начинает чтение партиции с заданного офсета в общую очередь.
    pub fn consume_start_queue(
        &self,
        partition: i32,
        offset: i64,
        queue: &RawQueue,
    ) -> Result<(), String> {
        let rc = unsafe { rd_kafka_consume_start_queue(self.ptr, partition, offset, queue.ptr) };
        if rc == -1 {
            return Err(last_error_str());
        }
        Ok(())
    }

    /// Останавливает чтение партиции. Best-effort: ошибку логируем, не
    /// падаем — зеркалит прежнее `let _ = consumer.unassign()`.
    pub fn consume_stop(&self, partition: i32) {
        let rc = unsafe { rd_kafka_consume_stop(self.ptr, partition) };
        if rc == -1 {
            eprintln!(
                "[raw_consumer] consume_stop(partition={partition}) failed: {}",
                last_error_str()
            );
        }
    }
}

impl Drop for RawTopic {
    fn drop(&mut self) {
        unsafe { rd_kafka_topic_destroy(self.ptr) };
    }
}

/// Сообщение из общей очереди. Может нести ошибку вместо данных (например,
/// `PARTITION_EOF`) — см. `err()`, тот же паттерн, что и у высокоуровневого
/// `consumer.poll()`.
pub struct RawMessage {
    ptr: *mut RDKafkaMessage,
}

/// Забирает разом до `max` событий из очереди — один FFI-вызов вместо `max`
/// вызовов `consume()`. Ждёт до `timeout_ms`, если очередь пуста, но
/// возвращается раньше, как только накопилось хоть что-то.
pub fn consume_batch(queue: &RawQueue, timeout_ms: i32, max: usize) -> Vec<RawMessage> {
    let mut slots: Vec<*mut RDKafkaMessage> = vec![std::ptr::null_mut(); max];
    let filled =
        unsafe { rd_kafka_consume_batch_queue(queue.ptr, timeout_ms, slots.as_mut_ptr(), max) };
    if filled <= 0 {
        return Vec::new();
    }
    slots
        .into_iter()
        .take(filled as usize)
        .map(|ptr| RawMessage { ptr })
        .collect()
}

impl RawMessage {
    pub fn err(&self) -> RDKafkaRespErr {
        unsafe { (*self.ptr).err }
    }

    pub fn partition(&self) -> i32 {
        unsafe { (*self.ptr).partition }
    }

    pub fn offset(&self) -> i64 {
        unsafe { (*self.ptr).offset }
    }

    pub fn key(&self) -> Option<&[u8]> {
        unsafe { ptr_to_opt_slice((*self.ptr).key, (*self.ptr).key_len) }
    }

    pub fn payload(&self) -> Option<&[u8]> {
        unsafe { ptr_to_opt_slice((*self.ptr).payload, (*self.ptr).len) }
    }

    pub fn timestamp_millis(&self) -> i64 {
        let mut kind = rd_kafka_timestamp_type_t::RD_KAFKA_TIMESTAMP_NOT_AVAILABLE;
        let ts = unsafe { rd_kafka_message_timestamp(self.ptr, &mut kind) };
        if ts < 0 {
            0
        } else {
            ts
        }
    }

    /// Заголовки сообщения. Повторяет паттерн приватного
    /// `BorrowedHeaders::try_get` из безопасного крейта (его конструктор
    /// недоступен отсюда — crate-private), только без zero-copy обёртки.
    pub fn headers(&self) -> Vec<(&str, &[u8])> {
        let mut hdrs_ptr = std::ptr::null_mut();
        let err = unsafe { rd_kafka_message_headers(self.ptr, &mut hdrs_ptr) };
        if err != RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR || hdrs_ptr.is_null() {
            return Vec::new();
        }

        let mut out = Vec::new();
        let mut idx = 0usize;
        loop {
            let mut name_ptr: *const c_char = std::ptr::null();
            let mut value_ptr: *const c_void = std::ptr::null();
            let mut value_len = 0usize;
            let err = unsafe {
                rd_kafka_header_get_all(
                    hdrs_ptr,
                    idx,
                    &mut name_ptr,
                    &mut value_ptr,
                    &mut value_len,
                )
            };
            if err != RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR {
                break;
            }
            let key = unsafe { CStr::from_ptr(name_ptr) }
                .to_str()
                .unwrap_or("<invalid utf-8>");
            let value: &[u8] = if value_ptr.is_null() {
                &[]
            } else {
                unsafe { slice::from_raw_parts(value_ptr as *const u8, value_len) }
            };
            out.push((key, value));
            idx += 1;
        }
        out
    }
}

impl Drop for RawMessage {
    fn drop(&mut self) {
        unsafe { rd_kafka_message_destroy(self.ptr) };
    }
}
