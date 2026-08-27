//! Пароли в системном хранилище секретов.
//!
//! macOS — Keychain, Windows — Credential Manager, Linux — Secret Service.
//! В `clusters.json` пароль не попадает никогда, и наружу через IPC он тоже не
//! уезжает: при подключении к сохранённому кластеру Rust достаёт пароль сам
//! по идентификатору, фронт его не видит.

use keyring::{Entry, Error};

const SERVICE: &str = "mikui";

fn entry(cluster_id: &str) -> Result<Entry, String> {
    Entry::new(SERVICE, cluster_id).map_err(|e| match e {
        Error::NoDefaultStore => {
            "no system credential store is available; passwords cannot be saved on this machine"
                .to_string()
        }
        other => format!("credential store unavailable: {other}"),
    })
}

pub fn store_password(cluster_id: &str, password: &str) -> Result<(), String> {
    entry(cluster_id)?
        .set_password(password)
        .map_err(|e| format!("can't save password: {e}"))
}

/// `Ok(None)` — пароля просто нет; это не ошибка.
pub fn read_password(cluster_id: &str) -> Result<Option<String>, String> {
    match entry(cluster_id)?.get_password() {
        Ok(password) => Ok(Some(password)),
        Err(Error::NoEntry) => Ok(None),
        Err(e) => Err(format!("can't read password: {e}")),
    }
}

/// Удаление отсутствующего пароля считается успехом — вызывающему нужен
/// результат «пароля нет», а не отчёт о том, был ли он там до этого.
pub fn delete_password(cluster_id: &str) -> Result<(), String> {
    match entry(cluster_id)?.delete_credential() {
        Ok(()) | Err(Error::NoEntry) => Ok(()),
        Err(e) => Err(format!("can't delete password: {e}")),
    }
}
