//! HTTP-клиент для всего, за чем приложение ходит по сети.
//!
//! Ходит оно за двумя вещами — схемами в Schema Registry и деревьями .proto в
//! GitLab, — и условия у обеих одинаковые: корпоративный TLS-инспектор
//! подписывает свой сертификат внутренним центром, которого в подборке Mozilla
//! нет и не будет, а в системное хранилище его ставят централизованно. Поэтому
//! по умолчанию доверяем системе, а свой PEM-бандл задаётся настройкой.
//!
//! Отдельным модулем, а не копией в каждом клиенте: настройка доверия — ровно
//! то место, где два способа сделать одно и то же расходятся молча, и
//! расхождение это выглядит как «у одного работает, у другого нет».

use std::sync::Arc;
use std::time::Duration;

/// Собирает агента с нужным доверием и потолком ожидания.
///
/// Таймаут параметром: у реестра он короткий намеренно — по тому пути ходит
/// отрисовка таблицы, и недоступный реестр обязан обернуться строчкой, а не
/// подвисшим окном. Загрузке архива, наоборот, нужны минуты.
pub fn agent(ca_bundle: Option<&str>, timeout: Duration) -> Result<ureq::Agent, String> {
    let tls = match ca_bundle.filter(|p| !p.is_empty()) {
        Some(path) => ureq::tls::TlsConfig::builder().root_certs(read_roots(path)?),
        None => ureq::tls::TlsConfig::builder().root_certs(ureq::tls::RootCerts::PlatformVerifier),
    };

    let config = ureq::Agent::config_builder()
        .timeout_global(Some(timeout))
        // Код ответа — это ответ, а не транспортная ошибка: 404 от реестра
        // означает «такой схемы нет», 401 от гитлаба — «не тот токен», и
        // рассказать об этом надо своими словами.
        .http_status_as_error(false)
        .tls_config(tls.build())
        .build();
    Ok(config.into())
}

/// Читает PEM-бандл и оставляет из него сертификаты.
pub fn read_roots(path: &str) -> Result<ureq::tls::RootCerts, String> {
    let pem = std::fs::read(path).map_err(|e| format!("can't read CA bundle {path}: {e}"))?;
    let mut certs = Vec::new();
    for item in ureq::tls::parse_pem(&pem) {
        match item {
            Ok(ureq::tls::PemItem::Certificate(cert)) => certs.push(cert),
            // Приватный ключ в бандле корней — не наше дело, но и не ошибка:
            // такие файлы собирают конкатенацией и кладут в них лишнее.
            Ok(_) => {}
            Err(e) => return Err(format!("can't parse CA bundle {path}: {e}")),
        }
    }
    if certs.is_empty() {
        return Err(format!("{path} contains no certificates"));
    }
    Ok(ureq::tls::RootCerts::Specific(Arc::new(certs)))
}
