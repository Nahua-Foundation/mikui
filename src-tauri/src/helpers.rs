use crate::ClusterConnectPayload;
use rdkafka::ClientConfig;

/// Механизмы SASL, которые реально поддержаны текущей сборкой.
/// GSSAPI требует Cyrus SASL, который линкуется только с фичей `gssapi`
/// (см. комментарий в Cargo.toml), поэтому список зависит от сборки.
pub fn supported_sasl_mechanisms() -> Vec<&'static str> {
    let mut mechs = vec!["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512", "OAUTHBEARER"];
    if cfg!(feature = "gssapi") {
        mechs.push("GSSAPI");
    }
    mechs
}

pub fn get_cluster_config(conf: &ClusterConnectPayload) -> ClientConfig {
    let mut cc = ClientConfig::new();

    cc.set("bootstrap.servers", &conf.brokers);
    cc.set("client.id", "mikui");

    // group.id уникален на процесс: несколько пользователей mikui больше не
    // толкаются в одной группе. В Фазе 1 переходим на assign() с явными
    // офсетами, и группа уходит совсем — вместе с JoinGroup/heartbeat/rebalance.
    cc.set("group.id", format!("mikui-{}", std::process::id()));

    // --- Метаданные -------------------------------------------------------
    // Было 10_000: метаданные всего кластера перезапрашивались каждые 10 секунд.
    // На кластере с тысячами топиков это мегабайты трафика в минуту на простое.
    cc.set("metadata.max.age.ms", "300000");

    // --- Сеть -------------------------------------------------------------
    cc.set("socket.timeout.ms", "10000"); // fail fast: UI не должен висеть минуту
    cc.set("socket.nagle.disable", "true"); // мелкие запросы уходят сразу
    // Было 30_000: каждые 30 секунд простоя соединение рвалось, и следующий
    // запрос платил заново за TCP + TLS handshake. Держим чуть меньше
    // брокерского дефолта (600_000), чтобы закрывать первыми и без гонки.
    cc.set("connections.max.idle.ms", "540000");
    cc.set("reconnect.backoff.ms", "300");
    cc.set("reconnect.backoff.max.ms", "10000");

    // --- Fetch ------------------------------------------------------------
    // Пара (min.bytes=1, wait.max.ms=500) — дефолт librdkafka, и она лучше
    // прежней (min.bytes=1MB, wait.max.ms=50) сразу по двум осям:
    //   * быстрее: брокер отвечает немедленно, как только есть хоть какие-то
    //     данные, а не ждёт накопления мегабайта;
    //   * дешевле: на пустом топике опрос раз в 500 мс вместо 20 раз в секунду.
    cc.set("fetch.min.bytes", "1");
    cc.set("fetch.wait.max.ms", "500");
    // Было 50 МБ — слишком крупный разовый спайк памяти на ответ.
    cc.set("fetch.max.bytes", "10485760");

    // --- Буферы потребителя ----------------------------------------------
    // queued.min.messages считается НА ПАРТИЦИЮ. Прежняя 1000 на топике
    // со 100 партициями означала до 100k сообщений в локальной очереди.
    cc.set("queued.min.messages", "200");
    cc.set("queued.max.messages.kbytes", "16384");

    // --- Офсеты -----------------------------------------------------------
    cc.set("enable.auto.commit", "false");
    cc.set("enable.auto.offset.store", "false");
    // Просмотрщику незачем создавать топики побочным эффектом.
    cc.set("allow.auto.create.topics", "false");
    // TODO(Фаза 1): убрать вместе с group.id — офсеты задаём явным seek().
    cc.set("auto.offset.reset", "earliest");
    // Нужно, чтобы понимать «партиция вычитана до конца» и прекращать опрос,
    // а не крутиться вхолостую. Фаза 1 опирается на этот сигнал.
    cc.set("enable.partition.eof", "true");

    apply_security(&mut cc, conf);

    cc
}

fn apply_security(cc: &mut ClientConfig, conf: &ClusterConnectPayload) {
    // Механизм по умолчанию. PLAIN/SCRAM/OAUTHBEARER librdkafka умеет сама,
    // без Cyrus SASL.
    let mechanism = || -> String {
        conf.sasl_mechanism
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or("SCRAM-SHA-512")
            .to_string()
    };

    let ca_bundle = conf
        .ssl_ca_bundle_path
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());

    let credentials = |cc: &mut ClientConfig| {
        if let Some(user) = &conf.username {
            cc.set("sasl.username", user);
        }
        if let Some(pwd) = &conf.password {
            cc.set("sasl.password", pwd);
        }
    };

    match conf.security_protocol.trim().to_ascii_uppercase().as_str() {
        "SSL" => {
            cc.set("security.protocol", "ssl");
            // Раньше здесь всё было закомментировано, из-за чего режим SSL
            // молча не работал: CA-бандл из UI никуда не доезжал.
            if let Some(path) = ca_bundle {
                cc.set("ssl.ca.location", path);
            }
        }
        "SASL_SSL" => {
            cc.set("security.protocol", "sasl_ssl");
            cc.set("sasl.mechanism", mechanism());
            if let Some(path) = ca_bundle {
                cc.set("ssl.ca.location", path);
            }
            credentials(cc);
        }
        "SASL_PLAINTEXT" => {
            cc.set("security.protocol", "sasl_plaintext");
            cc.set("sasl.mechanism", mechanism());
            credentials(cc);
        }
        _ => {
            cc.set("security.protocol", "plaintext");
        }
    }
}
