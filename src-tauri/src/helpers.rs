use crate::ClusterConnectPayload;
use rdkafka::ClientConfig;

pub fn get_cluster_config(conf: &ClusterConnectPayload) -> ClientConfig {
    let mut cc = ClientConfig::new();

    cc.set("bootstrap.servers", &conf.brokers);
    cc.set("group.id", "rkui-consumer"); // todo: не подойдет для kaas

    // Оптимизации для быстрого переназначения партиций
    cc.set("socket.timeout.ms", "10000");
    cc.set("session.timeout.ms", "10000");
    cc.set("metadata.max.age.ms", "10000");
    cc.set("connections.max.idle.ms", "30000");

    // Оптимизации для управления офсетами
    cc.set("enable.auto.offset.store", "false"); // Отключаем автоматическое сохранение офсетов
    cc.set("auto.offset.reset", "earliest");
    cc.set("enable.auto.commit", "false"); // Отключаем автокоммит
    cc.set("enable.partition.eof", "false");

    // Оптимизации для быстрого получения данных
    cc.set("fetch.wait.max.ms", "50");
    cc.set("fetch.min.bytes", "1048576");
    cc.set("fetch.max.bytes", "52428800");

    // Оптимизации производительности
    cc.set("queued.min.messages", "1000"); // Буферизация сообщений
    cc.set("queued.max.messages.kbytes", "51200"); // Максимальный размер буфера (50MB)

    // Оптимизации для работы с брокером
    cc.set("reconnect.backoff.ms", "300"); // Уменьшаем время между попытками реконнекта
    cc.set("reconnect.backoff.max.ms", "10000"); // Максимальное время между попытками
    cc.set("allow.auto.create.topics", "false"); // Отключаем автосоздание топиков

    // Determine effective security type with backward compatibility
    match conf.security_protocol.as_ref() {
        "SSL" => {
            cc.set("security.protocol", "ssl");
            // if let Some(path) = &conf.ssl_ca_path {
            //     cc.set("ssl.ca.location", path);
            // }
            // if let Some(path) = &conf.ssl_cert_path {
            //     cc.set("ssl.certificate.location", path);
            // }
            // if let Some(path) = &conf.ssl_key_path {
            //     cc.set("ssl.key.location", path);
            // }
        }
        "SASL_SSL" => {
            let mech = conf
                .sasl_mechanism
                .as_deref()
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .unwrap_or("SCRAM-SHA-512");
            cc.set("sasl.mechanism", mech);
            cc.set("security.protocol", "SASL_SSL");
            if let Some(ssl_ca_bundle_path) = &conf.ssl_ca_bundle_path {
                cc.set("ssl.ca.location", ssl_ca_bundle_path);
            }
            if let Some(user) = &conf.username {
                cc.set("sasl.username", user);
            }
            if let Some(pwd) = &conf.password {
                cc.set("sasl.password", pwd);
            }
        }
        "SASL_PLAINTEXT" => {
            cc.set("security.protocol", "sasl_plaintext");
            // Use provided mechanism or default to SCRAM-SHA-512
            let mech = conf
                .sasl_mechanism
                .as_deref()
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .unwrap_or("SCRAM-SHA-512");
            cc.set("sasl.mechanism", mech);
            if let Some(user) = &conf.username {
                cc.set("sasl.username", user);
            }
            if let Some(pwd) = &conf.password {
                cc.set("sasl.password", pwd);
            }
            // Prefer explicit username/password parsed from JAAS config string
            // if let Some(jaas) = &conf.sasl_jaas_config {
            //     if let Some((user, pass)) = parse_username_password_from_jaas(jaas) {
            //         cc.set("sasl.username", &user);
            //         cc.set("sasl.password", &pass);
            //     }
            // }
        }
        _ => {
            // plaintext (default): no extra settings
        }
    }

    cc
}
