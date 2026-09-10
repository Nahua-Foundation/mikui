use serde::{Deserialize, Serialize};

/// Версия схемы файла настроек. Пригодится, когда появятся миграции.
pub const SETTINGS_VERSION: u32 = 1;

/// Kafka-пользователь кластера.
///
/// Один кластер обычно смотрят из-под нескольких учёток с разными ACL, и
/// переключение между ними — рабочая операция, а не перенастройка подключения.
/// Поэтому логины живут списком, а не одним полем на кластер.
///
/// Пароля здесь нет: он лежит в системном хранилище секретов под ключом `id`
/// (см. `config::secrets`). `has_password` — флаг, а не пароль.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ClusterUser {
    pub id: String,
    pub username: String,
    #[serde(default)]
    pub has_password: bool,
}

/// Schema Registry кластера.
///
/// На кластере, а не на топике: реестр в кластере один, и вводить его заново
/// для каждого топика значило бы переписывать одно и то же по десять раз.
/// Топику остаётся выбор subject — см. `schema::types::AvroBinding`.
///
/// Пароля здесь, как и у учёток, нет: он лежит в системном хранилище секретов
/// под ключом `<id кластера>:sr`. `has_password` — признак, а не пароль.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct SchemaRegistry {
    pub url: String,
    /// `None` или пустая строка — реестр без авторизации.
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub has_password: bool,
    /// Свой CA-бандл. Отдельно от кластерного: реестр стоит за своим фронтом и
    /// вполне может быть подписан другим удостоверяющим центром.
    #[serde(default)]
    pub ssl_ca_bundle_path: Option<String>,
}

impl SchemaRegistry {
    /// Ключ пароля в системном хранилище.
    ///
    /// С суффиксом, потому что остальные ключи там — идентификаторы учёток
    /// (`ClusterUser::id`), и без него пароль реестра затёр бы пароль учётки,
    /// у которой id совпадает с id кластера. А такие есть: ровно такой id
    /// получают учётки, мигрированные из формата «одна пара на кластер».
    pub fn secret_key(cluster_id: &str) -> String {
        format!("{cluster_id}:sr")
    }
}

/// Сохранённое подключение.
///
/// Пароля здесь нет и быть не должно: он лежит в системном хранилище секретов
/// (см. `config::secrets`). В JSON на диске оседают только неопасные поля.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ClusterConfig {
    pub id: String,
    pub name: String,
    pub brokers: String,
    pub security_protocol: String,
    #[serde(default)]
    pub sasl_mechanism: Option<String>,
    #[serde(default)]
    pub ssl_ca_bundle_path: Option<String>,
    /// Клиентская пара для mTLS: сертификат и приватный ключ, оба PEM.
    /// Осмысленны только вместе — подключение с половиной пары отвергается
    /// (см. `helpers::apply_tls`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ssl_certificate_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ssl_key_path: Option<String>,
    /// Пароля ключа здесь, как и пароля учётки, нет: он лежит в системном
    /// хранилище секретов под ключом `<id кластера>:sslkey`.
    #[serde(default)]
    pub has_key_password: bool,
    /// Отказы от проверок TLS. Названы через «skip» намеренно: `#[serde(default)]`
    /// даёт `false`, то есть запись, сделанная прежней сборкой, читается как
    /// «проверять всё». Обратные по смыслу поля (`verify_*`) дали бы при том же
    /// умолчании молчаливое отключение проверок на всех старых записях.
    #[serde(default)]
    pub ssl_skip_hostname_check: bool,
    #[serde(default)]
    pub ssl_skip_certificate_verification: bool,
    pub created_at: String,
    #[serde(default)]
    pub last_used: Option<String>,
    /// `cluster.id`, которым кластер представился при последнем подключении.
    ///
    /// Единственное поле записи, которое одинаково у всех, кто подключён к тому
    /// же кластеру: `id` сгенерирован локально, `name` придуман человеком, а
    /// `brokers` у двух людей на один кластер сплошь и рядом разные. Поэтому
    /// ссылка на сообщение опознаёт кластер именно им (см. `crate::link`).
    ///
    /// `None` — к кластеру ещё ни разу не подключались: спросить идентификатор
    /// можно только у самого кластера. Ровно из-за этого случая ошибка
    /// «подключение не найдено» называет две причины, а не одну.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kafka_cluster_id: Option<String>,
    /// Известные учётки кластера. Пустой список допустим: PLAINTEXT-кластеру
    /// логин не нужен вовсе.
    #[serde(default)]
    pub users: Vec<ClusterUser>,
    /// Под кем подключались в прошлый раз. Именно эту учётку и предлагаем
    /// следующим подключением, иначе выбор пользователя не пережил бы перезапуск.
    #[serde(default)]
    pub active_user_id: Option<String>,
    /// Schema Registry кластера. `None` — не настроен, и тогда avro-топики
    /// читаются только по локальным .avsc.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_registry: Option<SchemaRegistry>,

    // --- Поля старого формата ------------------------------------------------
    // Читаются, но больше не пишутся: `skip_serializing` вычищает их из файла
    // при первом же сохранении. Одна пара логин/пароль на кластер — это то,
    // чем `users` и стал.
    #[serde(default, rename = "username", skip_serializing)]
    pub legacy_username: Option<String>,
    #[serde(default, rename = "has_password", skip_serializing)]
    pub legacy_has_password: bool,
}

impl ClusterConfig {
    /// Ключ пароля приватного ключа в системном хранилище.
    ///
    /// С суффиксом по той же причине, что и у реестра (`SchemaRegistry::secret_key`):
    /// остальные ключи там — идентификаторы учёток, а у мигрированной учётки
    /// идентификатор совпадает с идентификатором кластера. Без суффикса пароль
    /// ключа затёр бы её пароль.
    pub fn key_password_secret_key(cluster_id: &str) -> String {
        format!("{cluster_id}:sslkey")
    }

    /// Приводит запись к текущему формату.
    ///
    /// Идентификатор мигрированной учётки намеренно равен идентификатору
    /// кластера: ровно под этим ключом её пароль и лежит в keychain со времён
    /// «одна пара на кластер». Так миграция обходится без похода в хранилище
    /// секретов — которое на macOS на каждый запрос показывает диалог доступа.
    pub fn migrate(&mut self) {
        let legacy_username = self.legacy_username.take();
        let legacy_has_password = std::mem::take(&mut self.legacy_has_password);

        if self.users.is_empty() {
            if let Some(username) = legacy_username.filter(|u| !u.is_empty()) {
                self.users.push(ClusterUser {
                    id: self.id.clone(),
                    username,
                    has_password: legacy_has_password,
                });
            }
        }

        // Учётку могли удалить из-под нас в прошлой сессии — ссылка на неё
        // означала бы подключение без пароля вместо внятного выбора.
        let active_is_known = self
            .active_user_id
            .as_deref()
            .is_some_and(|id| self.users.iter().any(|u| u.id == id));
        if !active_is_known {
            self.active_user_id = self.users.first().map(|u| u.id.clone());
        }
    }

    pub fn user(&self, id: &str) -> Option<&ClusterUser> {
        self.users.iter().find(|u| u.id == id)
    }
}

/// Файл настроек приложения.
///
/// Собственных полей пока нет — файл заводится заранее, чтобы потом не
/// разбираться со случаем «файла не существовало». Всё, чего эта сборка не
/// знает, сохраняется в `extra` и записывается обратно нетронутым: иначе
/// старая версия молча затёрла бы настройки, добавленные новой.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct Settings {
    pub version: u32,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            version: SETTINGS_VERSION,
            extra: serde_json::Map::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(json: &str) -> ClusterConfig {
        let mut cfg: ClusterConfig = serde_json::from_str(json).unwrap();
        cfg.migrate();
        cfg
    }

    /// Записи, лежащие на дисках прямо сейчас, — старого формата. Потерять на
    /// них логин значит потерять и пароль: ключом в keychain служит id.
    #[test]
    fn legacy_single_login_becomes_the_first_user() {
        let cfg = parse(
            r#"{
                "id": "c1", "name": "prod", "brokers": "b:9092",
                "security_protocol": "SASL_SSL", "sasl_mechanism": "PLAIN",
                "username": "alice", "has_password": true,
                "created_at": "2026-01-01T00:00:00Z"
            }"#,
        );

        assert_eq!(cfg.users.len(), 1);
        assert_eq!(cfg.users[0].username, "alice");
        assert!(cfg.users[0].has_password);
        // Ключ в keychain остаётся прежним — иначе пароль осиротеет.
        assert_eq!(cfg.users[0].id, "c1");
        assert_eq!(cfg.active_user_id.as_deref(), Some("c1"));
    }

    #[test]
    fn legacy_fields_are_never_written_back() {
        let cfg = parse(
            r#"{
                "id": "c1", "name": "prod", "brokers": "b:9092",
                "security_protocol": "SASL_SSL",
                "username": "alice", "has_password": true,
                "created_at": "2026-01-01T00:00:00Z"
            }"#,
        );

        let json = serde_json::to_value(&cfg).unwrap();
        assert!(json.get("username").is_none());
        assert!(json.get("has_password").is_none());
        assert_eq!(json["users"][0]["username"], "alice");
    }

    /// Кластер без SASL логинов не имеет вовсе — и это не повод заводить
    /// пустого пользователя, под которым потом нечем подключаться.
    #[test]
    fn plaintext_cluster_stays_without_users() {
        let cfg = parse(
            r#"{
                "id": "c2", "name": "local", "brokers": "localhost:9092",
                "security_protocol": "PLAINTEXT",
                "created_at": "2026-01-01T00:00:00Z"
            }"#,
        );

        assert!(cfg.users.is_empty());
        assert!(cfg.active_user_id.is_none());
    }

    /// Запись, сделанная сборкой без полей TLS, обязана читаться как «проверять
    /// сертификат и имя хоста». Умолчание здесь — вопрос безопасности, а не
    /// удобства.
    #[test]
    fn a_record_without_tls_fields_keeps_every_check_on() {
        let cfg = parse(
            r#"{
                "id": "c4", "name": "prod", "brokers": "b:9093",
                "security_protocol": "SASL_SSL",
                "created_at": "2026-01-01T00:00:00Z"
            }"#,
        );

        assert!(!cfg.ssl_skip_hostname_check);
        assert!(!cfg.ssl_skip_certificate_verification);
        assert!(cfg.ssl_certificate_path.is_none());
        assert!(!cfg.has_key_password);
    }

    /// Три вида секретов кластера лежат в одном хранилище и различаются только
    /// ключом. Совпади любые два — один пароль затёр бы другой.
    #[test]
    fn secret_keys_of_one_cluster_never_collide() {
        let cluster = "c1";
        // Идентификатор мигрированной учётки равен идентификатору кластера —
        // именно поэтому у остальных секретов суффиксы.
        let user = cluster.to_string();
        let registry = SchemaRegistry::secret_key(cluster);
        let key = ClusterConfig::key_password_secret_key(cluster);

        assert_ne!(user, registry);
        assert_ne!(user, key);
        assert_ne!(registry, key);
    }

    #[test]
    fn active_user_pointing_at_a_deleted_login_falls_back_to_the_first() {
        let cfg = parse(
            r#"{
                "id": "c3", "name": "prod", "brokers": "b:9092",
                "security_protocol": "SASL_SSL",
                "created_at": "2026-01-01T00:00:00Z",
                "users": [{"id": "u1", "username": "alice", "has_password": true}],
                "active_user_id": "gone"
            }"#,
        );

        assert_eq!(cfg.active_user_id.as_deref(), Some("u1"));
    }
}
