//! Ссылка на сообщение: чем поделиться с коллегой, у которого тоже есть mikui.
//!
//! # Чем в ссылке назван кластер
//!
//! Идентификатором, который выдал сам кластер (`cluster.id` из метаданных), и
//! только им. Сохранённое подключение — запись на диске у КОНКРЕТНОГО человека:
//! его `id` сгенерирован локально, его `name` он придумал сам, а список
//! брокеров у двух людей на один и тот же кластер сплошь и рядом разный —
//! подмножество, DNS-алиас, VIP вместо перечисления. Ни одно из трёх не
//! опознаёт кластер на чужой машине.
//!
//! У этого выбора есть цена, и она заметная: получатель, который к кластеру ещё
//! ни разу не подключался, его идентификатора не знает — узнать его можно
//! только у самого кластера. Такая ссылка не откроется, даже когда подключение
//! у человека настроено. Поэтому текст ошибки называет оба случая сразу
//! (см. `store::find_by_cluster_id`), а сам идентификатор запоминается при
//! первом же успешном подключении.
//!
//! Взамен в ссылке не оказывается ни одного имени хоста: она уезжает в
//! мессенджер, а внутренние адреса — не то, что стоит рассылать ради удобства.
//!
//! # Устройство
//!
//! ```text
//! mikui://message/v1?cid=…&topic=…&p=3&o=182934&ts=…&name=…&fmt=…&type=…
//! ```
//!
//! Хост — вид ссылки, путь — её версия. Разделены затем, что следующим захотят
//! поделиться не сообщением, а видом («вот этот фильтр на этом диапазоне»), и
//! пусть это будет `mikui://view/v1`, а не второй разбор внутри первого.
//!
//! Параметры обычные, не base64. Ссылка на чтение — сама себе объяснение: по
//! ней видно, куда она ведёт, ещё до клика, и это единственное, чем в переписке
//! можно отличить свою ссылку от чужой.
//!
//! Незнакомые параметры разбор игнорирует, а обязательных ровно четыре
//! (`cid`, `topic`, `p`, `o`): всё остальное — подсказки для показа, и ссылка
//! без них обязана открываться.

use serde::{Deserialize, Serialize};
use url::Url;

use crate::config::ClusterConfig;

/// Схема, которую приложение регистрирует в системе.
pub const SCHEME: &str = "mikui";

/// Вид ссылки. Он же хост: `mikui://message/…`.
const KIND: &str = "message";

/// Версия формата. Она же путь. Меняется, только если у ссылки поменяется
/// СМЫСЛ поля, — добавление нового параметра версии не требует, потому что
/// незнакомые параметры разбор и так пропускает.
const VERSION: &str = "v1";

/// Куда ведёт ссылка. Разобранная, но ещё ни с чем не сопоставленная: связать
/// `cluster_id` с подключением на этой машине — дело `schema`-независимого
/// `config`, а не разбора текста.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct MessageLink {
    /// `cluster.id`, выданный самим кластером.
    pub cluster_id: String,
    /// Как кластер назывался У ОТПРАВИТЕЛЯ. Только чтобы было что показать в
    /// ошибке: искать подключение по этому имени нельзя — у получателя оно
    /// своё.
    pub cluster_name: Option<String>,
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    /// Время сообщения, unix millis. Не для поиска — офсеты в Kafka неизменны,
    /// искать по времени незачем, — а для ошибки: «сообщение от 3 сентября
    /// больше не в топике» объясняет retention, а «offset out of range» нет.
    pub timestamp: Option<i64>,
    /// Каким форматом отправитель читает этот топик. Схема по ссылке не едет и
    /// ехать не может: локальные .proto лежат у отправителя на диске. Зато с
    /// этой подсказкой получатель, у которого схемы нет, видит не двоичный
    /// мусор без объяснений, а то, чего именно ему не хватает.
    pub format: Option<String>,
    /// Имя типа внутри схемы: message у protobuf, запись или subject у
    /// остальных. Дополняет `format` — «нужен protobuf» без имени message
    /// подсказка только наполовину.
    pub type_name: Option<String>,
}

impl MessageLink {
    pub fn to_url(&self) -> String {
        let partition = self.partition.to_string();
        let offset = self.offset.to_string();
        let mut params: Vec<(&str, &str)> = vec![
            ("cid", &self.cluster_id),
            ("topic", &self.topic),
            ("p", &partition),
            ("o", &offset),
        ];

        // Пустые значения не пишем вовсе: ссылка и так не короткая, а `name=`
        // без имени ничего не сообщает никому.
        let timestamp = self.timestamp.map(|ts| ts.to_string());
        if let Some(ts) = timestamp.as_deref() {
            params.push(("ts", ts));
        }
        for (key, value) in [
            ("name", self.cluster_name.as_deref()),
            ("fmt", self.format.as_deref()),
            ("type", self.type_name.as_deref()),
        ] {
            if let Some(value) = value.filter(|v| !v.is_empty()) {
                params.push((key, value));
            }
        }

        // База собирается из тех же констант, по которым идёт разбор: разъехаться
        // сборке и разбору нельзя, а проверить это одним тестом на круговорот
        // можно только если константа одна.
        let base = format!("{SCHEME}://{KIND}/{VERSION}");
        Url::parse_with_params(&base, params)
            .expect("константная база и уже проверенные параметры")
            .to_string()
    }
}

/// Чем поделиться.
///
/// Одной структурой, а не россыпью аргументов, по той же причине, что и у
/// `SaveFavoriteRequest`: половина полей описывает, ГДЕ лежит сообщение, и
/// разъезжаться им нельзя.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ShareRequest {
    /// Ключ схем открытого топика — идентификатор сохранённого подключения либо
    /// имя подключения из формы. Нужен ТОЛЬКО ради подсказки о формате тела;
    /// сам кластер в ссылке назван идентификатором брокера. `None` — схем не
    /// смотрим вовсе.
    #[serde(default)]
    pub cluster: Option<String>,
    /// Как кластер назван у нас. Уезжает в ссылку подсказкой для получателя.
    #[serde(default)]
    pub cluster_name: Option<String>,
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    /// Unix millis. Неположительное — времени у сообщения нет (его не выставил
    /// ни продюсер, ни брокер), и в ссылку оно не поедет.
    pub timestamp: i64,
}

/// Куда ссылка ведёт НА ЭТОЙ машине.
///
/// Отличается от `MessageLink` ровно тем, ради чего и заведена: кластер здесь
/// назван идентификатором сохранённого подключения — тем самым локальным,
/// которым его знает всё остальное приложение. Никакой другой код про
/// `cluster.id` знать не обязан.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct Target {
    /// Идентификатор сохранённого подключения.
    pub cluster: String,
    /// Как оно называется ЗДЕСЬ. Имя отправителя показывать нельзя: у
    /// получателя тот же кластер вполне может называться иначе, и подтверждение
    /// «открыть в prod-fireg?» указывало бы не на ту строку в его списке.
    pub cluster_name: String,
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: Option<i64>,
    pub format: Option<String>,
    pub type_name: Option<String>,
}

/// Разбирает ссылку и находит подключение, к которому она относится.
///
/// Единственный признак — `cluster.id`. Ни по имени, ни по списку брокеров
/// подключение здесь не ищется, и это не упущение: имя локальное, а брокеров у
/// двух людей на один кластер разное количество (см. заголовок модуля).
pub fn resolve(text: &str, clusters: &[ClusterConfig]) -> Result<Target, String> {
    let link = parse(text)?;

    let found = clusters
        .iter()
        .find(|c| c.kafka_cluster_id.as_deref() == Some(link.cluster_id.as_str()));
    let Some(cluster) = found else {
        return Err(no_such_connection(&link));
    };

    Ok(Target {
        cluster: cluster.id.clone(),
        cluster_name: cluster.name.clone(),
        topic: link.topic,
        partition: link.partition,
        offset: link.offset,
        timestamp: link.timestamp,
        format: link.format,
        type_name: link.type_name,
    })
}

/// Обе причины названы намеренно.
///
/// Ссылка не открывается не только у того, кто кластер не настраивал: у того,
/// кто настроил, но ни разу не подключался, запись на диске идентификатора не
/// содержит — спросить его можно только у самого кластера. Со стороны эти два
/// случая неотличимы, а действия разные: одному надо завести подключение,
/// другому — просто подключиться. Не сказав этого, мы отправили бы половину
/// людей заводить второе подключение к кластеру, который у них уже есть.
fn no_such_connection(link: &MessageLink) -> String {
    let named = match &link.cluster_name {
        Some(name) => format!(" (the sender calls it '{name}')"),
        None => String::new(),
    };
    format!(
        "no connection here matches the cluster this link points to{named}. \
         Either it is not configured on this machine, or it is configured but \
         has never been connected to — mikui learns a cluster's id from the \
         cluster itself, on the first successful connection."
    )
}

/// Разбирает ссылку. Ошибки отсюда показываются человеку как есть — поэтому
/// они называют, что именно не так, а не «invalid link».
pub fn parse(text: &str) -> Result<MessageLink, String> {
    // Из мессенджера ссылку приносят с чем угодно вокруг: переносом строки,
    // пробелами, угловыми скобками, которые ставит почта. Отказывать из-за
    // этого значило бы отказывать по причине, которую человек не видит.
    let text = text.trim().trim_start_matches('<').trim_end_matches('>');
    if text.is_empty() {
        return Err("paste a mikui link first".into());
    }

    let url = Url::parse(text).map_err(|_| "this does not look like a mikui link".to_string())?;

    if url.scheme() != SCHEME {
        return Err(format!(
            "this link opens with '{}', not with mikui",
            url.scheme()
        ));
    }

    let kind = url.host_str().unwrap_or_default();
    if !kind.eq_ignore_ascii_case(KIND) {
        return Err(format!("mikui does not know links of kind '{kind}'"));
    }

    let version = url.path().trim_matches('/');
    if version.is_empty() {
        return Err("this mikui link is truncated: it has no version".into());
    }
    if !version.eq_ignore_ascii_case(VERSION) {
        // Именно «новее», а не «неизвестная»: единственный способ получить
        // здесь чужую версию — ссылка из сборки, которая ушла вперёд.
        return Err(format!(
            "this link is version '{version}'; update mikui to open it"
        ));
    }

    let mut cluster_id = None;
    let mut cluster_name = None;
    let mut topic = None;
    let mut partition = None;
    let mut offset = None;
    let mut timestamp = None;
    let mut format = None;
    let mut type_name = None;

    for (key, value) in url.query_pairs() {
        let value = value.into_owned();
        match key.as_ref() {
            "cid" => cluster_id = Some(value),
            "topic" => topic = Some(value),
            "p" => partition = Some(number::<i32>(&value, "partition")?),
            "o" => offset = Some(number::<i64>(&value, "offset")?),
            "ts" => timestamp = Some(number::<i64>(&value, "timestamp")?),
            "name" => cluster_name = Some(value),
            "fmt" => format = Some(value),
            "type" => type_name = Some(value),
            // Незнакомое — молча мимо: иначе ссылка, в которую следующая версия
            // допишет одно поле, перестанет открываться в этой.
            _ => {}
        }
    }

    let cluster_id = required(cluster_id, "cluster id")?;
    let topic = required(topic, "topic")?;
    let partition = required(partition, "partition")?;
    let offset = required(offset, "offset")?;

    // Отрицательные координаты не бывают ни у одного сообщения, а вот
    // `Offset::Offset(-1)` в librdkafka означает «с конца» — то есть ошибка,
    // пропущенная здесь, читала бы не то, на что ссылались.
    if partition < 0 {
        return Err(format!("the link points at partition {partition}"));
    }
    if offset < 0 {
        return Err(format!("the link points at offset {offset}"));
    }

    Ok(MessageLink {
        cluster_id,
        cluster_name: cluster_name.filter(|v| !v.is_empty()),
        topic,
        partition,
        offset,
        timestamp,
        format: format.filter(|v| !v.is_empty()),
        type_name: type_name.filter(|v| !v.is_empty()),
    })
}

fn required<T>(value: Option<T>, what: &str) -> Result<T, String> {
    value.ok_or_else(|| format!("the link is incomplete: no {what} in it"))
}

fn number<T: std::str::FromStr>(value: &str, what: &str) -> Result<T, String> {
    value
        .parse()
        .map_err(|_| format!("the link has a malformed {what}: '{value}'"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> MessageLink {
        MessageLink {
            cluster_id: "lkc-8f2a41".into(),
            cluster_name: Some("prod fireg".into()),
            topic: "fireg.securities".into(),
            partition: 3,
            offset: 182_934,
            timestamp: Some(1_757_012_345_678),
            format: Some("proto".into()),
            type_name: Some("fireg.Security".into()),
        }
    }

    /// Главный тест модуля: собранное разбирается обратно в то же самое. Всё
    /// остальное здесь — про то, как ведёт себя разбор с чужим вводом.
    #[test]
    fn a_built_link_parses_back_into_itself() {
        let link = sample();
        assert_eq!(parse(&link.to_url()).unwrap(), link);
    }

    /// Имя кластера человек пишет как хочет — с пробелами и кириллицей.
    #[test]
    fn cluster_name_survives_spaces_and_non_ascii() {
        let link = MessageLink {
            cluster_name: Some("прод / основной".into()),
            ..sample()
        };
        assert_eq!(parse(&link.to_url()).unwrap(), link);
    }

    /// Подсказок может не быть вовсе — ссылка обязана открываться.
    #[test]
    fn hints_are_optional() {
        let link = MessageLink {
            cluster_name: None,
            timestamp: None,
            format: None,
            type_name: None,
            ..sample()
        };
        let url = link.to_url();
        assert!(!url.contains("name="), "{url}");
        assert!(!url.contains("ts="), "{url}");
        assert_eq!(parse(&url).unwrap(), link);
    }

    /// Ссылку приносят из мессенджера вместе с обёрткой вокруг неё.
    #[test]
    fn surrounding_whitespace_and_brackets_are_not_an_error() {
        let url = sample().to_url();
        assert_eq!(parse(&format!("  <{url}>\n")).unwrap(), sample());
    }

    /// Незнакомый параметр — это ссылка из будущей версии, а не поломка.
    #[test]
    fn unknown_parameters_are_ignored() {
        let url = format!("{}&highlight=key", sample().to_url());
        assert_eq!(parse(&url).unwrap(), sample());
    }

    #[test]
    fn a_foreign_scheme_is_named_in_the_error() {
        let e = parse("https://example.com/whatever").unwrap_err();
        assert!(e.contains("https"), "{e}");
    }

    #[test]
    fn a_newer_version_asks_to_update() {
        let url = sample().to_url().replace("/v1", "/v2");
        let e = parse(&url).unwrap_err();
        assert!(e.contains("update mikui"), "{e}");
    }

    #[test]
    fn a_missing_offset_says_which_field_is_missing() {
        let url = "mikui://message/v1?cid=c&topic=t&p=0";
        let e = parse(url).unwrap_err();
        assert!(e.contains("offset"), "{e}");
    }

    #[test]
    fn a_malformed_number_shows_what_was_written() {
        let url = "mikui://message/v1?cid=c&topic=t&p=0&o=последнее";
        let e = parse(url).unwrap_err();
        assert!(e.contains("offset") && e.contains("последнее"), "{e}");
    }

    /// `-1` у librdkafka означает «с конца» — прочитать по такой ссылке НЕ ТО,
    /// на что ссылались, хуже, чем не прочитать ничего.
    #[test]
    fn negative_coordinates_are_rejected() {
        assert!(parse("mikui://message/v1?cid=c&topic=t&p=0&o=-1").is_err());
        assert!(parse("mikui://message/v1?cid=c&topic=t&p=-1&o=0").is_err());
    }

    /// Ссылку читают глазами в переписке — значит она должна читаться.
    ///
    /// Проверяем целиком, а не по кускам: единственный способ заметить, что
    /// сборка вдруг начала экранировать точки в имени топика или переставлять
    /// параметры местами.
    #[test]
    fn the_url_reads_the_way_it_is_meant_to() {
        assert_eq!(
            sample().to_url(),
            "mikui://message/v1\
             ?cid=lkc-8f2a41&topic=fireg.securities&p=3&o=182934\
             &ts=1757012345678&name=prod+fireg&fmt=proto&type=fireg.Security"
        );
    }

    #[test]
    fn a_link_without_a_version_says_so() {
        let e = parse("mikui://message").unwrap_err();
        assert!(e.contains("no version"), "{e}");
    }

    // --- Сопоставление с сохранёнными подключениями --------------------------

    fn cluster(id: &str, name: &str, kafka_cluster_id: Option<&str>) -> ClusterConfig {
        ClusterConfig {
            id: id.into(),
            name: name.into(),
            brokers: "b:9092".into(),
            security_protocol: "PLAINTEXT".into(),
            sasl_mechanism: None,
            ssl_ca_bundle_path: None,
            ssl_certificate_path: None,
            ssl_key_path: None,
            has_key_password: false,
            ssl_skip_hostname_check: false,
            ssl_skip_certificate_verification: false,
            created_at: "2026-01-01T00:00:00Z".into(),
            last_used: None,
            kafka_cluster_id: kafka_cluster_id.map(str::to_string),
            users: Vec::new(),
            active_user_id: None,
            schema_registry: None,
            legacy_username: None,
            legacy_has_password: false,
        }
    }

    /// Кластер опознаётся по идентификатору брокера, а НЕ по имени и не по
    /// порядку в списке: у получателя и то, и другое своё.
    #[test]
    fn the_connection_is_found_by_the_broker_assigned_id() {
        let clusters = [
            cluster("local-1", "staging", Some("other-cluster")),
            cluster("local-2", "боевой", Some("lkc-8f2a41")),
        ];
        let target = resolve(&sample().to_url(), &clusters).unwrap();

        assert_eq!(target.cluster, "local-2");
        // Имя — местное, а не то, под которым кластер знает отправитель.
        assert_eq!(target.cluster_name, "боевой");
        assert_eq!(target.topic, "fireg.securities");
        assert_eq!((target.partition, target.offset), (3, 182_934));
    }

    /// Подключение к этому кластеру есть, но к нему ни разу не подключались —
    /// идентификатора у записи нет, и ссылка не откроется. Ошибка обязана
    /// назвать и этот случай тоже: настраивать заново тут нечего.
    #[test]
    fn a_never_connected_cluster_is_reported_together_with_a_missing_one() {
        let clusters = [cluster("local-1", "боевой", None)];
        let e = resolve(&sample().to_url(), &clusters).unwrap_err();

        assert!(e.contains("not configured"), "{e}");
        assert!(e.contains("never been connected"), "{e}");
        // И имя отправителя — чтобы было понятно, о каком кластере речь.
        assert!(e.contains("prod fireg"), "{e}");
    }

    /// Испорченную ссылку разбор отвергает раньше, чем дело дойдёт до поиска
    /// подключения: иначе человек получил бы «нет такого кластера» на ссылку,
    /// в которой не хватает офсета.
    #[test]
    fn a_broken_link_fails_before_the_lookup() {
        let e = resolve("mikui://message/v1?cid=x&topic=t&p=0", &[]).unwrap_err();
        assert!(e.contains("offset"), "{e}");
    }
}
