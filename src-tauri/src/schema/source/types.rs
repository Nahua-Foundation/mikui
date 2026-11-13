use serde::{Deserialize, Serialize};

/// Откуда взялось дерево источника.
///
/// Папка — это не ступенька к гитлабу, а самостоятельный случай: на машине с
/// чекаутами указать каталог быстрее, чем заводить токен. Общего у них всё,
/// кроме способа добыть файлы: дальше все — дерево в кэше.
///
/// GitLab и GitHub различаются тремя вещами (база API, адрес архива, заголовок
/// с токеном) и ничем больше — см. шапку `fetch`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SourceKind {
    Folder,
    Gitlab,
    Github,
}

impl SourceKind {
    /// Дерево приезжает по сети, а не лежит на диске: у такого источника есть
    /// ветка, токен и кнопка «забрать заново».
    pub fn is_remote(self) -> bool {
        matches!(self, Self::Gitlab | Self::Github)
    }
}

/// Источник наборов .proto. Ровно то, что лежит в `sources.json`.
///
/// Список глобальный, не на кластер: один контракт-репозиторий обслуживает и
/// dev, и prod, а кластер и так записан в привязке топика.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct SchemaSource {
    /// Случайный и неизменный: им назван каталог дерева и ключ токена, так что
    /// переименование источника не должно его менять.
    pub id: String,
    /// Как источник показан человеку. По умолчанию — последний сегмент пути.
    pub label: String,
    pub kind: SourceKind,
    /// Папка — путь на диске; GitLab — URL проекта целиком, как его копируют из
    /// адресной строки.
    pub location: String,
    /// Ветка или тег. Пусто — ветка проекта по умолчанию, её спрашивают у API.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub git_ref: Option<String>,
    /// Где внутри дерева искать КОНТРАКТЫ. Пусто — по всему дереву.
    ///
    /// Сужает только выбор, а не дерево: `pkg/` с общими типами обязан
    /// остаться видимым импортам, даже когда контракты ищут в `internal/`.
    /// Сперва это поле обрезало архив на стороне GitLab — и ломало резолв у
    /// всех контрактов, которые импортируют хоть что-то из-за своей папки.
    /// См. `tree::entries` и шапку `fetch`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subdir: Option<String>,
    /// Свой PEM-бандл: корпоративный GitLab стоит за тем же инспектором, что и
    /// реестр, но подписан может быть не тем же центром.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ca_bundle_path: Option<String>,
    /// Когда дерево забрали в последний раз. `None` — ещё ни разу.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fetched_at: Option<String>,
    /// Сколько .proto лежит в дереве. Хранится, а не считается на каждый показ:
    /// список источников открывают чаще, чем обновляют.
    #[serde(default)]
    pub files: usize,
    /// Токен лежит в системном хранилище, здесь — только признак его наличия.
    /// То же правило, что у паролей кластера: секрет не попадает ни в JSON, ни
    /// в IPC.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub has_token: bool,
}

/// Набор — контракт одного топика внутри источника.
///
/// Задаётся входным файлом: дерево лежит в кэше одно на весь репозиторий, а
/// набор — это пара «источник плюс входной файл». Копий файлов при этом не
/// возникает вовсе.
///
/// `Deserialize` — ради слепка (`digest`): это и есть то, что в нём хранится,
/// и заводить рядом второй такой же тип значило бы держать два описания одного.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct SetView {
    /// Входной файл — он же идентификатор набора внутри источника.
    pub entry: String,
    /// Единственный корневой тип. Им топик и будет декодироваться, без выбора
    /// руками. `None` — корней несколько или ни одного, и тогда тип выбирают
    /// сами либо подбирают по телам.
    pub root: Option<String>,
    /// Сколько типов объявлено — на случай, когда корня нет и выбирать всё-таки
    /// придётся.
    pub messages: usize,
    /// Набор не разбирается. Показываем всё равно: иначе человеку нечего
    /// чинить, а причина (незарезолвленный импорт из чужого репозитория) видна
    /// только в тексте.
    pub error: Option<String>,
}

/// Источник вместе с тем, что в нём нашлось.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct SourceView {
    #[serde(flatten)]
    pub source: SchemaSource,
    pub sets: Vec<SetView>,
    /// Дерева нет или оно не читается. Отдельно от ошибок наборов: это беда
    /// источника целиком, а не одного контракта в нём.
    pub error: Option<String>,
}

/// Привязка топика к набору из библиотеки — вместо своих копий файлов.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct SourceBinding {
    /// `SchemaSource::id`.
    pub source: String,
    /// Входной файл набора внутри дерева источника.
    pub entry: String,
}
