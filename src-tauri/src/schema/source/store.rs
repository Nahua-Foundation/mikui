//! Источники на диске: `sources.json` и деревья в кэше.
//!
//! Правило то же, что и у схем топиков: изменение применяется, только если
//! после него дерево на месте и читается. Каталог собирается рядом и
//! подменяется целиком (`files::Staged`), запись в `sources.json` идёт
//! последней — неудавшаяся загрузка не должна испортить то, что работало.
//!
//! Про Tauri здесь не знают: каталог настроек приходит аргументом, как и во
//! всём `schema`, — ровно затем, чтобы модуль прогонялся на временном каталоге.

use std::path::{Path, PathBuf};

use super::super::files::{Pending, Staged};
use super::super::proto::linked;
use super::digest;
use super::fetch as remote;
use super::tree;
use super::types::{SchemaSource, SetView, SourceKind, SourceView};
use crate::config;

const SOURCES_FILE: &str = "sources.json";
const SOURCES_DIR: &str = "proto-src";

/// Сколько .proto готовы принять из одного источника.
///
/// Репозиторий контрактов — это десятки файлов; тысячи означают, что указали не
/// туда (монорепозиторий целиком, каталог сборки). Обрезать молча нельзя, и
/// `ingest` про превышение говорит текстом.
pub(super) const MAX_FILES: usize = 2000;

/// И сколько байт. Защита не от злого умысла, а от опечатки в подкаталоге.
pub(super) const MAX_BYTES: usize = 32 * 1024 * 1024;

// --- Записи -----------------------------------------------------------------

pub fn list(root: &Path) -> Result<Vec<SchemaSource>, String> {
    config::read_json(&root.join(SOURCES_FILE))
}

fn save(root: &Path, sources: &[SchemaSource]) -> Result<(), String> {
    let json = serde_json::to_vec_pretty(sources)
        .map_err(|e| format!("can't serialize schema sources: {e}"))?;
    config::write_atomic(&root.join(SOURCES_FILE), &json)
}

/// Каталог дерева источника.
pub fn dir_of(root: &Path, id: &str) -> PathBuf {
    root.join(SOURCES_DIR).join(id)
}

pub fn find(sources: &[SchemaSource], id: &str) -> Option<usize> {
    sources.iter().position(|s| s.id == id)
}

// --- Дерево -------------------------------------------------------------------

/// Кладёт набор файлов в каталог источника.
///
/// Файлы приезжают уже в памяти — и из папки, и из архива, — потому что
/// проверить их число и объём надо ДО того, как что-то окажется на диске.
pub fn ingest(root: &Path, id: &str, files: Vec<(String, Vec<u8>)>) -> Result<usize, String> {
    if files.is_empty() {
        return Err("no .proto files in this source".to_string());
    }
    if files.len() > MAX_FILES {
        return Err(format!(
            "this source has {} .proto files, more than the {MAX_FILES} this app will take — \
             it looks like a monorepo rather than a contract repository",
            files.len()
        ));
    }
    let bytes: usize = files.iter().map(|(_, b)| b.len()).sum();
    if bytes > MAX_BYTES {
        return Err(format!(
            "this source weighs {} MiB of .proto, more than the {} MiB this app will take",
            bytes / 1024 / 1024,
            MAX_BYTES / 1024 / 1024
        ));
    }

    let count = files.len();
    let pending: Vec<Pending> = files
        .into_iter()
        .map(|(name, bytes)| Pending::picked(name, String::new(), bytes))
        .collect();

    let target = dir_of(root, id);
    // Разбор прошлого дерева относится к прежнему содержимому и с этого момента
    // врёт — по всем наборам сразу, потому что каталог у них общий.
    let staged = Staged::write(target.clone(), &pending)?;
    linked::invalidate(&target);
    staged.commit()?;
    Ok(count)
}

/// Читает .proto из папки на диске — источник вида `folder`.
///
/// Копия, а не ссылка на исходный каталог: схема обязана пережить и перезапуск
/// приложения, и переезд каталога, из которого её взяли. Ровно то же правило,
/// что у файлов, выбранных руками (`schema::files`).
pub fn read_folder(path: &Path) -> Result<Vec<(String, Vec<u8>)>, String> {
    if !path.is_dir() {
        return Err(format!("{} is not a directory", path.display()));
    }
    let names = tree::proto_files(path);
    if names.is_empty() {
        return Err(format!("no .proto files under {}", path.display()));
    }

    let mut files = Vec::with_capacity(names.len());
    for name in names {
        let bytes = std::fs::read(path.join(&name))
            .map_err(|e| format!("can't read {name}: {e}"))?;
        files.push((name, bytes));
    }
    Ok(files)
}

// --- Операции -----------------------------------------------------------------

/// Заводит источник и сразу забирает его дерево.
///
/// Запись сохраняется ПОСЛЕ того, как дерево легло: источник, у которого нечего
/// читать, в списке не нужен — он выглядел бы настроенным и молчал.
pub fn add(root: &Path, mut source: SchemaSource, files: Vec<(String, Vec<u8>)>) -> Result<SchemaSource, String> {
    let mut sources = list(root)?;
    if sources.iter().any(|s| s.id == source.id) {
        return Err(format!("source {} already exists", source.id));
    }

    source.files = ingest(root, &source.id, files)?;
    source.fetched_at = Some(chrono::Utc::now().to_rfc3339());
    sources.push(source.clone());
    save(root, &sources)?;
    Ok(source)
}

/// Заменяет дерево источника новым.
pub fn replace_tree(root: &Path, id: &str, files: Vec<(String, Vec<u8>)>) -> Result<SchemaSource, String> {
    let mut sources = list(root)?;
    let index = find(&sources, id).ok_or_else(|| format!("no schema source {id}"))?;

    sources[index].files = ingest(root, id, files)?;
    sources[index].fetched_at = Some(chrono::Utc::now().to_rfc3339());
    save(root, &sources)?;
    Ok(sources[index].clone())
}

/// Убирает источник вместе с деревом.
///
/// Привязки топиков к его наборам не трогаются: топик не перестаёт быть
/// протобафным оттого, что источник убрали, и затирать его настройки — значит
/// решать за человека. Такая привязка покажется ошибкой «источника больше нет»,
/// и это честнее, чем молча сбросить формат.
pub fn delete(root: &Path, id: &str) -> Result<(), String> {
    let mut sources = list(root)?;
    let Some(index) = find(&sources, id) else {
        return Ok(());
    };
    let dir = dir_of(root, id);
    linked::invalidate(&dir);
    let _ = std::fs::remove_dir_all(&dir);
    sources.remove(index);
    save(root, &sources)
}

/// Переименование и смена настроек, без похода за деревом.
pub fn update(root: &Path, id: &str, edit: impl FnOnce(&mut SchemaSource)) -> Result<SchemaSource, String> {
    let mut sources = list(root)?;
    let index = find(&sources, id).ok_or_else(|| format!("no schema source {id}"))?;
    edit(&mut sources[index]);
    save(root, &sources)?;
    Ok(sources[index].clone())
}

// --- Чтение -------------------------------------------------------------------

/// Каталоги включения и входные файлы набора — всё, что нужно разбору.
///
/// Каталогов несколько, потому что раскладки репозиториев разные; какая именно
/// — заранее неизвестно (см. `tree::include_roots`).
pub fn set_inputs(root: &Path, id: &str, entry: &str) -> (Vec<PathBuf>, Vec<String>) {
    let dir = dir_of(root, id);
    (tree::include_roots(&dir), vec![entry.to_string()])
}

/// Источники вместе с наборами — содержимое окна библиотеки.
///
/// Файлы дерева здесь не читаются вовсе, пока дерево не менялось: во что оно
/// делится и что в наборах объявлено, посчитано один раз и лежит слепком
/// (`digest`). На дерево приходится ровно одна проверка — на месте ли оно.
pub fn views(root: &Path) -> Result<Vec<SourceView>, String> {
    let sources = list(root)?;
    let mut digest = digest::read(root);
    // Переписывать файл стоит только когда он и правда разошёлся с записями:
    // показ библиотеки — чтение, и превращать его в запись на каждый взгляд
    // незачем.
    let mut stale = false;

    let mut views = Vec::with_capacity(sources.len());
    for source in sources {
        let dir = dir_of(root, &source.id);
        if !dir.is_dir() {
            // Слепок не трогаем: дерево вернётся перезабором, а тот сменит
            // метку и пересчитает всё сам.
            views.push(SourceView {
                source,
                sets: Vec::new(),
                error: Some("the files of this source are gone — refresh it".to_string()),
            });
            continue;
        }

        let stamp = digest::Stamp::of(&source);
        let entry = match digest.get(&source.id) {
            Some(known) if known.stamp == stamp => known.clone(),
            _ => {
                let fresh = scan(root, &source, &dir, stamp);
                digest.insert(source.id.clone(), fresh.clone());
                stale = true;
                fresh
            }
        };

        views.push(SourceView {
            source,
            sets: entry.sets,
            error: entry.error,
        });
    }

    // Слепки убранных источников держать незачем, и `delete` их не чистит
    // нарочно: он сносит каталог и запись, а слепок — производное, и место ему
    // отваливаться там же, где оно пересчитывается.
    let before = digest.len();
    digest.retain(|id, _| views.iter().any(|view| view.source.id == *id));
    stale |= digest.len() != before;

    if stale {
        digest::write(root, &digest);
    }
    Ok(views)
}

/// Делит дерево на наборы и разбирает каждый — то, ради чего и заведён слепок.
///
/// Единственное место, которое читает .proto источника, и зовётся оно только на
/// изменившемся дереве.
fn scan(root: &Path, source: &SchemaSource, dir: &Path, stamp: digest::Stamp) -> digest::Entry {
    let files = tree::proto_files(dir);
    let scope = source.subdir.as_deref();
    let entries = tree::entries(dir, &files, scope);
    // Три разных беды, и советы у них разные: пустое дерево чинят
    // источником, пустая область — подкаталогом, а круг импортов — самими
    // файлами. Один текст на все три отправлял бы чинить не туда.
    let error = entries.is_empty().then(|| match (files.is_empty(), scope) {
        (true, _) => "no .proto files in this source".to_string(),
        (false, Some(scope)) if !scope.trim().is_empty() => format!(
            "none of the {} .proto files here is a contract under `{scope}` — \
             check the subdirectory, or clear it to search the whole tree",
            files.len()
        ),
        // Входных нет, а файлы есть — значит они импортируют друг друга по
        // кругу. Сказать об этом надо: список наборов иначе просто пуст.
        (false, _) => format!(
            "all {} .proto files here import one another — none of them is a contract of its own",
            files.len()
        ),
    });

    let sets = entries
        .into_iter()
        .map(|entry| describe_set(root, &source.id, entry))
        .collect();
    digest::Entry { stamp, sets, error }
}

fn describe_set(root: &Path, id: &str, entry: String) -> SetView {
    let (include, inputs) = set_inputs(root, id, &entry);
    // Мимо кэша разбора (`parse_set`, а не `linked_set`) намеренно. Граф здесь
    // нужен на одну строчку вывода и больше никогда, а кэш держит восемь
    // последних и общий с декодером открытого топика: сложи сюда девяносто
    // шесть наборов — и схема, которой таблица прямо сейчас читает тела,
    // вылетит из него вся. Набор, к которому топик привяжут, попадёт туда сам,
    // когда им начнут декодировать.
    match linked::parse_set(&include, &inputs) {
        Ok(linked) => SetView {
            entry,
            root: linked.root.clone(),
            messages: linked.messages.len(),
            error: None,
        },
        Err(e) => SetView {
            entry,
            root: None,
            messages: 0,
            error: Some(e),
        },
    }
}

/// Забирает дерево источника тем способом, каким он заведён.
///
/// Одна точка входа на оба вида: дальше — `ingest`, и ему всё равно, пришли
/// файлы из папки или из архива.
pub fn fetch(source: &SchemaSource, token: Option<&str>) -> Result<Vec<(String, Vec<u8>)>, String> {
    match source.kind {
        SourceKind::Folder => read_folder(Path::new(&source.location)),
        // Подкаталог не передаётся: он сужает поиск контрактов, а не дерево.
        // См. шапку `fetch`.
        kind => remote::repo(
            kind,
            &source.location,
            source.git_ref.as_deref(),
            token,
            source.ca_bundle_path.as_deref(),
        ),
    }
}

/// Метка удалённого источника — «группа/репозиторий» из пути проекта.
///
/// Одного имени репозитория не хватает, и это не придирка: `contracts` лежит и
/// в `invest-execution`, и в `invest-market-data`, и в списке источников две
/// такие записи не отличить ничем. Группа — ровно то, что их различает, и то,
/// как репозиторий называют в разговоре. У GitHub та же пара и есть весь путь
/// целиком (`owner/repo`), так что правило совпадает само собой.
///
/// Берутся ДВА последних сегмента, а не весь путь: у вложенных подгрупп GitLab
/// он длиной в четыре-пять имён, а различает соседей ближайшая. Полный путь при
/// этом никуда не девается — он виден в строке источника как `location`.
///
/// Разбор URL тут не свой, а общий (`fetch::split_project`): своя разборка
/// здесь и была источником беды — она брала последний сегмент как есть, и
/// ссылка на ветку (`…/contracts/-/tree/master?ref_type=heads`) давала источник
/// по имени `master?ref_type=heads`.
pub fn repo_label(url: &str, kind: SourceKind) -> String {
    let Ok((_, path)) = remote::split_project(url, kind) else {
        // URL не разобрался — дерево за ним всё равно не приедет, и запись не
        // сохранится. Метка тут нужна лишь затем, чтобы не паниковать.
        return url.trim().to_string();
    };
    let parts: Vec<&str> = path.split('/').filter(|part| !part.is_empty()).collect();
    match parts.as_slice() {
        [] => path,
        [only] => (*only).to_string(),
        [.., group, project] => format!("{group}/{project}"),
    }
}

/// Готовая запись удалённого источника — GitLab или GitHub.
pub fn repo_source(
    id: String,
    kind: SourceKind,
    url: &str,
    git_ref: Option<String>,
    subdir: Option<String>,
    ca_bundle_path: Option<String>,
    has_token: bool,
) -> SchemaSource {
    SchemaSource {
        id,
        label: repo_label(url, kind),
        kind,
        location: url.trim().to_string(),
        git_ref: git_ref.filter(|r| !r.trim().is_empty()),
        subdir: subdir.filter(|s| !s.trim().is_empty()),
        ca_bundle_path: ca_bundle_path.filter(|p| !p.trim().is_empty()),
        fetched_at: None,
        files: 0,
        has_token,
    }
}

/// Готовая запись источника-папки.
pub fn folder_source(id: String, path: &Path) -> SchemaSource {
    SchemaSource {
        label: path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| path.display().to_string()),
        id,
        kind: SourceKind::Folder,
        location: path.display().to_string(),
        git_ref: None,
        subdir: None,
        ca_bundle_path: None,
        fetched_at: None,
        files: 0,
        has_token: false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Sandbox {
        root: PathBuf,
        outside: PathBuf,
    }

    impl Sandbox {
        fn new(name: &str) -> Self {
            let base = std::env::temp_dir().join(format!("mikui-source-store-{name}"));
            let _ = std::fs::remove_dir_all(&base);
            let sandbox = Self {
                root: base.join("config"),
                outside: base.join("repo"),
            };
            std::fs::create_dir_all(&sandbox.root).unwrap();
            std::fs::create_dir_all(&sandbox.outside).unwrap();
            sandbox
        }

        fn author(&self, relative: &str, text: &str) {
            let path = self.outside.join(relative);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, text).unwrap();
        }

        fn add_folder(&self, id: &str) -> Result<SchemaSource, String> {
            let files = read_folder(&self.outside)?;
            add(
                &self.root,
                folder_source(id.to_string(), &self.outside),
                files,
            )
        }

        /// То же, но с областью поиска контрактов. Запись собирается как у
        /// GitLab только ради поля `subdir` — сеть здесь ни при чём, а дерево
        /// всё равно читается с диска.
        fn add_scoped(&self, id: &str, scope: &str) -> Result<SchemaSource, String> {
            let files = read_folder(&self.outside)?;
            let mut source = folder_source(id.to_string(), &self.outside);
            source.subdir = Some(scope.to_string());
            add(&self.root, source, files)
        }
    }

    impl Drop for Sandbox {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(self.root.parent().unwrap());
        }
    }

    const ORDERS: &str = r#"
        syntax = "proto3";
        package orders;
        import "common/types.proto";
        message OrderChanged { common.Money total = 1; Line line = 2; }
        message Line { string sku = 1; }
    "#;

    const WALLETS: &str = r#"
        syntax = "proto3";
        package wallets;
        import "common/types.proto";
        message WalletChanged { common.Money balance = 1; }
    "#;

    const COMMON: &str = r#"
        syntax = "proto3";
        package common;
        message Money { int64 amount = 1; }
    "#;

    /// Контракт в области, импортирующий общие типы ИЗ-ЗА неё — та самая
    /// раскладка, на которой подкаталог и сломался.
    const SCOPED_ORDERS: &str = r#"
        syntax = "proto3";
        package orders;
        import "pkg/common/types.proto";
        message OrderChanged { common.Money total = 1; Line line = 2; }
        message Line { string sku = 1; }
    "#;

    /// Копия для аналитиков: лежит вне области и контрактом считаться не должна.
    const DOCS_COPY: &str = r#"
        syntax = "proto3";
        package docs;
        message OrderChangedCopy { int64 amount = 1; }
    "#;

    /// Главный случай: репозиторий с двумя контрактами разворачивается в два
    /// набора, у каждого свой корень, а общий тип в наборы не попадает.
    #[test]
    fn a_folder_source_expands_into_one_set_per_contract() {
        let sandbox = Sandbox::new("expand");
        sandbox.author("orders.proto", ORDERS);
        sandbox.author("wallets.proto", WALLETS);
        sandbox.author("common/types.proto", COMMON);

        let source = sandbox.add_folder("s1").unwrap();
        assert_eq!(source.files, 3);
        assert!(source.fetched_at.is_some());

        let views = views(&sandbox.root).unwrap();
        assert_eq!(views.len(), 1);
        assert_eq!(views[0].error, None);

        let sets: Vec<(&str, Option<&str>)> = views[0]
            .sets
            .iter()
            .map(|s| (s.entry.as_str(), s.root.as_deref()))
            .collect();
        assert_eq!(
            sets,
            [
                ("orders.proto", Some("orders.OrderChanged")),
                ("wallets.proto", Some("wallets.WalletChanged")),
            ]
        );
    }

    /// Метка источника — «группа/репозиторий», и это не косметика: `contracts`
    /// лежит и в `invest-execution`, и в `invest-market-data`, а одним именем
    /// репозитория две такие записи не отличить ничем.
    #[test]
    fn the_label_carries_the_group_not_just_the_repository() {
        for (url, expected) in [
            (
                "https://gitlab.example.com/invest-execution/contracts",
                "invest-execution/contracts",
            ),
            (
                "https://gitlab.example.com/invest-market-data/contracts",
                "invest-market-data/contracts",
            ),
            ("https://gitlab.example.com/dealer/internalizer", "dealer/internalizer"),
            // Вложенные подгруппы: весь путь в метку не влезает и не нужен —
            // различает соседей ближайшая группа. Полный путь виден в
            // `location`.
            (
                "https://gitlab.example.com/invest/core/trading/orders",
                "trading/orders",
            ),
            // Проект в корне инсталляции — группы нет, и выдумывать её нечего.
            ("https://gitlab.example.com/standalone", "standalone"),
        ] {
            assert_eq!(repo_label(url, SourceKind::Gitlab), expected, "{url}");
        }
    }

    /// Та самая ссылка, которую копируют из адресной строки. Прежнее правило
    /// («последний сегмент как есть») давало из неё источник по имени
    /// `master?ref_type=heads`.
    #[test]
    fn a_link_copied_from_the_browser_still_names_the_project() {
        assert_eq!(
            repo_label("https://gitlab.example.com/invest-execution/contracts/-/tree/master?ref_type=heads", SourceKind::Gitlab),
            "invest-execution/contracts"
        );
        assert_eq!(
            repo_label("https://gitlab.example.com/dealer/internalizer.git", SourceKind::Gitlab),
            "dealer/internalizer"
        );
    }

    /// Область поиска контрактов сужает СПИСОК, а дерево остаётся целым: набор
    /// из области по-прежнему разбирается импортом из-за её пределов.
    ///
    /// Это главный случай всей затеи с областью, и он куплен разбирательством
    /// на живом репозитории: подкаталог обрезал архив на стороне GitLab,
    /// общие типы из соседней папки не приезжали, и восемнадцать наборов из
    /// девятнадцати не разбирались с «not found in import path».
    #[test]
    fn a_scope_narrows_the_contracts_but_imports_still_reach_outside_it() {
        let sandbox = Sandbox::new("scope");
        sandbox.author("internal/orders.proto", SCOPED_ORDERS);
        sandbox.author("pkg/common/types.proto", COMMON);
        sandbox.author("docs/analysts/orders.proto", DOCS_COPY);

        let source = sandbox.add_scoped("s1", "internal").unwrap();
        // Дерево приехало целиком — иначе импорт резолвить не от чего.
        assert_eq!(source.files, 3);

        let views = views(&sandbox.root).unwrap();
        assert_eq!(views[0].error, None);
        let sets: Vec<(&str, Option<&str>, Option<&str>)> = views[0]
            .sets
            .iter()
            .map(|s| (s.entry.as_str(), s.root.as_deref(), s.error.as_deref()))
            .collect();
        assert_eq!(
            sets,
            [("internal/orders.proto", Some("orders.OrderChanged"), None)],
            "контракт один — и он РАЗОБРАЛСЯ, хотя импортирует из-за области"
        );
    }

    /// Область указана с опечаткой — наборов нет, и сказать надо именно про
    /// область. Текст про «все файлы импортируют друг друга» отправил бы чинить
    /// не туда.
    #[test]
    fn a_scope_that_matches_nothing_says_so_about_the_scope() {
        let sandbox = Sandbox::new("scope-typo");
        sandbox.author("internal/orders.proto", SCOPED_ORDERS);
        sandbox.author("pkg/common/types.proto", COMMON);
        sandbox.add_scoped("s1", "intrenal").unwrap();

        let views = views(&sandbox.root).unwrap();
        let error = views[0].error.as_deref().unwrap();
        assert!(error.contains("intrenal"), "{error}");
        assert!(error.contains("subdirectory"), "{error}");
        assert!(views[0].sets.is_empty());
    }

    /// Два набора одного источника делят каталог — и обязаны разбираться
    /// независимо. Ловушка кэша: ключ из одного лишь каталога отдал бы второму
    /// набору разбор первого.
    #[test]
    fn two_sets_of_one_source_do_not_share_a_parse() {
        let sandbox = Sandbox::new("cache");
        sandbox.author("orders.proto", ORDERS);
        sandbox.author("wallets.proto", WALLETS);
        sandbox.author("common/types.proto", COMMON);
        sandbox.add_folder("s1").unwrap();

        let (include, inputs) = set_inputs(&sandbox.root, "s1", "orders.proto");
        let orders = linked::linked_set(&include, &inputs).unwrap();
        let (include, inputs) = set_inputs(&sandbox.root, "s1", "wallets.proto");
        let wallets = linked::linked_set(&include, &inputs).unwrap();

        assert_eq!(orders.root.as_deref(), Some("orders.OrderChanged"));
        assert_eq!(wallets.root.as_deref(), Some("wallets.WalletChanged"));
        assert_eq!(orders.messages, ["orders.Line", "orders.OrderChanged"]);
        assert_eq!(wallets.messages, ["wallets.WalletChanged"]);
    }

    /// Раскладка Maven: импорт отсчитан от `src/main/proto` соседнего модуля.
    #[test]
    fn a_maven_layout_resolves_across_modules() {
        let sandbox = Sandbox::new("maven");
        sandbox.author(
            "common/src/main/proto/acme/common/v1/types.proto",
            "syntax = \"proto3\"; package acme.common.v1; message Money { int64 amount = 1; }",
        );
        sandbox.author(
            "orders/src/main/proto/acme/orders/v1/events.proto",
            r#"
                syntax = "proto3";
                package acme.orders.v1;
                import "acme/common/v1/types.proto";
                message OrderChanged { acme.common.v1.Money total = 1; }
            "#,
        );

        sandbox.add_folder("s1").unwrap();
        let views = views(&sandbox.root).unwrap();
        assert_eq!(views[0].sets.len(), 1);
        assert_eq!(views[0].sets[0].error, None, "{:?}", views[0].sets[0].error);
        assert_eq!(
            views[0].sets[0].root.as_deref(),
            Some("acme.orders.v1.OrderChanged")
        );
    }

    /// Незарезолвленный импорт — беда одного набора, а не всего источника:
    /// соседние контракты обязаны остаться на месте.
    #[test]
    fn a_broken_set_does_not_take_the_others_with_it() {
        let sandbox = Sandbox::new("broken");
        sandbox.author("orders.proto", ORDERS);
        sandbox.author("common/types.proto", COMMON);
        sandbox.author(
            "broken.proto",
            "syntax = \"proto3\"; import \"nowhere/absent.proto\"; message B {}",
        );

        sandbox.add_folder("s1").unwrap();
        let views = views(&sandbox.root).unwrap();
        let sets = &views[0].sets;
        assert_eq!(sets.len(), 2);

        let broken = sets.iter().find(|s| s.entry == "broken.proto").unwrap();
        assert!(broken.error.is_some());
        let orders = sets.iter().find(|s| s.entry == "orders.proto").unwrap();
        assert_eq!(orders.error, None);
        assert_eq!(orders.root.as_deref(), Some("orders.OrderChanged"));
    }

    /// Перезагрузка дерева заменяет его целиком и сбрасывает разбор: иначе
    /// обновлённый контракт показывался бы старым.
    #[test]
    fn replacing_the_tree_is_seen_by_the_next_parse() {
        let sandbox = Sandbox::new("refresh");
        sandbox.author("orders.proto", "syntax = \"proto3\"; package o; message Before {}");
        sandbox.add_folder("s1").unwrap();

        let (include, inputs) = set_inputs(&sandbox.root, "s1", "orders.proto");
        assert_eq!(
            linked::linked_set(&include, &inputs).unwrap().messages,
            ["o.Before"]
        );

        sandbox.author("orders.proto", "syntax = \"proto3\"; package o; message After {}");
        let files = read_folder(&sandbox.outside).unwrap();
        replace_tree(&sandbox.root, "s1", files).unwrap();

        assert_eq!(
            linked::linked_set(&include, &inputs).unwrap().messages,
            ["o.After"]
        );
    }

    /// Папка без .proto — это не источник, и заводить запись о ней незачем.
    #[test]
    fn a_folder_without_protos_is_refused() {
        let sandbox = Sandbox::new("empty");
        sandbox.author("README.md", "nothing here");
        assert!(sandbox.add_folder("s1").is_err());
        assert!(list(&sandbox.root).unwrap().is_empty());
    }

    /// Главное свойство слепка: на неизменившемся дереве файлы НЕ читаются.
    ///
    /// Проверяется прямым способом — после первого показа .proto из каталога
    /// убираются, а сам каталог остаётся. Ответ обязан не измениться ни на
    /// букву: читай `views` дерево, наборов стало бы ноль.
    ///
    /// Это не экономия на мелочи. На библиотеке владельца (пять источников,
    /// 232 файла, 96 наборов) обход и разбор стоили полторы секунды в
    /// отладочной сборке, и платились они на каждое открытие настроек топика —
    /// кэш разбора при 96 наборах не попадал никогда.
    #[test]
    fn a_warm_digest_answers_without_reading_the_tree() {
        let sandbox = Sandbox::new("digest-warm");
        sandbox.author("orders.proto", ORDERS);
        sandbox.author("wallets.proto", WALLETS);
        sandbox.author("common/types.proto", COMMON);
        sandbox.add_folder("s1").unwrap();

        let before = views(&sandbox.root).unwrap();
        assert_eq!(before[0].sets.len(), 2);

        // Дерево на месте, а файлов в нём больше нет.
        let dir = dir_of(&sandbox.root, "s1");
        for name in tree::proto_files(&dir) {
            std::fs::remove_file(dir.join(name)).unwrap();
        }
        assert!(tree::proto_files(&dir).is_empty());

        let after = views(&sandbox.root).unwrap();
        assert_eq!(after[0].error, None);
        let sets: Vec<(&str, Option<&str>)> = after[0]
            .sets
            .iter()
            .map(|s| (s.entry.as_str(), s.root.as_deref()))
            .collect();
        assert_eq!(
            sets,
            [
                ("orders.proto", Some("orders.OrderChanged")),
                ("wallets.proto", Some("wallets.WalletChanged")),
            ]
        );
    }

    /// Перезабор дерева слепок обесценивает — иначе обновлённый контракт
    /// показывался бы старым.
    ///
    /// Случай нарочно такой, где число файлов не меняется: единственное, чем
    /// новое дерево отличается от прежнего, — это `fetched_at`, и метка обязана
    /// держаться в том числе на нём.
    #[test]
    fn refetching_the_tree_recomputes_the_digest() {
        let sandbox = Sandbox::new("digest-refetch");
        sandbox.author("orders.proto", "syntax = \"proto3\"; package o; message Before {}");
        sandbox.add_folder("s1").unwrap();
        assert_eq!(
            views(&sandbox.root).unwrap()[0].sets[0].root.as_deref(),
            Some("o.Before")
        );

        sandbox.author("orders.proto", "syntax = \"proto3\"; package o; message After {}");
        let files = read_folder(&sandbox.outside).unwrap();
        let refetched = replace_tree(&sandbox.root, "s1", files).unwrap();
        assert_eq!(refetched.files, 1, "файлов столько же, что и было");

        assert_eq!(
            views(&sandbox.root).unwrap()[0].sets[0].root.as_deref(),
            Some("o.After")
        );
    }

    /// Смена области поиска контрактов — единственное, что меняет деление на
    /// наборы БЕЗ похода за деревом. Метка держится и на ней.
    #[test]
    fn changing_the_scope_recomputes_the_digest() {
        let sandbox = Sandbox::new("digest-scope");
        sandbox.author("internal/orders.proto", SCOPED_ORDERS);
        sandbox.author("pkg/common/types.proto", COMMON);
        sandbox.author("docs/analysts/orders.proto", DOCS_COPY);
        sandbox.add_folder("s1").unwrap();

        // Без области контрактами считаются оба самостоятельных файла.
        let wide = views(&sandbox.root).unwrap();
        assert_eq!(wide[0].sets.len(), 2);

        update(&sandbox.root, "s1", |source| {
            source.subdir = Some("internal".to_string())
        })
        .unwrap();

        let narrow = views(&sandbox.root).unwrap();
        assert_eq!(narrow[0].sets.len(), 1);
        assert_eq!(narrow[0].sets[0].entry, "internal/orders.proto");
    }

    /// Слепок убранного источника не остаётся лежать: `delete` его не трогает
    /// нарочно, отваливается он там же, где пересчитывается.
    #[test]
    fn deleting_a_source_drops_its_digest_too() {
        let sandbox = Sandbox::new("digest-delete");
        sandbox.author("orders.proto", ORDERS);
        sandbox.author("common/types.proto", COMMON);
        sandbox.add_folder("s1").unwrap();
        views(&sandbox.root).unwrap();
        assert!(digest::read(&sandbox.root).contains_key("s1"));

        delete(&sandbox.root, "s1").unwrap();
        assert!(views(&sandbox.root).unwrap().is_empty());
        assert!(digest::read(&sandbox.root).is_empty());
    }

    /// Битый слепок — не беда: это производное, и пересчитать его дешевле, чем
    /// объяснять человеку, что сломался файл, которого он не заводил.
    #[test]
    fn a_broken_digest_is_recomputed_rather_than_reported() {
        let sandbox = Sandbox::new("digest-broken");
        sandbox.author("orders.proto", ORDERS);
        sandbox.author("common/types.proto", COMMON);
        sandbox.add_folder("s1").unwrap();
        views(&sandbox.root).unwrap();

        std::fs::write(sandbox.root.join("proto-sets.json"), b"{not json").unwrap();

        let views = views(&sandbox.root).unwrap();
        assert_eq!(views[0].sets.len(), 1);
        assert_eq!(views[0].sets[0].root.as_deref(), Some("orders.OrderChanged"));
        // И починился на диске сам: обломков вроде `.bad` после него не
        // остаётся, в отличие от настоящих файлов настроек.
        assert!(digest::read(&sandbox.root).contains_key("s1"));
        assert!(!sandbox.root.join("proto-sets.json.bad").exists());
    }

    #[test]
    fn deleting_a_source_takes_its_tree_with_it() {
        let sandbox = Sandbox::new("delete");
        sandbox.author("orders.proto", ORDERS);
        sandbox.author("common/types.proto", COMMON);
        sandbox.add_folder("s1").unwrap();

        let dir = dir_of(&sandbox.root, "s1");
        assert!(dir.is_dir());
        delete(&sandbox.root, "s1").unwrap();
        assert!(!dir.exists());
        assert!(list(&sandbox.root).unwrap().is_empty());
    }
}
