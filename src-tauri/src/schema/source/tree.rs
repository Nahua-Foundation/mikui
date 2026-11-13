//! Что лежит в дереве источника: какие файлы входные и откуда отсчитывать
//! импорты.
//!
//! Дерево — это репозиторий целиком, и контракт в нём не один. Разбирать его
//! целиком нельзя: корней окажется несколько, а правило корня
//! (`linked::Linked::root`) — главное, чем приложение определяет тип топика, —
//! работает только когда корень один. Поэтому дерево делится на НАБОРЫ, по
//! одному на входной файл.
//!
//! Где искать контракты, сужает область (`entries`, параметр `scope`). ЧТО
//! видно импортам — не сужает ничто: контракты из `internal/` импортируют общие
//! типы из `pkg/`, и обрезанное дерево не разберётся вовсе.

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use super::super::proto::imports;

/// Каталоги, в которые заходить незачем: там сборочный мусор, и .proto оттуда —
/// в лучшем случае чья-то копия, которая выиграла бы у настоящей. Тот же
/// список, что у поиска импортов на диске (`proto::resolve`).
const SKIP: [&str; 9] = [
    "node_modules",
    "target",
    "build",
    "dist",
    "out",
    "vendor",
    "__pycache__",
    "bin",
    "obj",
];

/// Подкаталоги, от которых .proto отсчитывают импорты по соглашению сборки.
/// Порядок значим: чем выше, тем вероятнее.
const CONVENTIONAL: [&str; 6] = [
    "proto",
    "protos",
    "src/main/proto",
    "src/main/protobuf",
    "src/proto",
    "protobuf",
];

/// Сколько каталогов включения готовы передать разбору.
///
/// Потолок, а не расчёт: у многомодульного репозитория их набирается по числу
/// модулей, и список, в котором их сотня, — это уже не помощь разбору, а способ
/// найти чужой файл с подходящим именем.
const MAX_INCLUDE: usize = 64;

/// Все .proto дерева — путями относительно его корня, по алфавиту.
///
/// Относительными именно потому, что это и есть имена, под которыми файлы
/// видны в `import`: путь от корня включения — ровно то, что пишут в .proto.
pub fn proto_files(root: &Path) -> Vec<String> {
    let mut out = Vec::new();
    walk(root, root, &mut out);
    out.sort();
    out
}

fn walk(root: &Path, dir: &Path, out: &mut Vec<String>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    let mut paths: Vec<PathBuf> = entries.flatten().map(|e| e.path()).collect();
    // Обход каталога порядка не обещает, а список уезжает человеку на экран.
    paths.sort();

    for path in paths {
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if path.is_dir() {
            if !name.starts_with('.') && !SKIP.contains(&name) {
                walk(root, &path, out);
            }
            continue;
        }
        if path.extension().is_some_and(|e| e == "proto") {
            if let Ok(relative) = path.strip_prefix(root) {
                out.push(relative.to_string_lossy().replace('\\', "/"));
            }
        }
    }
}

/// Входные файлы — те, которых в этом дереве никто не импортирует.
///
/// Это и есть контракты: файл на топик, общие типы в импортах. Остальные —
/// зависимости, и предлагать их как самостоятельный набор значило бы
/// предлагать декодировать топик типом `common.Decimal`.
///
/// `scope` — подкаталог, в котором ищут КОНТРАКТЫ, и только их. Остальное
/// дерево никуда не девается и остаётся видимым импортам: рядом с `internal/`
/// в репозитории лежит и `pkg/` с общими типами, откуда контракты импортируют,
/// и `docs/` с копиями для аналитиков. Первое обязано резолвиться, второе —
/// не попадаться в списке контрактов. Сузить тут надо выбор, а не дерево.
/// Пусто — контракты ищутся по всему дереву.
///
/// Импорты собираются ТОЖЕ только по файлам области. Иначе копия контракта в
/// `docs/` отняла бы у настоящего звание контракта: тот оказался бы
/// «востребован», хотя востребован он тем, что в области не лежит.
///
/// Сопоставление идёт по ХВОСТУ пути, а не по полному равенству: импорт
/// отсчитан от корня включения, а имя файла — от корня дерева, и в раскладке
/// Maven это разные вещи (`acme/common/v1/types.proto` против
/// `common/src/main/proto/acme/common/v1/types.proto`).
pub fn entries(root: &Path, files: &[String], scope: Option<&str>) -> Vec<String> {
    let prefix = scope_prefix(scope);
    let in_scope = |name: &str| {
        prefix
            .as_deref()
            .is_none_or(|prefix| name.starts_with(prefix))
    };

    let mut imported: HashSet<String> = HashSet::new();
    for name in files.iter().filter(|name| in_scope(name)) {
        let Ok(bytes) = std::fs::read(root.join(name)) else {
            continue;
        };
        imported.extend(imports::imports_of_bytes(&bytes));
    }

    files
        .iter()
        .filter(|name| in_scope(name))
        .filter(|name| !is_imported(name, &imported))
        .cloned()
        .collect()
}

/// Область поиска контрактов в виде префикса пути.
///
/// Пишут её как придётся — `internal`, `internal/`, `/internal`,
/// `./internal`, — а сравнивать надо с завершающей косой чертой: голое
/// `internal` совпало бы ещё и с `internal-tools/`.
fn scope_prefix(scope: Option<&str>) -> Option<String> {
    let raw = scope?.trim().replace('\\', "/");
    let trimmed = raw
        .trim_matches('/')
        .trim_start_matches("./")
        .trim_matches('/');
    (!trimmed.is_empty()).then(|| format!("{trimmed}/"))
}

fn is_imported(file: &str, imports: &HashSet<String>) -> bool {
    imports
        .iter()
        .any(|import| file == import || file.ends_with(&format!("/{import}")))
}

/// Откуда разбору отсчитывать импорты.
///
/// Корень дерева плюс типовые подкаталоги сборки — и то же у прямых детей,
/// ради многомодульного репозитория, где общие типы лежат в соседнем модуле.
/// Ровно та же логика, что у поиска импортов на диске (`proto::resolve`), но
/// применённая заранее ко всему дереву: искать по одному файлу здесь незачем,
/// дерево у нас целиком.
pub fn include_roots(root: &Path) -> Vec<PathBuf> {
    let mut roots = vec![root.to_path_buf()];
    let mut add = |dir: &Path| {
        if dir.is_dir() && !roots.contains(&dir.to_path_buf()) {
            roots.push(dir.to_path_buf());
        }
    };

    for sub in CONVENTIONAL {
        add(&root.join(sub));
    }

    let Ok(entries) = std::fs::read_dir(root) else {
        return roots;
    };
    let mut children: Vec<PathBuf> = entries
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.is_dir())
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| !n.starts_with('.') && !SKIP.contains(&n))
        })
        .collect();
    children.sort();

    for child in children {
        for sub in CONVENTIONAL {
            add(&child.join(sub));
        }
    }
    roots.truncate(MAX_INCLUDE);
    roots
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Tree(PathBuf);

    impl Tree {
        fn new(name: &str) -> Self {
            let path = std::env::temp_dir().join(format!("mikui-source-tree-{name}"));
            let _ = std::fs::remove_dir_all(&path);
            std::fs::create_dir_all(&path).unwrap();
            Self(path)
        }

        fn write(&self, relative: &str, text: &str) {
            let path = self.0.join(relative);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, text).unwrap();
        }
    }

    impl Drop for Tree {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// Репозиторий с двумя контрактами и общими типами: входных файлов два,
    /// общий — зависимость.
    #[test]
    fn entry_files_are_the_ones_nobody_imports() {
        let tree = Tree::new("entries");
        tree.write(
            "orders.proto",
            "syntax = \"proto3\"; import \"common/types.proto\"; message O {}",
        );
        tree.write(
            "wallets.proto",
            "syntax = \"proto3\"; import \"common/types.proto\"; message W {}",
        );
        tree.write("common/types.proto", "syntax = \"proto3\"; message T {}");
        // Не .proto и сборочный мусор в дерево не попадают вовсе.
        tree.write("README.md", "nope");
        tree.write("target/generated/copy.proto", "syntax = \"proto3\";");

        let files = proto_files(&tree.0);
        assert_eq!(files, ["common/types.proto", "orders.proto", "wallets.proto"]);
        assert_eq!(entries(&tree.0, &files, None), ["orders.proto", "wallets.proto"]);
    }

    /// Подкаталог сужает ВЫБОР контрактов, а не дерево: `docs/` с копиями для
    /// аналитиков в список не попадает, а импорт из `pkg/` за пределами области
    /// обязан остаться резолвимым — файл просто остаётся в дереве.
    ///
    /// Ровно этот случай и стоил восемнадцати неразобравшихся наборов из
    /// девятнадцати: подкаталог обрезал архив, и `pkg/` не приезжал вовсе.
    #[test]
    fn a_scope_picks_the_contracts_and_leaves_the_rest_of_the_tree_alone() {
        let tree = Tree::new("scope");
        tree.write(
            "internal/kafka/orders.proto",
            "syntax = \"proto3\"; import \"pkg/decimalpb/decimal.proto\"; message O {}",
        );
        tree.write(
            "internal/kafka/common/shared.proto",
            "syntax = \"proto3\"; message S {}",
        );
        tree.write(
            "internal/kafka/wallets.proto",
            "syntax = \"proto3\"; import \"internal/kafka/common/shared.proto\"; message W {}",
        );
        tree.write("pkg/decimalpb/decimal.proto", "syntax = \"proto3\"; message D {}");
        tree.write("docs/analysts/orders.proto", "syntax = \"proto3\"; message Copy {}");

        let files = proto_files(&tree.0);
        assert_eq!(files.len(), 5, "дерево приезжает целиком: {files:?}");

        let scoped = entries(&tree.0, &files, Some("internal"));
        assert_eq!(
            scoped,
            ["internal/kafka/orders.proto", "internal/kafka/wallets.proto"],
            "в области — только контракты: общий `shared.proto` востребован, \
             `pkg/` и `docs/` вне области"
        );
        // Зависимость из-за области осталась в дереве — иначе её нечем
        // резолвить, и набор не разберётся.
        assert!(files.contains(&"pkg/decimalpb/decimal.proto".to_string()));
        // И корень включения у неё есть: импорт отсчитан от корня дерева.
        assert!(include_roots(&tree.0).contains(&tree.0.clone()));
    }

    /// Область пишут как придётся, и все написания значат одно и то же. А вот
    /// соседний каталог с тем же началом в неё попадать не должен.
    #[test]
    fn a_scope_is_matched_by_path_segment_however_it_is_spelled() {
        let tree = Tree::new("scope-spelling");
        tree.write("internal/a.proto", "syntax = \"proto3\"; message A {}");
        tree.write("internal-tools/b.proto", "syntax = \"proto3\"; message B {}");

        let files = proto_files(&tree.0);
        for spelling in ["internal", "internal/", "/internal", "./internal", " internal "] {
            assert_eq!(
                entries(&tree.0, &files, Some(spelling)),
                ["internal/a.proto"],
                "написание {spelling:?}"
            );
        }
        // Пустая строка — это «вся область», а не «ничего».
        assert_eq!(entries(&tree.0, &files, Some("  ")).len(), 2);
    }

    /// Копия контракта в `docs/` не отнимает у настоящего звание контракта.
    ///
    /// Импорты собираются только по файлам области ровно за этим: собери их по
    /// всему дереву, и `internal/kafka/orders.proto` оказался бы «востребован»
    /// копией из `docs/`, то есть исчез бы из списка.
    #[test]
    fn a_copy_outside_the_scope_does_not_demote_the_real_contract() {
        let tree = Tree::new("scope-demote");
        tree.write("internal/kafka/orders.proto", "syntax = \"proto3\"; message O {}");
        tree.write(
            "docs/analysts/bundle.proto",
            "syntax = \"proto3\"; import \"internal/kafka/orders.proto\";",
        );

        let files = proto_files(&tree.0);
        assert_eq!(
            entries(&tree.0, &files, Some("internal")),
            ["internal/kafka/orders.proto"]
        );
    }

    /// Раскладка Maven: импорт отсчитан от `src/main/proto` соседнего модуля, а
    /// имя файла — от корня дерева. Сопоставление по хвосту это и разрешает.
    #[test]
    fn an_import_from_a_sibling_module_marks_the_file_as_a_dependency() {
        let tree = Tree::new("maven");
        tree.write(
            "common/src/main/proto/acme/common/v1/types.proto",
            "syntax = \"proto3\"; package acme.common.v1; message T {}",
        );
        tree.write(
            "orders/src/main/proto/acme/orders/v1/events.proto",
            r#"
                syntax = "proto3";
                package acme.orders.v1;
                import "acme/common/v1/types.proto";
                message Event {}
            "#,
        );

        let files = proto_files(&tree.0);
        assert_eq!(
            entries(&tree.0, &files, None),
            ["orders/src/main/proto/acme/orders/v1/events.proto"]
        );
        // И корень включения соседнего модуля найден — иначе импорт не
        // разрешить ничем.
        let roots = include_roots(&tree.0);
        assert!(
            roots.contains(&tree.0.join("common/src/main/proto")),
            "{roots:?}"
        );
        assert!(roots.contains(&tree.0.join("orders/src/main/proto")), "{roots:?}");
    }

    /// Файл, который импортирует сам себя по кругу с соседом, входным быть
    /// перестаёт — и это верно: самостоятельным контрактом он не является.
    #[test]
    fn a_tree_without_entry_files_yields_none() {
        let tree = Tree::new("cycle");
        tree.write("a.proto", "syntax = \"proto3\"; import \"b.proto\";");
        tree.write("b.proto", "syntax = \"proto3\"; import \"a.proto\";");

        let files = proto_files(&tree.0);
        assert!(entries(&tree.0, &files, None).is_empty());
    }
}
