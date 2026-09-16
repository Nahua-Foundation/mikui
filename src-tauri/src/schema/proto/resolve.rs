//! Поиск импортируемых .proto на диске.
//!
//! Задача та же, что у `protoc -I`: импорт `internal/common_proto/common.proto`
//! — это путь ОТ КОРНЯ ВКЛЮЧЕНИЯ, а не от каталога файла, и чтобы его
//! разрешить, надо знать корень. `protoc` получает его аргументом, у нас
//! спрашивать некого — значит надо найти.
//!
//! Находится он подъёмом по предкам: файл лежит в
//! `<проект>/internal/adapters/kafka/consumer/ode/`, импорт начинается с
//! `internal/`, и первый же предок, у которого путь импорта существует, и есть
//! корень. Ни обхода дерева, ни угадывания имён это не требует.
//!
//! Раскладки, впрочем, бывают не только такие. У Go путь импорта отсчитывается
//! от корня модуля (предок находится сразу), у Java и Scala файлы лежат в
//! `src/main/proto` внутри МОДУЛЯ, и общие типы приезжают из соседнего модуля,
//! а не из предка. Поэтому проверяются, в порядке убывания надёжности: сам
//! предок, его типовые подкаталоги и то же у его прямых детей — ровно один шаг
//! в сторону от линии предков, которого хватает на многомодульный
//! репозиторий.
//!
//! Подъём ограничен границей проекта — ближайшим предком с `.git`, `go.mod`,
//! `pom.xml` и им подобными. Без этого перебор дошёл бы до корня файловой
//! системы и начал бы перебирать содержимое домашнего каталога, а найденная
//! там схема была бы случайной.
//!
//! Найденное складывается в каталог схемы под именем СВОЕГО ИМПОРТА — того
//! самого, по которому его искали. Тем самым разбор видит файл ровно там, где
//! его ждёт `import`, и никаких include-каталогов, кроме одного, не нужно.

use std::collections::{HashMap, HashSet};
use std::path::{Component, Path, PathBuf};

use super::imports;

/// Файл, найденный по импорту.
pub struct Found {
    /// Путь импорта — он же имя внутри каталога схемы.
    pub import: String,
    /// Где нашли. По нему работает «перечитать с диска».
    pub source: PathBuf,
    pub bytes: Vec<u8>,
}

/// Импорт, которого не нашлось.
pub struct Missing {
    pub import: String,
    /// Имя файла, который его требует, — иначе в наборе из сорока файлов
    /// непонятно, откуда взялось требование.
    pub wanted_by: String,
}

/// Чем закончился поиск.
pub struct Outcome {
    pub found: Vec<Found>,
    pub missing: Vec<Missing>,
    /// Каталоги, в которых искали. Нужны сообщению об ошибке: «не нашёл
    /// common.proto» без «искал там-то» оставляет пользователя гадать, то ли
    /// файла нет, то ли приложение смотрит не туда.
    pub looked: Vec<PathBuf>,
}

impl Outcome {
    /// Человеческое объяснение, чего не хватило. `None` — хватило всего.
    pub fn explain(&self) -> Option<String> {
        if self.missing.is_empty() {
            return None;
        }
        let mut lines = vec!["can't find the imported .proto files:".to_string()];
        for miss in &self.missing {
            lines.push(format!(
                "  {} — imported by {}",
                miss.import, miss.wanted_by
            ));
        }
        match self.looked.split_first() {
            None => {
                lines.push("there was nowhere to look: add the missing files by hand".to_string())
            }
            // Перечисляется только верхняя граница поиска: печатать все
            // двенадцать предков значит спрятать в них ту единственную строку,
            // которая пользователю о чём-то говорит.
            Some(_) => {
                let top = self.looked.last().unwrap();
                lines.push(format!("looked under {} and below", top.display()));
            }
        }
        Some(lines.join("\n"))
    }
}

/// Файл, от которого начинается поиск: уже выбранный пользователем .proto.
pub struct Seed {
    /// Имя внутри каталога схемы — оно же то, чем импорт считается закрытым.
    pub name: String,
    pub source: PathBuf,
    pub bytes: Vec<u8>,
}

/// Подкаталоги, в которых .proto лежат по соглашению сборки.
///
/// Порядок значим: чем выше, тем вероятнее. `src/main/proto` — раскладка Maven
/// и Gradle, `src/main/protobuf` — ScalaPB, остальное встречается у всех
/// понемногу.
const CONVENTIONAL: [&str; 9] = [
    "proto",
    "protos",
    "src/main/proto",
    "src/main/protobuf",
    "src/proto",
    "protobuf",
    "idl",
    "api",
    "schemas",
];

/// Каталоги, в которые заходить незачем: там лежит сборочный мусор, и .proto
/// оттуда — в лучшем случае чья-то копия, которая выиграла бы у настоящей.
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

/// Файлы, по которым узнаётся корень проекта. Выше него импорт отсчитывать
/// уже не от чего.
const MARKERS: [&str; 11] = [
    ".git",
    "go.mod",
    "buf.yaml",
    "buf.work.yaml",
    "pom.xml",
    "build.sbt",
    "build.gradle",
    "build.gradle.kts",
    "Cargo.toml",
    "pyproject.toml",
    "CMakeLists.txt",
];

/// Предел подъёма. Глубже не ходим и при найденном маркере: путь бывает и
/// патологически глубоким.
///
/// Подниматься до этого предела не опасно, потому что проверка на каждом
/// предке — точная: под ним должен существовать ПОЛНЫЙ путь импорта, вплоть до
/// `internal/common_proto/common_proto.proto`. Случайно совпасть такому
/// нечем. А вот перебор детей — дело другое, и он ограничен границей проекта,
/// см. `Search::wide`.
const MAX_DEPTH: usize = 12;

/// Сколько компонентов пути должно остаться, чтобы каталог вообще стоило
/// перебирать. `/` и `/Users` корнями включения не бывают.
const MIN_COMPONENTS: usize = 2;

/// Сколько файлов готовы подтянуть. Замыкание в двести файлов — это уже не
/// схема одного топика, а чей-то монорепозиторий целиком.
const MAX_FILES: usize = 200;

/// Разрешает импорты набора, спускаясь по ним рекурсивно.
///
/// `seeds` — файлы, выбранные пользователем: они и дают начальные импорты, и
/// закрывают часть их собой. `extra` — корни включения, известные заранее
/// (сейчас таких нет; параметр оставлен затем, что список в настройках —
/// очевидное продолжение, и добавлять его не должно значить переписывать
/// резолвер).
pub fn closure(seeds: &[Seed], extra: &[PathBuf]) -> Outcome {
    let mut search = Search::new(seeds, extra);
    search.run(seeds);

    let mut found: Vec<Found> = search.found.into_values().collect();
    // Порядок находок не должен зависеть от обхода хэш-карты: он уезжает в
    // список файлов схемы, то есть пользователю на экран.
    found.sort_by(|a, b| a.import.cmp(&b.import));
    search.missing.sort_by(|a, b| a.import.cmp(&b.import));

    Outcome {
        found,
        missing: search.missing,
        looked: search.bases,
    }
}

struct Search {
    /// Корни включения, в которых уже что-то нашлось.
    ///
    /// Самое важное место всей затеи с точки зрения цены: первый импорт
    /// обходится перебором предков и их детей, а все остальные — попаданием в
    /// уже найденный корень с первой попытки. Замыкание из сорока файлов
    /// поэтому стоит примерно сорок обращений к файловой системе, а не сорок
    /// обходов дерева.
    roots: Vec<PathBuf>,
    /// Каталоги, которые перебираем: предки выбранных файлов, от самых
    /// глубоких к границе проекта.
    bases: Vec<PathBuf>,
    /// Те из них, у которых есть смысл перебирать ещё и детей.
    ///
    /// Только каталоги внутри проекта, чья граница нашлась по маркеру.
    /// Обходить детей у предков, про которые ничего не известно, — значит
    /// перебирать содержимое домашнего каталога и считать схемой первое, что
    /// в нём совпало по имени.
    wide: Vec<PathBuf>,
    /// Каталоги, чьи предки уже разложены в `bases`.
    anchored: HashSet<PathBuf>,
    /// Импорты, которые уже закрыты: именами выбранных файлов или находками.
    closed: HashSet<String>,
    found: HashMap<String, Found>,
    missing: Vec<Missing>,
}

impl Search {
    fn new(seeds: &[Seed], extra: &[PathBuf]) -> Self {
        let mut search = Self {
            roots: extra.to_vec(),
            bases: Vec::new(),
            wide: Vec::new(),
            anchored: HashSet::new(),
            closed: seeds.iter().map(|s| s.name.clone()).collect(),
            found: HashMap::new(),
            missing: Vec::new(),
        };
        for seed in seeds {
            search.anchor(&seed.source);
        }
        search
    }

    /// Добавляет предков каталога файла к тем, что перебираем.
    ///
    /// Зовётся и на находки: файл из соседнего модуля тянет свои импорты
    /// оттуда же, где лежит сам, и его корень включения может быть не тем, от
    /// которого нашёлся он.
    fn anchor(&mut self, file: &Path) {
        let Some(dir) = file.parent() else {
            return;
        };
        if !self.anchored.insert(dir.to_path_buf()) {
            return;
        }
        let (chain, bounded) = walk_up(dir);
        for base in chain {
            if bounded && !self.wide.contains(&base) {
                self.wide.push(base.clone());
            }
            if !self.bases.contains(&base) {
                self.bases.push(base);
            }
        }
    }

    fn run(&mut self, seeds: &[Seed]) {
        // Очередь, а не рекурсия: глубина замыкания ничем не ограничена, а
        // порядок обхода безразличен — важно лишь закрыть всё достижимое.
        let mut queue: Vec<(String, String)> = Vec::new();
        for seed in seeds {
            for import in imports::imports_of_bytes(&seed.bytes) {
                queue.push((import, seed.name.clone()));
            }
        }

        let mut seen: HashSet<String> = HashSet::new();
        while let Some((import, wanted_by)) = queue.pop() {
            // Well-known типы парсер несёт в себе; искать их на диске значило
            // бы найти чью-то копию из vendor-каталога.
            if imports::is_bundled(&import) || self.closed.contains(&import) {
                continue;
            }
            if !seen.insert(import.clone()) {
                continue;
            }
            // Путь из текста схемы — это данные, и выходить по нему за пределы
            // дерева нельзя даже чтением.
            if !imports::is_safe_import(&import) || self.found.len() >= MAX_FILES {
                self.missing.push(Missing { import, wanted_by });
                continue;
            }

            let Some((source, bytes)) = self.locate(&import) else {
                self.missing.push(Missing { import, wanted_by });
                continue;
            };

            for next in imports::imports_of_bytes(&bytes) {
                queue.push((next, import.clone()));
            }
            self.anchor(&source);
            self.found.insert(
                import.clone(),
                Found {
                    import,
                    source,
                    bytes,
                },
            );
        }
    }

    /// Ищет один импорт, запоминая корень, в котором он нашёлся.
    ///
    /// Проходов три, и они идут именно в этом порядке: точное попадание под
    /// далёким предком надёжнее, чем совпадение в соседнем модуле под близким,
    /// — а обход детей ещё и самый дорогой из трёх.
    fn locate(&mut self, import: &str) -> Option<(PathBuf, Vec<u8>)> {
        for root in &self.roots {
            if let Some(hit) = read_at(&root.join(import)) {
                return Some(hit);
            }
        }

        let bases = self.bases.clone();
        for base in &bases {
            for root in
                std::iter::once(base.clone()).chain(CONVENTIONAL.iter().map(|sub| base.join(sub)))
            {
                if let Some(hit) = read_at(&root.join(import)) {
                    self.remember(root);
                    return Some(hit);
                }
            }
        }
        for base in &self.wide.clone() {
            for child in children(base) {
                for root in std::iter::once(child.clone())
                    .chain(CONVENTIONAL.iter().map(|sub| child.join(sub)))
                {
                    if let Some(hit) = read_at(&root.join(import)) {
                        self.remember(root);
                        return Some(hit);
                    }
                }
            }
        }
        None
    }

    fn remember(&mut self, root: PathBuf) {
        if !self.roots.contains(&root) {
            self.roots.push(root);
        }
    }
}

/// Предки каталога — от него самого до границы проекта включительно.
///
/// Второе возвращаемое — нашлась ли граница. По нему решается, перебирать ли
/// ещё и детей: внутри проекта это оправданный шаг в сторону, а снаружи —
/// перебор чужих каталогов.
fn walk_up(dir: &Path) -> (Vec<PathBuf>, bool) {
    let mut chain: Vec<PathBuf> = Vec::new();
    for base in dir.ancestors().take(MAX_DEPTH) {
        if count_components(base) < MIN_COMPONENTS {
            break;
        }
        chain.push(base.to_path_buf());
        if is_project_root(base) {
            return (chain, true);
        }
    }
    (chain, false)
}

fn is_project_root(dir: &Path) -> bool {
    MARKERS.iter().any(|marker| dir.join(marker).exists())
}

fn count_components(path: &Path) -> usize {
    path.components()
        .filter(|c| matches!(c, Component::Normal(_)))
        .count()
}

/// Прямые подкаталоги, в которых стоит искать. Порядок — по имени: обход
/// каталога сам по себе никакого порядка не обещает, а находка должна быть
/// одной и той же от запуска к запуску.
fn children(base: &Path) -> Vec<PathBuf> {
    let Ok(entries) = std::fs::read_dir(base) else {
        return Vec::new();
    };
    let mut out: Vec<PathBuf> = entries
        .flatten()
        .filter(|entry| entry.file_type().is_ok_and(|t| t.is_dir()))
        .filter(|entry| match entry.file_name().to_str() {
            // Скрытые каталоги — это `.git` и его родня: искать там нечего.
            Some(name) => !name.starts_with('.') && !SKIP.contains(&name),
            None => false,
        })
        .map(|entry| entry.path())
        .collect();
    out.sort();
    out
}

fn read_at(path: &Path) -> Option<(PathBuf, Vec<u8>)> {
    // Без отдельной проверки `is_file`: чтение каталога и так вернёт ошибку, а
    // лишнее обращение на каждый промах — это цена всего перебора.
    std::fs::read(path)
        .ok()
        .map(|bytes| (path.to_path_buf(), bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Tree(PathBuf);

    impl Tree {
        /// Дерево всегда с маркером в корне: без него подъём ограничен
        /// четырьмя уровнями, а временный каталог лежит глубоко.
        fn new(name: &str) -> Self {
            let path = std::env::temp_dir().join(format!("mikui-resolve-test-{name}"));
            let _ = std::fs::remove_dir_all(&path);
            std::fs::create_dir_all(path.join(".git")).unwrap();
            Self(path)
        }

        fn write(&self, rel: &str, text: &str) -> PathBuf {
            let path = self.0.join(rel);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(&path, text).unwrap();
            path
        }

        fn seed(&self, rel: &str, name: &str) -> Seed {
            let source = self.0.join(rel);
            Seed {
                name: name.to_string(),
                bytes: std::fs::read(&source).unwrap(),
                source,
            }
        }
    }

    impl Drop for Tree {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    const COMMON: &str = r#"
        syntax = "proto3";
        package common_proto;
        enum TradingMode { UNKNOWN = 0; }
    "#;

    /// Раскладка Go из реального проекта: импорт отсчитан от корня модуля,
    /// файл лежит пятью каталогами ниже.
    #[test]
    fn an_import_from_the_module_root_is_found_by_walking_up() {
        let tree = Tree::new("go");
        tree.write("internal/common_proto/common_proto.proto", COMMON);
        tree.write(
            "internal/adapters/kafka/consumer/ode/ode_events.proto",
            r#"
                syntax = "proto3";
                package ode;
                import "google/protobuf/timestamp.proto";
                import "internal/common_proto/common_proto.proto";
                message OrderBookEvent { common_proto.TradingMode m = 1; }
            "#,
        );

        let seed = tree.seed(
            "internal/adapters/kafka/consumer/ode/ode_events.proto",
            "ode_events.proto",
        );
        let outcome = closure(&[seed], &[]);

        assert!(
            outcome.missing.is_empty(),
            "{}",
            outcome.explain().unwrap_or_default()
        );
        assert_eq!(outcome.found.len(), 1);
        assert_eq!(
            outcome.found[0].import,
            "internal/common_proto/common_proto.proto"
        );
    }

    /// Раскладка Maven/Gradle: общие типы в соседнем модуле, под
    /// `src/main/proto`. Вверх по предкам такой файл не найти — он лежит в
    /// стороне.
    #[test]
    fn an_import_from_a_sibling_module_is_found_too() {
        let tree = Tree::new("maven");
        tree.write("common/src/main/proto/acme/common/v1/types.proto", COMMON);
        tree.write(
            "orders/src/main/proto/acme/orders/v1/events.proto",
            r#"
                syntax = "proto3";
                package acme.orders.v1;
                import "acme/common/v1/types.proto";
                message Event { common_proto.TradingMode m = 1; }
            "#,
        );

        let seed = tree.seed(
            "orders/src/main/proto/acme/orders/v1/events.proto",
            "events.proto",
        );
        let outcome = closure(&[seed], &[]);

        assert!(
            outcome.missing.is_empty(),
            "{}",
            outcome.explain().unwrap_or_default()
        );
        assert_eq!(outcome.found[0].import, "acme/common/v1/types.proto");
    }

    /// Импорты транзитивны, и замыкание обязано спускаться до конца.
    #[test]
    fn imports_of_the_found_files_are_resolved_as_well() {
        let tree = Tree::new("transitive");
        tree.write("proto/deep/leaf.proto", COMMON);
        tree.write(
            "proto/mid/middle.proto",
            "syntax = \"proto3\"; import \"deep/leaf.proto\"; message M {}",
        );
        tree.write(
            "svc/root.proto",
            "syntax = \"proto3\"; import \"mid/middle.proto\"; message R {}",
        );

        let seed = tree.seed("svc/root.proto", "root.proto");
        let outcome = closure(&[seed], &[]);

        assert!(
            outcome.missing.is_empty(),
            "{}",
            outcome.explain().unwrap_or_default()
        );
        let names: Vec<&str> = outcome.found.iter().map(|f| f.import.as_str()).collect();
        assert_eq!(names, vec!["deep/leaf.proto", "mid/middle.proto"]);
    }

    /// То, что уже выбрано пользователем, искать не надо.
    #[test]
    fn an_import_already_covered_by_the_selection_is_not_searched_for() {
        let tree = Tree::new("covered");
        tree.write("a/root.proto", "syntax = \"proto3\"; import \"dep.proto\";");
        tree.write("a/dep.proto", COMMON);

        let root = tree.seed("a/root.proto", "root.proto");
        let dep = tree.seed("a/dep.proto", "dep.proto");
        let outcome = closure(&[root, dep], &[]);

        assert!(outcome.missing.is_empty());
        assert!(outcome.found.is_empty(), "искали то, что уже есть");
    }

    #[test]
    fn a_missing_import_is_reported_with_who_wants_it_and_where_we_looked() {
        let tree = Tree::new("missing");
        tree.write(
            "svc/root.proto",
            "syntax = \"proto3\"; import \"nowhere/absent.proto\";",
        );

        let seed = tree.seed("svc/root.proto", "root.proto");
        let outcome = closure(&[seed], &[]);

        assert!(outcome.found.is_empty());
        assert_eq!(outcome.missing.len(), 1);
        let explained = outcome.explain().unwrap();
        assert!(explained.contains("nowhere/absent.proto"), "{explained}");
        assert!(explained.contains("root.proto"), "{explained}");
    }

    /// Путь из текста схемы — это данные. Выходить по нему за пределы дерева
    /// нельзя даже чтением.
    #[test]
    fn an_import_escaping_upwards_is_refused_rather_than_searched() {
        let tree = Tree::new("escape");
        tree.write(
            "svc/root.proto",
            "syntax = \"proto3\"; import \"../../../../etc/passwd\";",
        );

        let seed = tree.seed("svc/root.proto", "root.proto");
        let outcome = closure(&[seed], &[]);
        assert!(outcome.found.is_empty());
        assert_eq!(outcome.missing.len(), 1);
    }

    /// Сборочный мусор в обход не попадает: копия схемы из `target/` не должна
    /// выиграть у настоящей.
    #[test]
    fn build_directories_are_not_searched() {
        let tree = Tree::new("skip");
        tree.write("target/generated/dep.proto", COMMON);
        tree.write(
            "svc/root.proto",
            "syntax = \"proto3\"; import \"generated/dep.proto\";",
        );

        let seed = tree.seed("svc/root.proto", "root.proto");
        let outcome = closure(&[seed], &[]);
        assert_eq!(outcome.missing.len(), 1, "нашли копию в target/");
    }

    /// Граница проекта — предел подъёма: файл из СОСЕДНЕГО репозитория не
    /// должен находиться сам собой, иначе схема собиралась бы из случайного.
    #[test]
    fn the_search_stops_at_the_project_boundary() {
        let tree = Tree::new("boundary");
        // Общий предок обоих проектов, и в нём — то, что ищет `inner`.
        tree.write("outside/dep.proto", COMMON);
        std::fs::create_dir_all(tree.0.join("inner/.git")).unwrap();
        tree.write(
            "inner/svc/root.proto",
            "syntax = \"proto3\"; import \"outside/dep.proto\";",
        );

        let seed = tree.seed("inner/svc/root.proto", "root.proto");
        let outcome = closure(&[seed], &[]);
        assert_eq!(
            outcome.missing.len(),
            1,
            "поиск вышел за границу проекта: {:?}",
            outcome.found.iter().map(|f| &f.source).collect::<Vec<_>>()
        );
    }

    /// Заранее известный корень включения работает и тогда, когда сам по себе
    /// он не нашёлся бы: на нём держится будущий список в настройках.
    #[test]
    fn an_explicit_include_root_resolves_what_the_walk_cannot() {
        let tree = Tree::new("explicit");
        tree.write("outside/dep.proto", COMMON);
        std::fs::create_dir_all(tree.0.join("inner/.git")).unwrap();
        tree.write(
            "inner/svc/root.proto",
            "syntax = \"proto3\"; import \"outside/dep.proto\";",
        );

        let seed = tree.seed("inner/svc/root.proto", "root.proto");
        let outcome = closure(&[seed], &[tree.0.clone()]);
        assert!(
            outcome.missing.is_empty(),
            "{}",
            outcome.explain().unwrap_or_default()
        );
        assert_eq!(outcome.found[0].import, "outside/dep.proto");
    }
}
