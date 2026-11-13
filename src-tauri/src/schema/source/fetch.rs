//! Дерево .proto из GitLab или GitHub.
//!
//! Архивом, одним запросом: `/repository/archive.tar.gz` у GitLab и `/tarball`
//! у GitHub отдают всё дерево сразу. Ходить по файлам через API дерева плюс
//! «сырое содержимое» значило бы сделать сотню запросов там, где хватает
//! одного, — а на корпоративной сети каждый из них ещё и через инспектор.
//!
//! # Чем отличаются провайдеры
//!
//! Ровно тремя вещами: базой API, адресом архива и заголовком с токеном. Всё
//! остальное — ветка по умолчанию (поле `default_branch` называется одинаково у
//! обоих), пределы, распаковка, снятие верхнего каталога — совпадает буквально,
//! поэтому скачивание здесь одно, а различия собраны в четырёх коротких
//! функциях рядом с ним. Заводить второй модуль ради трёх строк значило бы
//! развести по двум местам то единственное, что у них общего и ломается, —
//! распаковку.
//!
//! Токен — personal access token: у GitLab в заголовке `PRIVATE-TOKEN`, у
//! GitHub в `Authorization: Bearer` (так принимаются и классические, и
//! fine-grained токены, тогда как устаревший `token <...>` — только классические).
//! Лежит в системном хранилище (`config::secrets`), в `sources.json` не попадает
//! и наружу через IPC не уезжает: то же правило, что у паролей кластера.
//!
//! # Перенаправление у GitHub
//!
//! `/tarball` отвечает не архивом, а 302 на `codeload.github.com` с подписанной
//! ссылкой. Работает это на двух умолчаниях `ureq`, и оба нам нужны ровно
//! такими: перенаправления он проходит сам (до десяти), а заголовок с токеном
//! на ЧУЖОЙ хост при этом не переносит (`RedirectAuthHeaders::Never`). Второе
//! важнее: ссылка и так подписана, а токен на сторонний хост уезжать не должен.
//!
//! # Почему дерево приезжает ЦЕЛИКОМ
//!
//! Подкаталог источника (`SchemaSource::subdir`) сюда не передаётся, хотя у
//! архива GitLab есть параметр `path`, который обрезал бы выдачу прямо на
//! сервере (у GitHub такого нет вовсе). Так и было сделано сперва — и оказалось ошибкой: контракты в
//! `internal/` импортируют общие типы из `pkg/`, обрезанный архив приезжал без
//! `pkg/`, и восемнадцать наборов из девятнадцати не разбирались с
//! «`pkg/decimalpb/decimal.proto` is not found in import path». Подкаталог
//! отвечает за то, где искать КОНТРАКТЫ (`tree::entries`), а не за то, что
//! вообще видно импортам; сузить дерево значит сломать резолв.
//!
//! Скачивается при этом архив репозитория целиком, а на диск ложатся только
//! `*.proto` (`unpack`) — пределы стоят и на скачанное, и на отобранное.

use std::io::Read;
use std::time::Duration;

use super::super::http;
use super::store::{MAX_BYTES, MAX_FILES};
use super::types::SourceKind;

/// Потолок ожидания. Не пять секунд, как у реестра: тот стоит на пути
/// отрисовки таблицы, а здесь тянется архив репозитория, и минута на медленной
/// сети — это норма, а не зависание.
const TIMEOUT: Duration = Duration::from_secs(120);

/// Сколько сжатых байт готовы прочитать.
///
/// Предел ставится ДО распаковки и отдельно от предела на распакованное:
/// плотно сжатый архив разворачивается во много раз, и проверять только
/// результат значило бы сначала положить его в память, а потом уже
/// возмутиться.
const MAX_DOWNLOAD: u64 = 64 * 1024 * 1024;

/// Забирает дерево проекта. `git_ref` пуст — спросить ветку по умолчанию.
///
/// Подкаталога здесь нет намеренно — см. шапку модуля.
pub fn repo(
    kind: SourceKind,
    url: &str,
    git_ref: Option<&str>,
    token: Option<&str>,
    ca_bundle: Option<&str>,
) -> Result<Vec<(String, Vec<u8>)>, String> {
    // Папке здесь делать нечего, и молчать об этом нельзя: ниже всё решается
    // через `match` с запасным рукавом на GitLab, то есть папка уехала бы в
    // GitLab-запрос по пути на диске. Разводит их `store::fetch`, а это —
    // страховка на случай, если позовут мимо него.
    if !kind.is_remote() {
        return Err("a folder source is read from disk, not fetched over the network".to_string());
    }
    let (site, project) = split_project(url, kind)?;
    let api = api_base(&site, kind);
    let agent = http::agent(ca_bundle, TIMEOUT)?;

    let git_ref = match git_ref.map(str::trim).filter(|r| !r.is_empty()) {
        Some(explicit) => explicit.to_string(),
        None => default_branch(&agent, &api, &project, kind, token)?,
    };

    let mut response = request(&agent, &archive_url(&api, &project, &git_ref, kind), kind, token)?;
    let status = response.status().as_u16();
    if status != 200 {
        return Err(explain(status, &project, &git_ref, kind));
    }

    let archive = response
        .body_mut()
        .with_config()
        .limit(MAX_DOWNLOAD)
        .read_to_vec()
        .map_err(|e| format!("can't download the archive of {project}: {e}"))?;

    unpack(&archive)
}

/// Как зовут провайдера в тексте ошибки.
fn provider(kind: SourceKind) -> &'static str {
    match kind {
        SourceKind::Github => "GitHub",
        _ => "GitLab",
    }
}

/// База API сервера.
///
/// У GitLab она совпадает с самим сайтом, у GitHub — нет, и это не мелочь:
/// публичный GitHub отвечает на `api.github.com`, а GitHub Enterprise Server —
/// на `<хост>/api/v3`. Различить их можно только по хосту, и другого признака
/// не существует.
fn api_base(site: &str, kind: SourceKind) -> String {
    match kind {
        SourceKind::Github if is_github_com(site) => "https://api.github.com".to_string(),
        SourceKind::Github => format!("{site}/api/v3"),
        _ => site.to_string(),
    }
}

/// Это публичный github.com, а не своя инсталляция.
///
/// Сверяется хост целиком, а не вхождение подстроки: `github.company.com` —
/// это Enterprise Server, и отправить его запросы на `api.github.com` значило
/// бы спросить публичный GitHub про чужой приватный репозиторий.
fn is_github_com(site: &str) -> bool {
    let host = site
        .split_once("://")
        .map_or(site, |(_, rest)| rest)
        .split('/')
        .next()
        .unwrap_or_default()
        .split(':')
        .next()
        .unwrap_or_default()
        .to_ascii_lowercase();
    host == "github.com" || host == "www.github.com"
}

/// Адрес самого проекта — по нему спрашивают ветку по умолчанию.
fn project_url(api: &str, project: &str, kind: SourceKind) -> String {
    match kind {
        SourceKind::Github => format!("{api}/repos/{}", encode_path(project)),
        _ => format!("{api}/api/v4/projects/{}", encode(project)),
    }
}

/// Адрес архива дерева.
///
/// Ветка у GitLab едет параметром запроса, у GitHub — последним сегментом пути,
/// и слэши в ней (`release/2024`) там остаются слэшами: `%2F` этот адрес
/// ломает.
fn archive_url(api: &str, project: &str, git_ref: &str, kind: SourceKind) -> String {
    match kind {
        SourceKind::Github => format!(
            "{api}/repos/{}/tarball/{}",
            encode_path(project),
            encode_path(git_ref)
        ),
        _ => format!(
            "{api}/api/v4/projects/{}/repository/archive.tar.gz?sha={}",
            encode(project),
            encode(git_ref)
        ),
    }
}

/// Ветка проекта по умолчанию.
///
/// Лишний запрос, и он того стоит: у репозиториев то `master`, то `main`, то
/// своё, и заставлять человека это выяснять — значит заставлять его
/// выяснять то, что API скажет само.
///
/// Разбор ответа общий на обоих: поле называется `default_branch` и у GitLab, и
/// у GitHub.
fn default_branch(
    agent: &ureq::Agent,
    api: &str,
    project: &str,
    kind: SourceKind,
    token: Option<&str>,
) -> Result<String, String> {
    let url = project_url(api, project, kind);
    let mut response = request(agent, &url, kind, token)?;
    let status = response.status().as_u16();
    if status != 200 {
        return Err(explain(status, project, "the default branch", kind));
    }
    let body = response.body_mut().read_to_string().map_err(|e| {
        format!("can't read the {} response for {project}: {e}", provider(kind))
    })?;

    #[derive(serde::Deserialize)]
    struct Project {
        default_branch: Option<String>,
    }
    let parsed: Project = serde_json::from_str(&body)
        .map_err(|e| format!("unexpected {} response for {project}: {e}", provider(kind)))?;
    parsed.default_branch.ok_or_else(|| {
        format!("{project} has no default branch — name the branch or tag explicitly")
    })
}

fn request(
    agent: &ureq::Agent,
    url: &str,
    kind: SourceKind,
    token: Option<&str>,
) -> Result<ureq::http::Response<ureq::Body>, String> {
    let mut request = agent.get(url);
    if let Some(token) = token.filter(|t| !t.is_empty()) {
        request = match kind {
            // `Bearer`, а не устаревший `token <...>`: так принимаются и
            // классические PAT, и fine-grained, а вторые — единственный способ
            // выдать доступ на один репозиторий.
            SourceKind::Github => request.header("Authorization", &format!("Bearer {token}")),
            _ => request.header("PRIVATE-TOKEN", token),
        };
    }
    if matches!(kind, SourceKind::Github) {
        // Версию API просит называть сам GitHub: без заголовка он отвечает
        // текущей, а она однажды меняется под ногами.
        request = request
            .header("Accept", "application/vnd.github+json")
            .header("X-GitHub-Api-Version", "2022-11-28");
    }
    request
        .call()
        .map_err(|e| format!("can't reach {} at {url}: {e}", provider(kind)))
}

/// Код ответа своими словами.
///
/// Каждый из них означает своё действие, и «HTTP 404» не говорит ни об одном:
/// у приватного репозитория без токена ответ тот же, что у несуществующего.
///
/// Права называются теми словами, какими они названы у провайдера: советовать
/// обладателю fine-grained токена GitHub «добавить scope read_repository»
/// значило бы отправить его искать то, чего в его интерфейсе нет.
fn explain(status: u16, project: &str, git_ref: &str, kind: SourceKind) -> String {
    let who = provider(kind);
    let scope = match kind {
        SourceKind::Github => "it needs the `repo` scope (classic) or Contents: read (fine-grained)",
        _ => "it needs the read_api or read_repository scope",
    };
    match status {
        401 => format!("{who} did not accept the token for {project} (401) — it may be expired or revoked"),
        403 => format!("the {who} token has no access to {project} (403) — {scope}"),
        404 => format!(
            "{who} has no {project} at {git_ref} (404) — check the project URL, the branch, \
             and that the token can see private repositories"
        ),
        other => format!("{who} answered {other} for {project} at {git_ref}"),
    }
}

/// Делит URL проекта на базу сайта и путь проекта.
///
/// Принимает и то, что копируют из адресной строки: ссылку на ветку или
/// каталог (`…/-/tree/master/api` у GitLab, `…/tree/main/api` у GitHub), строку
/// запроса (`?ref_type=heads`), якорь, `.git` на конце, лишний слэш. Требовать
/// «чистый» URL значило бы требовать от человека знать, какой из них чистый.
///
/// Единственный разбор URL на весь модуль: по нему же собирается и МЕТКА
/// источника (`store::repo_label`). Своя разборка там была и оказалась
/// ошибкой — брала последний сегмент как есть, и ссылка на ветку превращалась в
/// источник по имени `master?ref_type=heads`.
pub(super) fn split_project(url: &str, kind: SourceKind) -> Result<(String, String), String> {
    let url = url.trim().trim_end_matches('/');
    let (scheme, rest) = url
        .split_once("://")
        .ok_or_else(|| format!("{url} is not an http(s) URL"))?;
    let (host, path) = rest
        .split_once('/')
        .ok_or_else(|| format!("{url} names a host but no project"))?;

    // Склейка двух URL: в поле уже стояла приставка `https://<хост>/`,
    // подставленная по прошлому источнику, а человек вставил туда полный адрес
    // целиком. Второй адрес и есть ответ — первый был лишь заготовкой.
    if let Some(at) = path.find("://") {
        let scheme_start = path[..at].rfind('/').map_or(0, |slash| slash + 1);
        return split_project(&path[scheme_start..], kind);
    }

    // Всё, что GitLab дописывает к пути проекта, начинается с `/-/`.
    let path = path.split("/-/").next().unwrap_or(path);
    // Запрос и якорь — не часть пути. `/-/` их чаще всего уносит с собой, но не
    // всегда: ссылка на сам проект приходит и без него (`…/contracts?foo=bar`).
    let path = path.split(['?', '#']).next().unwrap_or(path);
    let path = path.trim_matches('/').trim_end_matches(".git");
    if path.is_empty() {
        return Err(format!("{url} names a host but no project"));
    }

    let path = match kind {
        // У GitHub путь проекта — РОВНО два сегмента, владелец и репозиторий:
        // вложенных групп там нет, а всё, что идёт дальше, приписал интерфейс
        // (`/tree/main/api`, `/blob/...`, `/pull/12`). Разделителя вроде `/-/`
        // перед этим хвостом у GitHub не существует, поэтому отрезать его
        // можно только счётом сегментов.
        SourceKind::Github => {
            let mut parts = path.split('/').filter(|part| !part.is_empty());
            let owner = parts.next().unwrap_or_default();
            let repo = parts
                .next()
                .ok_or_else(|| format!("{url} names {owner} but no repository"))?;
            format!("{owner}/{}", repo.trim_end_matches(".git"))
        }
        _ => path.to_string(),
    };
    Ok((format!("{scheme}://{host}"), path))
}

/// Процентное кодирование для пути проекта и ветки — ОДНИМ сегментом.
///
/// Путь проекта в API GitLab идёт одним сегментом, то есть слэши в нём обязаны
/// стать `%2F`, — именно поэтому нельзя просто подставить его в URL.
fn encode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char)
            }
            other => out.push_str(&format!("%{other:02X}")),
        }
    }
    out
}

/// То же, но слэши остаются слэшами.
///
/// Для GitHub: и путь проекта (`owner/repo`), и ветка (`release/2024`) едут
/// там НЕСКОЛЬКИМИ сегментами пути, и `%2F` вместо разделителя даёт 404.
fn encode_path(value: &str) -> String {
    value
        .split('/')
        .map(encode)
        .collect::<Vec<_>>()
        .join("/")
}

/// Достаёт .proto из `tar.gz`.
///
/// Три вещи, которые здесь обязаны происходить.
///
/// Верхний каталог снимается: GitLab кладёт всё в `project-ref-sha/`, и без
/// этого каждый импорт отсчитывался бы от имени с хэшем коммита — то есть
/// менялся бы при каждом обновлении.
///
/// Берутся только `*.proto`: в репозитории лежит и код, и картинки, а хранить
/// их в каталоге схем незачем.
///
/// Имена проверяются: путь из архива — это ДАННЫЕ, и `..` в нём не должен
/// превращаться в запись куда угодно. Окончательную проверку делает
/// `files::Staged`, но отобрать мусор дешевле здесь, до того как он займёт
/// память.
pub fn unpack(archive: &[u8]) -> Result<Vec<(String, Vec<u8>)>, String> {
    let decoder = flate2::read::GzDecoder::new(archive);
    let mut tar = tar::Archive::new(decoder);
    let entries = tar
        .entries()
        .map_err(|e| format!("the downloaded archive is not a tar: {e}"))?;

    let mut files: Vec<(String, Vec<u8>)> = Vec::new();
    let mut bytes = 0usize;
    for entry in entries {
        let mut entry = entry.map_err(|e| format!("can't read the archive: {e}"))?;
        let path = entry
            .path()
            .map_err(|e| format!("the archive has an unreadable path: {e}"))?
            .to_string_lossy()
            .replace('\\', "/");

        let Some(name) = strip_top(&path) else {
            continue;
        };
        if !name.ends_with(".proto") || !is_safe(name) {
            continue;
        }

        let mut buf = Vec::new();
        entry
            .read_to_end(&mut buf)
            .map_err(|e| format!("can't read {name} from the archive: {e}"))?;

        bytes += buf.len();
        if files.len() >= MAX_FILES || bytes > MAX_BYTES {
            // Подкаталог здесь не поможет и советовать его нельзя: он сужает
            // поиск контрактов, а не выдачу архива, и дерево приедет то же
            // самое. Остаётся назвать предел и репозиторий.
            return Err(format!(
                "this repository holds more .proto than the app will take \
                 ({MAX_FILES} files or {} MiB) — it looks like a monorepo rather than a \
                 contract repository",
                MAX_BYTES / 1024 / 1024
            ));
        }
        files.push((name.to_string(), buf));
    }

    if files.is_empty() {
        return Err("the archive has no .proto files in it".to_string());
    }
    files.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(files)
}

/// Снимает верхний каталог архива. `None` — записи верхнего уровня (сам
/// каталог) и всё, у чего нет вложенного пути.
fn strip_top(path: &str) -> Option<&str> {
    let (_, rest) = path.split_once('/')?;
    (!rest.is_empty()).then_some(rest)
}

fn is_safe(name: &str) -> bool {
    !name.starts_with('/') && !name.split('/').any(|part| part == ".." || part.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_project_url_is_split_into_api_base_and_path() {
        assert_eq!(
            split_project("https://gitlab.example.com/trading/orders-contracts", SourceKind::Gitlab).unwrap(),
            (
                "https://gitlab.example.com".to_string(),
                "trading/orders-contracts".to_string()
            )
        );
    }

    /// То, что копируют из адресной строки: ссылка на ветку, строка запроса,
    /// якорь, `.git`, лишний слэш, пробелы по краям. Всё это один и тот же
    /// проект, и спрашивать с человека «чистый» URL незачем — он не обязан
    /// знать, какой из них чистый.
    #[test]
    fn the_forms_people_actually_paste_are_accepted() {
        let expected = (
            "https://gitlab.example.com".to_string(),
            "group/sub/proj".to_string(),
        );
        for url in [
            "https://gitlab.example.com/group/sub/proj",
            "https://gitlab.example.com/group/sub/proj/",
            "https://gitlab.example.com/group/sub/proj.git",
            "  https://gitlab.example.com/group/sub/proj  ",
            "https://gitlab.example.com/group/sub/proj/-/tree/master/api/proto",
            // Ровно то, что отдаёт кнопка «копировать ссылку» в интерфейсе
            // GitLab: ветка плюс `ref_type`.
            "https://gitlab.example.com/group/sub/proj/-/tree/master?ref_type=heads",
            // Запрос и якорь без `/-/` — их тоже надо снять, иначе они уедут
            // в путь проекта и в метку.
            "https://gitlab.example.com/group/sub/proj?ref_type=heads",
            "https://gitlab.example.com/group/sub/proj#readme",
        ] {
            assert_eq!(split_project(url, SourceKind::Gitlab).unwrap(), expected, "{url}");
        }
    }

    /// Два URL подряд: поле предзаполнено приставкой по прошлому источнику, а
    /// человек вставил полный адрес, не стерев её. Отвечает второй — первый был
    /// заготовкой. Своя беда, заведённая предзаполнением формы, поэтому и
    /// лечится здесь.
    #[test]
    fn a_url_pasted_over_the_prefilled_one_still_resolves() {
        let expected = (
            "https://gitlab.example.com".to_string(),
            "group/proj".to_string(),
        );
        for url in [
            "https://gitlab.example.com/https://gitlab.example.com/group/proj",
            // И когда приставка была от другого сервера.
            "https://old.example.com/https://gitlab.example.com/group/proj",
            "https://old.example.com/group/https://gitlab.example.com/group/proj",
        ] {
            assert_eq!(split_project(url, SourceKind::Gitlab).unwrap(), expected, "{url}");
        }
    }

    #[test]
    fn a_url_without_a_project_is_refused() {
        assert!(split_project("https://gitlab.example.com", SourceKind::Gitlab).is_err());
        assert!(split_project("gitlab.example.com/group/proj", SourceKind::Gitlab).is_err());
    }

    /// У GitHub путь проекта — ровно владелец и репозиторий, а всё, что
    /// дописал интерфейс, отрезается счётом сегментов: разделителя вроде `/-/`
    /// там нет, и опереться больше не на что.
    #[test]
    fn a_github_url_keeps_only_the_owner_and_the_repository() {
        let expected = ("https://github.com".to_string(), "acme/contracts".to_string());
        for url in [
            "https://github.com/acme/contracts",
            "https://github.com/acme/contracts/",
            "https://github.com/acme/contracts.git",
            "  https://github.com/acme/contracts  ",
            // То, что копируют из адресной строки, глядя на файл или ветку.
            "https://github.com/acme/contracts/tree/main",
            "https://github.com/acme/contracts/tree/main/api/proto",
            "https://github.com/acme/contracts/blob/main/api/orders.proto",
            "https://github.com/acme/contracts/pull/12",
            "https://github.com/acme/contracts?tab=readme-ov-file",
            "https://github.com/acme/contracts#readme",
        ] {
            assert_eq!(split_project(url, SourceKind::Github).unwrap(), expected, "{url}");
        }
    }

    /// Своя инсталляция: хост другой, правило пути то же.
    #[test]
    fn a_github_enterprise_url_is_split_the_same_way() {
        assert_eq!(
            split_project("https://github.company.com/platform/contracts/tree/main", SourceKind::Github)
                .unwrap(),
            (
                "https://github.company.com".to_string(),
                "platform/contracts".to_string()
            )
        );
    }

    /// Владелец без репозитория — это не проект, и 404 по такому адресу
    /// объяснял бы не то.
    #[test]
    fn a_github_url_without_a_repository_is_refused() {
        assert!(split_project("https://github.com/acme", SourceKind::Github).is_err());
        assert!(split_project("https://github.com", SourceKind::Github).is_err());
    }

    /// Публичный GitHub отвечает на отдельном хосте, Enterprise — на своём, по
    /// пути `/api/v3`. Перепутать их значит спросить публичный GitHub про чужой
    /// приватный репозиторий.
    #[test]
    fn the_api_base_tells_public_github_from_an_enterprise_server() {
        assert_eq!(
            api_base("https://github.com", SourceKind::Github),
            "https://api.github.com"
        );
        assert_eq!(
            api_base("https://www.github.com", SourceKind::Github),
            "https://api.github.com"
        );
        // Имя, в котором `github.com` лишь встречается, — это чужой сервер.
        assert_eq!(
            api_base("https://github.company.com", SourceKind::Github),
            "https://github.company.com/api/v3"
        );
        assert_eq!(
            api_base("https://not-github.com", SourceKind::Github),
            "https://not-github.com/api/v3"
        );
        // У GitLab база и есть сам сайт.
        assert_eq!(
            api_base("https://gitlab.example.com", SourceKind::Gitlab),
            "https://gitlab.example.com"
        );
    }

    /// Адреса архива у провайдеров разные, и ветка в них кодируется по-разному:
    /// у GitLab это параметр запроса (слэш → `%2F`), у GitHub — сегменты пути,
    /// где `%2F` даёт 404.
    #[test]
    fn the_archive_url_matches_what_each_provider_serves() {
        assert_eq!(
            archive_url("https://api.github.com", "acme/contracts", "release/2024", SourceKind::Github),
            "https://api.github.com/repos/acme/contracts/tarball/release/2024"
        );
        assert_eq!(
            archive_url(
                "https://gitlab.example.com",
                "group/sub/proj",
                "release/2024",
                SourceKind::Gitlab
            ),
            "https://gitlab.example.com/api/v4/projects/group%2Fsub%2Fproj\
             /repository/archive.tar.gz?sha=release%2F2024"
        );
    }

    /// Совет про права называется словами того провайдера, у которого их
    /// выдают: `read_repository` в интерфейсе GitHub искать негде.
    #[test]
    fn the_scope_advice_names_what_the_provider_calls_it() {
        let github = explain(403, "acme/contracts", "main", SourceKind::Github);
        assert!(github.contains("GitHub"), "{github}");
        assert!(github.contains("Contents: read"), "{github}");

        let gitlab = explain(403, "group/proj", "master", SourceKind::Gitlab);
        assert!(gitlab.contains("GitLab"), "{gitlab}");
        assert!(gitlab.contains("read_repository"), "{gitlab}");
    }

    /// Путь проекта едет в API одним сегментом: слэши обязаны стать `%2F`.
    #[test]
    fn the_project_path_is_encoded_as_a_single_segment() {
        assert_eq!(encode("group/sub/proj"), "group%2Fsub%2Fproj");
        assert_eq!(encode("release-1.2"), "release-1.2");
        assert_eq!(encode("feature/привет"), "feature%2F%D0%BF%D1%80%D0%B8%D0%B2%D0%B5%D1%82");
    }

    /// Архив собирается прямо здесь: проверять распаковку по сети незачем, а
    /// `tar` и `flate2` умеют и писать.
    fn archive(files: &[(&str, &str)]) -> Vec<u8> {
        let mut builder = tar::Builder::new(Vec::new());
        for (name, text) in files {
            let mut header = tar::Header::new_gnu();
            header.set_size(text.len() as u64);
            header.set_mode(0o644);
            header.set_cksum();
            builder
                .append_data(&mut header, name, text.as_bytes())
                .unwrap();
        }
        let tar = builder.into_inner().unwrap();

        let mut encoder =
            flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        std::io::Write::write_all(&mut encoder, &tar).unwrap();
        encoder.finish().unwrap()
    }

    /// Главное свойство распаковки: верхний каталог с хэшем коммита снят, иначе
    /// имена файлов менялись бы при каждом обновлении — а по ним резолвятся
    /// импорты.
    #[test]
    fn the_archive_top_directory_is_stripped() {
        let gz = archive(&[
            ("proj-master-abc123/orders.proto", "syntax = \"proto3\";"),
            ("proj-master-abc123/common/types.proto", "syntax = \"proto3\";"),
        ]);

        let files = unpack(&gz).unwrap();
        let names: Vec<&str> = files.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(names, ["common/types.proto", "orders.proto"]);
    }

    #[test]
    fn everything_that_is_not_a_proto_is_dropped() {
        let gz = archive(&[
            ("proj/README.md", "docs"),
            ("proj/orders.proto", "syntax = \"proto3\";"),
            ("proj/build/generated.pb.go", "code"),
        ]);

        let files = unpack(&gz).unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].0, "orders.proto");
    }

    /// Путь из архива — данные. Выходить по нему за пределы каталога нельзя
    /// даже одним файлом.
    ///
    /// Проверяется сама защита, а не путь через `unpack`: собрать такой архив
    /// здесь нечем — писатель `tar` сам отказывается класть `..` в имя. Это
    /// добрый знак, но ЧУЖОЙ архив такой гарантии не даёт, и полагаться на
    /// вежливость того, кто его собрал, нельзя.
    #[test]
    fn a_path_escaping_the_tree_is_refused() {
        assert!(!is_safe("../../etc/passwd.proto"));
        assert!(!is_safe("api/../../../evil.proto"));
        assert!(!is_safe("/etc/evil.proto"));
        assert!(!is_safe("api//evil.proto"));
        assert!(is_safe("common/types.proto"));
        assert!(is_safe("orders.proto"));
    }

    /// Верхний каталог — это именно первый сегмент, и у записи самого каталога
    /// вложенного пути нет вовсе.
    #[test]
    fn stripping_the_top_directory_handles_the_directory_entry_itself() {
        assert_eq!(strip_top("proj-master-abc/orders.proto"), Some("orders.proto"));
        assert_eq!(
            strip_top("proj-master-abc/common/types.proto"),
            Some("common/types.proto")
        );
        assert_eq!(strip_top("proj-master-abc/"), None);
        assert_eq!(strip_top("orders.proto"), None);
    }

    #[test]
    fn an_archive_without_protos_says_so() {
        let gz = archive(&[("proj/README.md", "docs")]);
        assert!(unpack(&gz).unwrap_err().contains("no .proto"));
    }
}
