use serde::{Deserialize, Serialize};

use crate::kafka::MessageHeader;
use crate::schema::BodyFormat;

/// Запись индекса: всё про сохранённое сообщение, кроме его тела.
///
/// Тело живёт отдельным файлом и хранится СЫРЫМИ БАЙТАМИ (см. `store`), поэтому
/// здесь лежит только то, что уже текст и от схемы не зависит, — плюс `preview`
/// и `binary`, посчитанные схемой на момент сохранения. Их правит `store::list`,
/// когда схема у топика появилась или пропала.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct FavoriteRecord {
    pub id: String,
    /// Ключ кластера — тот же, которым привязаны схемы топиков: у сохранённого
    /// подключения это его id, у подключения из формы — показанное имя.
    pub cluster: String,
    /// Как кластер назывался в момент сохранения. Идентификатор вида
    /// `1735-a3f2` глазу ничего не сообщает, а список общий на все кластеры.
    pub cluster_name: String,
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    /// Unix millis — того сообщения, а не момента сохранения.
    pub timestamp: i64,
    pub key: String,
    pub preview: String,
    /// Превью считалось при наличии схемы у топика. Не «разобралось», а именно
    /// «схема была»: иначе сообщение, которое схеме не поддалось, пересчитывалось
    /// бы на каждом открытии списка и никогда не сходилось.
    #[serde(default)]
    pub with_schema: bool,
    /// Размер тела в байтах — столько же, сколько занимает файл с ним.
    pub value_size: usize,
    pub binary: bool,
    /// Заголовки — тоже текст, и тоже независимы от схемы. Лежат в индексе, а не
    /// рядом с телом: их единицы, и второй файл на запись они не окупают.
    #[serde(default)]
    pub headers: Vec<MessageHeader>,
    /// Когда сохранили, RFC 3339.
    pub saved_at: String,
}

impl FavoriteRecord {
    /// То, что делает сообщение тем же самым сообщением. Повторное сохранение
    /// по этому набору и узнаётся.
    pub fn same_message(&self, cluster: &str, topic: &str, partition: i32, offset: i64) -> bool {
        self.cluster == cluster
            && self.topic == topic
            && self.partition == partition
            && self.offset == offset
    }
}

/// Запись в том виде, в каком её видит список.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct FavoriteView {
    #[serde(flatten)]
    pub record: FavoriteRecord,
    /// Сколько запись занимает на диске ПРЯМО СЕЙЧАС — размер файла тела.
    /// Не хранится в индексе: записанное однажды число врёт после любой правки
    /// каталога руками, а `stat` на пару сотен файлов бесплатен.
    pub bytes: u64,
    /// Файла тела нет. Запись всё равно показываем: иначе пользователю нечего
    /// удалить, а строчка в индексе так и останется висеть.
    pub missing: bool,
}

/// Весь архив: и список, и то, во что он обходится.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct FavoritesView {
    /// Порядок хранения — от старых к новым. Сортировкой заведует список.
    pub items: Vec<FavoriteView>,
    /// Сумма файлов тел вместе с самим индексом: пользователь спрашивает,
    /// сколько занимает избранное, а не сколько занимают его тела.
    pub total_bytes: u64,
}

/// Что сохранять.
///
/// Одной структурой, а не россыпью аргументов: половина полей описывает, ГДЕ
/// взять сообщение (индекс строки плюс координаты для сверки), и разъезжаться
/// им нельзя.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct SaveFavoriteRequest {
    pub cluster: String,
    pub cluster_name: String,
    pub topic: String,
    /// Индекс строки в текущем представлении — по нему воркер отдаёт сырые байты.
    pub index: usize,
    /// Ожидаемые координаты сообщения. Индекс живёт ровно до ближайшей смены
    /// фильтра или сортировки, а модалка к этому моменту может стоять открытой.
    pub partition: i32,
    pub offset: i64,
}

/// Ответ на сохранение.
///
/// `updated` отличает «сохранили» от «обновили уже сохранённое»: без этого
/// признака повторный клик по звезде выглядел бы как добавление второй копии,
/// хотя записей по-прежнему одна.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct SaveFavoriteResult {
    #[serde(flatten)]
    pub favorites: FavoritesView,
    pub id: String,
    pub updated: bool,
}

/// Открытое сохранённое сообщение.
///
/// Тело собрано ПРЯМО СЕЙЧАС и по той схеме, которая привязана к топику
/// сегодня: на диске лежат сырые байты, а не наш вчерашний способ их показать.
/// Формат едет рядом, потому что окно просмотра решает по нему, раскладывать
/// тело отступами или показывать как есть.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct SavedMessage {
    #[serde(flatten)]
    pub message: crate::kafka::FullMessage,
    pub format: BodyFormat,
}
