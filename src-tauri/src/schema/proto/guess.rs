//! Подбор message, которым записан топик, по телам его сообщений.
//!
//! Нужен там, где спросить больше некого. Тело голого protobuf не несёт ни
//! имени своего типа, ни id схемы: в отличие от Avro и JSON Schema, где за
//! confluent-заголовком стоит реестр, здесь внешнего источника истины нет
//! вовсе. Имена топиков с именами message тоже соотносятся как придётся.
//! Остаются сами байты — и то, насколько они легли на кандидата.
//!
//! Главное свойство задачи названо в `decoder`: protobuf разберёт чужое тело
//! охотно и без единой жалобы, выдав правдоподобный мусор. Поэтому
//! «разобралось» здесь не ответ, а лишь пропуск к остальным проверкам.
//!
//! И по той же причине ни одна из проверок не складывается в одно число
//! «уверенность»: наружу едут все счётчики до единого, ничья объявляется
//! ничьёй (`Candidate::rank`), а выбор остаётся за человеком. Процент без
//! расшифровки в этом деле — обещание, которое нечем подкрепить: два
//! структурно одинаковых типа неразличимы по байтам в принципе, сколько ни
//! считай.
//!
//! Главное же, чего одними байтами не решить, решается устройством схемы:
//! конверт и его внутренний тип ложатся на тело одинаково, а загруженный
//! .proto сплошь и рядом урезан, так что чужих байт хватает и у верного типа.
//! Поэтому первым в `Candidate::rank` стоит корень набора
//! (`linked::Linked::root`), а данные — после него.

use protobuf::reflect::{MessageDescriptor, ReflectFieldRef, ReflectValueRef};
use protobuf::MessageDyn;
use serde::Serialize;

use crate::schema::wire::{proto_framing, ProtoFraming};

/// Сколько тел брать на пробу.
///
/// Снизу предел ставит разнообразие: топик с конвертом `oneof` несёт в себе
/// несколько видов сообщений, и по трём телам подряд легко увидеть один-
/// единственный. Сверху — ничего особенного не ставит, разбор тела стоит
/// микросекунды; но и расти числу незачем, потому что чужой тип отваливается на
/// первых же телах, а неразличимый не отвалится и на тысяче.
pub const WANTED_SAMPLES: usize = 32;

/// Докуда спускаться, считая чужие байты во вложенных сообщениях.
///
/// Сам разбор ограничен рекурсией парсера, так что дерево и без того конечно;
/// предел здесь — страховка от того, чтобы обход стоил больше самого разбора.
const MAX_DEPTH: usize = 32;

/// Насколько хорошо тип лёг на данные: пять долей в промилле, от важной к
/// второстепенной. См. `Fit::quality_key`.
type QualityKey = (u64, u64, u64, u64, u64);

/// Место кандидата в списке целиком: устройство схемы, затем данные.
/// См. `Candidate::rank`.
type RankKey = (bool, u64, bool, bool, QualityKey);

/// Насколько тип подошёл телам топика.
///
/// Счётчики, а не оценка: каждый отвечает на свой вопрос, и складывать их в
/// одно число значило бы спрятать, какой именно из них просел. Ранжирование
/// всё-таки нужно, и его даёт `quality_key` — но и он остаётся кортежем, по
/// которому видно, чем один кандидат лучше другого.
#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct Fit {
    /// Сколько тел оценивали.
    pub samples: usize,
    /// Из скольких тело разобралось.
    ///
    /// Меньше `samples` — повод для подозрений, но НЕ приговор, и это стоило
    /// одной неудачной итерации. Сперва такие кандидаты не показывались вовсе
    /// («топик пишется одним типом»), и на живых топиках это обернулось тем,
    /// что верный тип молча исчезал из списка: в настоящем топике попадаются и
    /// сообщения другого вида, и хвосты старого контракта, и одного такого тела
    /// хватало, чтобы вычеркнуть правильный ответ.
    pub parsed: usize,
    /// Сколько тел разобралось БЕЗ единого чужого байта.
    ///
    /// Главный признак качества разбора и первая составляющая `quality_key`.
    /// Он же объясняет, почему «разобралось не всё» не приговор: чужой тип
    /// разбирает охотно, но грязно, и доля чистых разборов у него
    /// околонулевая, тогда как у верного типа, споткнувшегося об одно тело из
    /// тридцати двух, — тридцать один из тридцати двух.
    ///
    /// Чего по нему судить НЕЛЬЗЯ — верный ли тип вообще. Загруженный .proto
    /// сплошь и рядом урезан, и у верного типа чистых разборов не бывает вовсе;
    /// см. `Candidate::rank`.
    pub clean: usize,
    /// У скольких тел обратная сборка дала те же байты.
    ///
    /// Сильный признак попадания: большинство сериализаторов пишут поля по
    /// возрастанию номера, и совпасть байт в байт случайному типу нечем. Но
    /// именно признак, а не требование — порядок полей на проводе ничем не
    /// закреплён, и верный тип вполне может его не воспроизвести.
    pub exact: usize,
    /// Сколько байт ушло в поля, которых в типе нет, — вместе с вложенными.
    ///
    /// Главный сигнал всего подбора. У верного типа это обычно ноль, у чужого —
    /// почти всё тело, потому что чужие теги парсер складывает в unknown
    /// fields, а не отвергает.
    pub unknown_bytes: u64,
    /// Сколько байт было всего (после снятия confluent-обёртки).
    pub body_bytes: u64,
    /// Сколько полей объявлено в типе на верхнем уровне.
    pub fields_declared: usize,
    /// Сколько из них встретилось хоть в одном теле.
    ///
    /// Слабый сигнал, и годится только на разрешение ничьей: разреженное тело —
    /// обычное дело, а в proto3 поля со значением по умолчанию не пишутся
    /// вовсе, так что низкое покрытие само по себе ещё ничего не значит.
    pub fields_seen: usize,
}

impl Fit {
    /// Доля тел, разобранных начисто.
    pub fn clean_ratio(&self) -> f64 {
        match self.samples {
            0 => 0.0,
            samples => self.clean as f64 / samples as f64,
        }
    }

    pub fn parsed_ratio(&self) -> f64 {
        match self.samples {
            0 => 0.0,
            samples => self.parsed as f64 / samples as f64,
        }
    }

    /// Доля байт, ушедших в чужие поля.
    pub fn unknown_ratio(&self) -> f64 {
        match self.body_bytes {
            0 => 0.0,
            total => self.unknown_bytes as f64 / total as f64,
        }
    }

    pub fn exact_ratio(&self) -> f64 {
        match self.samples {
            0 => 0.0,
            samples => self.exact as f64 / samples as f64,
        }
    }

    pub fn coverage(&self) -> f64 {
        match self.fields_declared {
            0 => 0.0,
            declared => self.fields_seen as f64 / declared as f64,
        }
    }

    /// Насколько хорошо тип лёг на ДАННЫЕ. Чем больше, тем лучше.
    ///
    /// Доли округлены до промилле намеренно — по этому ключу определяется ещё и
    /// НИЧЬЯ, а сравнивать `f64` на точное равенство значило бы объявить
    /// разными кандидатов, разошедшихся на 1e-17.
    ///
    /// Только про данные: устройство схемы (свой ли это набор, корень ли он)
    /// сюда не входит и учитывается выше, в `Candidate::rank`.
    ///
    /// Первой стоит доля ЧИСТЫХ разборов, а не доля разборов вообще, и это
    /// важно: иначе чужой тип, который разбирает все тела подряд, но почти
    /// целиком в чужие поля, обошёл бы верный тип, споткнувшийся об одно
    /// сообщение из тридцати двух. Чистота важнее полноты, и только потом идёт
    /// полнота — она отделяет «разобралось грязно» от «не разобралось вовсе».
    pub fn quality_key(&self) -> QualityKey {
        (
            per_mille(self.clean_ratio()),
            per_mille(self.parsed_ratio()),
            1000 - per_mille(self.unknown_ratio()),
            per_mille(self.exact_ratio()),
            per_mille(self.coverage()),
        )
    }
}

/// Кандидат вместе с ответом на вопрос «а чей это .proto».
///
/// Откуда взят набор — не украшение: имя типа само по себе не говорит, где
/// лежат его файлы, а предложить тип, файлов которого у топика нет, значит
/// предложить работу, которую ещё надо сделать. Форма по этому полю и решает,
/// можно ли кандидата просто выбрать или сперва придётся загрузить .proto.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct Candidate {
    /// Полное имя message.
    pub message: String,
    /// Топик, которому загружен набор, где этот тип объявлен.
    pub from_topic: String,
    /// Набор принадлежит тому самому топику, которому подбирают. Такого
    /// кандидата можно выбрать прямо в селекторе — он уже в его списке.
    pub own: bool,
    /// Это ЕДИНСТВЕННЫЙ корневой тип своего набора (`linked::Linked::root`).
    ///
    /// Разрешает ничью там, где байты бессильны, и именно этот случай и был
    /// главной жалобой: набор из обёртки и её внутренних типов ложится на тело
    /// одинаково чисто и обёрткой, и внутренним типом — тот просто видит поле
    /// 1 как своё. Различить их по данным нельзя, а по устройству схемы —
    /// можно: топик читается конвертом.
    pub root: bool,
    pub fit: Fit,
}

impl Candidate {
    /// Место кандидата в списке. Чем больше, тем выше.
    ///
    /// Первым стоит не качество разбора, а УСТРОЙСТВО СХЕМЫ — корень набора,
    /// загруженного этому самому топику, если он вообще что-то разобрал. Так
    /// решено по замеру: правило корня совпало с назначенным вручную типом
    /// четырнадцать раз из четырнадцати, а чистота разбора у тех же топиков
    /// гуляет как угодно.
    ///
    /// Гуляет она по понятной причине, и это главное, что здесь надо знать:
    /// загруженный .proto сплошь и рядом УРЕЗАН — в нём оставлены нужные поля,
    /// а остальные выброшены. Тело от этого разбирается правильно, но половина
    /// байт уходит в поля, которых в типе нет. Чужой тип, который видит то же
    /// тело одним непрозрачным `bytes`, по чистоте выглядит безупречно — и
    /// обошёл бы верный, стой качество разбора первым.
    ///
    /// Остальное — запасной порядок на случай, когда своего набора у топика нет
    /// вовсе: доля разобранного, свой ли набор, корень ли, и только потом сами
    /// данные.
    pub fn rank(&self) -> RankKey {
        (
            self.own && self.root && self.fit.parsed > 0,
            per_mille(self.fit.parsed_ratio()),
            self.own,
            self.root,
            self.fit.quality_key(),
        )
    }
}

/// Чем закончился подбор.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct Guess {
    /// По скольким телам подбирали. Число важно само по себе: подбор по трём
    /// сообщениям и по двумстам — разговоры разной силы, и форма обязана
    /// сказать, какой из них идёт.
    pub samples: usize,
    /// Кандидаты от лучшего к худшему: сперва разобравшие все тела, затем
    /// справившиеся с частью.
    pub candidates: Vec<Candidate>,
    /// Сколько кандидатов делят первое место.
    ///
    /// Отдельным числом, а не «пусть пользователь сравнит столбцы»: ничья здесь
    /// — частый и законный исход (структурно одинаковые типы неразличимы в
    /// принципе), и сказать о ней надо словами, иначе верхняя строка списка
    /// читается как ответ.
    pub tied_at_top: usize,
    /// Сколько кандидатов из ЧУЖИХ наборов не поместилось в список.
    ///
    /// Молчаливого обрезания здесь быть не должно: «показали всё, что подошло»
    /// и «показали первые десять из сотни» читаются одинаково, а значат
    /// противоположное.
    pub dropped: usize,
    /// Сколько кандидатов объясняют тела ЦЕЛИКОМ: разобрали все до единого и не
    /// оставили ни одного чужого байта.
    ///
    /// Ноль — повод сказать об этом вслух, и это не придирка. Подбор на
    /// настоящем топике показал ровно такую беду: верный тип выбыл (его .proto
    /// оказался урезанной копией контракта), а наверху списка встал чужой,
    /// «разобравший» треть тела в мусор. Список без этого числа читается как
    /// «вот ваш тип», хотя верный ответ — «ни один не подходит, нужного .proto
    /// здесь нет».
    pub fitting: usize,
}

/// Сколько ЧУЖИХ кандидатов показывать.
///
/// Чужие — из наборов, загруженных другим топикам. Они нужны ровно для одного
/// случая: у топика своего .proto нет вовсе, и тогда это единственное, из чего
/// выбирать. А когда свой набор есть, они сплошь шум — на живом топике их
/// набралось больше сотни, и список перестал читаться совсем.
///
/// Остаток не скрывается молча: `Guess::dropped`.
const MAX_FOREIGN: usize = 10;

impl Guess {
    /// Ранжирует и сворачивает повторы.
    ///
    /// Повторы неизбежны: файлы схемы копируются в каталог каждого топика, и
    /// один и тот же контракт лежит в наборах нескольких топиков сразу. Два
    /// одинаковых имени в списке — это не два ответа, а один; остаётся лучший
    /// из них, он же называет свой набор.
    pub fn new(samples: usize, mut candidates: Vec<Candidate>) -> Self {
        candidates.sort_by(|a, b| {
            b.rank()
                .cmp(&a.rank())
                // Имя — ради устойчивости порядка: обход каталогов его не
                // обещает, а список уезжает человеку на экран.
                .then_with(|| a.message.cmp(&b.message))
        });
        // Через множество, а не `dedup_by`: тот сворачивает только соседей, а
        // копии одного контракта разъезжаются по списку, стоит им разойтись
        // версией — и тогда одно имя показалось бы дважды.
        let mut seen = std::collections::HashSet::new();
        candidates.retain(|candidate| seen.insert(candidate.message.clone()));

        // Разобравшие всё — все до единого: их и пришли смотреть, и отрезать
        // тут нечего, даже если их сорок (сорок одинаково подходящих — это
        // ответ «различить нечем», а не повод показать первые восемь).
        // Частичные — горстью, с честным остатком.
        //
        // Счётчиком по уже отсортированному, а не обрезанием хвоста: полные
        // кандидаты НЕ обязаны идти первыми. Тип, разобравший все тела, но
        // целиком в чужие поля, стоит ниже того, кто споткнулся об одно тело,
        // а остальные разобрал начисто, — так и задумано (`rank_key`), и резать
        // список по числу полных значило бы отрезать как раз их.
        // Свой набор остаётся целиком и при любых числах: человек видит его
        // своими глазами, и пропавший из списка тип — это вопрос без ответа.
        // Чужих — горсть лучших, по уже посчитанному порядку.
        let foreign = candidates.iter().filter(|c| !c.own).count();
        let mut kept = 0;
        candidates.retain(|candidate| {
            if candidate.own {
                return true;
            }
            kept += 1;
            kept <= MAX_FOREIGN
        });

        // Ничья — это когда различить нечем НИЧЕМ: ни данными, ни устройством
        // схемы. Корень, обошедший своё же нутро, ничьёй не считается — в том и
        // смысл правила.
        let tied_at_top = match candidates.first() {
            None => 0,
            Some(best) => candidates
                .iter()
                .take_while(|c| c.rank() == best.rank())
                .count(),
        };
        let fitting = candidates
            .iter()
            .filter(|c| c.fit.samples > 0 && c.fit.clean == c.fit.samples)
            .count();

        Self {
            samples,
            candidates,
            tied_at_top,
            dropped: foreign.saturating_sub(MAX_FOREIGN),
            fitting,
        }
    }
}

/// Доля в промилле, с потолком и полом: доля приходит из деления, а деление
/// бывает и чуть больше единицы (чужих байт насчиталось больше, чем тело, —
/// такое возможно, потому что размер unknown fields считается с их тегами).
fn per_mille(ratio: f64) -> u64 {
    (ratio * 1000.0).round().clamp(0.0, 1000.0) as u64
}

/// Примеряет один тип к телам.
///
/// `samples` — тела как они приехали из Kafka, вместе с обёрткой, если она
/// была: снимается она здесь, тем же `proto_framing`, которым её снимает показ.
/// Иначе подбор и показ видели бы разное, и выбранный подбором тип не
/// декодировал бы топик.
///
/// Пустые тела вызывающий обязан отсеять сам: они подходят любому типу и
/// засчитывались бы попаданием всем подряд.
pub fn fit(message: &MessageDescriptor, samples: &[Vec<u8>]) -> Fit {
    let mut fit = Fit {
        samples: samples.len(),
        fields_declared: message.fields().count(),
        ..Fit::default()
    };
    // Поля считаются по всему набору, а не по каждому телу: одно сообщение
    // заполняет не все поля, и покрытие по одному телу говорило бы не о типе, а
    // о конкретном сообщении.
    let mut seen: Vec<bool> = vec![false; fit.fields_declared];

    for sample in samples {
        let datum = match proto_framing(sample) {
            ProtoFraming::Confluent { datum, .. } => datum,
            ProtoFraming::Bare(body) => body,
        };
        fit.body_bytes += datum.len() as u64;

        let Ok(parsed) = message.parse_from_bytes(datum) else {
            continue;
        };
        // Необязательная в proto3, но обязательная в proto2 проверка: тело без
        // required-поля контракту не удовлетворяет, и считать его разобранным
        // значило бы предложить тип, который на этом топике будет спотыкаться.
        if parsed.check_initialized_dyn().is_err() {
            continue;
        }

        fit.parsed += 1;
        // По каждому телу отдельно, а не по сумме: тридцать один чистый разбор
        // и один грязный — это совсем не то же самое, что тридцать два слегка
        // грязных, а в сумме байт они выглядят похоже.
        let unknown = unknown_bytes(&*parsed, 0);
        if unknown == 0 {
            fit.clean += 1;
        }
        fit.unknown_bytes += unknown;
        if parsed.write_to_bytes_dyn().is_ok_and(|back| back == datum) {
            fit.exact += 1;
        }
        for (index, field) in message.fields().enumerate() {
            if !seen[index] && present(&field.get_reflect(&*parsed)) {
                seen[index] = true;
            }
        }
    }

    fit.fields_seen = seen.iter().filter(|hit| **hit).count();
    fit
}

/// Есть ли у поля значение в этом теле.
///
/// У динамического сообщения это настоящая проверка присутствия, а не сравнение
/// с нулём: разобранное поле лежит в своей ячейке, неразобранное — пусто
/// (`reflect::dynamic`). Для proto3 это и нужно: ноль там на проводе не пишется,
/// так что «поле присутствует» и означает «поле встретилось в байтах».
fn present(field: &ReflectFieldRef<'_>) -> bool {
    match field {
        ReflectFieldRef::Optional(value) => value.value().is_some(),
        ReflectFieldRef::Repeated(values) => !values.is_empty(),
        ReflectFieldRef::Map(entries) => !entries.is_empty(),
    }
}

/// Сколько байт в этом сообщении и во всех вложенных ушло в чужие поля.
///
/// Спуск по вложенным обязателен, и это не педантизм: конверт из одного поля
/// (`Envelope { Payload body = 1; }`) на верхнем уровне ляжет без единого
/// чужого байта вообще на любые данные, и вся разница между кандидатами сидит
/// как раз внутри. Считать только верхний уровень значило бы не различать
/// конверты совсем.
fn unknown_bytes(message: &dyn MessageDyn, depth: usize) -> u64 {
    let mut total = protobuf::rt::unknown_fields_size(message.unknown_fields_dyn());
    if depth >= MAX_DEPTH {
        return total;
    }
    for field in message.descriptor_dyn().fields() {
        match field.get_reflect(message) {
            ReflectFieldRef::Optional(value) => {
                if let Some(value) = value.value() {
                    total += nested(value, depth);
                }
            }
            ReflectFieldRef::Repeated(values) => {
                for value in &values {
                    total += nested(value, depth);
                }
            }
            // Ключ map'а скаляр по определению формата, вложенного в нём быть
            // не может — обходим только значения.
            ReflectFieldRef::Map(entries) => {
                for (_, value) in &entries {
                    total += nested(value, depth);
                }
            }
        }
    }
    total
}

fn nested(value: ReflectValueRef<'_>, depth: usize) -> u64 {
    match value.to_message() {
        Some(message) => unknown_bytes(&*message, depth + 1),
        None => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::proto::linked::{self, Linked};
    use crate::schema::types::SchemaFile;
    use std::path::PathBuf;

    /// Каталог с одним .proto, разобранный тем же кодом, что и настоящая схема
    /// топика. Разбирать отдельным способом нельзя: подбор обязан видеть ровно
    /// те типы, которые видит приложение.
    struct Schema {
        dir: PathBuf,
        linked: Linked,
    }

    impl Schema {
        fn new(name: &str, text: &str) -> Self {
            let dir = std::env::temp_dir().join(format!("mikui-guess-test-{name}"));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join("t.proto"), text).unwrap();

            let files = vec![SchemaFile {
                name: "t.proto".to_string(),
                source: dir.join("t.proto").display().to_string(),
                auto: false,
                pasted: false,
            }];
            let linked = linked::parse(&dir, &files).unwrap();
            Self { dir, linked }
        }

        fn message(&self, name: &str) -> MessageDescriptor {
            self.linked.message(name).expect(name)
        }
    }

    impl Drop for Schema {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    /// Тело собирается руками, а не сериализатором: тест о том, как читаются
    /// БАЙТЫ, и строить их тем же кодом, который их потом разбирает, значило бы
    /// проверять согласие библиотеки с самой собой.
    fn tag(number: u32, wire: u8) -> u8 {
        u8::try_from(number << 3).unwrap() | wire
    }

    fn varint(number: u32, value: u8) -> Vec<u8> {
        vec![tag(number, 0), value]
    }

    fn bytes(number: u32, payload: &[u8]) -> Vec<u8> {
        let mut out = vec![tag(number, 2), u8::try_from(payload.len()).unwrap()];
        out.extend_from_slice(payload);
        out
    }

    /// Верный тип обязан выигрывать у типа, который те же байты разберёт, но
    /// целиком в чужие поля.
    #[test]
    fn the_right_type_wins_over_a_stranger() {
        let schema = Schema::new(
            "stranger",
            r#"
                syntax = "proto3";
                package t;
                message Order { string id = 1; int64 amount = 2; }
                message Trade { bytes blob = 5; }
            "#,
        );
        let mut body = bytes(1, b"abc");
        body.extend(varint(2, 7));

        let order = fit(&schema.message("t.Order"), std::slice::from_ref(&body));
        let trade = fit(&schema.message("t.Trade"), std::slice::from_ref(&body));

        assert!((order.parsed == order.samples) && (trade.parsed == trade.samples), "оба типа разбирают это тело");
        assert_eq!(order.unknown_bytes, 0);
        assert_eq!(trade.unknown_bytes, body.len() as u64);
        assert!(order.quality_key() > trade.quality_key());
    }

    /// Структурно одинаковые типы по байтам неразличимы вообще никак. Ответ —
    /// ничья, а не произвольный победитель: молча выбрать из двух значило бы
    /// половину таких случаев декодировать чужим типом.
    #[test]
    fn structurally_identical_types_tie() {
        let schema = Schema::new(
            "tie",
            r#"
                syntax = "proto3";
                package t;
                message A { string id = 1; int64 ts = 2; }
                message B { string id = 1; int64 ts = 2; }
            "#,
        );
        let mut body = bytes(1, b"xyz");
        body.extend(varint(2, 9));

        let a = fit(&schema.message("t.A"), std::slice::from_ref(&body));
        let b = fit(&schema.message("t.B"), std::slice::from_ref(&body));

        assert_eq!(a.quality_key(), b.quality_key());
    }

    /// Тип без полей разбирает всё на свете. Проиграть он обязан всем.
    #[test]
    fn a_fieldless_type_loses_to_a_real_one() {
        let schema = Schema::new(
            "empty",
            r#"
                syntax = "proto3";
                package t;
                message Empty {}
                message Order { string id = 1; }
            "#,
        );
        let body = bytes(1, b"abc");

        let empty = fit(&schema.message("t.Empty"), std::slice::from_ref(&body));
        let order = fit(&schema.message("t.Order"), std::slice::from_ref(&body));

        assert!((empty.parsed == empty.samples), "пустой тип разбирает что угодно — в этом и дело");
        assert!(order.quality_key() > empty.quality_key());
    }

    /// Чужие поля внутри вложенного сообщения обязаны считаться: конверт из
    /// одного поля на верхнем уровне чист всегда, и без спуска вниз два конверта
    /// были бы неотличимы.
    #[test]
    fn unknown_fields_inside_a_nested_message_are_counted() {
        let schema = Schema::new(
            "nested",
            r#"
                syntax = "proto3";
                package t;
                message Outer { Inner inner = 1; }
                message Inner { int32 a = 1; }
            "#,
        );
        // Внутри конверта — своё поле 1 и чужое поле 9.
        let mut inner = varint(1, 1);
        inner.extend(varint(9, 1));
        let body = bytes(1, &inner);

        let outer = fit(&schema.message("t.Outer"), std::slice::from_ref(&body));

        assert!((outer.parsed == outer.samples));
        // Тег поля 9 плюс его varint: на верхнем уровне тело безупречно, и
        // вся разница сидит внутри.
        assert_eq!(outer.unknown_bytes, 2);
    }

    /// Обёртка `KafkaProtobufSerializer` снимается тем же кодом, что и в показе:
    /// подбор обязан работать и на обёрнутых телах, и на голых, одинаково.
    #[test]
    fn a_confluent_wrapped_body_is_matched_like_a_bare_one() {
        let schema = Schema::new(
            "confluent",
            r#"
                syntax = "proto3";
                package t;
                message Order { string id = 1; }
            "#,
        );
        let bare = bytes(1, b"abc");
        // Маркер, id = 1, сокращённая запись индексов (`[0]`), затем тело.
        let mut wrapped = vec![0x00, 0x00, 0x00, 0x00, 0x01, 0x00];
        wrapped.extend_from_slice(&bare);

        let order = schema.message("t.Order");
        assert_eq!(
            fit(&order, &[bare]).quality_key(),
            fit(&order, &[wrapped]).quality_key()
        );
    }

    /// Невалидный UTF-8 там, где объявлена строка, — отказ самого парсера, и
    /// кандидат выбывает. Это самый дешёвый из сигналов: ничего считать не
    /// надо, разбор просто не удаётся.
    #[test]
    fn invalid_utf8_in_a_string_field_disqualifies() {
        let schema = Schema::new(
            "utf8",
            r#"
                syntax = "proto3";
                package t;
                message Text { string value = 1; }
                message Blob { bytes value = 1; }
            "#,
        );
        let body = bytes(1, &[0xff, 0xfe]);

        let text = fit(&schema.message("t.Text"), std::slice::from_ref(&body));
        let blob = fit(&schema.message("t.Blob"), std::slice::from_ref(&body));

        assert!(text.parsed != text.samples, "строковое поле приняло не-UTF-8");
        assert_eq!(text.parsed, 0);
        assert!((blob.parsed == blob.samples));
    }

    /// Верный тип, споткнувшийся об одно чужое тело, обязан стоять ВЫШЕ чужого
    /// типа, который разобрал все тела, но целиком в чужие поля.
    ///
    /// Ради этого правила переделано ранжирование. Сперва «разобралось не всё»
    /// было приговором, и на живом топике верный тип молча исчезал из списка:
    /// в настоящем топике попадаются и сообщения другого вида, и хвосты
    /// старого контракта.
    #[test]
    fn a_clean_match_on_most_bodies_beats_a_dirty_match_on_all() {
        let schema = Schema::new(
            "mixed",
            r#"
                syntax = "proto3";
                package t;
                message Order { string id = 1; int64 amount = 2; }
                message Blob { bytes any = 15; }
            "#,
        );
        let mut good = bytes(1, b"abc");
        good.extend(varint(2, 7));
        // Чужое сообщение в том же топике: строковое поле приняло не-UTF-8, и
        // `Order` об него споткнётся, а `Blob` проглотит, как и всё остальное.
        let alien = bytes(1, &[0xff, 0xfe]);

        let samples = vec![good.clone(), good.clone(), good, alien];
        let order = fit(&schema.message("t.Order"), &samples);
        let blob = fit(&schema.message("t.Blob"), &samples);

        assert_eq!((order.parsed, order.clean), (3, 3));
        assert_eq!((blob.parsed, blob.clean), (4, 0), "Blob разобрал всё, но грязно");
        assert!(order.parsed != order.samples && (blob.parsed == blob.samples));
        assert!(
            order.quality_key() > blob.quality_key(),
            "полнота перевесила чистоту: order={:?} blob={:?}",
            order.quality_key(),
            blob.quality_key()
        );
    }

    // --- Порядок кандидатов ------------------------------------------------

    fn candidate(message: &str, own: bool, root: bool, fit: Fit) -> Candidate {
        Candidate {
            message: message.to_string(),
            from_topic: if own { "ours" } else { "theirs" }.to_string(),
            own,
            root,
            fit,
        }
    }

    /// Урезанный .proto своего топика выигрывает у чужого типа, которому то же
    /// тело легло безупречно.
    ///
    /// Это главное правило порядка, и оно стоило двух заходов. Чужой тип,
    /// объявивший на месте вложенного сообщения непрозрачный `bytes`, чист
    /// всегда — ему просто нечему не совпасть. А верный тип урезанного
    /// контракта оставляет половину байт в полях, которых не объявляет, потому
    /// что поля из контракта выбросили намеренно. Суди по чистоте — и ответом
    /// каждый раз будет случайный чужой тип.
    #[test]
    fn the_own_root_outranks_a_cleaner_stranger() {
        let trimmed = Fit {
            samples: 32,
            parsed: 32,
            clean: 0,
            unknown_bytes: 9906,
            body_bytes: 20735,
            fields_declared: 1,
            fields_seen: 1,
            ..Fit::default()
        };
        let spotless = Fit {
            samples: 32,
            parsed: 32,
            clean: 32,
            unknown_bytes: 0,
            body_bytes: 20735,
            exact: 32,
            fields_declared: 2,
            fields_seen: 2,
        };

        let ours = candidate("fireg.Asset", true, true, trimmed);
        let theirs = candidate("other.Opaque", false, false, spotless);

        assert!(
            ours.rank() > theirs.rank(),
            "ours={:?} theirs={:?}",
            ours.rank(),
            theirs.rank()
        );
    }

    /// Но корень, не разобравший НИ ОДНОГО тела, ответом не становится: правило
    /// опирается на устройство схемы, а не отменяет данные совсем.
    #[test]
    fn an_own_root_that_decodes_nothing_does_not_win() {
        let nothing = Fit {
            samples: 32,
            parsed: 0,
            body_bytes: 20735,
            fields_declared: 3,
            ..Fit::default()
        };
        let works = Fit {
            samples: 32,
            parsed: 32,
            clean: 0,
            unknown_bytes: 9906,
            body_bytes: 20735,
            fields_declared: 1,
            fields_seen: 1,
            ..Fit::default()
        };

        let root = candidate("ours.Broken", true, true, nothing);
        let other = candidate("ours.Works", true, false, works);

        assert!(other.rank() > root.rank());
    }

    /// Внутренний тип своего же набора стоит ниже конверта — по байтам они
    /// неразличимы, и решает только устройство схемы.
    #[test]
    fn the_envelope_outranks_its_own_inner_type() {
        let same = Fit {
            samples: 4,
            parsed: 4,
            clean: 4,
            unknown_bytes: 0,
            body_bytes: 100,
            exact: 4,
            fields_declared: 1,
            fields_seen: 1,
        };

        let envelope = candidate("ours.Envelope", true, true, same.clone());
        let inner = candidate("ours.Inner", true, false, same);

        assert!(envelope.rank() > inner.rank());
        // И ничьёй это не считается: различить их есть чем.
        let guess = Guess::new(4, vec![inner, envelope]);
        assert_eq!(guess.tied_at_top, 1);
        assert_eq!(guess.candidates[0].message, "ours.Envelope");
    }

    /// Частичный разбор виден числом, а не исчезновением кандидата.
    #[test]
    fn a_type_that_fails_on_one_sample_is_not_complete() {
        let schema = Schema::new(
            "partial",
            r#"
                syntax = "proto3";
                package t;
                message Text { string value = 1; }
            "#,
        );
        let good = bytes(1, b"ok");
        let bad = bytes(1, &[0xff]);

        let text = fit(&schema.message("t.Text"), &[good, bad]);

        assert_eq!(text.parsed, 1);
        assert_eq!(text.samples, 2);
        assert!(text.parsed != text.samples);
    }
}
