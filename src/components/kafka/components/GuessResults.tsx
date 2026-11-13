import { Check } from 'lucide-react';
import { SchemaCandidate, SchemaGuess } from '../types';

interface GuessResultsProps {
  guess: SchemaGuess;
  /** Что выбрано в селекторе сейчас — чтобы отметить это в списке. */
  chosen: string | null;
  /** Выбрать кандидата. Зовётся только для типов из набора ЭТОГО топика:
   *  чужой сперва надо загрузить, и выбирать пока нечего. */
  onPick: (message: string) => void;
}

/**
 * Числа подбора одной строкой.
 *
 * Все до единого и без сведения в проценты. Проценты здесь были бы враньём:
 * два структурно одинаковых типа ложатся на байты совершенно одинаково, и
 * «98% против 97%» нарисовало бы разницу там, где её нет. Человек смотрит на
 * чужие байты, потом на обратную сборку, потом на поля — ровно в том порядке,
 * в каком им доверяет и сам подбор (`Fit::rank_key`).
 */
function numbers(candidate: SchemaCandidate): string {
  const { parsed, clean, unknown_bytes, body_bytes, exact, samples, fields_seen, fields_declared } =
    candidate.fit;
  const parts = [];
  // Неполный разбор — первым делом и своими словами. Молча опускать такого
  // кандидата в конец списка значило бы оставить «почему-то не угадал» без
  // объяснения; на живых топиках это первое, на что надо смотреть.
  if (parsed < samples) {
    parts.push(`decoded only ${parsed} of ${samples} bodies`);
  }
  parts.push(
    clean === samples
      ? 'no unknown bytes'
      : `${clean}/${samples} clean · ${unknown_bytes} of ${body_bytes} bytes unknown`,
  );
  parts.push(`${exact}/${samples} byte-exact`);
  parts.push(`${fields_seen}/${fields_declared} fields seen`);
  return parts.join(' · ');
}

/**
 * Чем может быть записан топик — по его же телам.
 *
 * Список, а не один ответ, и это главное свойство всей затеи: у голого protobuf
 * тело не называет своего типа, и подбор отвечает «вот что подошло», а не «вот
 * чем это записано». Решает человек, поэтому ему показывают основания.
 */
export function GuessResults({ guess, chosen, onPick }: GuessResultsProps) {
  if (guess.candidates.length === 0) {
    return (
      <div className="rounded border border-edge border-dashed p-3 font-mono text-xs text-dim">
        Nothing to match against: no .proto is loaded for any topic of this cluster.
      </div>
    );
  }

  const best = guess.candidates[0].fit;
  /** Какую долю тела лучший кандидат не объяснил. */
  const unexplained = best.body_bytes === 0 ? 0 : Math.round((100 * best.unknown_bytes) / best.body_bytes);

  return (
    <div className="space-y-1.5">
      <div className="font-mono text-[11px] text-dim">
        Matched against {guess.samples} bodies already read — no new reads.
        {guess.dropped > 0 && ` ${guess.dropped} more partial matches not shown.`}
        {/* Ничья называется ничьёй. Без этой строки верхний кандидат читается
            как ответ, хотя за ним стоит ещё несколько ровно таких же. */}
        {guess.fitting > 0 && guess.tied_at_top > 1 && (
          <>
            {' '}
            <span className="text-soft">
              {guess.tied_at_top} types fit identically — the bytes cannot tell them apart.
            </span>
          </>
        )}
      </div>

      {/* Ничего не разобралось вовсе — вот это и правда беда: нужного .proto
          здесь нет. */}
      {best.parsed === 0 && (
        <div className="rounded border border-edge bg-sunken p-2 font-mono text-[11px] text-dim">
          Not one loaded type decodes these bodies. The .proto this topic is written with is not
          here — or the one that is here has a field of the wrong type inside a nested message,
          which protobuf refuses outright rather than skipping.
        </div>
      )}

      {/* А вот чужие байты при удавшемся разборе — обычное дело, и пугать ими
          нельзя. Загруженный .proto сплошь и рядом УРЕЗАН: нужные поля
          оставлены, остальные выброшены, и объявленные всё равно декодируются
          верно. Раньше здесь стояло «ни один тип не объясняет эти тела»,
          и на урезанном контракте это читалось как «тип не тот», хотя тип был
          ровно тот. */}
      {best.parsed > 0 && guess.fitting === 0 && (
        <div className="rounded border border-edge bg-sunken p-2 font-mono text-[11px] text-dim">
          The best match leaves <span className="text-soft">{unexplained}%</span> of the bytes in
          fields it does not declare
          {best.parsed < best.samples && `, and decodes only ${best.parsed} of ${best.samples}`}.
          That is normal when the loaded .proto is a trimmed copy of the contract — the fields it
          does declare still decode correctly.
        </div>
      )}

      <div className="max-h-40 overflow-auto rounded border border-edge divide-y divide-edge">
        {guess.candidates.map((candidate) => (
          <button
            key={candidate.message}
            type="button"
            disabled={!candidate.own}
            onClick={() => onPick(candidate.message)}
            title={
              candidate.own
                ? candidate.message
                : `${candidate.message} — declared in the .proto loaded for ${candidate.from_topic}`
            }
            className="w-full p-2 text-left bg-transparent border-none enabled:cursor-pointer enabled:hover:bg-edge disabled:cursor-default min-w-0 block"
          >
            <div className="flex items-center gap-1.5 min-w-0">
              <Check
                className={`size-3.5 shrink-0 text-brand ${
                  candidate.message === chosen ? 'opacity-100' : 'opacity-0'
                }`}
              />
              <span
                className={`font-mono text-xs truncate ${candidate.own ? 'text-strong' : 'text-soft'}`}
              >
                {candidate.message}
              </span>
              {/* Корень набора. Сказать об этом надо: при равных числах именно
                  он и выбран, и иначе непонятно, почему из двух одинаковых
                  строк отмечена эта. */}
              {candidate.root && (
                <span
                  className="shrink-0 font-mono text-[10px] text-dim border border-edge rounded px-1"
                  title="The only type of its .proto that nothing else contains — a topic is read by its envelope"
                >
                  root
                </span>
              )}
            </div>
            <div className="font-mono text-[11px] text-dim pl-5">{numbers(candidate)}</div>
            {/* Тип из чужого набора выбрать нечем: его .proto этому топику не
                загружены. Промолчать об этом значило бы показать строку, по
                которой непонятно, почему она не нажимается. */}
            {!candidate.own && (
              <div className="font-mono text-[11px] text-dim pl-5 truncate">
                from the .proto loaded for {candidate.from_topic} — load it here to use it
              </div>
            )}
          </button>
        ))}
      </div>
    </div>
  );
}
