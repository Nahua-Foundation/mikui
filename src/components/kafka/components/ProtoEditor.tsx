import { useCallback, useRef } from 'react';
import { highlightProto } from '../syntax';

/**
 * Место под полосу прокрутки резервируется всегда — и в поле ввода, и в слое
 * подсветки под ним.
 *
 * Иначе появившаяся полоса сужает поле, но не слой: строки начинают
 * переноситься в двух местах по-разному, и подсветка расходится с текстом ровно
 * тогда, когда контракт перерос окно. Утилиты в Tailwind под это нет, поэтому
 * стилем. То же самое и по той же причине сделано в форме отправки.
 */
const SCROLLBAR_GUTTER: React.CSSProperties = { scrollbarGutter: 'stable' };

/** Всё, от чего зависит раскладка текста: шрифт, кегль, высота строки, отступы
 *  и правила переноса. Одна строка на оба слоя — разойдись они, подсветка
 *  поедет относительно букв. */
const LAYERS =
  'm-0 p-3 font-mono text-xs leading-6 whitespace-pre-wrap break-words';

interface ProtoEditorProps {
  value: string;
  onChange: (value: string) => void;
  disabled?: boolean;
  placeholder?: string;
}

/**
 * Поле для текста .proto — с подсветкой.
 *
 * Устроено так же, как поле тела в форме отправки, и намеренно: редактора в
 * приложении нет, а другого способа раскрасить РЕДАКТИРУЕМЫЙ текст, кроме как
 * положить раскрашенную копию слоем под прозрачную `textarea`, не существует.
 * Набор, выделение, курсор, отмена и вставка при этом остаются родными — слой
 * подсветки во вводе не участвует вовсе и сломать его не может.
 */
export function ProtoEditor({ value, onChange, disabled, placeholder }: ProtoEditorProps) {
  const layer = useRef<HTMLPreElement>(null);
  const syncScroll = useCallback((event: React.UIEvent<HTMLTextAreaElement>) => {
    const node = layer.current;
    if (!node) return;
    node.scrollTop = event.currentTarget.scrollTop;
    node.scrollLeft = event.currentTarget.scrollLeft;
  }, []);

  return (
    <div className="relative rounded border border-edge bg-sunken">
      {/* aria-hidden: настоящее поле — textarea ниже, и скринридеру этот слой
          только мешал бы. overflow-hidden, а не auto: своей полосы прокрутки у
          него быть не должно, она встала бы поверх настоящей — прокручиваем его
          мы сами. */}
      <pre
        ref={layer}
        aria-hidden
        style={SCROLLBAR_GUTTER}
        className={`absolute inset-0 overflow-hidden pointer-events-none ${LAYERS}`}
      >
        {/* Строки — inline-span'ы, а перевод строки между ними настоящим
            символом. Блоками (`div` на строку) было бы короче, но пустая строка
            схлопнулась бы в нулевую высоту, и вся подсветка ниже неё уехала бы
            вверх относительно поля ввода. Пустых строк в контракте хватает —
            ими отделяют типы друг от друга. */}
        {value.split('\n').map((line, index, all) => (
          <span key={index}>
            {highlightProto(line)}
            {index < all.length - 1 ? '\n' : ''}
          </span>
        ))}
      </pre>
      <textarea
        value={value}
        onChange={(e) => onChange(e.target.value)}
        onScroll={syncScroll}
        disabled={disabled}
        placeholder={placeholder}
        // Подчёркивание опечаток — ровно то, чего в контракте видеть не надо:
        // браузер подчеркнёт `uint64` и не поможет этим никому.
        spellCheck={false}
        autoCapitalize="off"
        autoCorrect="off"
        autoComplete="off"
        style={SCROLLBAR_GUTTER}
        className={`relative block h-56 w-full resize-none border-none bg-transparent
          text-transparent caret-strong outline-none placeholder:text-dim
          disabled:cursor-not-allowed ${LAYERS}`}
      />
    </div>
  );
}
