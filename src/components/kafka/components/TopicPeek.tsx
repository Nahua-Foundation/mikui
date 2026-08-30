import { createPortal } from 'react-dom';

/** Геометрия выноса, снятая в момент наведения. Координаты вьюпорта: оверлей
 *  живёт в портале на `body`, а не внутри панели. */
export interface PeekAnchor {
  name: string;
  /** Левый край ТЕКСТА имени, а не строки. Вынос дорисовывает имя ровно с
   *  того места, где оно начинается в списке, и не накрывает кнопку слева —
   *  та остаётся настоящей и по-прежнему подсвечивается под курсором. */
  left: number;
  /** Верх строки списка. */
  top: number;
  height: number;
}

/** Полное имя топика, дорисованное за правую границу панели.
 *
 *  Отдельный слой в портале, а не расширение строки на месте, потому что
 *  строку изнутри списка наружу не выпустить: Virtuoso прокручивается по
 *  вертикали, а `overflow-y: auto` по спецификации превращает и
 *  `overflow-x: visible` в `auto` — горизонтально содержимое всё равно
 *  обрежется по краю скроллера.
 *
 *  Весь вынос — `pointer-events: none`, и это не деталь оформления, а то, на
 *  чём держится управление. Мышь всегда достаётся строке списка под выносом:
 *  клик и подсветка работают как обычно, а курсор, ушедший правее панели,
 *  покидает строку — то есть выходит из области, которой на самом деле
 *  ничего не принадлежит, — и вынос гаснет сам. Будь хвост кликабельным,
 *  курсор жил бы в пустоте за краем списка, и поведение стало бы
 *  неинтуитивным. */
export function TopicPeek({ anchor }: { anchor: PeekAnchor }) {
  return createPortal(
    <div
      className="fixed z-50 pointer-events-none flex items-center overflow-hidden text-ellipsis whitespace-nowrap rounded-r-md bg-surface pr-2 font-mono font-[450] text-[14px] leading-[20px] text-slate-50 shadow-lg shadow-black/50"
      style={{
        left: anchor.left,
        top: anchor.top,
        height: anchor.height,
        // Совсем длинное имя упирается в край окна, а не уезжает за него.
        maxWidth: `calc(100vw - ${anchor.left + 8}px)`,
      }}
    >
      {anchor.name}
    </div>,
    document.body,
  );
}
