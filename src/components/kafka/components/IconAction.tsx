import { ReactNode } from 'react';
import { MenuItem } from './MenuItem';

/**
 * Иконка-действие в шапке.
 *
 * Отступы живут на самой кнопке, а не на обёртке вокруг неё. Раньше было
 * наоборот, и цель для клика совпадала с самой иконкой — 16 пикселей, в которые
 * приходилось попадать пиксель в пиксель, хотя места в шапке под неё отведено
 * втрое больше. Ячейка шапки выглядит так же, а нажимается вся.
 */
export function IconAction({
  onClick,
  title,
  disabled = false,
  children,
}: {
  onClick?: () => void;
  title: string;
  disabled?: boolean;
  children: ReactNode;
}) {
  return (
    <MenuItem>
      <button
        onClick={onClick}
        disabled={disabled}
        title={title}
        className={`flex items-center justify-center px-4 py-4 bg-transparent border-none outline-none transition-colors ${
          disabled
            ? 'text-dim cursor-not-allowed opacity-50'
            : 'text-soft hover:text-brand cursor-pointer'
        }`}
      >
        {children}
      </button>
    </MenuItem>
  );
}
