import { Moon, Sun } from 'lucide-react';
import { IconAction } from './IconAction';
import { useTheme } from '../../../theme';

/**
 * Переключатель светлой и тёмной темы.
 *
 * Стоит последним в правой группе шапки — то есть в самом углу окна, и ни под
 * каким условием, в отличие от соседей: тему меняют и на пустом приложении, к
 * которому не подключено ничего. Всё, что зависит от открытого топика, стоит
 * левее и исчезает вместе с ним, а этот угол занят всегда.
 *
 * Иконка показывает, ЧТО СТАНЕТ, а не что есть: солнце в тёмной теме, месяц в
 * светлой. Состояние и без иконки видно — оно на весь экран; кнопке остаётся
 * сообщить результат нажатия.
 */
export function ThemeToggle() {
  const { theme, toggle } = useTheme();
  const dark = theme === 'dark';
  return (
    <IconAction
      onClick={toggle}
      title={dark ? 'Switch to the light theme' : 'Switch to the dark theme'}
    >
      {dark ? <Sun className="size-4" /> : <Moon className="size-4" />}
    </IconAction>
  );
}
