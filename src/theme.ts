import { useCallback, useSyncExternalStore } from 'react';

/**
 * Светлая и тёмная тема: где хранится выбор и кто его применяет.
 *
 * Единственный носитель темы — класс `dark` на `<html>`; всё остальное в
 * приложении читает цвета из CSS-переменных под ним (см. `styles/globals.css`).
 * Класс ставит загрузочный скрипт в `index.html` — ДО того, как выполнится этот
 * модуль и вообще любой код бандла. Иначе первый кадр рисуется темой из
 * `:root`, то есть светлой, и переключение на тёмную видно вспышкой; такое
 * мигание в этом приложении уже случалось, когда тему навешивали в рендере
 * компонента (см. комментарий в `App.tsx`).
 *
 * Поэтому здесь нет собственного «состояния темы»: состояние — это DOM, а
 * модуль лишь читает его и оповещает подписчиков. Второй источник правды
 * (React-стейт, инициализированный по своим правилам) неизбежно расходился бы
 * с тем, что уже нарисовано.
 */

export type Theme = 'dark' | 'light';

/** Ключ в localStorage. С префиксом приложения: webview один на весь Tauri. */
const STORAGE_KEY = 'mikui.theme';

/**
 * Чем приложение открывается, когда выбора ещё не делали.
 *
 * Тёмная, а не системная: приложение жило тёмным с самого начала, и подхват
 * системной настройки означал бы, что у части пользователей внешний вид
 * поменяется сам от одного обновления. Выбор запоминается, так что цена
 * решения — один клик, причём однажды.
 */
export const DEFAULT_THEME: Theme = 'dark';

function isTheme(value: unknown): value is Theme {
  return value === 'dark' || value === 'light';
}

/**
 * Выбор, сохранённый на диске, или тема по умолчанию.
 *
 * localStorage в webview может быть недоступен (приватный режим, политика
 * хранилища) — обращение к нему тогда бросает, а не возвращает null. Тема из-за
 * этого падать не должна: тему по умолчанию мы знаем и без хранилища.
 *
 * Экспортируется ради загрузочного скрипта в `index.html`, который делает то же
 * самое, — держать эти два места согласованными проще, когда правило записано
 * один раз здесь.
 */
export function storedTheme(): Theme {
  try {
    const saved = window.localStorage.getItem(STORAGE_KEY);
    return isTheme(saved) ? saved : DEFAULT_THEME;
  } catch {
    return DEFAULT_THEME;
  }
}

/** Что нарисовано прямо сейчас. Спрашиваем DOM, а не хранилище. */
export function currentTheme(): Theme {
  return document.documentElement.classList.contains('dark') ? 'dark' : 'light';
}

/** Подписчики на смену темы. Смена бывает только по клику, так что их единицы. */
const listeners = new Set<() => void>();

function subscribe(listener: () => void): () => void {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

/**
 * Применить тему: класс на `<html>`, запись в хранилище, оповещение подписчиков.
 *
 * `color-scheme` рядом с классом — не дубль: от него зависит вид того, что
 * рисует не приложение, а движок, — полос прокрутки и системного меню
 * автозаполнения. Тёмные полосы на белом фоне выдают тему, которой уже нет.
 */
export function applyTheme(theme: Theme): void {
  document.documentElement.classList.toggle('dark', theme === 'dark');
  document.documentElement.style.colorScheme = theme;
  try {
    window.localStorage.setItem(STORAGE_KEY, theme);
  } catch {
    // Выбор не переживёт перезапуск — это всё, чего мы лишились. Тема,
    // которую пользователь только что включил, уже применена выше.
  }
  listeners.forEach((listener) => listener());
}

/**
 * Текущая тема и переключатель к ней.
 *
 * `useSyncExternalStore`, а не контекст: тему читают два несвязанных места —
 * кнопка в шапке и `Toaster`, который живёт в другой ветке дерева, — и
 * протаскивать провайдер через всё приложение ради двух читателей незачем.
 */
export function useTheme(): { theme: Theme; toggle: () => void } {
  const theme = useSyncExternalStore(subscribe, currentTheme, () => DEFAULT_THEME);
  const toggle = useCallback(
    () => applyTheme(currentTheme() === 'dark' ? 'light' : 'dark'),
    [],
  );
  return { theme, toggle };
}
