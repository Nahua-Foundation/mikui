"use client";

import { Toaster as Sonner, ToasterProps } from "sonner";
import { useTheme } from "../../theme";

/**
 * Тост берёт тему у приложения, а не держит свою.
 *
 * Цвета ему задают переменные `--normal-*` ниже, и одних их хватило бы, но
 * `theme` у sonner управляет ещё и тем, что переменными не выражено (иконки
 * состояний, тень). С зашитым `theme="dark"` тост оставался тёмным пятном на
 * светлом интерфейсе.
 */
const Toaster = ({ ...props }: ToasterProps) => {
  const { theme } = useTheme();
  return (
    <Sonner
      theme={theme}
      className="toaster group"
      style={
        {
          "--normal-bg": "var(--popover)",
          "--normal-text": "var(--popover-foreground)",
          "--normal-border": "var(--border)",
        } as React.CSSProperties
      }
      {...props}
    />
  );
};

export { Toaster };
