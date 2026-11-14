import { useEffect, useState } from "react";

export type Theme = "light" | "dark" | "system";

function apply(theme: Theme) {
  const root = document.documentElement;
  if (theme === "system") root.removeAttribute("data-theme");
  else root.setAttribute("data-theme", theme);
}

export function useTheme(): [Theme, (t: Theme) => void] {
  const [theme, setTheme] = useState<Theme>(() => {
    try {
      return (localStorage.getItem("quorumdb.theme") as Theme) || "light";
    } catch {
      return "light";
    }
  });
  useEffect(() => {
    apply(theme);
    try {
      localStorage.setItem("quorumdb.theme", theme);
    } catch {
      /* ignore */
    }
  }, [theme]);
  return [theme, setTheme];
}
