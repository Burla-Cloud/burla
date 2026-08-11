import { useSyncExternalStore } from "react";

export type Theme = "dark" | "light";

// Dark is the product default; index.html applies it pre-paint from the same key.
const STORAGE_KEY = "theme";

const listeners = new Set<() => void>();

function subscribe(listener: () => void) {
    listeners.add(listener);
    return () => listeners.delete(listener);
}

export function getTheme(): Theme {
    return localStorage.getItem(STORAGE_KEY) === "light" ? "light" : "dark";
}

export function setTheme(theme: Theme) {
    localStorage.setItem(STORAGE_KEY, theme);
    document.documentElement.classList.toggle("dark", theme === "dark");
    listeners.forEach((listener) => listener());
}

export function useTheme() {
    const theme = useSyncExternalStore(subscribe, getTheme);
    return { theme, setTheme };
}
