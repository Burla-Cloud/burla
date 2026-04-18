import { useTheme } from "next-themes";
import { useEffect, useState } from "react";
import { Moon, Sun, Monitor } from "lucide-react";
import { cn } from "@/lib/utils";

const options = [
    { value: "light", label: "Light", icon: Sun },
    { value: "system", label: "System", icon: Monitor },
    { value: "dark", label: "Dark", icon: Moon },
] as const;

const ThemeToggle = () => {
    const { theme, setTheme } = useTheme();
    const [mounted, setMounted] = useState(false);

    useEffect(() => setMounted(true), []);

    const active = mounted ? theme ?? "system" : "system";

    return (
        <div
            role="radiogroup"
            aria-label="Theme"
            className="inline-grid grid-cols-3 rounded-full bg-gray-200/70 dark:bg-gray-800/70 p-1"
        >
            {options.map(({ value, label, icon: Icon }) => {
                const isActive = active === value;
                return (
                    <button
                        key={value}
                        type="button"
                        role="radio"
                        aria-checked={isActive}
                        aria-label={label}
                        title={label}
                        onClick={() => setTheme(value)}
                        className={cn(
                            "flex items-center justify-center h-7 w-7 rounded-full transition-colors",
                            "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring",
                            isActive
                                ? "bg-white text-gray-900 shadow-sm dark:bg-gray-700 dark:text-gray-100"
                                : "text-gray-500 hover:text-gray-700 dark:text-gray-400 dark:hover:text-gray-200"
                        )}
                    >
                        <Icon className="h-3.5 w-3.5" />
                    </button>
                );
            })}
        </div>
    );
};

export default ThemeToggle;
