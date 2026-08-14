import { useState, useEffect, useRef } from "react";
import { LogOut, Moon, Sun, User } from "lucide-react";
import { useTheme, Theme } from "@/lib/theme";
import { cn } from "@/lib/utils";

// Avatar menu, top right. Follows the Stripe dashboard pattern: the theme
// ("appearance") control lives inside the profile dropdown. Renders even when
// nobody is logged in (client-hosted mode) so the theme stays reachable.
export default function ProfilePicture() {
    const [profilePicUrl, setProfilePicUrl] = useState<string | null>(null);
    const [userName, setUserName] = useState<string | null>(null);
    const [userEmail, setUserEmail] = useState<string | null>(null);
    const [isOpen, setIsOpen] = useState<boolean>(false);
    const containerRef = useRef<HTMLDivElement>(null);
    const { theme, setTheme } = useTheme();

    useEffect(() => {
        const fetchUserInfo = async () => {
            try {
                const response = await fetch("/api/user");
                if (response.ok) {
                    const data = await response.json();
                    setProfilePicUrl(data.profile_pic);
                    setUserName(data.name);
                    setUserEmail(data.email);
                }
            } catch (error) {
                console.error("Failed to fetch user info:", error);
            }
        };

        fetchUserInfo();
    }, []);

    useEffect(() => {
        function handleClickOutside(event: MouseEvent) {
            if (containerRef.current && !containerRef.current.contains(event.target as Node)) {
                setIsOpen(false);
            }
        }
        if (isOpen) {
            document.addEventListener("mousedown", handleClickOutside);
        }
        return () => {
            document.removeEventListener("mousedown", handleClickOutside);
        };
    }, [isOpen]);

    const handleLogout = async () => {
        await fetch("/api/logout", { method: "POST", credentials: "include" });
        window.location.reload();
    };

    const themeOption = (value: Theme, label: string, Icon: typeof Sun) => (
        <button
            type="button"
            onClick={() => setTheme(value)}
            aria-pressed={theme === value}
            className={cn(
                "flex flex-1 items-center justify-center gap-1.5 rounded-md px-2 py-1.5 text-xs font-medium transition-colors duration-150",
                theme === value
                    ? "border border-border bg-card text-foreground shadow-sm"
                    : "text-muted-foreground hover:text-foreground",
            )}
        >
            <Icon className="h-3.5 w-3.5" />
            {label}
        </button>
    );

    return (
        <div ref={containerRef} className="fixed right-5 top-5 z-50">
            {profilePicUrl ? (
                <img
                    src={profilePicUrl}
                    alt="User profile"
                    className="h-8 w-8 cursor-pointer rounded-full border border-border object-cover shadow-sm"
                    onClick={() => setIsOpen(!isOpen)}
                />
            ) : (
                <button
                    type="button"
                    aria-label="Account menu"
                    onClick={() => setIsOpen(!isOpen)}
                    className="flex h-8 w-8 items-center justify-center rounded-full border border-border bg-card text-muted-foreground shadow-sm transition-colors duration-150 hover:text-foreground"
                >
                    <User className="h-4 w-4" />
                </button>
            )}

            {isOpen && (
                <div className="absolute right-0 top-full z-50 mt-2 w-60 rounded-xl border border-border bg-popover p-1.5 shadow-lg">
                    {userName && (
                        <div className="flex items-center gap-3 px-2.5 py-2.5">
                            {profilePicUrl && (
                                <img
                                    src={profilePicUrl}
                                    alt=""
                                    className="h-8 w-8 rounded-full object-cover"
                                />
                            )}
                            <div className="min-w-0">
                                <p className="truncate text-sm font-medium text-foreground">
                                    {userName}
                                </p>
                                <p className="truncate text-xs text-muted-foreground">
                                    {userEmail}
                                </p>
                            </div>
                        </div>
                    )}

                    {userName && <div className="mx-2.5 my-1 h-px bg-border" />}

                    <div className="flex items-center justify-between gap-3 px-2.5 py-2">
                        <span className="text-[13px] text-muted-foreground">Theme</span>
                        <div className="flex flex-1 rounded-lg bg-muted/80 p-0.5">
                            {themeOption("light", "Light", Sun)}
                            {themeOption("dark", "Dark", Moon)}
                        </div>
                    </div>

                    {profilePicUrl && (
                        <>
                            <div className="mx-2.5 my-1 h-px bg-border" />
                            <button
                                onClick={handleLogout}
                                className="flex w-full items-center gap-2.5 rounded-lg px-2.5 py-2 text-left text-[13px] font-medium text-foreground transition-colors duration-150 hover:bg-accent"
                            >
                                <LogOut className="h-4 w-4 text-muted-foreground" />
                                Log out
                            </button>
                        </>
                    )}
                </div>
            )}
        </div>
    );
}
