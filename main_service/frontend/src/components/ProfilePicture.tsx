import { useState, useEffect, useRef } from "react";
import { Moon, Sun, User } from "lucide-react";
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
    const firstName = userName?.split(" ")[0] || "";

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
                "flex flex-1 items-center justify-center gap-1.5 rounded-[8px] px-2 py-1.5 font-mono text-xs transition-colors",
                theme === value
                    ? "bg-card text-foreground border border-border shadow-sm"
                    : "text-muted-foreground hover:text-foreground",
            )}
        >
            <Icon className="h-3.5 w-3.5" />
            {label}
        </button>
    );

    return (
        <div ref={containerRef} className="fixed top-6 right-6 z-50">
            {profilePicUrl ? (
                <img
                    src={profilePicUrl}
                    alt="User profile"
                    className="h-9 w-9 rounded-full border-2 border-border shadow-md object-cover cursor-pointer"
                    onClick={() => setIsOpen(!isOpen)}
                />
            ) : (
                <button
                    type="button"
                    aria-label="Account menu"
                    onClick={() => setIsOpen(!isOpen)}
                    className="flex h-9 w-9 items-center justify-center rounded-full border border-border bg-card text-muted-foreground shadow-md transition-colors hover:border-primary/60 hover:text-foreground"
                >
                    <User className="h-4 w-4" />
                </button>
            )}

            {isOpen && (
                <div className="absolute top-full mt-2 right-0 z-50 bg-popover border border-border rounded-xl shadow-lg px-4 py-4 w-56">
                    {profilePicUrl && (
                        <div className="text-center pt-2">
                            <img
                                src={profilePicUrl}
                                alt="User profile large"
                                className="h-20 w-20 rounded-full mx-auto object-cover"
                            />
                            <p className="mt-2 font-semibold text-foreground">Hi {firstName} !</p>
                            <p className="text-sm text-muted-foreground mt-1">
                                logged in as {userEmail}
                            </p>
                            <hr className="border-t border-border mt-6 mb-4" />
                        </div>
                    )}

                    <div className="flex items-center justify-between gap-3">
                        <span className="text-sm text-muted-foreground">Theme</span>
                        <div className="flex flex-1 rounded-[10px] bg-muted/80 p-1">
                            {themeOption("light", "Light", Sun)}
                            {themeOption("dark", "Dark", Moon)}
                        </div>
                    </div>

                    {profilePicUrl && (
                        <>
                            <hr className="border-t border-border mt-4 mb-2" />
                            <button
                                onClick={handleLogout}
                                className="flex items-center w-full text-left px-2 py-1 hover:bg-accent rounded-md"
                            >
                                <svg
                                    xmlns="http://www.w3.org/2000/svg"
                                    className="h-5 w-5 text-muted-foreground mr-2 stroke-current"
                                    fill="none"
                                    viewBox="0 0 24 24"
                                    stroke="currentColor"
                                >
                                    <path
                                        strokeLinecap="round"
                                        strokeLinejoin="round"
                                        strokeWidth="2"
                                        d="M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4"
                                    />
                                    <polyline
                                        strokeLinecap="round"
                                        strokeLinejoin="round"
                                        strokeWidth="2"
                                        points="16 17 21 12 16 7"
                                    />
                                    <line
                                        strokeLinecap="round"
                                        strokeLinejoin="round"
                                        strokeWidth="2"
                                        x1="21"
                                        y1="12"
                                        x2="9"
                                        y2="12"
                                    />
                                </svg>
                                <span className="text-sm text-foreground">Log out</span>
                            </button>
                        </>
                    )}
                </div>
            )}
        </div>
    );
}
