import { Link, NavLink } from "react-router-dom";
import { BookOpen, FolderClosed, LifeBuoy, ListChecks, Server, Settings } from "lucide-react";
import { cn } from "@/lib/utils";
import { BrandLockup } from "@/components/BrandLockup";

interface SidebarProps {
    disabled?: boolean;
}

// Injected by main_service's dashboard endpoint; false when the cluster has
// no shared-workspace bucket (the client-hosted default).
declare global {
    interface Window {
        __BURLA_FILESYSTEM_ENABLED__?: boolean;
    }
}

const NAV_ITEMS = [
    { to: "/", label: "Cluster", icon: Server, end: true },
    { to: "/jobs", label: "Jobs", icon: ListChecks, end: false },
    { to: "/filesystem", label: "Filesystem", icon: FolderClosed, end: false },
    { to: "/settings", label: "Settings", icon: Settings, end: false },
];

const Sidebar = ({ disabled = false }: SidebarProps) => {
    const filesystemEnabled = window.__BURLA_FILESYSTEM_ENABLED__ !== false;
    const items = NAV_ITEMS.filter((item) => item.to !== "/filesystem" || filesystemEnabled);

    return (
        <div
            className={cn(
                "flex min-h-screen w-56 flex-col border-r border-sidebar-border bg-sidebar px-3 pb-4 pt-6 transition-opacity duration-200",
                disabled && "pointer-events-none select-none opacity-60",
            )}
        >
            <Link to="/" className="px-3">
                <BrandLockup />
            </Link>

            <nav className="mt-7 space-y-0.5">
                {items.map(({ to, label, icon: Icon, end }) => (
                    <NavLink
                        key={to}
                        to={to}
                        end={end}
                        className={({ isActive }) =>
                            cn(
                                "flex items-center gap-2.5 rounded-lg px-3 py-2 text-sm font-medium transition-colors duration-150",
                                isActive
                                    ? "bg-primary/10 text-primary"
                                    : "text-sidebar-foreground hover:bg-sidebar-accent hover:text-sidebar-accent-foreground",
                            )
                        }
                    >
                        <Icon className="h-4 w-4 shrink-0" strokeWidth={1.75} />
                        {label}
                    </NavLink>
                ))}
            </nav>

            <div className="mt-auto space-y-0.5 border-t border-sidebar-border pt-4">
                <a
                    href="https://burla.dev/docs"
                    target="_blank"
                    rel="noopener noreferrer"
                    className="flex items-center gap-2.5 rounded-lg px-3 py-2 text-[13px] font-medium text-sidebar-foreground transition-colors duration-150 hover:bg-sidebar-accent hover:text-sidebar-accent-foreground"
                >
                    <BookOpen className="h-4 w-4 shrink-0" strokeWidth={1.75} />
                    Documentation
                </a>
                <a
                    href="mailto:jake@burla.dev"
                    className="flex items-center gap-2.5 rounded-lg px-3 py-2 text-[13px] font-medium text-sidebar-foreground transition-colors duration-150 hover:bg-sidebar-accent hover:text-sidebar-accent-foreground"
                >
                    <LifeBuoy className="h-4 w-4 shrink-0" strokeWidth={1.75} />
                    Support
                </a>
            </div>
        </div>
    );
};

export default Sidebar;
