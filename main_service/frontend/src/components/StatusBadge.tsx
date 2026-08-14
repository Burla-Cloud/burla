import { cn } from "@/lib/utils";

// Stripe-style status pill: tinted background, matching ink, hairline border.
// Color carries meaning and nothing else does: green = good, brand = working,
// amber = transitional, red = failed, gray = inert.

export type BadgeTone = "success" | "progress" | "warning" | "danger" | "neutral";

const toneClasses: Record<BadgeTone, string> = {
    success:
        "bg-emerald-500/10 text-emerald-700 border-emerald-600/20 dark:bg-emerald-400/10 dark:text-emerald-300 dark:border-emerald-400/25",
    progress:
        "bg-primary/10 text-primary border-primary/25 dark:bg-primary/10 dark:border-primary/30",
    warning:
        "bg-amber-400/15 text-amber-700 border-amber-500/30 dark:bg-amber-300/10 dark:text-amber-300 dark:border-amber-300/25",
    danger: "bg-destructive/10 text-destructive border-destructive/25",
    neutral: "bg-muted/70 text-muted-foreground border-border",
};

const toneDotClasses: Record<BadgeTone, string> = {
    success: "bg-emerald-500 dark:bg-emerald-400",
    progress: "bg-primary",
    warning: "bg-amber-500 dark:bg-amber-300",
    danger: "bg-destructive",
    neutral: "bg-muted-foreground/50",
};

interface StatusBadgeProps {
    tone: BadgeTone;
    label: string;
    pulse?: boolean;
    className?: string;
}

export const StatusBadge = ({ tone, label, pulse = false, className }: StatusBadgeProps) => (
    <span
        className={cn(
            "inline-flex items-center gap-1.5 rounded-md border px-2 py-[3px] text-xs font-medium leading-none",
            toneClasses[tone],
            className,
        )}
    >
        <span
            className={cn(
                "h-1.5 w-1.5 shrink-0 rounded-full",
                toneDotClasses[tone],
                pulse && "animate-pulse",
            )}
        />
        {label}
    </span>
);

const title = (status: string) =>
    status.charAt(0).toUpperCase() + status.slice(1).toLowerCase();

type BadgeSpec = { tone: BadgeTone; label: string; pulse?: boolean };

export const clusterStatusBadge = (status: string | null | undefined): BadgeSpec => {
    switch (status) {
        case "ON":
            return { tone: "success", label: "On" };
        case "BOOTING":
            return { tone: "warning", label: "Starting", pulse: true };
        case "REBOOTING":
            return { tone: "warning", label: "Restarting", pulse: true };
        case "STOPPING":
            return { tone: "warning", label: "Stopping", pulse: true };
        default:
            return { tone: "neutral", label: "Off" };
    }
};

export const nodeStatusBadge = (status: string | null | undefined): BadgeSpec => {
    const key = (status ?? "").toUpperCase();
    switch (key) {
        case "READY":
            return { tone: "success", label: "Ready" };
        case "RUNNING":
            return { tone: "progress", label: "Running", pulse: true };
        case "BOOTING":
            return { tone: "warning", label: "Booting", pulse: true };
        case "STOPPING":
            return { tone: "warning", label: "Stopping", pulse: true };
        case "FAILED":
            return { tone: "danger", label: "Failed" };
        case "DELETED":
            return { tone: "neutral", label: "Deleted" };
        default:
            return { tone: "neutral", label: key ? title(key) : "Unknown" };
    }
};

export const jobStatusBadge = (status: string | null | undefined): BadgeSpec => {
    switch (status) {
        case "RUNNING":
            return { tone: "progress", label: "Running", pulse: true };
        case "COMPLETED":
            return { tone: "success", label: "Completed" };
        case "FAILED":
            return { tone: "danger", label: "Failed" };
        case "CANCELED":
            return { tone: "neutral", label: "Canceled" };
        case "PENDING":
            return { tone: "warning", label: "Pending", pulse: true };
        default:
            return { tone: "neutral", label: status ? title(status) : "Unknown" };
    }
};
