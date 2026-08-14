import { useState } from "react";
import { AlertCircle, Check, Copy } from "lucide-react";
import { toast } from "@/hooks/use-toast";

export type ErrorToastDetail = {
    title: string;
    message: string;
    // Shell command that fixes the problem, rendered as a copyable chip.
    command?: string;
    // Raw underlying error, rendered small and muted for debugging.
    error?: string;
};

const CommandChip = ({ command }: { command: string }) => {
    const [copied, setCopied] = useState(false);

    const handleCopy = async () => {
        await navigator.clipboard.writeText(command);
        setCopied(true);
        window.setTimeout(() => setCopied(false), 1500);
    };

    return (
        <div className="flex items-center gap-2 rounded-md border border-border bg-muted/50 py-1.5 pl-3 pr-1.5">
            {/* The command is the payload: wrap rather than ever truncating it. */}
            <code className="min-w-0 flex-1 break-words font-mono text-[13px] leading-5 text-foreground">
                {command}
            </code>
            <button
                onClick={handleCopy}
                className="flex h-7 w-7 shrink-0 items-center justify-center rounded-md text-muted-foreground transition-colors hover:bg-muted hover:text-foreground"
                aria-label={copied ? "Command copied" : "Copy command"}
                title={copied ? "Copied" : "Copy"}
            >
                {copied ? (
                    <Check className="h-3.5 w-3.5 text-emerald-500" />
                ) : (
                    <Copy className="h-3.5 w-3.5" />
                )}
            </button>
        </div>
    );
};

const ErrorToastContent = ({ detail }: { detail: ErrorToastDetail }) => (
    <div className="flex gap-3">
        <AlertCircle className="mt-px h-5 w-5 shrink-0 text-red-400" />
        <div className="min-w-0 flex-1">
            <p className="text-sm font-semibold leading-5 text-foreground">{detail.title}</p>
            <p className="mt-1 break-words text-sm leading-relaxed text-muted-foreground">
                {detail.message}
            </p>
            {detail.command && (
                <div className="mt-2.5">
                    <CommandChip command={detail.command} />
                </div>
            )}
            {detail.error && (
                <p
                    className="mt-2 truncate font-mono text-[11px] leading-4 text-muted-foreground/60"
                    title={detail.error}
                >
                    {detail.error}
                </p>
            )}
        </div>
    </div>
);

// Errors here carry instructions the user has to act on, so they stay up
// until dismissed.
export const showErrorToast = (detail: ErrorToastDetail) =>
    toast({
        description: <ErrorToastContent detail={detail} />,
        duration: Infinity,
        className: "p-4 pr-9",
    });
