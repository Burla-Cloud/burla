import { ReactNode, useState } from "react";
import { ArrowUpRight, Check, Copy, X } from "lucide-react";
import { Highlight, themes } from "prism-react-renderer";
import { Card, CardContent } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { useSettings } from "@/contexts/SettingsContext";
import { cn } from "@/lib/utils";
import { useTheme } from "@/lib/theme";

// Injected by main_service's dashboard endpoint when the head runs inside the
// burla pip package on the user's own machine.
declare global {
    interface Window {
        __BURLA_CLIENT_HOSTED_MODE__?: boolean;
    }
}

const QUICKSTART_CODE = `from burla import remote_parallel_map

def my_function(x):
    print(f"processing input {x} on a machine in the cloud")
    return x * 2

results = remote_parallel_map(my_function, list(range(100)))`;

const DARK_CODE_THEME = themes.nightOwl;
const LIGHT_CODE_THEME = themes.github;

interface CopyButtonProps {
    text: string;
    label: string;
}

const CopyButton = ({ text, label }: CopyButtonProps) => {
    const [copied, setCopied] = useState(false);

    const handleCopy = async () => {
        await navigator.clipboard.writeText(text);
        setCopied(true);
        window.setTimeout(() => setCopied(false), 1500);
    };

    return (
        <button
            type="button"
            onClick={handleCopy}
            className="absolute right-2 top-2 z-10 inline-flex h-7 w-7 items-center justify-center rounded-md border border-black/10 bg-white/85 text-slate-700 shadow-sm backdrop-blur transition-colors duration-150 hover:bg-white dark:border-white/15 dark:bg-white/5 dark:text-slate-200 dark:hover:bg-white/10"
            aria-label={copied ? `${label} copied` : `Copy ${label}`}
            title={copied ? "Copied" : `Copy ${label}`}
        >
            {copied ? <Check className="h-3.5 w-3.5" /> : <Copy className="h-3.5 w-3.5" />}
        </button>
    );
};

const Step = ({ number, title, children }: { number: number; title: ReactNode; children?: ReactNode }) => (
    <li className="flex gap-3.5">
        <span className="mt-px flex h-5 w-5 shrink-0 items-center justify-center rounded-full bg-primary/10 text-[11px] font-semibold tabular-nums text-primary">
            {number}
        </span>
        <div className="min-w-0 flex-1 space-y-2.5">
            <div className="text-sm text-foreground">{title}</div>
            {children}
        </div>
    </li>
);

interface QuickstartCardProps {
    onDismiss: () => void;
}

// First-run "get started" card. Dismissable; state lives in localStorage.
export const QuickstartCard = ({ onDismiss }: QuickstartCardProps) => {
    const isClientHosted = window.__BURLA_CLIENT_HOSTED_MODE__ === true;
    const { settings, loading, error } = useSettings();
    const cloudResource =
        settings.cloudProvider === "aws"
            ? "AWS account"
            : settings.cloudProvider === "azure"
              ? "Azure subscription"
              : "GCP project";
    const resourceName = (
        settings.cloudProvider === "gcp" ? settings.googleCloudProjectId : settings.cloudAccountName
    )!;
    const vcpusPerVm = settings.options?.machine_types.find(
        (machine) => machine.machine_type === settings.machineType,
    )?.vcpu_count;
    const machineDescription =
        settings.machineQuantity === 1
            ? `one ${vcpusPerVm} vCPU VM`
            : `${settings.machineQuantity.toLocaleString()} VMs with ${vcpusPerVm} vCPUs each`;
    const { theme } = useTheme();
    const codeTheme = theme === "dark" ? DARK_CODE_THEME : LIGHT_CODE_THEME;

    let step = 0;
    const nextStep = () => {
        step += 1;
        return step;
    };

    return (
        <Card className="relative">
            <button
                onClick={onDismiss}
                className="absolute right-3 top-3 rounded-md p-1.5 text-muted-foreground transition-colors duration-150 hover:bg-accent hover:text-foreground"
                aria-label="Dismiss quickstart"
            >
                <X className="h-4 w-4" />
            </button>

            <CardContent className="p-5">
                <h2 className="text-base font-semibold tracking-tight text-foreground">Get started</h2>
                <p className="mt-1 text-[13px] text-muted-foreground">Run your first job in about two minutes.</p>

                <ol className="mt-5 space-y-5">
                    <Step
                        number={nextStep()}
                        title={
                            loading ? (
                                <Skeleton className="h-5 w-96 max-w-full" />
                            ) : error ? (
                                <span className="text-muted-foreground">Could not load cluster settings.</span>
                            ) : (
                                <>
                                    Hit{" "}
                                    <span
                                        className="mx-0.5 inline-flex h-7 items-center gap-1.5 rounded-lg bg-primary px-2.5 align-middle text-[12px] font-semibold leading-none text-primary-foreground shadow-[inset_0_1px_0_0_hsl(0_0%_100%/0.12),0_1px_2px_0_rgb(16_24_40/0.12)]"
                                        aria-label="Start button"
                                    >
                                        <span aria-hidden="true" className="text-sm">
                                            ⏻
                                        </span>
                                        Start
                                    </span>{" "}
                                    to boot {machineDescription} in the {cloudResource}: {resourceName}
                                </>
                            )
                        }
                    />

                    {!isClientHosted && (
                        <Step number={nextStep()} title="Connect your computer to the cluster:">
                            <div className="relative w-fit min-w-48 rounded-lg border bg-[#f6f8fa] py-2.5 pl-3.5 pr-12 font-mono text-[13px] text-[#393a34] dark:bg-[#011627] dark:text-[#d6deeb]">
                                <code>burla login</code>
                                <CopyButton text="burla login" label="burla login command" />
                            </div>
                        </Step>
                    )}

                    <Step number={nextStep()} title="Run some code in the cloud:">
                        <div className="relative overflow-hidden rounded-lg border bg-[#f6f8fa] dark:bg-[#011627]">
                            <CopyButton text={QUICKSTART_CODE} label="Python code" />
                            <Highlight theme={codeTheme} code={QUICKSTART_CODE} language="python">
                                {({ className, style, tokens, getLineProps, getTokenProps }) => (
                                    <pre
                                        className={cn(className, "overflow-x-auto p-3.5 pr-14 text-[13px] leading-6")}
                                        style={{
                                            ...style,
                                            backgroundColor: "transparent",
                                            margin: 0,
                                        }}
                                    >
                                        {tokens.map((line, lineIndex) => (
                                            <div key={lineIndex} {...getLineProps({ line })}>
                                                {line.map((token, tokenIndex) => (
                                                    <span key={tokenIndex} {...getTokenProps({ token })} />
                                                ))}
                                            </div>
                                        ))}
                                    </pre>
                                )}
                            </Highlight>
                        </div>
                    </Step>
                </ol>

                <a
                    href="https://burla.dev/docs/examples"
                    target="_blank"
                    rel="noopener noreferrer"
                    className="mt-5 inline-flex items-center gap-1 text-[13px] font-medium text-primary hover:underline"
                >
                    Explore more examples
                    <ArrowUpRight className="h-3.5 w-3.5" />
                </a>
            </CardContent>
        </Card>
    );
};
