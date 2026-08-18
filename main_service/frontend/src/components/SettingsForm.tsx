import React, { useState, useImperativeHandle, forwardRef } from "react";
import { useSettings } from "@/contexts/SettingsContext";
import { Card } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { InfoIcon, X } from "lucide-react";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip";
import { Select, SelectTrigger, SelectValue, SelectContent, SelectItem } from "@/components/ui/select";

interface SettingsFormProps {
    onChange: () => void;
}

const RECOMMENDED_GPU_IMAGE = "pytorch/pytorch:2.13.0-cuda12.6-cudnn9-runtime";

function isKnownCpuOnlyImage(image: string, repositories: string[]): boolean {
    const repo = image.trim().toLowerCase().split(":")[0];
    const name = repo.replace(/^docker\.io\//, "").replace(/^library\//, "");
    return repositories.includes(name);
}

export const SettingsForm = forwardRef<
    { isRegionValid: () => boolean; isImageValid: () => boolean },
    SettingsFormProps
>(({ onChange }, ref) => {
    const { settings, setSettings } = useSettings();
    const users = settings.users ?? [];
    const isClientHosted = window.__BURLA_CLIENT_HOSTED_MODE__ === true;
    const [newUser, setNewUser] = useState("");
    const isAws = settings.cloudProvider === "aws";
    const isAzure = settings.cloudProvider === "azure";

    const machineOptions = settings.options?.machine_types ?? [];
    const constraints = settings.options!.constraints;
    const cpuOptions = machineOptions
        .filter((machine) => machine.gpu_count === 0)
        .map((machine) => ({
            label: `${machine.vcpu_count}vCPU / ${Math.round(machine.memory_bytes / 1024 ** 3)}G RAM`,
            value: machine.machine_type,
        }));
    const gpuCpuMap = Object.fromEntries(
        machineOptions
            .filter((machine) => machine.gpu_count > 0)
            .map((machine) => {
                return [
                    machine.gpu_display!,
                    {
                        label: `${machine.vcpu_count}vCPU / ${Math.round(machine.memory_bytes / 1024 ** 3)}G RAM`,
                        value: machine.machine_type,
                    },
                ];
            }),
    );

    // Build variant -> supported counts (e.g., "A100 40G" -> [1,2,4,8,16])
    const VARIANT_INFO: Record<string, number[]> = {};
    Object.entries(gpuCpuMap).forEach(([display]) => {
        const [countWithX, model, vramWithG] = display.split(" ");
        const count = parseInt(countWithX.slice(0, -1), 10);
        const vram = vramWithG; // include the trailing 'G'
        const variant = `${model} ${vram}`; // e.g., "A100 40G"
        if (!VARIANT_INFO[variant]) VARIANT_INFO[variant] = [];
        if (!VARIANT_INFO[variant].includes(count)) VARIANT_INFO[variant].push(count);
    });
    // sort counts ascending
    Object.values(VARIANT_INFO).forEach((arr) => arr.sort((a, b) => a - b));

    const gpuVariants = ["None", ...Object.keys(VARIANT_INFO)];

    // Initialize GPU state from settings
    const initialEntry = Object.entries(gpuCpuMap).find(([, v]) => v.value === settings.machineType);
    const initialDisplay = initialEntry ? initialEntry[0] : ""; // e.g., '4x A100 40G'
    const initialVariant = initialEntry ? initialDisplay.split(" ").slice(1).join(" ") : "None"; // A100 40G
    const initialGpuCount = initialEntry ? parseInt(initialDisplay.split("x")[0], 10) : 1;

    const [gpuVariant, setGpuVariant] = useState(initialVariant);
    const [gpusPerVm, setGpusPerVm] = useState(initialGpuCount);
    const [cpuChoice, setCpuChoice] = useState(
        initialVariant === "None"
            ? settings.machineType
            : (cpuOptions[1]?.value ?? cpuOptions[0]?.value ?? settings.machineType),
    );

    // synchronize form state with backend settings.machineType to ensure correct initial values
    React.useEffect(() => {
        const entry = Object.entries(gpuCpuMap).find(([, v]) => v.value === settings.machineType);
        if (entry) {
            const displayKey = entry[0];
            const [countWithX, ...variantParts] = displayKey.split(" ");
            const count = parseInt(countWithX.slice(0, -1), 10);
            const variant = variantParts.join(" ");
            setGpuVariant(variant);
            setGpusPerVm(count);
        } else {
            setGpuVariant("None");
            setGpusPerVm(1);
            setCpuChoice(settings.machineType);
        }
    }, [settings.machineType]);

    React.useEffect(() => {
        let newMachineType;
        if (gpuVariant === "None") {
            newMachineType = cpuChoice;
        } else {
            const key = `${gpusPerVm}x ${gpuVariant}`;
            newMachineType = gpuCpuMap[key]?.value || settings.machineType;
        }

        if (settings.machineType !== newMachineType) {
            setSettings((prev) => ({ ...prev, machineType: newMachineType }));
            onChange();
        }
    }, [gpuVariant, gpusPerVm, cpuChoice]);

    const handleInputChange = (key, value) => {
        setSettings((prev) => {
            const changed = prev[key] !== value;
            const next = changed ? { ...prev, [key]: value } : prev;
            if (changed) onChange();
            return next;
        });
    };

    const addUser = () => {
        const email = newUser.trim();
        if (!email) return;

        const exists = users.some((u) => u.toLowerCase() === email.toLowerCase());
        if (exists) {
            setNewUser("");
            return;
        }

        const nextUsers = [...users, email];
        setSettings((prev) => ({ ...prev, users: nextUsers }));
        setNewUser("");
        onChange();
    };

    const removeUser = (user: string) => {
        const nextUsers = users.filter((u) => u !== user);
        setSettings((prev) => ({ ...prev, users: nextUsers }));
        onChange();
    };

    const labelClass = "block text-[13px] font-medium text-foreground";

    const regionOptions = (
        machineOptions.find((machine) => machine.machine_type === settings.machineType)?.regions ?? []
    ).map((region) => ({ value: region, label: region }));
    const isRegionValid = regionOptions.some((r) => r.value === settings.gcpRegion);

    const gpuModel = gpuVariant.split(" ")[0]; // e.g. "A100"
    const isImageValid =
        gpuVariant === "None" ||
        !isKnownCpuOnlyImage(settings.containerImage, settings.options!.cpu_only_image_repositories);

    // Expose validity to parent via ref so save can be blocked
    useImperativeHandle(
        ref,
        () => ({
            isRegionValid: () => isRegionValid,
            isImageValid: () => isImageValid,
        }),
        [isRegionValid, isImageValid],
    );

    return (
        <Card className="w-full divide-y divide-border/70">
            <section className="px-5 py-5">
                <h2 className="text-sm font-semibold text-foreground">Container image</h2>
                <div className="mt-4 max-w-2xl">
                    <div className="mb-1.5 flex items-center gap-1.5">
                        <label className={labelClass}>Image URI</label>
                        <TooltipProvider>
                            <Tooltip>
                                <TooltipTrigger asChild>
                                    <InfoIcon className="h-3.5 w-3.5 cursor-help text-muted-foreground/70 hover:text-muted-foreground" />
                                </TooltipTrigger>
                                <TooltipContent>
                                    <p>
                                        This can be any image, as long as it has Python installed. Private images are
                                        pulled using the host VM's service account credentials.
                                    </p>
                                </TooltipContent>
                            </Tooltip>
                        </TooltipProvider>
                    </div>
                    <Input
                        className={`w-full font-mono text-[13px] ${
                            !isImageValid ? "border-destructive/60 ring-[3px] ring-destructive/15" : ""
                        }`}
                        value={settings.containerImage}
                        onChange={(e) => handleInputChange("containerImage", e.target.value)}
                    />
                    {!isImageValid && (
                        <span className="mt-1.5 block text-xs text-destructive">
                            This image has no CUDA libraries, so the {gpuModel}s won't be usable.{" "}
                            <button
                                type="button"
                                className="font-medium underline underline-offset-2"
                                onClick={() => handleInputChange("containerImage", RECOMMENDED_GPU_IMAGE)}
                            >
                                Use {RECOMMENDED_GPU_IMAGE}
                            </button>
                        </span>
                    )}
                </div>
            </section>

            <section className="px-5 py-5">
                <h2 className="text-sm font-semibold text-foreground">Virtual machines</h2>

                <div className="mt-4 grid grid-cols-1 gap-4 md:grid-cols-4">
                    {/* Quantity */}
                    <div>
                        <label className={`${labelClass} mb-1.5`}>Quantity</label>
                        <Input
                            type="text"
                            inputMode="numeric"
                            className="w-full"
                            value={settings.machineQuantity || ""}
                            onChange={(e) => {
                                const digits = e.target.value.replace(/\D/g, "");
                                const num = digits === "" ? 0 : parseInt(digits, 10);
                                handleInputChange("machineQuantity", num);
                            }}
                            onBlur={(e) => {
                                const val = parseInt(e.target.value, 10);
                                if (val < constraints.quantity.minimum) {
                                    handleInputChange("machineQuantity", constraints.quantity.minimum);
                                } else if (val > constraints.quantity.maximum) {
                                    handleInputChange("machineQuantity", constraints.quantity.maximum);
                                }
                            }}
                        />
                    </div>

                    {/* CPU / RAM */}
                    <div>
                        <label className={`${labelClass} mb-1.5`}>vCPU / RAM</label>
                        <Select
                            disabled={gpuVariant !== "None"}
                            value={gpuVariant === "None" ? cpuChoice : gpuCpuMap[`${gpusPerVm}x ${gpuVariant}`].value}
                            onValueChange={(val) => {
                                setCpuChoice(val);
                                setSettings((prev) => ({ ...prev, machineType: val }));
                                onChange();
                            }}
                        >
                            <SelectTrigger className="w-full">
                                <SelectValue />
                            </SelectTrigger>
                            <SelectContent>
                                {(gpuVariant === "None" ? cpuOptions : [gpuCpuMap[`${gpusPerVm}x ${gpuVariant}`]]).map(
                                    (o) => (
                                        <SelectItem key={o.value} value={o.value}>
                                            {o.label}
                                        </SelectItem>
                                    ),
                                )}
                            </SelectContent>
                        </Select>
                    </div>

                    {/* GPU */}
                    <div>
                        <div className="mb-1.5 flex items-center gap-1.5">
                            <label className={labelClass}>GPU</label>
                            {isAzure && (
                                <TooltipProvider>
                                    <Tooltip>
                                        <TooltipTrigger asChild>
                                            <InfoIcon className="h-3.5 w-3.5 cursor-help text-amber-500 hover:text-amber-600" />
                                        </TooltipTrigger>
                                        <TooltipContent>
                                            <p>
                                                GPUs aren't available on Azure clusters yet, we're working on it! (they
                                                work on GCP and AWS)
                                            </p>
                                        </TooltipContent>
                                    </Tooltip>
                                </TooltipProvider>
                            )}
                        </div>
                        <Select
                            disabled={isAzure}
                            value={gpuVariant}
                            onValueChange={(val) => {
                                setGpuVariant(val);
                                if (val === "None") {
                                    setGpusPerVm(1);
                                } else {
                                    const counts = VARIANT_INFO[val];
                                    setGpusPerVm(counts[0]);
                                }
                            }}
                        >
                            <SelectTrigger className="w-full">
                                <SelectValue />
                            </SelectTrigger>
                            <SelectContent>
                                {gpuVariants.map((model) => (
                                    <SelectItem key={model} value={model}>
                                        {model}
                                    </SelectItem>
                                ))}
                            </SelectContent>
                        </Select>
                    </div>

                    {/* GPUs per VM */}
                    <div>
                        <label className={`${labelClass} mb-1.5`}>GPUs per VM</label>
                        <Select
                            disabled={
                                gpuVariant === "None" ||
                                // only one size sold (e.g. AWS A100s): nothing to choose
                                VARIANT_INFO[gpuVariant].length === 1
                            }
                            value={gpuVariant === "None" ? "-" : gpusPerVm.toString()}
                            onValueChange={(val) => setGpusPerVm(parseInt(val, 10))}
                        >
                            <SelectTrigger className="w-full">
                                <SelectValue>{gpuVariant === "None" ? "-" : gpusPerVm.toString()}</SelectValue>
                            </SelectTrigger>
                            <SelectContent>
                                {(gpuVariant === "None" ? [] : VARIANT_INFO[gpuVariant]).map((n) => (
                                    <SelectItem key={n} value={n.toString()}>
                                        {n}
                                    </SelectItem>
                                ))}
                            </SelectContent>
                        </Select>
                    </div>
                </div>

                <div className="mt-4 grid grid-cols-1 gap-4 md:grid-cols-3">
                    {/* Disk Size */}
                    <div>
                        <label className={`${labelClass} mb-1.5`}>Disk size (GB)</label>
                        <Input
                            type="text"
                            inputMode="numeric"
                            className="w-full"
                            value={settings.diskSize || ""}
                            onChange={(e) => {
                                const digits = e.target.value.replace(/\D/g, "");
                                const num = digits === "" ? 0 : parseInt(digits, 10);
                                handleInputChange("diskSize", num);
                            }}
                            onBlur={(e) => {
                                const val = parseInt(e.target.value, 10);
                                if (val < constraints.disk_gb.minimum) {
                                    handleInputChange("diskSize", constraints.disk_gb.minimum);
                                } else if (val > constraints.disk_gb.maximum) {
                                    handleInputChange("diskSize", constraints.disk_gb.maximum);
                                }
                            }}
                        />
                    </div>

                    {/* Region Dropdown */}
                    <div>
                        <label className={`${labelClass} mb-1.5`}>
                            {isAws ? "AWS region" : isAzure ? "Azure region" : "GCP region"}
                        </label>
                        <Select
                            value={settings.gcpRegion || ""}
                            onValueChange={(val) => handleInputChange("gcpRegion", val)}
                        >
                            <SelectTrigger
                                className={`w-full ${
                                    !isRegionValid ? "border-destructive/60 ring-[3px] ring-destructive/15" : ""
                                }`}
                            >
                                <SelectValue />
                            </SelectTrigger>
                            <SelectContent>
                                {regionOptions.map((region) => (
                                    <SelectItem key={region.value} value={region.value}>
                                        {region.label}
                                    </SelectItem>
                                ))}
                            </SelectContent>
                        </Select>
                        {!isRegionValid && (
                            <span className="mt-1.5 block text-xs text-destructive">
                                Please select a region from the dropdown
                            </span>
                        )}
                    </div>

                    {/* Inactivity Timeout */}
                    <div>
                        <label className={`${labelClass} mb-1.5`}>Inactivity timeout (minutes)</label>
                        <Input
                            type="text"
                            inputMode="numeric"
                            className="w-full"
                            value={settings.inactivityTimeout ?? ""}
                            onChange={(e) => {
                                const digits = e.target.value.replace(/\D/g, "");
                                const num = digits === "" ? 0 : parseInt(digits, 10);
                                handleInputChange("inactivityTimeout", num);
                            }}
                            onBlur={(e) => {
                                const val = parseInt(e.target.value, 10);
                                const minimum = constraints.inactivity_timeout_seconds.minimum / 60;
                                const maximum = constraints.inactivity_timeout_seconds.maximum / 60;
                                if (val < minimum) {
                                    handleInputChange("inactivityTimeout", minimum);
                                } else if (val > maximum) {
                                    handleInputChange("inactivityTimeout", maximum);
                                }
                            }}
                        />
                    </div>
                </div>
            </section>

            {!isClientHosted && (
                <section className="px-5 py-5">
                    <h2 className="text-sm font-semibold text-foreground">Authorized users</h2>

                    <form
                        onSubmit={(e) => {
                            e.preventDefault();
                            addUser();
                        }}
                        className="mt-4 flex w-full max-w-2xl gap-2"
                    >
                        <Input
                            className="w-full"
                            placeholder="name@example.com"
                            value={newUser}
                            onChange={(e) => setNewUser(e.target.value)}
                        />
                        <Button type="button" onClick={addUser} variant="outline">
                            Add
                        </Button>
                    </form>

                    {users.length > 0 && (
                        <ul className="mt-4 max-w-2xl divide-y divide-border/70 rounded-lg border border-border">
                            {users.map((user) => (
                                <li key={user} className="flex items-center justify-between gap-3 px-3.5 py-2.5">
                                    <span className="min-w-0 truncate text-sm text-foreground">{user}</span>
                                    <button
                                        onClick={() => removeUser(user)}
                                        className="rounded-md p-1 text-muted-foreground transition-colors duration-150 hover:bg-accent hover:text-foreground"
                                        aria-label={`Remove ${user}`}
                                    >
                                        <X className="h-3.5 w-3.5" />
                                    </button>
                                </li>
                            ))}
                        </ul>
                    )}
                </section>
            )}
        </Card>
    );
});
