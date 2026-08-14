import React, { useState, useImperativeHandle, forwardRef } from "react";
import { useSettings } from "@/contexts/SettingsContext";
import { Card } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { InfoIcon, X } from "lucide-react";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip";
import {
    Select,
    SelectTrigger,
    SelectValue,
    SelectContent,
    SelectItem,
} from "@/components/ui/select";

interface SettingsFormProps {
    onChange: () => void;
}

export const SettingsForm = forwardRef<{ isRegionValid: () => boolean }, SettingsFormProps>(
    ({ onChange }, ref) => {
        const { settings, setSettings } = useSettings();
        const users = settings.users ?? [];
        const isClientHosted = window.__BURLA_CLIENT_HOSTED_MODE__ === true;
        const [newUser, setNewUser] = useState("");
        const isAws = settings.cloudProvider === "aws";
        const isAzure = settings.cloudProvider === "azure";

        // Machine lists mirror main_service/providers/catalog.py for each cloud.
        const AWS_CPU_OPTIONS = [
            { label: "2vCPU / 8G RAM", value: "m7i.large" },
            { label: "4vCPU / 16G RAM", value: "m7i.xlarge" },
            { label: "8vCPU / 32G RAM", value: "m7i.2xlarge" },
            { label: "16vCPU / 64G RAM", value: "m7i.4xlarge" },
            { label: "32vCPU / 128G RAM", value: "m7i.8xlarge" },
            { label: "64vCPU / 256G RAM", value: "m7i.16xlarge" },
        ];
        const AZURE_CPU_OPTIONS = [
            { label: "2vCPU / 8G RAM", value: "Standard_D2as_v5" },
            { label: "4vCPU / 16G RAM", value: "Standard_D4as_v5" },
            { label: "8vCPU / 32G RAM", value: "Standard_D8as_v5" },
            { label: "16vCPU / 64G RAM", value: "Standard_D16as_v5" },
            { label: "32vCPU / 128G RAM", value: "Standard_D32as_v5" },
            { label: "64vCPU / 256G RAM", value: "Standard_D64as_v5" },
        ];
        const GCP_CPU_OPTIONS = [
            { label: "2vCPU / 8G RAM", value: "n4-standard-2" },
            { label: "4vCPU / 16G RAM", value: "n4-standard-4" },
            { label: "8vCPU / 32G RAM", value: "n4-standard-8" },
            { label: "16vCPU / 64G RAM", value: "n4-standard-16" },
            { label: "32vCPU / 128G RAM", value: "n4-standard-32" },
            { label: "64vCPU / 256G RAM", value: "n4-standard-64" },
            { label: "80vCPU / 320G RAM", value: "n4-standard-80" },
        ];
        const cpuOptions = isAws
            ? AWS_CPU_OPTIONS
            : isAzure
            ? AZURE_CPU_OPTIONS
            : GCP_CPU_OPTIONS;

        // AWS sells A100s only in 8-GPU machines; sizes mirror
        // main_service/providers/catalog.py.
        const AWS_GPU_MAP = {
            "8x A100 40G": { label: "96vCPU / 1152G RAM", value: "p4d.24xlarge" },
            "8x A100 80G": { label: "96vCPU / 1152G RAM", value: "p4de.24xlarge" },
            "1x H100 80G": { label: "16vCPU / 256G RAM", value: "p5.4xlarge" },
            "8x H100 80G": { label: "192vCPU / 2048G RAM", value: "p5.48xlarge" },
        };
        // No GPU machines are offered on Azure yet: its provider rejects GPU
        // nodes until the burla node image ships with NVIDIA drivers.
        const gpuCpuMap = isAzure
            ? {}
            : isAws
            ? AWS_GPU_MAP
            : {
                  "1x A100 40G": { label: "12vCPU / 85G RAM", value: "a2-highgpu-1g" },
                  "2x A100 40G": { label: "24vCPU / 170G RAM", value: "a2-highgpu-2g" },
                  "4x A100 40G": { label: "48vCPU / 340G RAM", value: "a2-highgpu-4g" },
                  "8x A100 40G": { label: "96vCPU / 680G RAM", value: "a2-highgpu-8g" },
                  "1x A100 80G": { label: "12vCPU / 170G RAM", value: "a2-ultragpu-1g" },
                  "2x A100 80G": { label: "24vCPU / 340G RAM", value: "a2-ultragpu-2g" },
                  "4x A100 80G": { label: "48vCPU / 680G RAM", value: "a2-ultragpu-4g" },
                  "8x A100 80G": { label: "96vCPU / 1360G RAM", value: "a2-ultragpu-8g" },
                  "1x H100 80G": { label: "26vCPU / 234G RAM", value: "a3-highgpu-1g" },
                  "2x H100 80G": { label: "52vCPU / 468G RAM", value: "a3-highgpu-2g" },
                  "4x H100 80G": { label: "104vCPU / 936G RAM", value: "a3-highgpu-4g" },
                  "8x H100 80G": { label: "208vCPU / 1872G RAM", value: "a3-highgpu-8g" },
              };

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
        const initialEntry = Object.entries(gpuCpuMap).find(
            ([, v]) => v.value === settings.machineType
        );
        const initialDisplay = initialEntry ? initialEntry[0] : ""; // e.g., '4x A100 40G'
        const initialVariant = initialEntry ? initialDisplay.split(" ").slice(1).join(" ") : "None"; // A100 40G
        const initialGpuCount = initialEntry ? parseInt(initialDisplay.split("x")[0], 10) : 1;

        const [gpuVariant, setGpuVariant] = useState(initialVariant);
        const [gpusPerVm, setGpusPerVm] = useState(initialGpuCount);
        const [cpuChoice, setCpuChoice] = useState(
            initialVariant === "None" ? settings.machineType : cpuOptions[1].value
        );

        // synchronize form state with backend settings.machineType to ensure correct initial values
        React.useEffect(() => {
            const entry = Object.entries(gpuCpuMap).find(
                ([, v]) => v.value === settings.machineType
            );
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
          
            const exists = users.some(u => u.toLowerCase() === email.toLowerCase());
            if (exists) { setNewUser(""); return; }
          
            const nextUsers = [...users, email];
            setSettings(prev => ({ ...prev, users: nextUsers }));
            setNewUser("");
            onChange();
          };

          const removeUser = (user: string) => {
            const nextUsers = users.filter(u => u !== user);
            setSettings(prev => ({ ...prev, users: nextUsers }));
            onChange();
          };

        const labelClass = "block text-[13px] font-medium text-foreground";

        // --- REGION LOGIC ---
        // Region lists for each GPU type
        const GCP_REGION_OPTIONS = {
            "A100 40G": [
                { value: "us-central1", label: "us‑central1" },
                { value: "us-west3", label: "us‑west3" },
                { value: "us-east1", label: "us‑east1" },
                { value: "us-west4", label: "us‑west4" },
                { value: "us-west1", label: "us‑west1" },
                { value: "europe-west4", label: "europe‑west4" },
                { value: "asia-northeast1", label: "asia‑northeast1" },
                { value: "asia-northeast3", label: "asia‑northeast3" },
                { value: "me-west1", label: "me‑west1" },
                { value: "asia-southeast1", label: "asia‑southeast1" },
            ],
            "A100 80G": [
                { value: "us-central1", label: "us‑central1" },
                { value: "us-east5", label: "us‑east5" },
                { value: "us-east4", label: "us‑east4" },
                { value: "europe-west4", label: "europe‑west4" },
                { value: "asia-southeast1", label: "asia‑southeast1" },
            ],
            "H100 80G": [
                { value: "us-central1", label: "us‑central1" },
                { value: "us-east5", label: "us‑east5" },
                { value: "us-east4", label: "us‑east4" },
                { value: "us-west4", label: "us‑west4" },
                { value: "us-west1", label: "us‑west1" },
                { value: "europe-west1", label: "europe‑west1" },
                { value: "asia-northeast1", label: "asia‑northeast1" },
                { value: "asia-southeast1", label: "asia‑southeast1" },
            ],
            None: [
                { value: "us-central1", label: "us‑central1" },
                { value: "us-east5", label: "us‑east5" },
                { value: "us-east1", label: "us‑east1" },
                { value: "us-east4", label: "us‑east4" },
                { value: "us-south1", label: "us‑south1" },
                { value: "us-west3", label: "us‑west3" },
                { value: "us-west1", label: "us‑west1" },
                { value: "northamerica-northeast2", label: "northamerica‑northeast2" },
                { value: "northamerica-south1", label: "northamerica‑south1" },
                { value: "europe-west1", label: "europe‑west1" },
                { value: "europe-west2", label: "europe‑west2" },
                { value: "europe-west3", label: "europe‑west3" },
                { value: "europe-west4", label: "europe‑west4" },
                { value: "europe-west9", label: "europe‑west9" },
                { value: "europe-southwest1", label: "europe‑southwest1" },
                { value: "europe-north2", label: "europe‑north2" },
                { value: "asia-northeast1", label: "asia‑northeast1" },
                { value: "asia-northeast3", label: "asia‑northeast3" },
                { value: "asia-south1", label: "asia‑south1" },
                { value: "asia-southeast1", label: "asia‑southeast1" },
                { value: "australia-southeast1", label: "australia‑southeast1" },
            ],
        };

        // AWS regions where each machine family is actually offered, from
        // `aws ec2 describe-instance-type-offerings` (Aug 2026). CPU nodes are
        // m7i.*; GPU mappings come from providers/catalog.py. p5.4xlarge and
        // p5.48xlarge are sold in different region sets, hence two H100 lists.
        const AWS_REGION_OPTIONS = {
            "A100 40G": [
                { value: "us-east-1", label: "us-east-1" },
                { value: "us-east-2", label: "us-east-2" },
                { value: "us-west-2", label: "us-west-2" },
                { value: "ca-central-1", label: "ca-central-1" },
                { value: "sa-east-1", label: "sa-east-1" },
                { value: "eu-west-1", label: "eu-west-1" },
                { value: "eu-west-2", label: "eu-west-2" },
                { value: "eu-central-1", label: "eu-central-1" },
                { value: "eu-north-1", label: "eu-north-1" },
                { value: "ap-south-1", label: "ap-south-1" },
                { value: "ap-northeast-1", label: "ap-northeast-1" },
                { value: "ap-northeast-2", label: "ap-northeast-2" },
                { value: "ap-southeast-2", label: "ap-southeast-2" },
            ],
            "A100 80G": [
                { value: "us-east-1", label: "us-east-1" },
                { value: "us-west-2", label: "us-west-2" },
                { value: "eu-central-1", label: "eu-central-1" },
                { value: "ap-northeast-1", label: "ap-northeast-1" },
                { value: "ap-southeast-1", label: "ap-southeast-1" },
            ],
            "H100 80G 1x": [
                { value: "us-east-1", label: "us-east-1" },
                { value: "us-east-2", label: "us-east-2" },
                { value: "us-west-2", label: "us-west-2" },
                { value: "sa-east-1", label: "sa-east-1" },
                { value: "eu-west-2", label: "eu-west-2" },
                { value: "ap-south-1", label: "ap-south-1" },
                { value: "ap-northeast-1", label: "ap-northeast-1" },
                { value: "ap-southeast-2", label: "ap-southeast-2" },
            ],
            "H100 80G 8x": [
                { value: "us-east-1", label: "us-east-1" },
                { value: "us-east-2", label: "us-east-2" },
                { value: "us-west-1", label: "us-west-1" },
                { value: "us-west-2", label: "us-west-2" },
                { value: "ca-central-1", label: "ca-central-1" },
                { value: "sa-east-1", label: "sa-east-1" },
                { value: "eu-west-2", label: "eu-west-2" },
                { value: "eu-north-1", label: "eu-north-1" },
                { value: "ap-south-1", label: "ap-south-1" },
                { value: "ap-northeast-1", label: "ap-northeast-1" },
                { value: "ap-northeast-2", label: "ap-northeast-2" },
                { value: "ap-southeast-2", label: "ap-southeast-2" },
            ],
            None: [
                { value: "af-south-1", label: "af-south-1" },
                { value: "ap-east-1", label: "ap-east-1" },
                { value: "ap-east-2", label: "ap-east-2" },
                { value: "ap-northeast-1", label: "ap-northeast-1" },
                { value: "ap-northeast-2", label: "ap-northeast-2" },
                { value: "ap-northeast-3", label: "ap-northeast-3" },
                { value: "ap-south-1", label: "ap-south-1" },
                { value: "ap-south-2", label: "ap-south-2" },
                { value: "ap-southeast-1", label: "ap-southeast-1" },
                { value: "ap-southeast-2", label: "ap-southeast-2" },
                { value: "ap-southeast-3", label: "ap-southeast-3" },
                { value: "ap-southeast-4", label: "ap-southeast-4" },
                { value: "ap-southeast-5", label: "ap-southeast-5" },
                { value: "ap-southeast-6", label: "ap-southeast-6" },
                { value: "ap-southeast-7", label: "ap-southeast-7" },
                { value: "ca-central-1", label: "ca-central-1" },
                { value: "eu-central-1", label: "eu-central-1" },
                { value: "eu-central-2", label: "eu-central-2" },
                { value: "eu-north-1", label: "eu-north-1" },
                { value: "eu-south-1", label: "eu-south-1" },
                { value: "eu-south-2", label: "eu-south-2" },
                { value: "eu-west-1", label: "eu-west-1" },
                { value: "eu-west-2", label: "eu-west-2" },
                { value: "eu-west-3", label: "eu-west-3" },
                { value: "il-central-1", label: "il-central-1" },
                { value: "me-central-1", label: "me-central-1" },
                { value: "mx-central-1", label: "mx-central-1" },
                { value: "sa-east-1", label: "sa-east-1" },
                { value: "us-east-1", label: "us-east-1" },
                { value: "us-east-2", label: "us-east-2" },
                { value: "us-west-1", label: "us-west-1" },
                { value: "us-west-2", label: "us-west-2" },
            ],
        };

        // Azure regions where Dasv5 CPU nodes are widely available.
        const AZURE_REGION_OPTIONS = {
            None: [
                { value: "eastus", label: "eastus" },
                { value: "eastus2", label: "eastus2" },
                { value: "centralus", label: "centralus" },
                { value: "southcentralus", label: "southcentralus" },
                { value: "westus2", label: "westus2" },
                { value: "westus3", label: "westus3" },
                { value: "canadacentral", label: "canadacentral" },
                { value: "brazilsouth", label: "brazilsouth" },
                { value: "northeurope", label: "northeurope" },
                { value: "westeurope", label: "westeurope" },
                { value: "uksouth", label: "uksouth" },
                { value: "francecentral", label: "francecentral" },
                { value: "germanywestcentral", label: "germanywestcentral" },
                { value: "swedencentral", label: "swedencentral" },
                { value: "centralindia", label: "centralindia" },
                { value: "southeastasia", label: "southeastasia" },
                { value: "eastasia", label: "eastasia" },
                { value: "japaneast", label: "japaneast" },
                { value: "koreacentral", label: "koreacentral" },
                { value: "australiaeast", label: "australiaeast" },
            ],
        };

        // Helper to determine which region list to use
        function getRegionOptionsForGpu(gpuVariant) {
            if (isAzure) return AZURE_REGION_OPTIONS.None;
            if (isAws) {
                if (gpuVariant.includes("A100 40G")) return AWS_REGION_OPTIONS["A100 40G"];
                if (gpuVariant.includes("A100 80G")) return AWS_REGION_OPTIONS["A100 80G"];
                if (gpuVariant.includes("H100 80G")) {
                    return AWS_REGION_OPTIONS[gpusPerVm === 1 ? "H100 80G 1x" : "H100 80G 8x"];
                }
                return AWS_REGION_OPTIONS.None;
            }
            if (gpuVariant === "None") return GCP_REGION_OPTIONS.None;
            if (gpuVariant.includes("A100 40G")) return GCP_REGION_OPTIONS["A100 40G"];
            if (gpuVariant.includes("A100 80G")) return GCP_REGION_OPTIONS["A100 80G"];
            if (gpuVariant.includes("H100 80G")) return GCP_REGION_OPTIONS["H100 80G"];
            return GCP_REGION_OPTIONS.None;
        }

        const regionOptions = getRegionOptionsForGpu(gpuVariant);
        const isRegionValid = regionOptions.some((r) => r.value === settings.gcpRegion);

        // Expose isRegionValid to parent via ref
        useImperativeHandle(
            ref,
            () => ({
                isRegionValid: () => isRegionValid,
            }),
            [isRegionValid]
        );

        return (
            <Card className="w-full divide-y divide-border/70">
                <section className="px-5 py-5">
                    <h2 className="text-sm font-semibold text-foreground">Container image</h2>
                    <p className="mt-0.5 text-[13px] text-muted-foreground">
                        The Docker image your code runs inside.
                    </p>
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
                                            This can be any image, as long as it has Python
                                            installed. Private images are pulled using the host
                                            VM's service account credentials.
                                        </p>
                                    </TooltipContent>
                                </Tooltip>
                            </TooltipProvider>
                        </div>
                        <Input
                            className="w-full font-mono text-[13px]"
                            value={settings.containerImage}
                            onChange={(e) => handleInputChange("containerImage", e.target.value)}
                        />
                    </div>
                </section>

                <section className="px-5 py-5">
                    <h2 className="text-sm font-semibold text-foreground">Virtual machines</h2>
                    <p className="mt-0.5 text-[13px] text-muted-foreground">
                        The machines booted when the cluster starts.
                    </p>

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
                                    if (val < 1) {
                                        handleInputChange("machineQuantity", 1);
                                    } else if (val > 1000) {
                                        handleInputChange("machineQuantity", 1000);
                                    }
                                }}
                            />
                        </div>

                        {/* CPU / RAM */}
                        <div>
                            <label className={`${labelClass} mb-1.5`}>vCPU / RAM</label>
                            <Select
                                disabled={gpuVariant !== "None"}
                                value={
                                    gpuVariant === "None"
                                        ? cpuChoice
                                        : gpuCpuMap[`${gpusPerVm}x ${gpuVariant}`].value
                                }
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
                                    {(gpuVariant === "None"
                                        ? cpuOptions
                                        : [gpuCpuMap[`${gpusPerVm}x ${gpuVariant}`]]
                                    ).map((o) => (
                                        <SelectItem key={o.value} value={o.value}>
                                            {o.label}
                                        </SelectItem>
                                    ))}
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
                                                    GPUs aren't available on Azure clusters yet,
                                                    we're working on it! (they work on GCP and AWS)
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
                                    <SelectValue>
                                        {gpuVariant === "None" ? "-" : gpusPerVm.toString()}
                                    </SelectValue>
                                </SelectTrigger>
                                <SelectContent>
                                    {(gpuVariant === "None" ? [] : VARIANT_INFO[gpuVariant]).map(
                                        (n) => (
                                            <SelectItem key={n} value={n.toString()}>
                                                {n}
                                            </SelectItem>
                                        ),
                                    )}
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
                                    if (val < 10) {
                                        handleInputChange("diskSize", 10);
                                    } else if (val > 2000) {
                                        handleInputChange("diskSize", 2000);
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
                                        !isRegionValid
                                            ? "border-destructive/60 ring-[3px] ring-destructive/15"
                                            : ""
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
                            <label className={`${labelClass} mb-1.5`}>
                                Inactivity timeout (minutes)
                            </label>
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
                                    if (val < 0) {
                                        handleInputChange("inactivityTimeout", 0);
                                    } else if (val > 1440) {
                                        handleInputChange("inactivityTimeout", 1440);
                                    }
                                }}
                            />
                        </div>
                    </div>
                </section>

                {!isClientHosted && (
                    <section className="px-5 py-5">
                        <h2 className="text-sm font-semibold text-foreground">Authorized users</h2>
                        <p className="mt-0.5 text-[13px] text-muted-foreground">
                            Google accounts authorized to use this deployment.
                        </p>

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
                                    <li
                                        key={user}
                                        className="flex items-center justify-between gap-3 px-3.5 py-2.5"
                                    >
                                        <span className="min-w-0 truncate text-sm text-foreground">
                                            {user}
                                        </span>
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
    }
);
