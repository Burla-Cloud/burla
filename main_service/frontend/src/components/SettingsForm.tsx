import React, { useState, useImperativeHandle, forwardRef } from "react";
import { useSettings } from "@/contexts/SettingsContext";
import { Card, CardContent } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { InfoIcon } from "lucide-react";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip";
import {
    Select,
    SelectTrigger,
    SelectValue,
    SelectContent,
    SelectItem,
} from "@/components/ui/select";
import { toast } from "@/components/ui/use-toast";

// Add prop type for SettingsForm
interface SettingsFormProps {
    isEditing: boolean;
}

export const SettingsForm = forwardRef<{ isRegionValid: () => boolean }, SettingsFormProps>(
    ({ isEditing }, ref) => {
        const { settings, setSettings } = useSettings();
        const [newUser, setNewUser] = useState("");

        const cpuOptions = [
            { label: "2CPU / 8G RAM", value: "n4-standard-2" },
            { label: "4CPU / 16G RAM", value: "n4-standard-4" },
            { label: "8CPU / 32G RAM", value: "n4-standard-8" },
            { label: "16CPU / 64G RAM", value: "n4-standard-16" },
            { label: "32CPU / 128G RAM", value: "n4-standard-32" },
            { label: "64CPU / 256G RAM", value: "n4-standard-64" },
            { label: "80CPU / 320G RAM", value: "n4-standard-80" },
        ];

        const gpuCpuMap = {
            "1x A100 40G": { label: "12CPU / 85G RAM", value: "a2-highgpu-1g" },
            "2x A100 40G": { label: "24CPU / 170G RAM", value: "a2-highgpu-2g" },
            "4x A100 40G": { label: "48CPU / 340G RAM", value: "a2-highgpu-4g" },
            "8x A100 40G": { label: "96CPU / 680G RAM", value: "a2-highgpu-8g" },
            "16x A100 40G": { label: "96CPU / 1360G RAM", value: "a2-megagpu-16g" },
            "1x A100 80G": { label: "12CPU / 170G RAM", value: "a2-ultragpu-1g" },
            "2x A100 80G": { label: "24CPU / 340G RAM", value: "a2-ultragpu-2g" },
            "4x A100 80G": { label: "48CPU / 680G RAM", value: "a2-ultragpu-4g" },
            "8x A100 80G": { label: "96CPU / 1360G RAM", value: "a2-ultragpu-8g" },
            "1x H100 80G": { label: "26CPU / 234G RAM", value: "a3-highgpu-1g" },
            "2x H100 80G": { label: "52CPU / 468G RAM", value: "a3-highgpu-2g" },
            "4x H100 80G": { label: "104CPU / 936G RAM", value: "a3-highgpu-4g" },
            "8x H100 80G": { label: "208CPU / 1872G RAM", value: "a3-highgpu-8g" },
            "8x H200 141G": { label: "224CPU / 2952G RAM", value: "a3-ultragpu-8g" },
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
            if (gpuVariant === "None") {
                handleInputChange("machineType", cpuChoice);
            } else {
                const displayKey = `${gpusPerVm}x ${gpuVariant}`; // variant already includes memory G
                const machineValue = gpuCpuMap[displayKey].value;
                handleInputChange("machineType", machineValue);
            }
        }, [gpuVariant, gpusPerVm, cpuChoice]);

        const handleInputChange = (key, value) => {
            setSettings((prev) => ({ ...prev, [key]: value }));
        };

        const addUser = () => {
            if (newUser && !settings.users.includes(newUser)) {
                setSettings((prev) => ({ ...prev, users: [...prev.users, newUser] }));
                setNewUser("");
            }
        };

        const removeUser = (user) => {
            setSettings((prev) => ({
                ...prev,
                users: prev.users.filter((u) => u !== user),
            }));
        };

        const labelClass = "block text-sm font-medium text-gray-500 mb-1";

        // --- REGION LOGIC ---
        // Region lists for each GPU type
        const REGION_OPTIONS = {
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
            "H200 141G": [
                { value: "us-central1", label: "us‑central1" },
                { value: "us-south1", label: "us‑south1" },
                { value: "us-east4", label: "us‑east4" },
                { value: "us-east1", label: "us‑east1" },
                { value: "us-west1", label: "us‑west1" },
                { value: "europe-west4", label: "europe‑west4" },
                { value: "europe-west1", label: "europe‑west1" },
                { value: "asia-south2", label: "asia‑south2" },
                { value: "asia-south1", label: "asia‑south1" },
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

        // Helper to determine which region list to use
        function getRegionOptionsForGpu(gpuVariant) {
            if (gpuVariant === "None") return REGION_OPTIONS["None"];
            if (gpuVariant.includes("A100 40G")) return REGION_OPTIONS["A100 40G"];
            if (gpuVariant.includes("A100 80G")) return REGION_OPTIONS["A100 80G"];
            if (gpuVariant.includes("H100 80G")) return REGION_OPTIONS["H100 80G"];
            if (gpuVariant.includes("H200 141G")) return REGION_OPTIONS["H200 141G"];
            // fallback to None if unknown
            return REGION_OPTIONS["None"];
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

        // --- Save button logic ---
        // If parent controls isEditing, we need to notify parent to block save if region is invalid.
        // We'll assume SettingsForm gets a prop onInvalidRegion if needed, but for now, show toast and block save.
        React.useEffect(() => {
            if (!isEditing && !isRegionValid) {
                toast({
                    title: "Please select a region from dropdown",
                    variant: "destructive",
                });
            }
        }, [isEditing, isRegionValid]);

        return (
            <div className="space-y-12 overflow-hidden max-w-6xl mx-auto w-full">
                <Card className="w-full">
                    <CardContent className="space-y-12 pt-6">
                        <div className="space-y-2">
                            <h2 className="text-xl font-semibold text-primary">Container Image</h2>
                            <div className="grid grid-cols-1 gap-4">
                                <div>
                                    <div className="flex items-center gap-1">
                                        <label className={labelClass}>Image URI</label>
                                        <TooltipProvider>
                                            <Tooltip>
                                                <TooltipTrigger asChild>
                                                    <InfoIcon className="h-4 w-4 text-gray-400 hover:text-gray-600 cursor-help -mt-2" />
                                                </TooltipTrigger>
                                                <TooltipContent>
                                                    <p>
                                                        URI of the Docker image to run your code
                                                        inside.
                                                        <br />
                                                        This can be any image, as long as it has
                                                        Python installed.
                                                        <br />
                                                        Private images are pulled using the host
                                                        VM's service account credentials.
                                                    </p>
                                                </TooltipContent>
                                            </Tooltip>
                                        </TooltipProvider>
                                    </div>
                                    <Input
                                        disabled={!isEditing}
                                        className="w-full h-9.5"
                                        value={settings.containerImage}
                                        onChange={(e) =>
                                            handleInputChange("containerImage", e.target.value)
                                        }
                                    />
                                </div>
                            </div>
                        </div>

                        <div className="space-y-4">
                            <h2 className="text-xl font-semibold text-primary">Virtual Machines</h2>

                            {/* First row: four equal columns */}
                            <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
                                {/* Quantity */}
                                <div className="flex flex-col space-y-2">
                                    <label className={labelClass}>Quantity</label>
                                    <Input
                                        type="number"
                                        disabled={!isEditing}
                                        className="h-9.5 w-full"
                                        min={1}
                                        max={1000}
                                        value={settings.machineQuantity || ""}
                                        onChange={(e) => {
                                            const raw = e.target.value;
                                            const num = parseInt(raw, 10);
                                            if (!isNaN(num)) {
                                                handleInputChange("machineQuantity", num);
                                            } else if (raw === "") {
                                                handleInputChange("machineQuantity", 0);
                                            }
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
                                <div className="flex flex-col space-y-2">
                                    <label className={labelClass}>CPU / RAM</label>
                                    <Select
                                        disabled={!isEditing || gpuVariant !== "None"}
                                        value={
                                            gpuVariant === "None"
                                                ? cpuChoice
                                                : gpuCpuMap[`${gpusPerVm}x ${gpuVariant}`].value
                                        }
                                        onValueChange={(val) => setCpuChoice(val)}
                                    >
                                        <SelectTrigger className="w-full h-9.5">
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
                                <div className="flex flex-col space-y-2">
                                    <label className={labelClass}>GPU</label>
                                    <Select
                                        disabled={!isEditing}
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
                                        <SelectTrigger className="w-full h-9.5">
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

                                {/* GPUs per VM (hidden when None) */}
                                {gpuVariant !== "None" ? (
                                    <div className="flex flex-col space-y-2">
                                        <label className={labelClass}>GPUs per VM</label>
                                        <Select
                                            disabled={!isEditing}
                                            value={gpusPerVm.toString()}
                                            onValueChange={(val) => setGpusPerVm(parseInt(val, 10))}
                                        >
                                            <SelectTrigger className="w-full h-9.5">
                                                <SelectValue />
                                            </SelectTrigger>
                                            <SelectContent>
                                                {VARIANT_INFO[gpuVariant].map((n) => (
                                                    <SelectItem key={n} value={n.toString()}>
                                                        {n}
                                                    </SelectItem>
                                                ))}
                                            </SelectContent>
                                        </Select>
                                    </div>
                                ) : (
                                    // placeholder to maintain grid alignment
                                    <div className="hidden md:block" />
                                )}
                            </div>

                            {/* Second row: two equal columns */}
                            <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mt-6">
                                {/* Disk Size */}
                                <div className="flex flex-col space-y-2">
                                    <label className={labelClass}>Disk Size (GB)</label>
                                    <Input
                                        type="number"
                                        disabled={!isEditing}
                                        className="w-full h-9.5"
                                        min={10}
                                        max={2000}
                                        value={settings.diskSize || ""}
                                        onChange={(e) => {
                                            const raw = e.target.value;
                                            const num = parseInt(raw, 10);
                                            if (!isNaN(num)) {
                                                handleInputChange("diskSize", num);
                                            } else if (raw === "") {
                                                handleInputChange("diskSize", 0);
                                            }
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

                                {/* GCP Region Dropdown */}
                                <div className="flex flex-col space-y-2">
                                    <label className={labelClass}>GCP Region</label>
                                    <Select
                                        disabled={!isEditing}
                                        value={settings.gcpRegion || ""}
                                        onValueChange={(val) => handleInputChange("gcpRegion", val)}
                                    >
                                        <SelectTrigger
                                            className={`w-full h-9.5 ${
                                                !isRegionValid && isEditing
                                                    ? "border-red-500 focus:ring-red-500 ring-2"
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
                                    {!isRegionValid && isEditing && (
                                        <span className="text-xs text-red-600 mt-1">
                                            Please select a region from dropdown
                                        </span>
                                    )}
                                </div>

                                {/* Inactivity Timeout */}
                                <div className="flex flex-col space-y-2">
                                    <label className={labelClass}>
                                        Inactivity Timeout (minutes)
                                    </label>
                                    <Input
                                        type="number"
                                        disabled={!isEditing}
                                        className="w-full h-9.5"
                                        min={1}
                                        max={1440}
                                        value={settings.inactivityTimeout || ""}
                                        onChange={(e) => {
                                            const raw = e.target.value;
                                            const num = parseInt(raw, 10);
                                            if (!isNaN(num)) {
                                                handleInputChange("inactivityTimeout", num);
                                            } else if (raw === "") {
                                                handleInputChange("inactivityTimeout", 0);
                                            }
                                        }}
                                        onBlur={(e) => {
                                            const val = parseInt(e.target.value, 10);
                                            if (val < 1) {
                                                handleInputChange("inactivityTimeout", 1);
                                            } else if (val > 1440) {
                                                handleInputChange("inactivityTimeout", 1440);
                                            }
                                        }}
                                    />
                                </div>
                            </div>
                        </div>

                        <div className="space-y-2">
                            <h2 className="text-xl font-semibold text-primary">Authorized Users</h2>
                            <div>
                                <div className="flex items-center gap-1">
                                    <label className={labelClass}>Add User Email</label>
                                    <TooltipProvider>
                                        <Tooltip>
                                            <TooltipTrigger asChild>
                                                <InfoIcon className="h-4 w-4 text-gray-400 hover:text-gray-600 cursor-help -mt-2" />
                                            </TooltipTrigger>
                                            <TooltipContent>
                                                <p>
                                                    Google accounts authorized to use this
                                                    deployment.
                                                </p>
                                            </TooltipContent>
                                        </Tooltip>
                                    </TooltipProvider>
                                </div>
                                <form
                                    onSubmit={(e) => {
                                        e.preventDefault();
                                        if (isEditing) addUser();
                                    }}
                                    className="flex gap-2 w-full"
                                >
                                    <Input
                                        disabled={!isEditing}
                                        className="w-full h-9.5"
                                        value={newUser}
                                        onChange={(e) => setNewUser(e.target.value)}
                                    />
                                    <Button type="submit" disabled={!isEditing} variant="secondary">
                                        Add
                                    </Button>
                                </form>
                            </div>
                            <div className="flex flex-wrap gap-2">
                                {settings.users.map((user) => (
                                    <span
                                        key={user}
                                        className="bg-gray-100 border border-gray-300 text-gray-800 px-2 py-1 rounded-md flex items-center gap-1"
                                    >
                                        {user}
                                        {isEditing && (
                                            <button
                                                onClick={() => removeUser(user)}
                                                className="text-gray-500 hover:text-gray-700 text-xl leading-none"
                                            >
                                                ×
                                            </button>
                                        )}
                                    </span>
                                ))}
                            </div>
                        </div>
                    </CardContent>
                    {/* Footer for version and GCP project */}
                    {(settings.burlaVersion || settings.googleCloudProjectId) && (
                        <div
                            className="flex items-center justify-between rounded-b-lg"
                            style={{
                                background: "#f8f9fa",
                                borderTop: "1px solid #e5e7eb",
                                fontSize: "0.8em",
                                color: "#8c939f",
                                padding: "10px 24px",
                                minHeight: 40,
                            }}
                        >
                            <span style={{ fontWeight: 400 }}>
                                {settings.burlaVersion && <>Version: {settings.burlaVersion}</>}
                            </span>
                            <span style={{ fontWeight: 400, textAlign: "right" }}>
                                {settings.googleCloudProjectId && (
                                    <>Google Cloud Project: {settings.googleCloudProjectId}</>
                                )}
                            </span>
                        </div>
                    )}
                </Card>
            </div>
        );
    }
);
