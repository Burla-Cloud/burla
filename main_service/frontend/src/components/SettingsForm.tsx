import React, { useState } from "react";
import { useSettings } from "@/contexts/SettingsContext";
import { Card, CardContent } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { PYTHON_VERSIONS } from "@/types/constants";
import { InfoIcon } from "lucide-react";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip";
import {
    Select,
    SelectTrigger,
    SelectValue,
    SelectContent,
    SelectItem,
} from "@/components/ui/select";

export const SettingsForm = ({ isEditing }) => {
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

    return (
        <div className="space-y-12 overflow-hidden max-w-6xl mx-auto w-full">
            <Card className="w-full">
                <CardContent className="space-y-12 pt-6">
                    <div className="space-y-2">
                        <h2 className="text-xl font-semibold text-primary">Container Image</h2>
                        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
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
                                                    URI of the Docker image to run your code inside.
                                                    <br />
                                                    Private images are pulled using the host VM's
                                                    service account credentials.
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

                            <div>
                                <div className="flex items-center gap-1">
                                    <label className={labelClass}>Python Version</label>
                                    <TooltipProvider>
                                        <Tooltip>
                                            <TooltipTrigger asChild>
                                                <InfoIcon className="h-4 w-4 text-gray-400 hover:text-gray-600 cursor-help -mt-2" />
                                            </TooltipTrigger>
                                            <TooltipContent>
                                                <p>
                                                    Python version inside the container image.
                                                    <br />
                                                    This should be the same as your local python
                                                    version.
                                                </p>
                                            </TooltipContent>
                                        </Tooltip>
                                    </TooltipProvider>
                                </div>
                                <Select
                                    disabled={!isEditing}
                                    value={settings.pythonVersion}
                                    onValueChange={(val) => handleInputChange("pythonVersion", val)}
                                >
                                    <SelectTrigger className="w-full">
                                        <SelectValue />
                                    </SelectTrigger>
                                    <SelectContent>
                                        {PYTHON_VERSIONS.map((v) => (
                                            <SelectItem key={v} value={v}>
                                                {v}
                                            </SelectItem>
                                        ))}
                                    </SelectContent>
                                </Select>
                            </div>
                        </div>
                    </div>

                    <div className="space-y-2">
                        <h2 className="text-xl font-semibold text-primary">Virtual Machines</h2>
                        <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
                            <div>
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
                            {gpuVariant !== "None" && (
                                <div>
                                    <label className={labelClass}>GPUs per VM</label>
                                    <Select
                                        disabled={!isEditing}
                                        value={gpusPerVm.toString()}
                                        onValueChange={(val) => setGpusPerVm(parseInt(val, 10))}
                                    >
                                        <SelectTrigger className="w-full">
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
                            )}
                            <div>
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
                            <div>
                                <label className={labelClass}>Quantity</label>
                                <Input
                                    type="number"
                                    disabled={!isEditing}
                                    className="w-full h-9.5"
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
                                                Google accounts authorized to use this deployment.
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
            </Card>
        </div>
    );
};
