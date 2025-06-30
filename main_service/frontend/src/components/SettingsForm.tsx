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

    const gpuOptions = [
        "None",
        "1x A100 80G",
        "2x A100 80G",
        "4x A100 80G",
        "8x A100 80G",
        "1x H100 80G",
        "2x H100 80G",
        "4x H100 80G",
        "8x H100 80G",
        "8x H200 141G",
    ];

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

    const [selectedGpu, setSelectedGpu] = useState(() => {
        const entry = Object.entries(gpuCpuMap).find(([, v]) => v.value === settings.machineType);
        return entry ? entry[0] : "None";
    });

    const [selectedCpu, setSelectedCpu] = useState(() => {
        if (selectedGpu !== "None") return gpuCpuMap[selectedGpu].value;
        const cpu = cpuOptions.find((c) => c.value === settings.machineType);
        return cpu ? cpu.value : cpuOptions[1].value;
    });

    React.useEffect(() => {
        if (selectedGpu === "None") {
            handleInputChange("machineType", selectedCpu);
        } else {
            handleInputChange("machineType", gpuCpuMap[selectedGpu].value);
        }
    }, [selectedGpu, selectedCpu]);

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
                        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                            <div>
                                <label className={labelClass}>GPU</label>
                                <Select
                                    disabled={!isEditing}
                                    value={selectedGpu}
                                    onValueChange={(val) => {
                                        setSelectedGpu(val);
                                        if (val === "None") {
                                            const fallback = cpuOptions[1].value;
                                            setSelectedCpu(fallback);
                                        } else {
                                            setSelectedCpu(gpuCpuMap[val].value);
                                        }
                                    }}
                                >
                                    <SelectTrigger className="w-full">
                                        <SelectValue />
                                    </SelectTrigger>
                                    <SelectContent>
                                        {gpuOptions.map((o) => (
                                            <SelectItem key={o} value={o}>
                                                {o}
                                            </SelectItem>
                                        ))}
                                    </SelectContent>
                                </Select>
                            </div>
                            <div>
                                <label className={labelClass}>CPU / RAM</label>
                                <Select
                                    disabled={!isEditing || selectedGpu !== "None"}
                                    value={
                                        selectedGpu === "None"
                                            ? selectedCpu
                                            : gpuCpuMap[selectedGpu].value
                                    }
                                    onValueChange={(val) => setSelectedCpu(val)}
                                >
                                    <SelectTrigger className="w-full">
                                        <SelectValue />
                                    </SelectTrigger>
                                    <SelectContent>
                                        {(selectedGpu === "None"
                                            ? cpuOptions
                                            : [gpuCpuMap[selectedGpu]]
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
                                    className="bg-primary text-primary-foreground px-3 py-1 rounded-full flex items-center"
                                >
                                    {user}
                                    {isEditing && (
                                        <button
                                            onClick={() => removeUser(user)}
                                            className="ml-2 text-white hover:text-red-400"
                                        >
                                            x
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
