import React, { useState } from "react";
import { useSettings } from "@/contexts/SettingsContext";
import { Card, CardContent } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { PYTHON_VERSIONS } from "@/types/constants";
import { InfoIcon } from "lucide-react";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip";

interface SettingsFormProps {
    isEditing: boolean;
}

export const SettingsForm: React.FC<SettingsFormProps> = ({ isEditing }) => {
    const { settings, setSettings } = useSettings();
    const [newUser, setNewUser] = useState("");

    const gpuOptions = [
        "None",
        // "1x A100 80G",
        // "2x A100 80G",
        // "4x A100 80G",
        // "8x A100 80G",
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

    const gpuCpuMap: Record<string, { label: string; value: string }> = {
        // "1x A100 80G": { label: "12CPU / 170G RAM", value: "a2-ultragpu-1g" },
        // "2x A100 80G": { label: "24CPU / 340G RAM", value: "a2-ultragpu-2g" },
        // "4x A100 80G": { label: "48CPU / 680G RAM", value: "a2-ultragpu-4g" },
        // "8x A100 80G": { label: "96CPU / 1360G RAM", value: "a2-ultragpu-8g" },
        "1x H100 80G": { label: "26CPU / 234G RAM", value: "a3-highgpu-1g" },
        "2x H100 80G": { label: "52CPU / 468G RAM", value: "a3-highgpu-2g" },
        "4x H100 80G": { label: "104CPU / 936G RAM", value: "a3-highgpu-4g" },
        "8x H100 80G": { label: "208CPU / 1872G RAM", value: "a3-highgpu-8g" },
        "8x H200 141G": { label: "224CPU / 2952G RAM", value: "a3-ultragpu-8g" },
    };

    const [selectedGpu, setSelectedGpu] = useState<string>(() => {
        const m = settings.machineType;
        const entry = Object.entries(gpuCpuMap).find(([, v]) => v.value === m);
        return entry ? entry[0] : "None";
    });

    const [selectedCpu, setSelectedCpu] = useState<string>(() => {
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

    const handleInputChange = (key: keyof typeof settings, value: any) => {
        setSettings((prev) => ({ ...prev, [key]: value }));
    };

    const addUser = () => {
        if (newUser && !settings.users.includes(newUser)) {
            setSettings((prev) => ({ ...prev, users: [...prev.users, newUser] }));
            setNewUser("");
        }
    };

    const removeUser = (user: string) => {
        setSettings((prev) => ({
            ...prev,
            users: prev.users.filter((u) => u !== user),
        }));
    };

    const labelClass = "block text-sm font-medium text-gray-500 mb-1"; // Light grey labels
    const selectClass =
        "w-full rounded border border-input bg-background px-3 py-2 text-sm ring-offset-background focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:opacity-50 disabled:cursor-not-allowed";

    return (
        <div className="space-y-12 overflow-hidden max-w-6xl mx-auto w-full">
            <Card className="w-full">
                <CardContent className="space-y-12 pt-6">
                    {/* Section: Container Config */}
                    <div className="space-y-2">
                        <h2 className="text-xl font-semibold text-primary">
                            Container Image
                        </h2>
                        <div className="space-y-4">
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
                                                    Docker image Burla will run your code inside.
                                                    <br />
                                                    This can be the URI of any image as long as it
                                                    has Python 3.10+ installed.
                                                    <br />
                                                    If the image is private, Burla uses the Google
                                                    service account credentials attached to the host
                                                    VM to pull it.
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

                            {/* Python Version */}
                            <div className="grid grid-cols-1 gap-4">
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
                                                        Python version inside the container image to
                                                        use to run your code.
                                                        <br />
                                                        This should be the same as your local python
                                                        version.
                                                    </p>
                                                </TooltipContent>
                                            </Tooltip>
                                        </TooltipProvider>
                                    </div>
                                    <select
                                        disabled={!isEditing}
                                        className={selectClass}
                                        value={settings.pythonVersion}
                                        onChange={(e) =>
                                            handleInputChange("pythonVersion", e.target.value)
                                        }
                                    >
                                        {PYTHON_VERSIONS.map((v) => (
                                            <option key={v} value={v}>
                                                {v}
                                            </option>
                                        ))}
                                    </select>
                                </div>
                            </div>
                        </div>
                    </div>

                    {/* Section: Compute Resources */}
                    <div className="space-y-2">
                        <h2 className="text-xl font-semibold text-primary">
                            Virtual Machines
                        </h2>
                        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                            <div>
                                <label className={labelClass}>GPU</label>
                                <select
                                    disabled={!isEditing}
                                    className={selectClass}
                                    value={selectedGpu}
                                    onChange={(e) => {
                                        const val = e.target.value;
                                        setSelectedGpu(val);
                                        if (val === "None") {
                                            const fallback = cpuOptions[1].value;
                                            setSelectedCpu(fallback);
                                        } else {
                                            setSelectedCpu(gpuCpuMap[val].value);
                                        }
                                    }}
                                >
                                    {gpuOptions.map((o) => (
                                        <option key={o} value={o}>
                                            {o}
                                        </option>
                                    ))}
                                </select>
                            </div>
                            <div>
                                <label className={labelClass}>CPU / RAM</label>
                                <select
                                    disabled={!isEditing || selectedGpu !== "None"}
                                    className={selectClass}
                                    value={
                                        selectedGpu === "None"
                                            ? selectedCpu
                                            : gpuCpuMap[selectedGpu].value
                                    }
                                    onChange={(e) => setSelectedCpu(e.target.value)}
                                >
                                    {(selectedGpu === "None"
                                        ? cpuOptions
                                        : [gpuCpuMap[selectedGpu]]
                                    ).map((o) => (
                                        <option key={o.value} value={o.value}>
                                            {o.label}
                                        </option>
                                    ))}
                                </select>
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

                    {/* Section: Users */}
                    <div className="space-y-2">
                        <h2 className="text-xl font-semibold text-primary">
                            Authorized Users
                        </h2>
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
                                                Emails of people who are authorized to view this
                                                dashboard and run jobs on this Burla deployment.
                                                <br />
                                                Run `burla login` to authenticate your local client,
                                                and `burla dashboard` to login to this dashboard.
                                            </p>
                                        </TooltipContent>
                                    </Tooltip>
                                </TooltipProvider>
                            </div>
                            <div className="flex gap-2">
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
