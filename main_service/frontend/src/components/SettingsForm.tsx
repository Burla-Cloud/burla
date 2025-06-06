import React, { useState } from "react";
import { useSettings } from "@/contexts/SettingsContext";
import { Card, CardContent } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { PYTHON_VERSIONS, MACHINE_TYPES } from "@/types/constants";
import { Settings } from "@/types/coreTypes";
import { InfoIcon } from "lucide-react";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip";

interface SettingsFormProps {
    isEditing: boolean;
}

export const SettingsForm: React.FC<SettingsFormProps> = ({ isEditing }) => {
    const { settings, setSettings } = useSettings();
    const [newUser, setNewUser] = useState("");

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
                        <h2 className="text-xl font-semibold" style={{ color: "#3b5a64" }}>
                            Container Configuration
                        </h2>
                        <div className="space-y-4">
                            <div>
                                <div className="flex items-center gap-1">
                                    <label className={labelClass}>Image</label>
                                    <TooltipProvider>
                                        <Tooltip>
                                            <TooltipTrigger asChild>
                                                <InfoIcon className="h-4 w-4 text-gray-400 hover:text-gray-600 cursor-help -mt-2" />
                                            </TooltipTrigger>
                                            <TooltipContent>
                                                <p>
                                                    Docker image Burla will run your code inside.
                                                    <br />
                                                    This can be the URI of any image with Python and
                                                    Burla's worker service installed.
                                                    <br />
                                                    Private images stored in your Artifact Registry
                                                    or GCR work here too!
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
                                                        Python version Burla will use to run your
                                                        code.
                                                        <br />
                                                        This should match your local Python version,
                                                        and be callable at `python3.X` in the image
                                                        linked above.
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
                        <h2 className="text-xl font-semibold" style={{ color: "#3b5a64" }}>
                            Compute Resources
                        </h2>
                        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                            <div>
                                <label className={labelClass}>Machine Type</label>
                                <select
                                    disabled={!isEditing}
                                    className={selectClass}
                                    value={settings.machineType}
                                    onChange={(e) =>
                                        handleInputChange("machineType", e.target.value)
                                    }
                                >
                                    {MACHINE_TYPES.map((t) => (
                                        <option key={t} value={t}>
                                            {t}
                                        </option>
                                    ))}
                                </select>
                            </div>
                            <div>
                                <label className={labelClass}>Machine Quantity</label>
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
                        <h2 className="text-xl font-semibold" style={{ color: "#3b5a64" }}>
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
                                                <br />A list of emails who are authorized to view
                                                this dashboard and run workloads on this deployment.
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
                                    className="bg-[#3b5a64] text-white px-3 py-1 rounded-full flex items-center"
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
