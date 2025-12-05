import { useState, useEffect, useRef } from "react";
import { useSettings } from "@/contexts/SettingsContext";
import { SettingsForm } from "@/components/SettingsForm";
// import { ServiceAccounts } from "@/components/ServiceAccounts";
import { Button } from "@/components/ui/button";
import { useSaveSettings } from "@/hooks/useSaveSettings";
import { toast } from "@/components/ui/use-toast";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { AlertTriangle } from "lucide-react";

const SettingsPage = () => {
    const [isEditing, setIsEditing] = useState(false);
    const { settings, setSettings } = useSettings();
    const { saveSettings } = useSaveSettings();
    const [saveDisabled, setSaveDisabled] = useState(false);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);
    const settingsFormRef = useRef<{ isRegionValid: () => boolean } | null>(null);

    useEffect(() => {
        // Fetch settings from the backend
        const fetchSettings = async () => {
            setLoading(true);
            try {
                const res = await fetch("/v1/settings");
                if (!res.ok) {
                    throw new Error(`HTTP ${res.status}`);
                }
                const data = await res.json();
                setSettings((prev) => ({ ...prev, ...data }));
            } catch (err) {
                setError("Could not load settings");
                toast({ title: "Failed to load settings", variant: "destructive" });
            } finally {
                setLoading(false);
            }
        };
        fetchSettings();
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [setSettings]);

    const handleToggleEdit = async () => {
        if (isEditing) {
            // Check region validity before saving
            if (
                settingsFormRef.current &&
                typeof settingsFormRef.current.isRegionValid === "function"
            ) {
                if (!settingsFormRef.current.isRegionValid()) {
                    // SettingsForm will show toast and error, just block save
                    return;
                }
            }
            setSaveDisabled(true);
            const success = await saveSettings(settings);
            if (success) {
                toast({ title: "Settings saved successfully" });
            } else {
                toast({ title: "Failed to save settings", variant: "destructive" });
            }
            setSaveDisabled(false);
        }
        setIsEditing((prev) => !prev);
    };

    return (
        <div className="flex-1 flex flex-col justify-start px-12 pt-6">
            <div className="max-w-6xl mx-auto w-full flex-1 flex flex-col">
                <div className="flex items-center justify-between mt-2 mb-6">
                    <h1 className="text-3xl font-bold text-primary">Settings</h1>
                    <Button
                        onClick={handleToggleEdit}
                        variant="outline"
                        disabled={saveDisabled || loading || !!error}
                    >
                        {isEditing ? "Save" : "Edit"}
                    </Button>
                </div>
                <div className="space-y-8 flex-1">
                    {loading ? (
                        <Card className="w-full animate-pulse">
                            <CardHeader>
                                <CardTitle className="text-xl font-semibold text-primary">
                                    <Skeleton className="h-6 w-40 mb-2" />
                                </CardTitle>
                            </CardHeader>
                            <CardContent className="space-y-10 pt-8">
                                <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                                    <Skeleton className="h-10 w-full" />
                                    <Skeleton className="h-10 w-full" />
                                </div>
                                <div className="grid grid-cols-1 md:grid-cols-4 gap-6 mt-8">
                                    <Skeleton className="h-10 w-full" />
                                    <Skeleton className="h-10 w-full" />
                                    <Skeleton className="h-10 w-full" />
                                    <Skeleton className="h-10 w-full" />
                                </div>
                                <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mt-8">
                                    <Skeleton className="h-10 w-full" />
                                    <Skeleton className="h-10 w-full" />
                                </div>
                                <div className="mt-10">
                                    <Skeleton className="h-8 w-40 mb-2" />
                                    <Skeleton className="h-10 w-full" />
                                    <div className="flex gap-3 mt-3">
                                        <Skeleton className="h-8 w-24 rounded-md" />
                                        <Skeleton className="h-8 w-24 rounded-md" />
                                    </div>
                                </div>
                            </CardContent>
                        </Card>
                    ) : error ? (
                        <Alert variant="destructive" className="w-full">
                            <AlertTriangle className="h-4 w-4" />
                            <AlertTitle>Could not load settings</AlertTitle>
                            <AlertDescription>
                                Error: Sorry about this! Please refresh and log out /in again.
                                <br />
                                If the problem persists, please email me!
                                <br />
                                <a
                                    href="mailto:jake@burla.dev"
                                    className="text-blue-500 hover:underline"
                                >
                                    jake@burla.dev
                                </a>
                            </AlertDescription>
                        </Alert>
                    ) : (
                        <SettingsForm
                            ref={settingsFormRef}
                            key={settings.machineType}
                            isEditing={isEditing}
                        />
                    )}
                </div>
                <div className="text-center text-sm text-gray-500 mt-auto pt-8">
                    Need help? Email me!{" "}
                    <a href="mailto:jake@burla.dev" className="text-blue-500 hover:underline">
                        jake@burla.dev
                    </a>
                </div>
            </div>
        </div>
    );
};

export default SettingsPage;
