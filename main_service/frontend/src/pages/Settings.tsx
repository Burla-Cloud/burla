import { useState, useEffect } from "react";
import { useSettings } from "@/contexts/SettingsContext";
import { SettingsForm } from "@/components/SettingsForm";
// import { ServiceAccounts } from "@/components/ServiceAccounts";
import { Button } from "@/components/ui/button";
import { useSaveSettings } from "@/hooks/useSaveSettings";
import { toast } from "@/components/ui/use-toast";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";

const SettingsPage = () => {
    const [isEditing, setIsEditing] = useState(false);
    const { settings, setSettings } = useSettings();
    const { saveSettings } = useSaveSettings();
    const [saveDisabled, setSaveDisabled] = useState(false);
    const [loading, setLoading] = useState(true);

    useEffect(() => {
        // Fetch settings from the backend
        const fetchSettings = async () => {
            setLoading(true);
            try {
                const res = await fetch("/v1/settings");
                const data = await res.json();
                setSettings((prev) => ({ ...prev, ...data }));
            } catch (err) {
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
        <div className="flex-1 flex flex-col justify-start px-12 pt-0">
            <div className="max-w-6xl mx-auto w-full">
                <div className="flex items-center justify-between mt-[-4px] mb-[15px]">
                    <h1 className="text-3xl font-bold text-primary">Settings</h1>
                    <Button
                        onClick={handleToggleEdit}
                        variant="outline"
                        disabled={saveDisabled || loading}
                    >
                        {isEditing ? "Save" : "Edit"}
                    </Button>
                </div>
                <div className="space-y-6">
                    {loading ? (
                        <Card className="w-full animate-pulse">
                            <CardHeader>
                                <CardTitle className="text-xl font-semibold text-primary">
                                    <Skeleton className="h-6 w-40 mb-2" />
                                </CardTitle>
                            </CardHeader>
                            <CardContent className="space-y-8 pt-6">
                                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                                    <Skeleton className="h-10 w-full" />
                                    <Skeleton className="h-10 w-full" />
                                </div>
                                <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mt-6">
                                    <Skeleton className="h-10 w-full" />
                                    <Skeleton className="h-10 w-full" />
                                    <Skeleton className="h-10 w-full" />
                                    <Skeleton className="h-10 w-full" />
                                </div>
                                <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mt-6">
                                    <Skeleton className="h-10 w-full" />
                                    <Skeleton className="h-10 w-full" />
                                </div>
                                <div className="mt-8">
                                    <Skeleton className="h-8 w-40 mb-2" />
                                    <Skeleton className="h-10 w-full" />
                                    <div className="flex gap-2 mt-2">
                                        <Skeleton className="h-8 w-24 rounded-md" />
                                        <Skeleton className="h-8 w-24 rounded-md" />
                                    </div>
                                </div>
                            </CardContent>
                        </Card>
                    ) : (
                        <SettingsForm key={settings.machineType} isEditing={isEditing} />
                    )}
                </div>
            </div>
        </div>
    );
};

export default SettingsPage;
