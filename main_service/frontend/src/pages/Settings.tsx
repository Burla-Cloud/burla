import { useState } from "react";
import { useSettings } from "@/contexts/SettingsContext";
import { SettingsForm } from "@/components/SettingsForm";
import { ServiceAccounts } from "@/components/ServiceAccounts";
import { Button } from "@/components/ui/button";
import { useSaveSettings } from "@/hooks/useSaveSettings";
import { toast } from "@/components/ui/use-toast";

const SettingsPage = () => {
  const [isEditing, setIsEditing] = useState(false);
  const { settings } = useSettings();
  const { saveSettings } = useSaveSettings();

  const handleToggleEdit = async () => {
    if (isEditing) {
      const success = await saveSettings(settings);
      if (success) {
        toast({ title: "Settings saved successfully" });
      } else {
        toast({ title: "Failed to save settings", variant: "destructive" });
      }
    }
    setIsEditing((prev) => !prev);
  };

  return (
    <div className="flex-1 flex flex-col justify-start px-12 pt-0">
      <div className="max-w-6xl mx-auto w-full">
        <div className="flex items-center justify-between mt-[-4px] mb-[15px]">
          <h1 className="text-3xl font-bold" style={{ color: "#3b5a64" }}>
            Settings
          </h1>
          <Button onClick={handleToggleEdit} variant="outline">
            {isEditing ? "Save" : "Edit"}
          </Button>
        </div>
        <div className="space-y-6">
          <SettingsForm isEditing={isEditing} />
          <ServiceAccounts isEditing={isEditing} />
        </div>
      </div>
    </div>
  );
};

export default SettingsPage;