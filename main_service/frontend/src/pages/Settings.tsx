import { useState } from "react";
import { SettingsProvider, useSettings } from "@/contexts/SettingsContext";
import { SettingsForm } from "@/components/SettingsForm";
import { Button } from "@/components/ui/button";
import { useSaveSettings } from "@/hooks/useSaveSettings";
import { toast } from "@/components/ui/use-toast"; // Optional: if you want toast feedback
import { ServiceAccounts } from "@/components/ServiceAccounts";
import { ServiceAccountProvider } from "@/contexts/ServiceAccountContext"; // ✅ add this


const SettingsPageContent = () => {
  const [isEditing, setIsEditing] = useState(false);
  const { settings } = useSettings();  // Access settings from context
  const { saveSettings } = useSaveSettings(); // Use the hook to save settings

  const handleToggleEdit = async () => {
    if (isEditing) {
      const success = await saveSettings(settings); // Save settings when editing is off
      if (success) {
        toast({ title: "Settings saved successfully" });  // Success toast
      } else {
        toast({ title: "Failed to save settings", variant: "destructive" }); // Error toast
      }
    }
    setIsEditing((prev) => !prev); // Toggle editing mode
  };

  return (
    <>
      <div className="flex items-center justify-between mb-4">
        <h1 className="text-3xl font-bold" style={{ color: "#3b5a64" }}>
          Settings
        </h1>
        <Button onClick={handleToggleEdit} variant="outline">
          {isEditing ? "Save" : "Edit"}
        </Button>
      </div>
      <SettingsForm isEditing={isEditing} />
      <ServiceAccounts isEditing={isEditing}/> 
    </>
  );
};

const SettingsPage = () => (
    <div className="flex-1 flex flex-col justify-start px-12 pt-0 max-w-6xl mx-auto w-full">
      <SettingsProvider>
        <ServiceAccountProvider>
          <SettingsPageContent />
        </ServiceAccountProvider>
      </SettingsProvider>
    </div>
  );

export default SettingsPage;