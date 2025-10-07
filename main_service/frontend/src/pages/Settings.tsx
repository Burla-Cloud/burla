import { useState, useEffect, useRef } from "react";
import { useNavigate, useLocation } from "react-router-dom";
import { useSettings } from "@/contexts/SettingsContext";
import { SettingsForm } from "@/components/SettingsForm";
import { Button } from "@/components/ui/button";
import { useSaveSettings } from "@/hooks/useSaveSettings";
import { toast } from "@/components/ui/use-toast";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { AlertTriangle, Loader2 } from "lucide-react";

const SettingsPage = () => {
  const navigate = useNavigate();
  const location = useLocation();
  const { settings, setSettings } = useSettings();
  const { saveSettings } = useSaveSettings();

  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);
  const [hasUnsavedChanges, setHasUnsavedChanges] = useState(false);
  const settingsFormRef = useRef<{ isRegionValid: () => boolean } | null>(null);

  // Fetch settings
  useEffect(() => {
    const fetchSettings = async () => {
      try {
        const res = await fetch("/v1/settings");
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();
        setSettings((prev) => ({ ...prev, ...data }));
      } catch {
        setError("Could not load settings");
        toast({ title: "Failed to load settings", variant: "destructive" });
      } finally {
        setLoading(false);
      }
    };
    fetchSettings();
  }, [setSettings]);

  const confirmMsg = "You have unsaved changes. Leave this page?";

  // Warn on browser refresh/close
  useEffect(() => {
    const warn = (e: BeforeUnloadEvent) => {
      if (hasUnsavedChanges) {
        e.preventDefault();
        e.returnValue = "";
      }
    };
    window.addEventListener("beforeunload", warn);
    return () => window.removeEventListener("beforeunload", warn);
  }, [hasUnsavedChanges]);

  // Intercept in-app anchor navigation to use native confirm
  useEffect(() => {
    const handleBeforeNav = (e: MouseEvent) => {
      if (!hasUnsavedChanges) return;

      let el = e.target as HTMLElement | null;
      while (el && el.tagName !== "A") el = el.parentElement;
      if (!el) return;

      const href = el.getAttribute("href");
      if (!href) return;

      // ignore external, same-path, and non-left-click or modifier clicks
      const isExternal =
        /^https?:\/\//i.test(href) || el.getAttribute("target") === "_blank";
      const samePath = href === location.pathname;
      const me = e as MouseEvent;
      const modified =
        me.metaKey || me.ctrlKey || me.shiftKey || me.altKey || me.button !== 0;

      if (isExternal || samePath || modified) return;

      if (href.startsWith("/")) {
        const ok = window.confirm(confirmMsg);
        if (!ok) {
          e.preventDefault();
          e.stopPropagation();
        }
      }
    };

    document.addEventListener("click", handleBeforeNav, true);
    return () => document.removeEventListener("click", handleBeforeNav, true);
  }, [hasUnsavedChanges, location.pathname]);

  // Save handler
  const handleSave = async () => {
    if (
      settingsFormRef.current &&
      typeof settingsFormRef.current.isRegionValid === "function" &&
      !settingsFormRef.current.isRegionValid()
    ) {
      toast({
        title: "Please select a valid region before saving",
        variant: "destructive",
      });
      return false;
    }

    setSaving(true);
    const ok = await saveSettings(settings);
    toast({
      title: ok ? "Settings saved successfully" : "Failed to save settings",
      variant: ok ? "default" : "destructive",
    });
    setSaving(false);
    if (ok) setHasUnsavedChanges(false);
    return ok;
  };

  return (
    <div className="flex-1 flex flex-col justify-start px-12 pt-6">
      <div className="max-w-6xl mx-auto w-full flex-1 flex flex-col">
        <div className="flex items-center justify-between mt-2 mb-6">
          <h1 className="text-3xl font-bold text-primary">Settings</h1>

          {hasUnsavedChanges && (
            <div className="relative">
              {/* Outer halo pulse */}
              <div
                className="absolute -inset-2 rounded-lg bg-gray-400/40 blur-md"
                style={{ animation: "softPulse 1.8s ease-in-out infinite" }}
              />
              {/* Save button */}
              <Button
                onClick={handleSave}
                variant="outline"
                disabled={saving || loading || !!error}
                className="relative transition-all duration-500 shadow-md"
                style={{
                  animation:
                    hasUnsavedChanges && !saving
                      ? "pulseGrow 1.8s ease-in-out infinite"
                      : undefined,
                }}
              >
                <span className="flex items-center justify-center min-w-[48px]">
                  {saving ? (
                    <Loader2 className="h-4 w-4 animate-spin text-gray-600" />
                  ) : (
                    "Save"
                  )}
                </span>
              </Button>
              <style jsx>{`
                @keyframes pulseGrow {
                  0%,
                  100% {
                    transform: scale(1);
                    box-shadow: 0 0 6px rgba(0, 0, 0, 0.1);
                  }
                  50% {
                    transform: scale(1.05);
                    box-shadow: 0 0 14px rgba(0, 0, 0, 0.18);
                  }
                }
                @keyframes softPulse {
                  0%,
                  100% {
                    opacity: 0.25;
                    transform: scale(1);
                  }
                  50% {
                    opacity: 0.6;
                    transform: scale(1.06);
                  }
                }
              `}</style>
            </div>
          )}
        </div>

        <div className="space-y-8 flex-1">
          {loading ? (
            <Card className="w-full animate-pulse">
              <CardHeader>
                <CardTitle className="text-xl font-semibold text-primary">
                  <Skeleton className="h-6 w-40 mb-2" />
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-8 pt-8">
                <Skeleton className="h-10 w-full" />
                <Skeleton className="h-10 w-full" />
              </CardContent>
            </Card>
          ) : error ? (
            <Alert variant="destructive" className="w-full">
              <AlertTriangle className="h-4 w-4" />
              <AlertTitle>Could not load settings</AlertTitle>
              <AlertDescription>{error}</AlertDescription>
            </Alert>
          ) : (
            <SettingsForm
              ref={settingsFormRef}
              isEditing={true}
              onChange={() => setHasUnsavedChanges(true)}
            />
          )}
        </div>

        <div className="text-center text-sm text-gray-500 mt-auto pt-8">
          Need help? Email me!{" "}
          <a
            href="mailto:jake@burla.dev"
            className="text-blue-500 hover:underline"
          >
            jake@burla.dev
          </a>
        </div>
      </div>
    </div>
  );
};

export default SettingsPage;


