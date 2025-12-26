import { useState, useEffect, useRef, useMemo } from "react";
import { useNavigate, useLocation, useOutletContext } from "react-router-dom";
import { useSettings } from "@/contexts/SettingsContext";
import { UsageProvider } from "@/contexts/UsageContext";

import { SettingsForm } from "@/components/SettingsForm";
import UsageSettings from "@/components/UsageSettings";

import { Button } from "@/components/ui/button";
import { useSaveSettings } from "@/hooks/useSaveSettings";
import { toast } from "@/components/ui/use-toast";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { AlertTriangle, Loader2 } from "lucide-react";
import {
  AlertDialog,
  AlertDialogContent,
  AlertDialogTitle,
  AlertDialogAction,
  AlertDialogCancel,
} from "@/components/ui/alert-dialog";

const SettingsPage = () => {
  const navigate = useNavigate();
  const location = useLocation();
  const { settings, setSettings } = useSettings();
  const { saveSettings } = useSaveSettings();

  const { saving, setSaving } = useOutletContext<{
    saving: boolean;
    setSaving: React.Dispatch<React.SetStateAction<boolean>>;
  }>();

  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [hasUnsavedChanges, setHasUnsavedChanges] = useState(false);
  const [showExitDialog, setShowExitDialog] = useState(false);

  const pendingNavRef = useRef<string | null>(null);
  const settingsFormRef = useRef<{ isRegionValid: () => boolean } | null>(null);

  const section = useMemo(() => {
    const sp = new URLSearchParams(location.search);
    const raw = sp.get("section");
    return raw === "usage" ? "usage" : "cluster";
  }, [location.search]);

  useEffect(() => {
    const fetchSettings = async () => {
      try {
        const res = await fetch("/v1/settings", { credentials: "include" });
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

  useEffect(() => {
    const handleBeforeNav = (e: MouseEvent) => {
      if (!hasUnsavedChanges) return;

      let el = e.target as HTMLElement | null;
      while (el && el.tagName !== "A") el = el.parentElement;
      if (!el) return;

      const href = el.getAttribute("href");
      if (!href) return;

      const isExternal =
        /^https?:\/\//i.test(href) || el.getAttribute("target") === "_blank";
      const samePath = href === location.pathname;

      const me = e as MouseEvent;
      const modified =
        me.metaKey || me.ctrlKey || me.shiftKey || me.altKey || me.button !== 0;

      if (isExternal || samePath || modified) return;

      if (href.startsWith("/")) {
        e.preventDefault();
        e.stopPropagation();
        pendingNavRef.current = href;
        setShowExitDialog(true);
      }
    };

    document.addEventListener("click", handleBeforeNav, true);
    return () => document.removeEventListener("click", handleBeforeNav, true);
  }, [hasUnsavedChanges, location.pathname]);

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

  const attemptNavigate = (to: string) => {
    if (!hasUnsavedChanges) {
      navigate(to);
      return;
    }
    pendingNavRef.current = to;
    setShowExitDialog(true);
  };

  const handleSectionClick = (next: "cluster" | "usage") => {
    const sp = new URLSearchParams(location.search);
    sp.set("section", next);
    const to = `${location.pathname}?${sp.toString()}`;
    attemptNavigate(to);
  };

  const showSaveButton = section === "cluster" && hasUnsavedChanges;

  const content = (() => {
    if (loading) {
      return (
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
      );
    }

    if (error) {
      return (
        <Alert variant="destructive" className="w-full">
          <AlertTriangle className="h-4 w-4" />
          <AlertTitle>Could not load settings</AlertTitle>
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      );
    }

    if (section === "cluster") {
      return (
        <SettingsForm
          ref={settingsFormRef}
          isEditing={true}
          onChange={() => setHasUnsavedChanges(true)}
        />
      );
    }

    return (
      <UsageProvider>
        <UsageSettings />
      </UsageProvider>
    );
  })();

  return (
    <div className="flex-1 flex flex-col justify-start px-12 pt-6 min-w-0">
      <div className="max-w-6xl mx-auto w-full flex-1 flex flex-col min-w-0">
        <div className="mt-2 mb-10">
          <div className="flex items-center justify-between gap-6">
            <div className="min-w-0">
              <h1 className="text-2xl font-bold text-primary">Settings</h1>
            </div>

            <div className="min-w-[92px] flex justify-end">
              {showSaveButton ? (
                <Button
                  onClick={handleSave}
                  variant="ghost"
                  disabled={saving || loading || !!error}
                  className={[
                    "relative rounded-md bg-white text-gray-900",
                    "border border-border shadow-[0_1px_3px_rgba(0,0,0,0.04)]",
                    "transform-gpu transition-all duration-200 ease-in-out",
                    "hover:-translate-y-0.5 hover:bg-gray-50",
                    "hover:shadow-[0_6px_14px_rgba(0,0,0,0.08)]",
                    "!focus:outline-none !ring-0 focus:border-border",
                    "active:translate-y-0 active:shadow-[0_1px_3px_rgba(0,0,0,0.04)]",
                  ].join(" ")}
                >
                  <span className="flex items-center justify-center min-w-[48px]">
                    {saving ? (
                      <Loader2 className="h-4 w-4 animate-spin text-gray-600" />
                    ) : (
                      "Save"
                    )}
                  </span>
                </Button>
              ) : (
                <div className="h-9.5" />
              )}
            </div>
          </div>

          <div className="mt-8 border-b border-border">
            <nav className="-mb-px flex items-center gap-8">
              <button
                type="button"
                onClick={() => handleSectionClick("cluster")}
                className={[
                  "relative pb-3 text-sm font-medium transition-colors",
                  "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-gray-300 focus-visible:ring-offset-2 rounded-sm",
                  section === "cluster"
                    ? "text-primary"
                    : "text-gray-500 hover:text-primary",
                ].join(" ")}
              >
                Cluster
                {section === "cluster" && (
                  <span className="absolute left-0 right-0 -bottom-px h-0.5 bg-primary rounded-full" />
                )}
              </button>

              <button
                type="button"
                onClick={() => handleSectionClick("usage")}
                className={[
                  "relative pb-3 text-sm font-medium transition-colors",
                  "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-gray-300 focus-visible:ring-offset-2 rounded-sm",
                  section === "usage"
                    ? "text-primary"
                    : "text-gray-500 hover:text-primary",
                ].join(" ")}
              >
                Usage
                {section === "usage" && (
                  <span className="absolute left-0 right-0 -bottom-px h-0.5 bg-primary rounded-full" />
                )}
              </button>
            </nav>
          </div>
        </div>

        <div className="space-y-10 flex-1 min-w-0">{content}</div>

        <div className="text-center text-sm text-gray-500 mt-auto pt-8">
          Need help? Email me{" "}
          <a href="mailto:jake@burla.dev" className="text-blue-500 hover:underline">
            jake@burla.dev
          </a>
        </div>
      </div>

      <AlertDialog open={showExitDialog} onOpenChange={setShowExitDialog}>
        <AlertDialogContent className="max-w-[360px] mx-auto py-7 px-6 rounded-lg shadow-[0_8px_24px_rgba(0,0,0,0.06)] bg-white">
          <div className="text-center mb-2">
            <AlertDialogTitle className="text-[15px] font-medium text-gray-900">
              Unsaved changes
            </AlertDialogTitle>
          </div>

          <div className="flex justify-center gap-3">
            <AlertDialogCancel className="hidden" />

            <AlertDialogAction
              onClick={async () => {
                const ok = await handleSave();
                if (ok && pendingNavRef.current) navigate(pendingNavRef.current);
              }}
              className="bg-gray-700 text-white hover:bg-gray-800 rounded-md px-5 py-2.5 font-medium min-w-[130px] transition-all focus:outline-none"
            >
              Save & Exit
            </AlertDialogAction>

            <AlertDialogAction
              onClick={() => {
                if (pendingNavRef.current) navigate(pendingNavRef.current);
              }}
              className="border border-gray-200 bg-gray-50 text-gray-800 hover:bg-gray-100 rounded-md px-5 py-2.5 font-medium min-w-[130px] transition-all focus:outline-none"
            >
              Exit Without Saving
            </AlertDialogAction>
          </div>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  );
};

export default SettingsPage;
