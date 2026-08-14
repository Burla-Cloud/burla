import { useState, useEffect, useRef, useMemo } from "react";
import { useNavigate, useLocation, useOutletContext } from "react-router-dom";
import { useSettings } from "@/contexts/SettingsContext";
import { UsageProvider } from "@/contexts/UsageContext";

import { SettingsForm } from "@/components/SettingsForm";
import UsageSettings from "@/components/UsageSettings";
import { PageHeader } from "@/components/PageHeader";

import { Button } from "@/components/ui/button";
import { useSaveSettings } from "@/hooks/useSaveSettings";
import { toast } from "@/components/ui/use-toast";
import { Card, CardContent } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { AlertTriangle, Loader2 } from "lucide-react";
import { cn } from "@/lib/utils";
import {
  AlertDialog,
  AlertDialogContent,
  AlertDialogTitle,
  AlertDialogDescription,
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
  const settingsFormRef = useRef<{
    isRegionValid: () => boolean;
    isImageValid: () => boolean;
  } | null>(null);

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
    if (settingsFormRef.current && !settingsFormRef.current.isRegionValid()) {
      toast({
        title: "Please select a valid region before saving",
        variant: "destructive",
      });
      return false;
    }

    if (settingsFormRef.current && !settingsFormRef.current.isImageValid()) {
      toast({
        title: "This container image can't use GPUs",
        description: "Pick an image with CUDA libraries, or set GPU to None.",
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
  const isAws = settings.cloudProvider === "aws";
  const isAzure = settings.cloudProvider === "azure";
  const cloudLabel = isAws ? "AWS" : isAzure ? "Azure" : "GCP";
  const resourceLabel = isAws ? "Account" : isAzure ? "Subscription" : "Project";
  const resourceId =
    isAws || isAzure ? settings.cloudAccountName : settings.googleCloudProjectId;

  const tabClass = (active: boolean) =>
    cn(
      "relative -mb-px border-b-2 px-1 pb-2.5 text-sm font-medium transition-colors duration-150 focus-visible:outline-none",
      active
        ? "border-primary text-foreground"
        : "border-transparent text-muted-foreground hover:text-foreground",
    );

  const content = (() => {
    if (loading) {
      return (
        <Card className="w-full">
          <CardContent className="space-y-6 p-5">
            <Skeleton className="h-5 w-40" />
            <Skeleton className="h-9 w-full" />
            <Skeleton className="h-9 w-full" />
            <Skeleton className="h-9 w-2/3" />
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
    <div className="flex flex-1 flex-col min-w-0">
      <div className="mx-auto flex w-full max-w-6xl flex-1 flex-col min-w-0">
        <PageHeader
          title="Settings"
          actions={
            showSaveButton ? (
              <Button onClick={handleSave} disabled={saving || loading || !!error} className="min-w-20">
                {saving ? <Loader2 className="animate-spin" /> : "Save"}
              </Button>
            ) : undefined
          }
        />

        <div className="flex flex-wrap items-baseline justify-between gap-x-4 gap-y-2 border-b border-border">
          <nav className="flex items-center gap-5" aria-label="Settings sections">
            <button
              type="button"
              onClick={() => handleSectionClick("cluster")}
              className={tabClass(section === "cluster")}
              aria-pressed={section === "cluster"}
            >
              Cluster
            </button>
            <button
              type="button"
              onClick={() => handleSectionClick("usage")}
              className={tabClass(section === "usage")}
              aria-pressed={section === "usage"}
            >
              Usage
            </button>
          </nav>
          {!loading && !error && (
            <p
              className="min-w-0 truncate pb-2.5 text-xs text-muted-foreground"
              aria-label={`${cloudLabel} ${resourceLabel} ${resourceId}`}
            >
              {cloudLabel}
              <span className="mx-1.5">·</span>
              {resourceLabel}{" "}
              <code className="font-mono" title={resourceId}>
                {resourceId}
              </code>
            </p>
          )}
        </div>

        <div className="mt-6 flex-1 min-w-0">{content}</div>
      </div>

      <AlertDialog open={showExitDialog} onOpenChange={setShowExitDialog}>
        <AlertDialogContent className="max-w-sm rounded-xl border border-border bg-card p-6 shadow-xl">
          <AlertDialogTitle className="text-base font-semibold text-foreground">
            Unsaved changes
          </AlertDialogTitle>
          <AlertDialogDescription className="mt-1 text-[13px] text-muted-foreground">
            Your cluster settings have unsaved changes.
          </AlertDialogDescription>

          <div className="mt-5 flex justify-end gap-2">
            <AlertDialogCancel className="hidden" />

            <AlertDialogAction
              onClick={() => {
                if (pendingNavRef.current) navigate(pendingNavRef.current);
              }}
              className="inline-flex h-9 items-center justify-center rounded-lg border border-border bg-card px-3.5 text-[13px] font-semibold text-foreground shadow-sm transition-colors duration-150 hover:bg-muted/60 focus:outline-none"
            >
              Discard changes
            </AlertDialogAction>

            <AlertDialogAction
              onClick={async () => {
                const ok = await handleSave();
                if (ok && pendingNavRef.current) navigate(pendingNavRef.current);
              }}
              className="inline-flex h-9 items-center justify-center rounded-lg bg-primary px-3.5 text-[13px] font-semibold text-primary-foreground shadow-sm transition-colors duration-150 hover:bg-primary/90 focus:outline-none"
            >
              Save and exit
            </AlertDialogAction>
          </div>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  );
};

export default SettingsPage;
