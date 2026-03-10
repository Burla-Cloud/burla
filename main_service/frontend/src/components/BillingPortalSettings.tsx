import { useCallback, useEffect, useMemo, useState } from "react";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { AlertTriangle, CheckCircle2, CreditCard, ExternalLink, Loader2 } from "lucide-react";

type ViewState = "idle" | "processing" | "error" | "returned";
const PORTAL_REDIRECT_PATH = "/billing/portal-session/redirect";

const BillingPortalSettings = () => {
  const [viewState, setViewState] = useState<ViewState>("idle");
  const [error, setError] = useState<string | null>(null);

  const setupState = useMemo(() => {
    const params = new URLSearchParams(window.location.search);
    return {
      billingSetup: params.get("billing_setup"),
      checkoutSessionId: params.get("checkout_session_id"),
      didReturnFromPortal: params.get("billing_portal_return") === "1",
    };
  }, []);

  const clearQueryParam = useCallback((name: string) => {
    const url = new URL(window.location.href);
    if (!url.searchParams.has(name)) return;
    url.searchParams.delete(name);
    window.history.replaceState({}, "", url.toString());
  }, []);

  const clearSetupQueryParams = useCallback(() => {
    const url = new URL(window.location.href);
    let changed = false;
    for (const key of ["billing_setup", "checkout_session_id"]) {
      if (!url.searchParams.has(key)) continue;
      url.searchParams.delete(key);
      changed = true;
    }
    if (changed) {
      window.history.replaceState({}, "", url.toString());
    }
  }, []);

  const openPortalInNewTab = useCallback(() => {
    setError(null);
    const popup = window.open(PORTAL_REDIRECT_PATH, "_blank");
    if (popup) return;
    setError("Popup blocked by browser. Please allow popups for this site and try again.");
    setViewState("error");
  }, []);

  const confirmSetupSession = useCallback(async (checkoutSessionId: string) => {
    const res = await fetch("/billing/confirm-setup-session", {
      method: "POST",
      credentials: "include",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ session_id: checkoutSessionId }),
    });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const payload = await res.json().catch(() => ({}));
    if (payload?.has_payment_method === true) {
      window.dispatchEvent(new Event("burla:payment-method-updated"));
    }
  }, []);

  useEffect(() => {
    const run = async () => {
      if (setupState.billingSetup === "success" && setupState.checkoutSessionId) {
        setError(null);
        setViewState("processing");
        try {
          await confirmSetupSession(setupState.checkoutSessionId);
          setViewState("returned");
        } catch (err: any) {
          setError(err?.message || "Could not confirm Stripe setup session.");
          setViewState("error");
        } finally {
          clearSetupQueryParams();
        }
        return;
      }

      if (setupState.billingSetup === "cancel") {
        clearSetupQueryParams();
        setViewState("idle");
        return;
      }

      if (setupState.didReturnFromPortal) {
        clearQueryParam("billing_portal_return");
        setViewState("returned");
        return;
      }

      setViewState("idle");
    };

    run();
  }, [
    clearQueryParam,
    clearSetupQueryParams,
    confirmSetupSession,
    setupState.billingSetup,
    setupState.checkoutSessionId,
    setupState.didReturnFromPortal,
  ]);

  const processingView = (
    <div className="min-h-[420px] flex items-center justify-center">
      <div className="w-full max-w-md rounded-xl border border-border bg-card p-7 shadow-sm text-center">
        <div className="mx-auto w-14 h-14 rounded-full bg-primary/10 flex items-center justify-center">
          <Loader2 className="h-7 w-7 animate-spin text-primary" />
        </div>
        <h3 className="mt-4 text-xl font-semibold text-foreground">Finalizing payment setup</h3>
        <p className="mt-2 text-sm text-muted-foreground">
          Confirming your saved card details from Stripe.
        </p>
      </div>
    </div>
  );

  const idleView = (
    <div className="min-h-[420px] flex items-center justify-center">
      <div className="w-full max-w-md rounded-xl border border-border bg-card p-7 shadow-sm text-center">
        <div className="mx-auto w-14 h-14 rounded-full bg-primary/10 flex items-center justify-center">
          <CreditCard className="h-7 w-7 text-primary" />
        </div>
        <h3 className="mt-4 text-xl font-semibold text-foreground">Manage payment method</h3>
        <p className="mt-2 text-sm text-muted-foreground">
          Open Stripe in a new tab to securely update your card details.
        </p>
        <div className="mt-6 flex justify-center">
          <Button onClick={openPortalInNewTab}>
            <ExternalLink className="mr-2 h-4 w-4" />
            Open payment update
          </Button>
        </div>
      </div>
    </div>
  );

  const returnedView = (
    <div className="min-h-[420px] flex items-center justify-center">
      <div className="w-full max-w-md rounded-xl border border-border bg-card p-7 shadow-sm text-center">
        <div className="mx-auto w-14 h-14 rounded-full bg-emerald-100 flex items-center justify-center">
          <CheckCircle2 className="h-7 w-7 text-emerald-700" />
        </div>
        <h3 className="mt-4 text-xl font-semibold text-foreground">Payment method updated</h3>
        <p className="mt-2 text-sm text-muted-foreground">
          If you need to make another change, reopen Stripe below.
        </p>
        <div className="mt-6 flex justify-center">
          <Button onClick={openPortalInNewTab}>
            <ExternalLink className="mr-2 h-4 w-4" />
            Open payment update
          </Button>
        </div>
      </div>
    </div>
  );

  const errorView = (
    <div className="space-y-4">
      <Alert variant="destructive">
        <AlertTriangle className="h-4 w-4" />
        <AlertTitle>Could not open Stripe payment update</AlertTitle>
        <AlertDescription>{error || "Please try again."}</AlertDescription>
      </Alert>
      <Button onClick={openPortalInNewTab}>Try again</Button>
    </div>
  );

  return (
    <Card className="w-full">
      <CardHeader>
        <CardTitle className="text-xl font-semibold text-primary">Billing</CardTitle>
      </CardHeader>
      <CardContent>
        {viewState === "error" ? errorView : null}
        {viewState === "returned" ? returnedView : null}
        {viewState === "idle" ? idleView : null}
        {viewState === "processing" ? processingView : null}
      </CardContent>
    </Card>
  );
};

export default BillingPortalSettings;
