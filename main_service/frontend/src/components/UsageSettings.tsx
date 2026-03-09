import { useEffect, useMemo, useRef, useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog";
import { AlertTriangle, CreditCard, Loader2 } from "lucide-react";
import { Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { getOnDemandHourlyUsdForMachine, getVmCategory, VM_TYPES, type VmType } from "@/types/constants";
import { useUsage } from "@/contexts/UsageContext";

function money(n: number) {
  return new Intl.NumberFormat("en-US", { style: "currency", currency: "USD" }).format(
    Number.isFinite(n) ? n : 0
  );
}

function hours(n: number) {
  return `${(Number.isFinite(n) ? n : 0).toFixed(2)}h`;
}

const UTC_TZ = "UTC";

function fmtDayLabel(yyyyMmDd: string) {
  const [y, m, d] = yyyyMmDd.split("-").map((x) => Number(x));
  const dt = new Date(Date.UTC(y, (m || 1) - 1, d || 1));
  return dt.toLocaleDateString(undefined, { month: "short", day: "numeric", timeZone: UTC_TZ });
}

function fmtMonthLabel(yyyyMm: string) {
  const [y, m] = yyyyMm.split("-").map((x) => Number(x));
  const dt = new Date(Date.UTC(y, (m || 1) - 1, 1));
  return dt.toLocaleDateString(undefined, { month: "short", year: "numeric", timeZone: UTC_TZ });
}

function lastNMonthsUtc(n: number) {
  const out: string[] = [];
  const now = new Date();
  let y = now.getUTCFullYear();
  let m = now.getUTCMonth() + 1;

  for (let i = 0; i < n; i++) {
    out.push(`${y}-${String(m).padStart(2, "0")}`);
    m -= 1;
    if (m === 0) {
      m = 12;
      y -= 1;
    }
  }
  return out;
}

const PRIMARY = "hsl(var(--primary))";

const UsageSettings = () => {
  const { loading, error, daily, nodes, selectedMonth, setSelectedMonth, refresh } = useUsage();
  const [showCreditsUsedModal, setShowCreditsUsedModal] = useState(false);
  const [isStartingBillingSetup, setIsStartingBillingSetup] = useState(false);
  const hasConfirmedSetupRef = useRef(false);
  const hasWarmedMonthlyUsageCacheRef = useRef(false);

  const monthOptions = useMemo(() => lastNMonthsUtc(6), []);
  const monthLabel = useMemo(() => fmtMonthLabel(selectedMonth), [selectedMonth]);

  const totals = useMemo(() => {
    return {
      totalComputeHours: Number((Number(daily?.monthly_usage_hours || 0)).toFixed(2)),
      totalSpend: Number((Number(daily?.monthly_spend_dollars || 0)).toFixed(2)),
      credits: Number((Number(daily?.credits_usd || 0)).toFixed(2)),
      creditsUsed: Number((Number(daily?.credits_used_usd || 0)).toFixed(2)),
    };
  }, [daily]);

  const chartData = useMemo(() => {
  return (daily?.days || []).map((d) => {
    let daySpend = 0;
    let unknownHours = 0;

    for (const g of d.groups || []) {
      const rate = getOnDemandHourlyUsdForMachine(g.machine_type);
      const h = Number(g.total_node_hours || 0); // spend uses node-hours

      if (rate == null) {
        unknownHours += h;
        continue;
      }

      daySpend += h * rate;
    }

    return {
      date: d.date,
      day: fmtDayLabel(d.date),
      spend: Number(daySpend.toFixed(2)),
      unknownHours: Number(unknownHours.toFixed(2)),
      hours: Number(d.total_compute_hours || 0), // tooltip "total hours" (compute-hours)
    };
  });
}, [daily]);

  const vmRows = useMemo(() => {
    const buckets = new Map<VmType, { vm: VmType; totalComputeHours: number; cost: number; rateMissing: boolean }>();

    for (const vm of VM_TYPES) {
      buckets.set(vm, { vm, totalComputeHours: 0, cost: 0, rateMissing: false });
    }

    for (const n of nodes?.nodes || []) {
      const machineType = String(n.machine_type || "");
      const vm = getVmCategory(machineType);
      if (!vm) continue;

      const computeHours = Number(n.duration_compute_hours || 0);
      const nodeHours = Number(n.duration_hours || 0);
      const rate = getOnDemandHourlyUsdForMachine(machineType);

      const b = buckets.get(vm);
      if (!b) continue;

      b.totalComputeHours += computeHours;

      if (rate == null) {
        b.rateMissing = true;
      } else {
        b.cost += nodeHours * rate;
      }
    }

    const rows = Array.from(buckets.values())
      .map((r) => ({
        vm: r.vm,
        totalComputeHours: Number(r.totalComputeHours.toFixed(2)),
        cost: Number(r.cost.toFixed(2)),
        rateMissing: r.rateMissing,
      }))
      .filter((r) => r.totalComputeHours > 0);

    rows.sort((a, b) => b.cost - a.cost || b.totalComputeHours - a.totalComputeHours);

    return rows;
  }, [nodes]);

  const shouldShowCreditsUsedModal = useMemo(() => {
    if (loading || !!error || !daily) return false;
    if (!daily.credits) return false;
    if (daily.has_payment_method) return false;
    if (Number(daily.credits_usd || 0) <= 0) return false;
    return Number(daily.remaining_free_credit_usd || 0) <= 0;
  }, [loading, error, daily]);

  useEffect(() => {
    if (shouldShowCreditsUsedModal) {
      setShowCreditsUsedModal(true);
    }
  }, [shouldShowCreditsUsedModal]);

  useEffect(() => {
    if (hasWarmedMonthlyUsageCacheRef.current) return;
    hasWarmedMonthlyUsageCacheRef.current = true;

    const run = async () => {
      try {
        await fetch("/v1/nodes/monthly_hours?months_back=6", {
          credentials: "include",
          headers: { Accept: "application/json" },
        });
      } catch (err) {
        console.error("Failed to warm monthly usage cache", err);
      }
    };

    run();
  }, []);

  useEffect(() => {
    if (hasConfirmedSetupRef.current) return;

    const params = new URLSearchParams(window.location.search);
    const billingSetupStatus = params.get("billing_setup");
    const checkoutSessionId = params.get("checkout_session_id");
    if (billingSetupStatus !== "success" || !checkoutSessionId) return;

    hasConfirmedSetupRef.current = true;

    const run = async () => {
      try {
        const res = await fetch("/billing/confirm-setup-session", {
          method: "POST",
          credentials: "include",
          headers: {
            Accept: "application/json",
            "Content-Type": "application/json",
          },
          body: JSON.stringify({ session_id: checkoutSessionId }),
        });
        if (res.ok) {
          const payload = await res.json().catch(() => ({}));
          if (payload?.has_payment_method === true) {
            window.dispatchEvent(new Event("burla:payment-method-updated"));
          }
        }
      } catch (err) {
        console.error("Failed to confirm Stripe setup session", err);
      } finally {
        await refresh(selectedMonth);

        // Remove temporary Stripe setup params after processing.
        const nextUrl = new URL(window.location.href);
        nextUrl.searchParams.delete("billing_setup");
        nextUrl.searchParams.delete("checkout_session_id");
        window.history.replaceState({}, "", nextUrl.toString());
      }
    };

    run();
  }, [refresh, selectedMonth]);

  const handleAddPaymentMethod = async () => {
    if (isStartingBillingSetup) return;
    setIsStartingBillingSetup(true);
    try {
      const res = await fetch("/billing/setup-session", {
        method: "POST",
        credentials: "include",
        headers: { Accept: "application/json" },
      });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const payload = await res.json();
      const redirectUrl = typeof payload?.url === "string" ? payload.url : "";
      if (!redirectUrl) throw new Error("Missing Stripe setup redirect URL");
      window.location.assign(redirectUrl);
    } catch (err) {
      console.error("Failed to create Stripe setup session", err);
      setIsStartingBillingSetup(false);
    }
  };

  return (
    <>
      {isStartingBillingSetup ? (
        <div className="fixed inset-0 z-[120] bg-background/70 backdrop-blur-sm flex items-center justify-center px-6">
          <div className="w-full max-w-sm rounded-xl border border-border bg-card shadow-lg p-6">
            <div className="flex items-center justify-center">
              <div className="rounded-full bg-primary/10 p-3">
                <Loader2 className="h-6 w-6 animate-spin text-primary" />
              </div>
            </div>
            <div className="mt-4 text-center">
              <div className="text-base font-semibold text-foreground">Redirecting to payment portal</div>
              <div className="mt-1 text-sm text-muted-foreground">
                Please wait while we open Stripe Checkout.
              </div>
            </div>
          </div>
        </div>
      ) : null}

      <AlertDialog
        open={showCreditsUsedModal}
        onOpenChange={(next) => {
          if (isStartingBillingSetup) return;
          setShowCreditsUsedModal(next);
        }}
      >
        <AlertDialogContent className="sm:max-w-md border-border/80 shadow-xl">
          <AlertDialogHeader className="space-y-3 text-left">
            <div className="inline-flex h-10 w-10 items-center justify-center rounded-full bg-primary/10">
              <CreditCard className="h-5 w-5 text-primary" />
            </div>
            <AlertDialogTitle className="text-xl">Free credits used</AlertDialogTitle>
            <AlertDialogDescription className="text-sm leading-relaxed text-muted-foreground">
              You've used all free credits. Add a payment method to continue using Burla.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter className="!flex !justify-center !items-center sm:!justify-center sm:!space-x-0 pt-1">
            <AlertDialogAction
              onClick={handleAddPaymentMethod}
              disabled={isStartingBillingSetup}
              className="min-w-[220px] h-10 text-sm font-medium mx-auto"
            >
              {isStartingBillingSetup ? (
                <span className="inline-flex items-center gap-2">
                  <Loader2 className="h-4 w-4 animate-spin" />
                  Redirecting...
                </span>
              ) : (
                "Add payment method"
              )}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      <div className="space-y-6 max-w-6xl mx-auto w-full">
        <Card className="w-full">
        <CardHeader className="flex flex-row items-center justify-between gap-3">
          <CardTitle className="text-xl font-semibold text-primary">Usage</CardTitle>

          <select
            className="h-9 rounded-md border border-border bg-background px-3 text-sm"
            value={selectedMonth}
            onChange={(e) => setSelectedMonth(e.target.value)}
          >
            {monthOptions.map((m) => (
              <option key={m} value={m}>
                {fmtMonthLabel(m)}
              </option>
            ))}
          </select>
        </CardHeader>

        <CardContent className="space-y-6">
          {loading ? (
            <>
              <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-4">
                {[0, 1, 2, 3].map((idx) => (
                  <Card key={idx}>
                    <CardContent className="pt-6">
                      <Skeleton className="h-4 w-24" />
                      <Skeleton className="h-10 w-28 mt-2" />
                      <Skeleton className="h-4 w-28 mt-3" />
                    </CardContent>
                  </Card>
                ))}
              </div>

              <Card>
                <CardContent className="pt-6">
                  <Skeleton className="h-4 w-40" />
                  <Skeleton className="h-56 w-full mt-4" />
                </CardContent>
              </Card>

              <Card>
                <CardContent className="pt-6">
                  <Skeleton className="h-4 w-24" />
                  <Skeleton className="h-72 w-full mt-4" />
                </CardContent>
              </Card>
            </>
          ) : error ? (
            <Alert variant="destructive" className="w-full">
              <AlertTriangle className="h-4 w-4" />
              <AlertTitle>Could not load usage</AlertTitle>
              <AlertDescription>{error}</AlertDescription>
            </Alert>
          ) : !daily ? (
            <div className="text-sm text-muted-foreground">No usage yet.</div> 
          ) : (
            <>
              <div className={`grid grid-cols-1 md:grid-cols-2 ${daily.credits ? "xl:grid-cols-4" : "xl:grid-cols-2"} gap-4`}>
                <Card>
                  <CardContent className="pt-6">
                    <div className="text-sm text-muted-foreground">Usage</div>
                    <div className="text-3xl font-semibold mt-1">{hours(totals.totalComputeHours)}</div>
                    <div className="text-sm text-muted-foreground mt-2">{monthLabel}</div>
                  </CardContent>
                </Card>

                <Card>
                  <CardContent className="pt-6">
                    <div className="text-sm text-muted-foreground">Spend</div>
                    <div className="text-3xl font-semibold mt-1">{money(totals.totalSpend)}</div>
                    <div className="text-sm text-muted-foreground mt-2">{monthLabel}</div>
                  </CardContent>
                </Card>

                {daily.credits ? (
                  <>
                    <Card>
                      <CardContent className="pt-6">
                        <div className="text-sm text-muted-foreground">Credits</div>
                        <div className="text-3xl font-semibold mt-1">{money(totals.credits)}</div>
                        <div className="text-sm text-muted-foreground mt-2">All time</div>
                      </CardContent>
                    </Card>

                    <Card>
                      <CardContent className="pt-6">
                        <div className="text-sm text-muted-foreground">Credits Used</div>
                        <div className="text-3xl font-semibold mt-1">{money(totals.creditsUsed)}</div>
                        <div className="text-sm text-muted-foreground mt-2">All time</div>
                      </CardContent>
                    </Card>
                  </>
                ) : null}
              </div>

              <Card>
                <CardContent className="pt-6">
                  <div className="flex items-baseline justify-between gap-3">
                    <div className="text-sm text-muted-foreground">Daily spend</div>
                    <div className="text-sm text-muted-foreground">{monthLabel}</div>
                  </div>

                  <div className="h-64 mt-3">
                    <ResponsiveContainer width="100%" height="100%">
                      <BarChart data={chartData} margin={{ top: 8, right: 8, left: 0, bottom: 0 }}>
                        <CartesianGrid stroke="hsl(var(--border))" vertical={false} />
                        <XAxis
                          dataKey="day"
                          tickLine={false}
                          axisLine={false}
                          tick={{ fill: "hsl(var(--muted-foreground))", fontSize: 12 }}
                          minTickGap={18}
                        />
                        <YAxis
                          tickLine={false}
                          axisLine={false}
                          tick={{ fill: "hsl(var(--muted-foreground))", fontSize: 12 }}
                          tickFormatter={(v) => money(Number(v || 0))}
                        />
                        <Tooltip
                          content={({ active, payload }) => {
                              if (!active || !payload?.length) return null;

                              const p: any = payload[0]?.payload || {};
                              const spend = money(Number(p.spend || 0));
                              const h = Number(p.hours || 0);
                              const u = Number(p.unknownHours || 0);

                              return (
                                <div
                                  style={{
                                    borderRadius: 10,
                                    border: "1px solid hsl(var(--border))",
                                    background: "hsl(var(--background))",
                                    padding: "10px 12px",
                                  }}
                                >
                                  <div className="text-sm font-medium">{p.date}</div>
                                  <div className="text-sm mt-1">Spend: {spend}</div>
                                  {u > 0 ? (
                                    <div className="text-xs text-muted-foreground mt-1">
                                      Missing rate for {hours(u)}
                                    </div>
                                  ) : null}
                                </div>
                              );
                            }}
                          />
                        <Bar dataKey="spend" fill={PRIMARY} radius={[6, 6, 0, 0]} />
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                </CardContent>
              </Card>

              <Card>
                <CardContent className="pt-6">
                  <div className="text-sm text-muted-foreground">Compute types</div>

                  <div className="mt-4 rounded-md border border-border overflow-hidden">
                    <div className="grid grid-cols-12 bg-muted/30 text-xs font-medium text-muted-foreground">
                      <div className="col-span-6 px-4 py-3">Type</div>
                      <div className="col-span-3 px-4 py-3 text-right">Hours</div>
                      <div className="col-span-3 px-4 py-3 text-right">Cost</div>
                    </div>

                    {vmRows.length === 0 ? (
                      <div className="px-4 py-4 text-sm text-muted-foreground">No usage found for this month.</div>
                    ) : (
                      vmRows.map((r) => (
                        <div key={r.vm} className="grid grid-cols-12 border-t border-border items-center">
                          <div className="col-span-6 px-4 py-3 min-w-0">
                            <div className="font-medium text-sm truncate">{r.vm}</div>
                          </div>

                          <div className="col-span-3 px-4 py-3 text-right">
                            <div className="text-sm tabular-nums">{r.totalComputeHours.toFixed(2)}</div>
                          </div>

                          <div className="col-span-3 px-4 py-3 text-right">
                            <div className="font-semibold text-sm tabular-nums">{money(r.cost)}</div>
                            {r.rateMissing ? <div className="text-[10px] text-muted-foreground">missing rate</div> : null}
                          </div>
                        </div>
                      ))
                    )}
                  </div>
                </CardContent>
              </Card>
            </>
          )}
        </CardContent>
        </Card>
      </div>
    </>
  );
};

export default UsageSettings;
