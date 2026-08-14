import { useMemo } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { AlertTriangle } from "lucide-react";
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
  const { loading, error, daily, nodes, selectedMonth, setSelectedMonth } = useUsage();

  const monthOptions = useMemo(() => lastNMonthsUtc(6), []);
  const monthLabel = useMemo(() => fmtMonthLabel(selectedMonth), [selectedMonth]);

  const totals = useMemo(() => {
    let totalComputeHours = 0;
    let totalSpend = 0;
    let unknownNodeHours = 0;

    for (const day of daily?.days || []) {
      totalComputeHours += Number(day.total_compute_hours || 0);

      for (const g of day.groups || []) {
        const rate = getOnDemandHourlyUsdForMachine(g.machine_type);
        const nodeHours = Number(g.total_node_hours || 0);

        if (rate == null) {
          unknownNodeHours += nodeHours;
          continue;
        }

        totalSpend += nodeHours * rate;
      }
    }

    return {
      totalComputeHours,
      totalSpend: Number(totalSpend.toFixed(2)),
      unknownNodeHours,
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

  if (loading) {
    return (
      <div className="space-y-5">
        <div className="flex justify-end">
          <Skeleton className="h-9 w-40" />
        </div>
        <Card className="grid grid-cols-2 divide-x divide-border/70">
          <div className="px-5 py-4">
            <Skeleton className="h-4 w-24" />
            <Skeleton className="mt-2 h-7 w-20" />
          </div>
          <div className="px-5 py-4">
            <Skeleton className="h-4 w-24" />
            <Skeleton className="mt-2 h-7 w-20" />
          </div>
        </Card>
        <Card>
          <CardContent className="p-5">
            <Skeleton className="h-4 w-28" />
            <Skeleton className="mt-4 h-56 w-full" />
          </CardContent>
        </Card>
      </div>
    );
  }

  if (error) {
    return (
      <Alert variant="destructive" className="w-full">
        <AlertTriangle className="h-4 w-4" />
        <AlertTitle>Could not load usage</AlertTitle>
        <AlertDescription>{error}</AlertDescription>
      </Alert>
    );
  }

  return (
    <div className="space-y-5">
      <div className="flex items-center justify-end">
        <Select value={selectedMonth} onValueChange={setSelectedMonth}>
          <SelectTrigger className="w-40">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            {monthOptions.map((m) => (
              <SelectItem key={m} value={m}>
                {fmtMonthLabel(m)}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>

      {!daily ? (
        <Card>
          <CardContent className="p-5 text-sm text-muted-foreground">No usage yet.</CardContent>
        </Card>
      ) : (
        <>
          <Card className="grid grid-cols-1 divide-y divide-border/70 sm:grid-cols-2 sm:divide-x sm:divide-y-0">
            <div className="px-5 py-4">
              <div className="text-[13px] text-muted-foreground">Compute hours</div>
              <div className="mt-0.5 text-2xl font-semibold tabular-nums tracking-tight text-foreground">
                {hours(totals.totalComputeHours)}
              </div>
            </div>
            <div className="px-5 py-4">
              <div className="text-[13px] text-muted-foreground">Estimated spend</div>
              <div className="mt-0.5 text-2xl font-semibold tabular-nums tracking-tight text-foreground">
                {money(totals.totalSpend)}
              </div>
              {totals.unknownNodeHours > 0 && (
                <div className="mt-1 text-xs text-muted-foreground">
                  Missing pricing for {hours(totals.unknownNodeHours)}.
                </div>
              )}
            </div>
          </Card>

          <Card>
            <CardHeader className="flex-row items-baseline justify-between space-y-0 px-5 py-4">
              <CardTitle>Daily spend</CardTitle>
              <span className="text-[13px] text-muted-foreground">{monthLabel}</span>
            </CardHeader>
            <CardContent className="px-5 pb-5 pt-0">
              <div className="h-64">
                <ResponsiveContainer width="100%" height="100%">
                  <BarChart data={chartData} margin={{ top: 8, right: 8, left: 0, bottom: 0 }}>
                    <CartesianGrid stroke="hsl(var(--border) / 0.6)" vertical={false} />
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
                      cursor={{ fill: "hsl(var(--muted) / 0.5)" }}
                      content={({ active, payload }) => {
                        if (!active || !payload?.length) return null;

                        const p: any = payload[0]?.payload || {};
                        const spend = money(Number(p.spend || 0));
                        const u = Number(p.unknownHours || 0);

                        return (
                          <div className="rounded-lg border border-border bg-popover px-3 py-2 shadow-md">
                            <div className="text-xs font-medium text-muted-foreground">
                              {p.date}
                            </div>
                            <div className="mt-0.5 text-sm font-semibold tabular-nums text-foreground">
                              {spend}
                            </div>
                            {u > 0 && (
                              <div className="mt-0.5 text-xs text-muted-foreground">
                                Missing rate for {hours(u)}
                              </div>
                            )}
                          </div>
                        );
                      }}
                    />
                    <Bar dataKey="spend" fill={PRIMARY} radius={[4, 4, 0, 0]} maxBarSize={28} />
                  </BarChart>
                </ResponsiveContainer>
              </div>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="border-b border-border/70 px-5 py-4">
              <CardTitle>Compute types</CardTitle>
            </CardHeader>
            <CardContent className="p-0">
              {vmRows.length === 0 ? (
                <div className="px-5 py-6 text-sm text-muted-foreground">
                  No usage found for this month.
                </div>
              ) : (
                <Table>
                  <TableHeader>
                    <TableRow className="hover:bg-transparent">
                      <TableHead className="pl-5">Type</TableHead>
                      <TableHead className="text-right">Hours</TableHead>
                      <TableHead className="pr-5 text-right">Cost</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {vmRows.map((r) => (
                      <TableRow key={r.vm} className="hover:bg-transparent">
                        <TableCell className="pl-5 text-sm font-medium text-foreground">
                          {r.vm}
                        </TableCell>
                        <TableCell className="text-right tabular-nums">
                          {r.totalComputeHours.toFixed(2)}
                        </TableCell>
                        <TableCell className="pr-5 text-right">
                          <span className="text-sm font-medium tabular-nums text-foreground">
                            {money(r.cost)}
                          </span>
                          {r.rateMissing && (
                            <div className="text-[11px] text-muted-foreground">missing rate</div>
                          )}
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              )}
            </CardContent>
          </Card>
        </>
      )}
    </div>
  );
};

export default UsageSettings;
