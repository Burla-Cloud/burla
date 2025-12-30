// import { useMemo } from "react";
// import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
// import { Skeleton } from "@/components/ui/skeleton";
// import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
// import { AlertTriangle } from "lucide-react";
// import {
//   Area,
//   AreaChart,
//   CartesianGrid,
//   ResponsiveContainer,
//   Tooltip,
//   XAxis,
//   YAxis,
// } from "recharts";
// import { getOnDemandHourlyUsd } from "@/types/constants";
// import { useUsage } from "@/contexts/UsageContext";

// function money(n: number) {
//   return new Intl.NumberFormat("en-US", { style: "currency", currency: "USD" }).format(
//     Number.isFinite(n) ? n : 0
//   );
// }

// function hours(n: number) {
//   return `${(Number.isFinite(n) ? n : 0).toFixed(2)}h`;
// }

// const UTC_TZ = "UTC";

// function fmtDayLabel(yyyyMmDd: string) {
//   const [y, m, d] = yyyyMmDd.split("-").map((x) => Number(x));
//   const dt = new Date(Date.UTC(y, (m || 1) - 1, d || 1));
//   return dt.toLocaleDateString(undefined, { month: "short", day: "numeric", timeZone: UTC_TZ });
// }

// function fmtMonthLabel(yyyyMm: string) {
//   const [y, m] = yyyyMm.split("-").map((x) => Number(x));
//   const dt = new Date(Date.UTC(y, (m || 1) - 1, 1));
//   return dt.toLocaleDateString(undefined, { month: "short", year: "numeric", timeZone: UTC_TZ });
// }

// function fmtDateTime(ms: number) {
//   const d = new Date(ms);
//   return d.toLocaleString(undefined, {
//     year: "numeric",
//     month: "short",
//     day: "2-digit",
//     hour: "2-digit",
//     minute: "2-digit",
//   });
// }

// function lastNMonthsUtc(n: number) {
//   const out: string[] = [];
//   const now = new Date();
//   let y = now.getUTCFullYear();
//   let m = now.getUTCMonth() + 1; // 1-12

//   for (let i = 0; i < n; i++) {
//     out.push(`${y}-${String(m).padStart(2, "0")}`);
//     m -= 1;
//     if (m === 0) {
//       m = 12;
//       y -= 1;
//     }
//   }
//   return out;
// }

// const PRIMARY = "hsl(var(--primary))";

// const UsageSettings = () => {
//   const { loading, error, daily, nodes, selectedMonth, setSelectedMonth } = useUsage();

//   const monthOptions = useMemo(() => lastNMonthsUtc(6), []);
//   const monthLabel = useMemo(() => fmtMonthLabel(selectedMonth), [selectedMonth]);

//   const totals = useMemo(() => {
//     let totalHours = 0;
//     let totalSpend = 0;
//     let unknownHours = 0;

//     for (const day of daily?.days || []) {
//       totalHours += Number(day.total_node_hours || 0);

//       for (const g of day.groups || []) {
//         const rate = getOnDemandHourlyUsd(g.gcp_region, g.machine_type);
//         const h = Number(g.total_node_hours || 0);

//         if (rate == null) {
//           unknownHours += h;
//           continue;
//         }

//         totalSpend += h * rate;
//       }
//     }

//     return {
//       totalHours,
//       totalSpend: Number(totalSpend.toFixed(2)),
//       unknownHours,
//     };
//   }, [daily]);

//   const chartData = useMemo(() => {
//     let cum = 0;

//     return (daily?.days || []).map((d) => {
//       let daySpend = 0;

//       for (const g of d.groups || []) {
//         const rate = getOnDemandHourlyUsd(g.gcp_region, g.machine_type);
//         if (rate == null) continue;
//         daySpend += Number(g.total_node_hours || 0) * rate;
//       }

//       cum += daySpend;

//       return {
//         date: d.date,
//         day: fmtDayLabel(d.date),
//         spend: Number(cum.toFixed(2)),
//       };
//     });
//   }, [daily]);

//   const nodeRows = useMemo(() => {
//     const list = (nodes?.nodes || []).slice();
//     list.sort((a, b) => (b.started_at_ms || 0) - (a.started_at_ms || 0));

//     return list.map((n) => {
//       const rate = getOnDemandHourlyUsd(n.gcp_region, n.machine_type);
//       const cost = rate == null ? 0 : Number(n.duration_hours || 0) * rate;

//       return {
//         ...n,
//         cost: Number(cost.toFixed(2)),
//         rateMissing: rate == null,
//         purchaseType: n.spot ? "Spot" : "On-demand",
//       };
//     });
//   }, [nodes]);

//   return (
//     <div className="space-y-6 max-w-6xl mx-auto w-full">
//       <Card className="w-full">
//         <CardHeader className="flex flex-row items-center justify-between gap-3">
//           <CardTitle className="text-xl font-semibold text-primary">Usage</CardTitle>

//           <select
//             className="h-9 rounded-md border border-border bg-background px-3 text-sm"
//             value={selectedMonth}
//             onChange={(e) => setSelectedMonth(e.target.value)}
//           >
//             {monthOptions.map((m) => (
//               <option key={m} value={m}>
//                 {fmtMonthLabel(m)}
//               </option>
//             ))}
//           </select>
//         </CardHeader>

//         <CardContent className="space-y-6">
//           {loading ? (
//             <>
//               <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
//                 <Card>
//                   <CardContent className="pt-6">
//                     <Skeleton className="h-4 w-24" />
//                     <Skeleton className="h-10 w-28 mt-2" />
//                     <Skeleton className="h-4 w-28 mt-3" />
//                   </CardContent>
//                 </Card>
//                 <Card>
//                   <CardContent className="pt-6">
//                     <Skeleton className="h-4 w-24" />
//                     <Skeleton className="h-10 w-28 mt-2" />
//                     <Skeleton className="h-4 w-28 mt-3" />
//                   </CardContent>
//                 </Card>
//               </div>

//               <Card>
//                 <CardContent className="pt-6">
//                   <Skeleton className="h-4 w-40" />
//                   <Skeleton className="h-56 w-full mt-4" />
//                 </CardContent>
//               </Card>

//               <Card>
//                 <CardContent className="pt-6">
//                   <Skeleton className="h-4 w-24" />
//                   <Skeleton className="h-72 w-full mt-4" />
//                 </CardContent>
//               </Card>
//             </>
//           ) : error ? (
//             <Alert variant="destructive" className="w-full">
//               <AlertTriangle className="h-4 w-4" />
//               <AlertTitle>Could not load usage</AlertTitle>
//               <AlertDescription>{error}</AlertDescription>
//             </Alert>
//           ) : !daily ? (
//             <div className="text-sm text-muted-foreground">No usage yet.</div>
//           ) : (
//             <>
//               <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
//                 <Card>
//                   <CardContent className="pt-6">
//                     <div className="text-sm text-muted-foreground">Usage</div>
//                     <div className="text-3xl font-semibold mt-1">{hours(totals.totalHours)}</div>
//                     <div className="text-sm text-muted-foreground mt-2">{monthLabel}</div>
//                   </CardContent>
//                 </Card>

//                 <Card>
//                   <CardContent className="pt-6">
//                     <div className="text-sm text-muted-foreground">Spend</div>
//                     <div className="text-3xl font-semibold mt-1">{money(totals.totalSpend)}</div>
//                     <div className="text-sm text-muted-foreground mt-2">{monthLabel}</div>
//                     {totals.unknownHours > 0 ? (
//                       <div className="text-xs text-muted-foreground mt-2">
//                         Missing pricing for {hours(totals.unknownHours)}.
//                       </div>
//                     ) : null}
//                   </CardContent>
//                 </Card>
//               </div>

//               <Card>
//                 <CardContent className="pt-6">
//                   <div className="flex items-baseline justify-between gap-3">
//                     <div className="text-sm text-muted-foreground">Spend over time</div>
//                     <div className="text-sm text-muted-foreground">{monthLabel}</div>
//                   </div>

//                   <div className="h-64 mt-3">
//                     <ResponsiveContainer width="100%" height="100%">
//                       <AreaChart data={chartData} margin={{ top: 8, right: 8, left: 0, bottom: 0 }}>
//                         <CartesianGrid stroke="hsl(var(--border))" vertical={false} />
//                         <XAxis
//                           dataKey="day"
//                           tickLine={false}
//                           axisLine={false}
//                           tick={{ fill: "hsl(var(--muted-foreground))", fontSize: 12 }}
//                           minTickGap={18}
//                         />
//                         <YAxis
//                           tickLine={false}
//                           axisLine={false}
//                           tick={{ fill: "hsl(var(--muted-foreground))", fontSize: 12 }}
//                           tickFormatter={(v) => money(Number(v || 0))}
//                         />
//                         <Tooltip
//                           formatter={(value: any) => [money(Number(value || 0)), "Spend"]}
//                           labelFormatter={(_, payload) =>
//                             payload?.[0]?.payload?.date ? `${payload[0].payload.date}` : ""
//                           }
//                           contentStyle={{
//                             borderRadius: 10,
//                             borderColor: "hsl(var(--border))",
//                             background: "hsl(var(--background))",
//                           }}
//                         />
//                         <Area
//                           type="monotone"
//                           dataKey="spend"
//                           stroke={PRIMARY}
//                           fill={PRIMARY}
//                           fillOpacity={0.12}
//                           strokeWidth={2}
//                           dot={false}
//                         />
//                       </AreaChart>
//                     </ResponsiveContainer>
//                   </div>
//                 </CardContent>
//               </Card>

//               {/* UPDATED NODES TABLE ONLY */}
//               <Card>
//                 <CardContent className="pt-6">
//                   <div className="text-sm text-muted-foreground">Nodes</div>

//                   <div className="mt-4 rounded-md border border-border overflow-hidden">
//                     <div className="grid grid-cols-12 bg-muted/30 text-xs font-medium text-muted-foreground">
//                       <div className="col-span-4 px-3 py-2">Node</div>
//                       <div className="col-span-2 px-3 py-2">Machine</div>
//                       <div className="col-span-2 px-3 py-2">Type</div>
//                       <div className="col-span-2 px-3 py-2">Region</div>
//                       <div className="col-span-1 px-3 py-2 text-right">Hours</div>
//                       <div className="col-span-1 px-3 py-2 text-right">Cost</div>
//                     </div>

//                     {nodeRows.length === 0 ? (
//                       <div className="px-3 py-4 text-sm text-muted-foreground">
//                         No nodes found for this month.
//                       </div>
//                     ) : (
//                       nodeRows.map((n) => (
//                         <div key={n.id} className="grid grid-cols-12 border-t border-border text-sm items-center">
//                           <div className="col-span-4 px-3 py-2 min-w-0">
//                             <div className="font-mono text-xs truncate">{n.instance_name || n.id}</div>
//                           </div>

//                           <div className="col-span-2 px-3 py-2 min-w-0">
//                             <div className="text-xs text-muted-foreground truncate">{n.machine_type}</div>
//                           </div>

//                           <div className="col-span-2 px-3 py-2">
//                             <div className="text-xs text-muted-foreground">{n.purchaseType}</div>
//                           </div>

//                           <div className="col-span-2 px-3 py-2">
//                             <div className="text-xs text-muted-foreground">{n.gcp_region}</div>
//                           </div>

//                           <div className="col-span-1 px-3 py-2 text-right">
//                             <div className="text-xs tabular-nums">{Number(n.duration_hours || 0).toFixed(2)}</div>
//                           </div>

//                           <div className="col-span-1 px-3 py-2 text-right">
//                             <div className="font-semibold tabular-nums">{money(n.cost)}</div>
//                             {n.rateMissing ? (
//                               <div className="text-[10px] text-muted-foreground">missing rate</div>
//                             ) : null}
//                           </div>
//                         </div>
//                       ))
//                     )}
//                   </div>
//                 </CardContent>
//               </Card>
//             </>
//           )}
//         </CardContent>
//       </Card>
//     </div>
//   );
// };

// export default UsageSettings;

// src/pages/Settings/UsageSettings.tsx

// src/pages/Settings/UsageSettings.tsx

// src/pages/Settings/UsageSettings.tsx

import { useMemo } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { AlertTriangle } from "lucide-react";
import { Area, AreaChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { getOnDemandHourlyUsd } from "@/types/constants";
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
    let totalHours = 0;
    let totalSpend = 0;
    let unknownHours = 0;

    for (const day of daily?.days || []) {
      totalHours += Number(day.total_node_hours || 0);

      for (const g of day.groups || []) {
        const rate = getOnDemandHourlyUsd(g.gcp_region, g.machine_type);
        const h = Number(g.total_node_hours || 0);

        if (rate == null) {
          unknownHours += h;
          continue;
        }

        totalSpend += h * rate;
      }
    }

    return {
      totalHours,
      totalSpend: Number(totalSpend.toFixed(2)),
      unknownHours,
    };
  }, [daily]);

  const chartData = useMemo(() => {
    let cum = 0;

    return (daily?.days || []).map((d) => {
      let daySpend = 0;

      for (const g of d.groups || []) {
        const rate = getOnDemandHourlyUsd(g.gcp_region, g.machine_type);
        if (rate == null) continue;
        daySpend += Number(g.total_node_hours || 0) * rate;
      }

      cum += daySpend;

      return {
        date: d.date,
        day: fmtDayLabel(d.date),
        spend: Number(cum.toFixed(2)),
      };
    });
  }, [daily]);

  const machineRows = useMemo(() => {
    const map = new Map<
      string,
      {
        key: string;
        machine_type: string;
        spot: boolean;
        purchaseType: string;
        totalHours: number;
        cost: number;
        rateMissing: boolean;
        nodeIds: Set<string>;
      }
    >();

    for (const n of nodes?.nodes || []) {
      const machineType = String(n.machine_type || "");
      const key = `${machineType}::${n.spot ? "spot" : "ondemand"}`;

      const rate = getOnDemandHourlyUsd(n.gcp_region, machineType);
      const h = Number(n.duration_hours || 0);
      const addCost = rate == null ? 0 : h * rate;

      const nodeId = String(n.id || n.instance_name || "");

      const existing = map.get(key);
      if (existing) {
        existing.totalHours += h;
        existing.cost += addCost;
        existing.rateMissing = existing.rateMissing || rate == null;
        if (nodeId) existing.nodeIds.add(nodeId);
      } else {
        const s = new Set<string>();
        if (nodeId) s.add(nodeId);

        map.set(key, {
          key,
          machine_type: machineType,
          spot: !!n.spot,
          purchaseType: n.spot ? "Spot" : "On-demand",
          totalHours: h,
          cost: addCost,
          rateMissing: rate == null,
          nodeIds: s,
        });
      }
    }

    const rows = Array.from(map.values()).map((r) => ({
      ...r,
      nodeCount: r.nodeIds.size,
      totalHours: Number(r.totalHours.toFixed(2)),
      cost: Number(r.cost.toFixed(2)),
    }));

    rows.sort(
      (a, b) =>
        b.cost - a.cost ||
        b.totalHours - a.totalHours ||
        b.nodeCount - a.nodeCount ||
        a.machine_type.localeCompare(b.machine_type)
    );

    return rows;
  }, [nodes]);

  const machineSummary = useMemo(() => {
    let totalNodes = 0;
    let totalHours = 0;
    let totalCost = 0;

    for (const r of machineRows) {
      totalNodes += r.nodeCount;
      totalHours += Number(r.totalHours || 0);
      totalCost += Number(r.cost || 0);
    }

    return {
      totalNodes,
      totalHours: Number(totalHours.toFixed(2)),
      totalCost: Number(totalCost.toFixed(2)),
    };
  }, [machineRows]);

  return (
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
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <Card>
                  <CardContent className="pt-6">
                    <Skeleton className="h-4 w-24" />
                    <Skeleton className="h-10 w-28 mt-2" />
                    <Skeleton className="h-4 w-28 mt-3" />
                  </CardContent>
                </Card>
                <Card>
                  <CardContent className="pt-6">
                    <Skeleton className="h-4 w-24" />
                    <Skeleton className="h-10 w-28 mt-2" />
                    <Skeleton className="h-4 w-28 mt-3" />
                  </CardContent>
                </Card>
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
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <Card>
                  <CardContent className="pt-6">
                    <div className="text-sm text-muted-foreground">Usage</div>
                    <div className="text-3xl font-semibold mt-1">{hours(totals.totalHours)}</div>
                    <div className="text-sm text-muted-foreground mt-2">{monthLabel}</div>
                  </CardContent>
                </Card>

                <Card>
                  <CardContent className="pt-6">
                    <div className="text-sm text-muted-foreground">Spend</div>
                    <div className="text-3xl font-semibold mt-1">{money(totals.totalSpend)}</div>
                    <div className="text-sm text-muted-foreground mt-2">{monthLabel}</div>
                    {totals.unknownHours > 0 ? (
                      <div className="text-xs text-muted-foreground mt-2">
                        Missing pricing for {hours(totals.unknownHours)}.
                      </div>
                    ) : null}
                  </CardContent>
                </Card>
              </div>

              <Card>
                <CardContent className="pt-6">
                  <div className="flex items-baseline justify-between gap-3">
                    <div className="text-sm text-muted-foreground">Spend over time</div>
                    <div className="text-sm text-muted-foreground">{monthLabel}</div>
                  </div>

                  <div className="h-64 mt-3">
                    <ResponsiveContainer width="100%" height="100%">
                      <AreaChart data={chartData} margin={{ top: 8, right: 8, left: 0, bottom: 0 }}>
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
                          formatter={(value: any) => [money(Number(value || 0)), "Spend"]}
                          labelFormatter={(_, payload) =>
                            payload?.[0]?.payload?.date ? `${payload[0].payload.date}` : ""
                          }
                          contentStyle={{
                            borderRadius: 10,
                            borderColor: "hsl(var(--border))",
                            background: "hsl(var(--background))",
                          }}
                        />
                        <Area
                          type="monotone"
                          dataKey="spend"
                          stroke={PRIMARY}
                          fill={PRIMARY}
                          fillOpacity={0.12}
                          strokeWidth={2}
                          dot={false}
                        />
                      </AreaChart>
                    </ResponsiveContainer>
                  </div>
                </CardContent>
              </Card>

              <Card>
                <CardContent className="pt-6">
                  <div className="flex items-start justify-between gap-4">
                    <div>
                      <div className="text-sm text-muted-foreground">Machines</div>
                      <div className="mt-1 text-xs text-muted-foreground">
                        Total{" "}
                        <span className="font-medium text-foreground tabular-nums">
                          {machineSummary.totalNodes}
                        </span>{" "}
                        nodes
                        <span className="mx-2 text-muted-foreground/50">•</span>
                        <span className="font-medium text-foreground tabular-nums">
                          {machineSummary.totalHours.toFixed(2)}
                        </span>{" "}
                        hours
                        <span className="mx-2 text-muted-foreground/50">•</span>
                        <span className="font-medium text-foreground tabular-nums">
                          {money(machineSummary.totalCost)}
                        </span>
                      </div>
                    </div>
                  </div>

                  <div className="mt-4 rounded-md border border-border overflow-hidden">
                    <div className="grid grid-cols-12 bg-muted/30 text-xs font-medium text-muted-foreground">
                      <div className="col-span-5 px-4 py-3">Machine</div>
                      <div className="col-span-2 px-4 py-3">Type</div>
                      <div className="col-span-1 px-4 py-3 text-right">Nodes</div>
                      <div className="col-span-2 px-4 py-3 text-right">Hours</div>
                      <div className="col-span-2 px-4 py-3 text-right">Cost</div>
                    </div>

                    {machineRows.length === 0 ? (
                      <div className="px-4 py-4 text-sm text-muted-foreground">
                        No machines found for this month.
                      </div>
                    ) : (
                      machineRows.map((r) => (
                        <div key={r.key} className="grid grid-cols-12 border-t border-border items-center">
                          <div className="col-span-5 px-4 py-3 min-w-0">
                            <div className="font-medium text-sm truncate">{r.machine_type}</div>
                          </div>

                          <div className="col-span-2 px-4 py-3">
                            <span className="inline-flex items-center rounded-full bg-muted px-2 py-0.5 text-xs text-muted-foreground">
                              {r.purchaseType}
                            </span>
                          </div>

                          <div className="col-span-1 px-4 py-3 text-right">
                            <div className="text-sm tabular-nums">{r.nodeCount}</div>
                          </div>

                          <div className="col-span-2 px-4 py-3 text-right">
                            <div className="text-sm tabular-nums">{r.totalHours.toFixed(2)}</div>
                          </div>

                          <div className="col-span-2 px-4 py-3 text-right">
                            <div className="font-semibold text-sm tabular-nums">{money(r.cost)}</div>
                            {r.rateMissing ? (
                              <div className="text-[10px] text-muted-foreground">missing rate</div>
                            ) : null}
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
  );
};

export default UsageSettings;
