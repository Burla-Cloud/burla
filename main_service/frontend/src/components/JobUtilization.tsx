import { useCallback, useEffect, useMemo, useState } from "react";
import {
    ChartSkeletons,
    MetricChart,
    PRIMARY,
    SECONDARY,
    formatRate,
} from "@/components/JobMetricChart";

type JobPoint = {
    t: number;
    nodes: number;
    cpu: number;
    mem: number;
    net_rx: number;
    net_tx: number;
    disk_read: number;
    disk_write: number;
    gpu: number | null;
    gpu_mem: number | null;
};

type JobSeries = {
    has_metrics: boolean;
    has_gpu: boolean;
    bucket_sec: number;
    points: JobPoint[];
};

const JobUtilization = ({ jobId, jobStatus }: { jobId: string; jobStatus: string | null }) => {
    const [jobSeries, setJobSeries] = useState<JobSeries | null>(null);
    const [loadFailed, setLoadFailed] = useState(false);

    const isLive = jobStatus === "RUNNING" || jobStatus === "PENDING";

    const loadJobSeries = useCallback(async () => {
        try {
            const res = await fetch(`/v1/jobs/${jobId}/metrics`);
            if (!res.ok) throw new Error();
            setJobSeries(await res.json());
            setLoadFailed(false);
        } catch {
            setLoadFailed(true);
        }
    }, [jobId]);

    useEffect(() => {
        setJobSeries(null);
        setLoadFailed(false);
    }, [jobId]);

    useEffect(() => {
        void loadJobSeries();
        if (!isLive) return;
        const id = window.setInterval(() => void loadJobSeries(), 5000);
        return () => window.clearInterval(id);
    }, [isLive, loadJobSeries]);

    const jobData = useMemo(() => jobSeries?.points ?? [], [jobSeries]);
    const jobStartAt = jobData.length ? jobData[0].t : 0;

    return (
        <div className="rounded-xl border border-border bg-card shadow-sm">
            {loadFailed ? (
                <div className="px-5 py-6 text-sm text-muted-foreground">
                    Failed to load utilization data.{" "}
                    <button
                        onClick={() => void loadJobSeries()}
                        className="font-medium text-primary hover:underline"
                    >
                        Retry
                    </button>
                </div>
            ) : jobSeries == null ? (
                <div className="px-5 py-5">
                    <ChartSkeletons />
                </div>
            ) : !jobSeries.has_metrics ? (
                <div className="px-5 py-6">
                    <p className="text-sm font-medium text-foreground">No utilization data</p>
                    <p className="mt-1 text-[13px] text-muted-foreground">
                        {isLive
                            ? "Samples appear a few seconds after work starts."
                            : "This job ran before per-second resource metrics were collected."}
                    </p>
                </div>
            ) : (
                <div className="space-y-7 px-5 py-5">
                    <MetricChart
                        title="Nodes"
                        data={jobData}
                        series={[{ key: "nodes", label: "Nodes", color: PRIMARY }]}
                        startAt={jobStartAt}
                        format={(v) => String(Math.round(v))}
                        integerTicks
                    />
                    <MetricChart
                        title="CPU"
                        data={jobData}
                        series={[{ key: "cpu", label: "CPU", color: PRIMARY }]}
                        startAt={jobStartAt}
                        format={(v) => `${Math.round(v)}%`}
                        domainMax={100}
                    />
                    <MetricChart
                        title="Memory"
                        data={jobData}
                        series={[{ key: "mem", label: "Memory", color: PRIMARY }]}
                        startAt={jobStartAt}
                        format={(v) => `${Math.round(v)}%`}
                        domainMax={100}
                    />
                    <MetricChart
                        title="Network I/O"
                        data={jobData}
                        series={[
                            { key: "net_rx", label: "In", color: PRIMARY },
                            { key: "net_tx", label: "Out", color: SECONDARY },
                        ]}
                        startAt={jobStartAt}
                        format={formatRate}
                    />
                    <MetricChart
                        title="Disk I/O"
                        data={jobData}
                        series={[
                            { key: "disk_read", label: "Read", color: PRIMARY },
                            { key: "disk_write", label: "Write", color: SECONDARY },
                        ]}
                        startAt={jobStartAt}
                        format={formatRate}
                    />
                    {jobSeries.has_gpu && (
                        <>
                            <MetricChart
                                title="GPU"
                                data={jobData}
                                series={[{ key: "gpu", label: "GPU", color: PRIMARY }]}
                                startAt={jobStartAt}
                                format={(v) => `${Math.round(v)}%`}
                                domainMax={100}
                            />
                            <MetricChart
                                title="GPU memory"
                                data={jobData}
                                series={[{ key: "gpu_mem", label: "GPU memory", color: PRIMARY }]}
                                startAt={jobStartAt}
                                format={(v) => `${Math.round(v)}%`}
                                domainMax={100}
                            />
                        </>
                    )}
                </div>
            )}
        </div>
    );
};

export default JobUtilization;
