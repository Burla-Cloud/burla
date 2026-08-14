import { AWS_MACHINE_SPECS } from "@/types/constants";

// Machine-type -> hardware display helpers, shared by the cluster page's
// stats strip and the nodes table.

export const extractCpuCount = (type: string): number | null => {
    const awsSpec = AWS_MACHINE_SPECS[type.toLowerCase()];
    if (awsSpec) return awsSpec.cpus;

    const azureMatch = type.toLowerCase().match(/^standard_d(\d+)s_v6$/);
    if (azureMatch) return parseInt(azureMatch[1], 10);

    const customMatch = type.match(/^custom-(\d+)-/);
    if (customMatch) return parseInt(customMatch[1], 10);

    const standardMatch = type.match(/-(\d+)$/);
    if (standardMatch) return parseInt(standardMatch[1], 10);

    const gpuMatch = type.match(/^(a\d-(highgpu|ultragpu|megagpu|edgegpu))-(\d+)g$/);
    if (gpuMatch) {
        const family = gpuMatch[1];
        const gpus = parseInt(gpuMatch[3], 10);

        const cpuTable: Record<string, Record<number, number>> = {
            "a2-highgpu": { 1: 12, 2: 24, 4: 48, 8: 96 },
            "a2-ultragpu": { 1: 12, 2: 24, 4: 48, 8: 96 },
            "a2-megagpu": { 16: 96 },
            "a3-highgpu": { 1: 26, 2: 52, 4: 104, 8: 208 },
            "a3-ultragpu": { 8: 224 },
            "a3-edgegpu": { 8: 208 },
        };

        return cpuTable[family]?.[gpus] ?? null;
    }

    return null;
};

export const parseRamDisplay = (type: string): string => {
    const lower = type.toLowerCase();

    const awsSpec = AWS_MACHINE_SPECS[lower];
    if (awsSpec) return awsSpec.ram;

    if (lower.startsWith("n4-standard-") || lower.startsWith("standard_d")) {
        const cpu = extractCpuCount(type);
        if (cpu !== null) return `${cpu * 4}G`;
    }

    const ramTable: Record<string, Record<number, string>> = {
        "a2-highgpu": { 1: "85G", 2: "170G", 4: "340G", 8: "680G", 16: "1360G" },
        "a2-ultragpu": { 1: "170G", 2: "340G", 4: "680G", 8: "1360G" },
        "a2-megagpu": { 16: "1360G" },
        "a3-highgpu": { 1: "234G", 2: "468G", 4: "936G", 8: "1872G" },
        "a3-ultragpu": { 8: "2952G" },
    };

    const match = lower.match(/^(a\d-(highgpu|ultragpu|megagpu|edgegpu))-(\d+)g$/);
    if (match) {
        const family = match[1];
        const count = parseInt(match[3], 10);
        const sizes = ramTable[family];
        if (sizes && sizes[count]) return sizes[count];
    }

    return "-";
};

export const parseGpuDisplay = (type: string): string => {
    const lower = type.toLowerCase();

    const awsSpec = AWS_MACHINE_SPECS[lower];
    if (awsSpec) return awsSpec.gpu ?? "-";

    const gpuPatterns: { prefix: string; model: string; vram: string }[] = [
        { prefix: "a2-highgpu-", model: "A100", vram: "40G" },
        { prefix: "a2-ultragpu-", model: "A100", vram: "80G" },
        { prefix: "a2-megagpu-", model: "A100", vram: "40G" },
        { prefix: "a3-highgpu-", model: "H100", vram: "80G" },
        { prefix: "a3-ultragpu-", model: "H200", vram: "141G" },
    ];

    for (const { prefix, model, vram } of gpuPatterns) {
        if (lower.startsWith(prefix)) {
            const countMatch = lower.match(/-(\d+)g$/);
            if (countMatch) {
                const count = parseInt(countMatch[1], 10);
                return `${count}x ${model} ${vram}`;
            }
        }
    }

    return "-";
};
