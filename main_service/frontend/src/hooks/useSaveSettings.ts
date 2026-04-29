// src/hooks/useSaveSettings.ts
import { Settings } from "@/types/coreTypes";

export interface QuotaWarningDetails {
    machineType: string;
    region: string;
    limit: number;
    used?: number;
    available?: number;
    allowed?: number;
    countUnit?: string;
    quota?: string;
    units?: string;
    requested: number;
}

export type SaveSettingsResult =
    | { ok: true }
    | { ok: false; quota?: QuotaWarningDetails; errorMessage?: string };

const parseErrorResponse = async (res: Response): Promise<SaveSettingsResult> => {
    const { detail } = await res.json();
    if (detail.error_code === "quota_exceeded") {
        return {
            ok: false,
            quota: {
                machineType: detail.machine_type,
                region: detail.region,
                limit: detail.limit,
                used: detail.used,
                available: detail.available,
                allowed: detail.allowed,
                countUnit: detail.count_unit,
                quota: detail.quota,
                units: detail.units,
                requested: detail.requested,
            },
            errorMessage: detail.message,
        };
    }
    return { ok: false, errorMessage: detail.message || detail };
};

export const useSaveSettings = () => {
    const saveSettings = async (settings: Settings): Promise<SaveSettingsResult> => {
        try {
            const res = await fetch("/v1/settings", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(settings),
            });

            if (!res.ok) {
                return await parseErrorResponse(res);
            }
            return { ok: true };
        } catch (err) {
            console.error("Error updating settings:", err);
            return { ok: false };
        }
    };

    return { saveSettings };
};
