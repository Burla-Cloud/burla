// src/hooks/useSaveSettings.ts
import { Settings } from "@/types/coreTypes";

interface SaveSettingsResult {
    ok: boolean;
    errorCode?: string;
    errorMessage?: string;
    limit?: number;
    requested?: number;
    machineType?: string;
    region?: string;
}

interface ErrorDetail {
    error_code?: string;
    message?: string;
    limit?: number;
    requested?: number;
    machine_type?: string;
    region?: string;
}

interface ErrorPayload {
    detail?: string | ErrorDetail;
}

const parseErrorResponse = async (res: Response): Promise<Omit<SaveSettingsResult, "ok">> => {
    try {
        const payload = (await res.json()) as ErrorPayload;
        if (typeof payload?.detail === "string") {
            return { errorMessage: payload.detail };
        }
        if (payload?.detail && typeof payload.detail === "object") {
            return {
                errorCode:
                    typeof payload.detail.error_code === "string"
                        ? payload.detail.error_code
                        : undefined,
                errorMessage:
                    typeof payload.detail.message === "string"
                        ? payload.detail.message
                        : undefined,
                limit:
                    typeof payload.detail.limit === "number"
                        ? payload.detail.limit
                        : undefined,
                requested:
                    typeof payload.detail.requested === "number"
                        ? payload.detail.requested
                        : undefined,
                machineType:
                    typeof payload.detail.machine_type === "string"
                        ? payload.detail.machine_type
                        : undefined,
                region:
                    typeof payload.detail.region === "string"
                        ? payload.detail.region
                        : undefined,
            };
        }
    } catch {
        // keep fallback behavior below
    }
    return {};
};

export const useSaveSettings = () => {
    const saveSettings = async (settings: Settings): Promise<SaveSettingsResult> => {
        try {
            const res = await fetch("/v1/settings", {
                method: "POST",
                headers: {
                    "Content-Type": "application/json",
                    Email: "joe@burla.dev",
                },
                body: JSON.stringify(settings), // Ensure the full settings including users is sent
            });

            if (!res.ok) {
                const parsed = await parseErrorResponse(res);
                return { ok: false, ...parsed };
            }
            return { ok: true };
        } catch (err) {
            console.error("Error updating settings:", err);
            return { ok: false };
        }
    };

    return { saveSettings };
};
