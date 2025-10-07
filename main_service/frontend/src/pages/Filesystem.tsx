import React from "react";
import type { BeforeDownloadEventArgs } from "@syncfusion/ej2-filemanager";
import type { FileInfo } from "@syncfusion/ej2-inputs";
import {
    FileManagerComponent,
    Inject,
    NavigationPane,
    DetailsView,
    Toolbar,
    ContextMenu,
} from "@syncfusion/ej2-react-filemanager";

import "@syncfusion/ej2-base/styles/material.css";
import "@syncfusion/ej2-buttons/styles/material.css";
import "@syncfusion/ej2-inputs/styles/material.css";
import "@syncfusion/ej2-popups/styles/material.css";
import "@syncfusion/ej2-icons/styles/material.css";
import "@syncfusion/ej2-navigations/styles/material.css";
import "@syncfusion/ej2-layouts/styles/material.css";
import "@syncfusion/ej2-grids/styles/material.css";
import "@syncfusion/ej2-splitbuttons/styles/material.css";
import "@syncfusion/ej2-dropdowns/styles/material.css";
import "@syncfusion/ej2-react-filemanager/styles/material.css";

type ActiveUploadState = {
    name: string;
    uploadedBytes: number;
    totalBytes: number;
    state: "uploading" | "error" | "done" | "cancelled";
};

type FileManagerEntry = {
    name?: string;
    path?: string;
    filterPath?: string;
    isFile?: boolean;
    type?: string;
};

function formatBytes(bytes: number) {
    if (bytes === 0) return "0 B";
    const units = ["B", "KB", "MB", "GB", "TB", "PB"];
    const exponent = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1);
    const value = bytes / 1024 ** exponent;
    const precision = value >= 10 || exponent === 0 ? 0 : 1;
    return `${value.toFixed(precision)} ${units[exponent]}`;
}

function isFileEntry(entry: FileManagerEntry): boolean {
    if (typeof entry.isFile === "boolean") {
        return entry.isFile;
    }
    if (entry.type === "folder") {
        return false;
    }
    return true;
}

function normalizeServerPath(path: string | null | undefined): string {
    if (!path) {
        return "/";
    }
    let normalized = path.trim();
    if (normalized === "") {
        return "/";
    }
    if (!normalized.startsWith("/")) {
        normalized = `/${normalized}`;
    }
    if (normalized !== "/" && !normalized.endsWith("/")) {
        normalized = `${normalized}/`;
    }
    return normalized;
}

function storageObjectName(entry: FileManagerEntry, fallbackPath: string): string {
    const directoryPath = normalizeServerPath(entry.path ?? fallbackPath);
    const prefix = directoryPath === "/" ? "" : directoryPath.slice(1);
    if (!entry.name) {
        throw new Error("Entry name is required");
    }
    return `${prefix}${entry.name}`;
}

function triggerDownload(url: string, fileName: string) {
    const anchor = document.createElement("a");
    anchor.href = url;
    anchor.download = fileName;
    anchor.rel = "noopener";
    anchor.style.display = "none";
    document.body.append(anchor);
    anchor.click();
    anchor.remove();
}

function isAbortError(error: unknown): boolean {
    return (
        (error instanceof DOMException && error.name === "AbortError") ||
        (typeof error === "object" &&
            error !== null &&
            "name" in error &&
            (error as { name?: string }).name === "AbortError")
    );
}

function normalizeUploadRelativePath(path: string): string {
    const forwardSlashes = path.replace(/\\/g, "/");
    const withoutRelativePrefix = forwardSlashes.replace(/^(?:\.\/)+/, "");
    const trimmed = withoutRelativePrefix.startsWith("/")
        ? withoutRelativePrefix.slice(1)
        : withoutRelativePrefix;
    const segments = trimmed.split("/").filter(Boolean);
    if (segments.length === 0) {
        throw new Error("File path is required");
    }
    if (segments.some((segment) => segment === "..")) {
        throw new Error("Invalid file path");
    }
    return segments.join("/");
}

function uploadPrefixFromPath(path: string): string {
    const normalized = normalizeServerPath(path);
    if (normalized === "/") {
        return "";
    }
    return normalized.slice(1, -1);
}

function buildObjectName(basePath: string, relativePath: string): string {
    const prefix = uploadPrefixFromPath(basePath);
    const normalizedRelative = normalizeUploadRelativePath(relativePath);
    return prefix ? `${prefix}/${normalizedRelative}` : normalizedRelative;
}

export default function Filesystem() {
    const fmRef = React.useRef<FileManagerComponent | null>(null);
    const maxUploadSizeBytes = 10 * 1024 ** 4;
    const [activeUpload, setActiveUpload] = React.useState<ActiveUploadState | null>(null);
    const abortControllerRef = React.useRef<AbortController | null>(null);
    const detailsViewColumns = React.useMemo(
        () => [
            {
                field: "name",
                headerText: "Name",
                minWidth: 120,
                template: '<span class="e-fe-text">${name}</span>',
                customAttributes: { class: "e-fe-grid-name" },
            },
            {
                field: "_fm_modified",
                headerText: "DateModified",
                type: "dateTime",
                format: "MMMM dd, yyyy HH:mm",
                minWidth: 120,
                width: "260",
                template: '<span class="e-fe-date-value">${_fm_modified}</span>',
            },
            {
                field: "size",
                headerText: "Size",
                minWidth: 90,
                width: "200",
                template: '<span class="e-fe-size">${size}</span>',
                format: "n2",
            },
        ],
        []
    );

    React.useEffect(() => {
        if (!activeUpload || activeUpload.state === "uploading") return undefined;
        const timeout = window.setTimeout(
            () => setActiveUpload(null),
            activeUpload.state === "done" ? 1500 : activeUpload.state === "cancelled" ? 1500 : 4000
        );
        return () => window.clearTimeout(timeout);
    }, [activeUpload]);

    const uploadQueueRef = React.useRef<FileInfo[]>([]);
    const uploadPathRef = React.useRef<string>("/");
    const isProcessingUploadRef = React.useRef(false);

    const uploadFileToStorage = React.useCallback(
        async (fileInfo: FileInfo, objectName: string, displayName: string) => {
            const rawFile = fileInfo.rawFile as File | Blob | undefined;
            if (!rawFile) {
                throw new Error("Missing file data");
            }

            abortControllerRef.current?.abort();
            const controller = new AbortController();
            abortControllerRef.current = controller;

            const totalBytes = rawFile.size;
            setActiveUpload({
                name: displayName,
                uploadedBytes: 0,
                totalBytes,
                state: "uploading",
            });

            try {
                const signedResponse = await fetch(
                    `/signed-resumable?object_name=${encodeURIComponent(
                        objectName
                    )}&content_type=${encodeURIComponent(
                        rawFile.type || "application/octet-stream"
                    )}`,
                    { signal: controller.signal }
                );
                const { url } = await signedResponse.json();

                const sessionResponse = await fetch(url, {
                    method: "POST",
                    headers: {
                        "Content-Length": "0",
                        "x-goog-resumable": "start",
                        "Content-Type": rawFile.type || "application/octet-stream",
                    },
                    signal: controller.signal,
                });
                const sessionUrl = sessionResponse.headers.get("Location");
                if (!sessionUrl) {
                    throw new Error("Missing resumable session URL");
                }

                const chunkSize = 8 * 1024 * 1024;
                let offset = 0;
                while (offset < totalBytes) {
                    if (controller.signal.aborted) {
                        throw new DOMException("Upload aborted", "AbortError");
                    }

                    const end = Math.min(offset + chunkSize, totalBytes);
                    const chunk = rawFile.slice(offset, end);
                    const range = `bytes ${offset}-${end - 1}/${totalBytes}`;

                    const response = await fetch(sessionUrl, {
                        method: "PUT",
                        headers: {
                            "Content-Range": range,
                            "Content-Type": rawFile.type || "application/octet-stream",
                        },
                        body: chunk,
                        signal: controller.signal,
                    });

                    if (response.status === 308) {
                        const rangeHeader = response.headers.get("Range");
                        if (rangeHeader) {
                            const lastToken = rangeHeader.split("-").pop();
                            const lastByte = lastToken ? parseInt(lastToken, 10) : NaN;
                            offset = Number.isFinite(lastByte) ? lastByte + 1 : end;
                        } else {
                            offset = end;
                        }

                        setActiveUpload((current) =>
                            current && current.name === displayName
                                ? {
                                      ...current,
                                      uploadedBytes: offset,
                                  }
                                : current
                        );
                        continue;
                    }

                    if (!response.ok) {
                        setActiveUpload((current) =>
                            current && current.name === displayName
                                ? {
                                      ...current,
                                      state: "error",
                                  }
                                : current
                        );
                        throw new Error(`Chunk failed: ${response.status}`);
                    }

                    offset = end;

                    setActiveUpload((current) =>
                        current && current.name === displayName
                            ? {
                                  ...current,
                                  uploadedBytes: end,
                              }
                            : current
                    );
                }

                setActiveUpload((current) =>
                    current && current.name === displayName
                        ? {
                              ...current,
                              uploadedBytes: totalBytes,
                              state: "done",
                          }
                        : current
                );
            } catch (error) {
                if (isAbortError(error)) {
                    setActiveUpload((current) =>
                        current && current.name === displayName
                            ? {
                                  ...current,
                                  state: "cancelled",
                              }
                            : current
                    );
                } else {
                    console.error("Resumable upload failed", error);
                    setActiveUpload((current) =>
                        current && current.name === displayName
                            ? {
                                  ...current,
                                  state: "error",
                              }
                            : current
                    );
                }
                throw error;
            } finally {
                abortControllerRef.current = null;
            }
        },
        []
    );

    const processUploadQueue = React.useCallback(async () => {
        let uploadedAny = false;

        try {
            while (uploadQueueRef.current.length > 0) {
                const next = uploadQueueRef.current.shift();
                if (!next) {
                    continue;
                }

                const rawFile = next.rawFile as File | Blob | undefined;
                const displayName = next.name || (rawFile instanceof File ? rawFile.name : "");
                if (!displayName) {
                    continue;
                }
                const totalBytes = typeof next.size === "number" ? next.size : rawFile?.size ?? 0;

                let objectName: string;
                try {
                    objectName = buildObjectName(uploadPathRef.current, displayName);
                } catch (error) {
                    console.error("Resumable upload failed", error);
                    setActiveUpload({
                        name: displayName,
                        uploadedBytes: 0,
                        totalBytes,
                        state: "error",
                    });
                    window.alert("Upload failed. Please try again.");
                    break;
                }

                try {
                    await uploadFileToStorage(next, objectName, displayName);
                    uploadedAny = true;
                } catch (error) {
                    if (!isAbortError(error)) {
                        window.alert("Upload failed. Please try again.");
                    }
                    break;
                }
            }
        } finally {
            uploadQueueRef.current = [];
            abortControllerRef.current = null;
            if (uploadedAny) {
                fmRef.current?.refreshFiles();
            }
            fmRef.current?.uploadObj?.clearAll();
            isProcessingUploadRef.current = false;
        }
    }, [uploadFileToStorage]);

    const handleBeforeSend = React.useCallback(
        (args: any) => {
            if (args.action === "Search") {
                args.cancel = true;
                return;
            }

            if (args.action === "Upload") {
                args.cancel = true;

                const rawData = args.ajaxSettings?.data;
                let requestPath: string | undefined;
                if (typeof rawData === "string") {
                    try {
                        const parsed = JSON.parse(rawData) as Array<Record<string, unknown>>;
                        const pathEntry = parsed.find((entry) =>
                            Object.prototype.hasOwnProperty.call(entry, "path")
                        ) as { path?: string } | undefined;
                        if (pathEntry?.path) {
                            requestPath = pathEntry.path;
                        }
                    } catch {
                        requestPath = undefined;
                    }
                }

                const fallbackPath =
                    typeof fmRef.current?.path === "string" ? fmRef.current?.path : "/";
                uploadPathRef.current = normalizeServerPath(requestPath ?? fallbackPath);

                if (isProcessingUploadRef.current) {
                    return;
                }

                const filesData = fmRef.current?.uploadObj?.getFilesData() ?? [];
                if (!filesData.length) {
                    return;
                }

                uploadQueueRef.current = filesData.slice();
                fmRef.current?.uploadDialogObj?.hide();
                fmRef.current?.uploadObj?.clearAll();
                isProcessingUploadRef.current = true;
                void processUploadQueue();
            }
        },
        [processUploadQueue]
    );

    const handleCancelUpload = React.useCallback(() => {
        uploadQueueRef.current = [];
        abortControllerRef.current?.abort();
        abortControllerRef.current = null;
        isProcessingUploadRef.current = false;
        setActiveUpload((current) =>
            current && current.state === "uploading"
                ? {
                      ...current,
                      state: "cancelled",
                  }
                : current
        );
        fmRef.current?.uploadObj?.clearAll();
    }, []);

    const handleSuccess = React.useCallback((args: any) => {
        if (!args || args.action !== "move") return;
        fmRef.current?.refreshFiles();
    }, []);

    const handleBeforeDownload = React.useCallback(async (args: BeforeDownloadEventArgs) => {
        args.cancel = true;

        const payload = (args.data ?? {}) as {
            path?: string;
            names?: string[];
            data?: FileManagerEntry[];
        };

        const fallbackPath = normalizeServerPath(payload.path);
        const rawEntries = (payload.data ?? []).filter(Boolean) as FileManagerEntry[];
        const entries = rawEntries.length
            ? rawEntries.filter((entry) => isFileEntry(entry))
            : (payload.names ?? []).map((name) => ({
                  name,
                  path: payload.path,
                  isFile: true,
              }));

        if (!entries.length) {
            window.alert("Select a file to download.");
            return;
        }

        for (const entry of entries) {
            if (!isFileEntry(entry) || !entry.name) {
                continue;
            }
            const objectName = storageObjectName(entry, fallbackPath);
            try {
                const response = await fetch(
                    `/signed-download?object_name=${encodeURIComponent(
                        objectName
                    )}&download_name=${encodeURIComponent(entry.name)}`
                );
                if (!response.ok) {
                    throw new Error(`Request failed with status ${response.status}`);
                }
                const data = (await response.json()) as { url?: string };
                if (!data.url) {
                    throw new Error("Missing download URL");
                }
                triggerDownload(data.url, entry.name);
            } catch (error) {
                console.error("Download failed", error);
                window.alert("Download failed. Please try again.");
                break;
            }
        }
    }, []);

    return (
        <div className="flex-1 flex flex-col justify-start px-12 pt-6 pb-12 min-h-0">
            <div className="max-w-7xl mx-auto w-full flex-1 flex flex-col min-h-0">
                <div className="relative flex-1 rounded-lg border border-gray-200 bg-white shadow-sm filesystem-shell">
                    <FileManagerComponent
                        view="Details"
                        ref={fmRef}
                        allowDragAndDrop
                        ajaxSettings={{
                            url: "/api/sf/filemanager",
                            uploadUrl: "/api/sf/upload",
                        }}
                        uploadSettings={{
                            maxFileSize: maxUploadSizeBytes,
                            directoryUpload: true,
                        }}
                        detailsViewSettings={{
                            columns: detailsViewColumns,
                        }}
                        navigationPaneSettings={{ visible: false }}
                        contextMenuSettings={{
                            file: ["Download", "Delete", "Rename"],
                            folder: ["Open", "Delete", "Rename"],
                            layout: ["NewFolder", "Upload", "Refresh"],
                        }}
                        success={handleSuccess}
                        toolbarSettings={{
                            items: [
                                "NewFolder",
                                "Upload",
                                "Delete",
                                "Rename",
                                "Download",
                                "Refresh",
                                "Selection",
                            ],
                        }}
                        cssClass="filesystem-filemanager"
                        beforeDownload={handleBeforeDownload}
                        beforeSend={handleBeforeSend}
                        height="100%"
                        width="100%"
                    >
                        <Inject services={[NavigationPane, Toolbar, DetailsView, ContextMenu]} />
                    </FileManagerComponent>
                    {activeUpload && (
                        <div className="absolute inset-0 z-20 flex items-center justify-center bg-slate-900/45 backdrop-blur-sm">
                            <div className="pointer-events-auto relative w-80 max-w-full rounded-lg bg-white/95 p-5 shadow-2xl">
                                {activeUpload.state === "uploading" && (
                                    <button
                                        type="button"
                                        className="absolute right-4 top-4 inline-flex h-7 w-7 items-center justify-center rounded-full border border-gray-200 bg-white text-gray-500 transition hover:border-gray-300 hover:text-gray-800"
                                        onClick={handleCancelUpload}
                                    >
                                        <span className="sr-only">Cancel upload</span>
                                    </button>
                                )}
                                <p className="text-sm font-semibold text-gray-700 truncate">
                                    {activeUpload.state === "uploading"
                                        ? `Uploading: ${activeUpload.name}`
                                        : activeUpload.name}
                                </p>
                                <div className="mt-4 h-3 w-full overflow-hidden rounded-full bg-gray-200">
                                    <div
                                        className="h-full rounded-full"
                                        style={{
                                            width: `${Math.min(
                                                100,
                                                Math.floor(
                                                    (activeUpload.uploadedBytes /
                                                        activeUpload.totalBytes) *
                                                        100
                                                )
                                            )}%`,
                                            backgroundColor:
                                                activeUpload.state === "error"
                                                    ? "rgb(239 68 68)"
                                                    : activeUpload.state === "cancelled"
                                                    ? "rgb(245 158 11)"
                                                    : "hsl(var(--brand))",
                                        }}
                                    />
                                </div>
                                <p className="mt-3 text-xs font-medium text-gray-500">
                                    {activeUpload.state === "uploading"
                                        ? `${formatBytes(
                                              activeUpload.uploadedBytes
                                          )} of ${formatBytes(activeUpload.totalBytes)}`
                                        : activeUpload.state === "done"
                                        ? "Upload complete"
                                        : activeUpload.state === "cancelled"
                                        ? `Upload cancelled at ${formatBytes(
                                              activeUpload.uploadedBytes
                                          )}`
                                        : "Upload failed"}
                                </p>
                            </div>
                        </div>
                    )}
                </div>
            </div>
        </div>
    );
}
