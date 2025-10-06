import React from "react";
import {
    FileManagerComponent,
    Inject,
    NavigationPane,
    DetailsView,
    Toolbar,
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

function formatBytes(bytes: number) {
    if (bytes === 0) return "0 B";
    const units = ["B", "KB", "MB", "GB", "TB", "PB"];
    const exponent = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1);
    const value = bytes / 1024 ** exponent;
    const precision = value >= 10 || exponent === 0 ? 0 : 1;
    return `${value.toFixed(precision)} ${units[exponent]}`;
}

export default function Filesystem() {
    const fmRef = React.useRef<FileManagerComponent | null>(null);
    const maxUploadSizeBytes = 10 * 1024 ** 4;
    const [activeUpload, setActiveUpload] = React.useState<ActiveUploadState | null>(null);
    const abortControllerRef = React.useRef<AbortController | null>(null);

    React.useEffect(() => {
        if (!activeUpload || activeUpload.state === "uploading") return undefined;
        const timeout = window.setTimeout(
            () => setActiveUpload(null),
            activeUpload.state === "done" ? 1500 : activeUpload.state === "cancelled" ? 1500 : 4000
        );
        return () => window.clearTimeout(timeout);
    }, [activeUpload]);

    const handleBeforeSend = React.useCallback(async (args: any) => {
        console.log("FileManager beforeSend args:\n" + JSON.stringify(args, null, 2));

        if (args.action === "Search") {
            args.cancel = true;
            return;
        }

        if (args.action === "Upload") {
            args.cancel = true;

            const fileData = fmRef.current?.uploadObj?.getFilesData()?.[0];
            const file = fileData?.rawFile as File | undefined;

            if (!file) return;

            fmRef.current?.uploadDialogObj?.hide();

            let controller: AbortController | null = null;

            try {
                abortControllerRef.current?.abort();
                controller = new AbortController();
                abortControllerRef.current = controller;

                const resp = await fetch(
                    `/signed-resumable?object_name=${encodeURIComponent(
                        file.name
                    )}&content_type=${encodeURIComponent(file.type)}`,
                    { signal: controller.signal }
                );
                const { url } = await resp.json();

                setActiveUpload({
                    name: file.name,
                    uploadedBytes: 0,
                    totalBytes: file.size,
                    state: "uploading",
                });

                const start = await fetch(url, {
                    method: "POST",
                    headers: {
                        "Content-Length": "0",
                        "x-goog-resumable": "start",
                        "Content-Type": file.type || "application/octet-stream",
                    },
                    signal: controller.signal,
                });
                const sessionUrl = start.headers.get("Location");
                if (!sessionUrl) throw new Error("Missing resumable session URL");

                const chunkSize = 8 * 1024 * 1024;
                let offset = 0;
                while (offset < file.size) {
                    if (controller.signal.aborted) {
                        throw new DOMException("Upload aborted", "AbortError");
                    }

                    const end = Math.min(offset + chunkSize, file.size);
                    const chunk = file.slice(offset, end);
                    const range = `bytes ${offset}-${end - 1}/${file.size}`;

                    const response = await fetch(sessionUrl, {
                        method: "PUT",
                        headers: {
                            "Content-Range": range,
                            "Content-Type": file.type || "application/octet-stream",
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
                            current && current.name === file.name
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
                            current && current.name === file.name
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
                        current && current.name === file.name
                            ? {
                                  ...current,
                                  uploadedBytes: end,
                              }
                            : current
                    );
                }

                setActiveUpload((current) =>
                    current && current.name === file.name
                        ? {
                              ...current,
                              uploadedBytes: file.size,
                              state: "done",
                          }
                        : current
                );

                fmRef.current?.refreshFiles();
            } catch (error) {
                const isAbort =
                    (error instanceof DOMException && error.name === "AbortError") ||
                    (typeof error === "object" &&
                        error !== null &&
                        "name" in error &&
                        (error as { name?: string }).name === "AbortError");

                if (isAbort) {
                    setActiveUpload((current) =>
                        current && current.name === file.name
                            ? {
                                  ...current,
                                  state: "cancelled",
                              }
                            : current
                    );
                } else {
                    console.error("Resumable upload failed", error);
                    setActiveUpload((current) =>
                        current && current.name === file.name
                            ? {
                                  ...current,
                                  state: "error",
                              }
                            : current
                    );
                }
            } finally {
                abortControllerRef.current = null;
                fmRef.current?.uploadObj?.clearAll();
            }
        }
    }, []);

    const handleCancelUpload = React.useCallback(() => {
        abortControllerRef.current?.abort();
        abortControllerRef.current = null;
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

    return (
        <div className="flex-1 flex flex-col justify-start px-12 pt-6 pb-12 min-h-0">
            <div className="max-w-7xl mx-auto w-full flex-1 flex flex-col min-h-0">
                <div className="relative flex-1 rounded-3xl border border-gray-200 bg-white shadow-sm filesystem-shell">
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
                        }}
                        navigationPaneSettings={{ visible: false }}
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
                        beforeSend={handleBeforeSend}
                        height="100%"
                        width="100%"
                    >
                        <Inject services={[NavigationPane, Toolbar, DetailsView]} />
                    </FileManagerComponent>
                    {activeUpload && (
                        <div className="absolute inset-0 z-20 flex items-center justify-center bg-slate-900/45 backdrop-blur-sm">
                            <div className="pointer-events-auto relative w-80 max-w-full rounded-3xl bg-white/95 p-5 shadow-2xl">
                                {activeUpload.state === "uploading" && (
                                    <button
                                        type="button"
                                        className="absolute right-4 top-4 inline-flex h-7 w-7 items-center justify-center rounded-full border border-gray-200 bg-white text-gray-500 transition hover:border-gray-300 hover:text-gray-800"
                                        onClick={handleCancelUpload}
                                    >
                                        <span className="sr-only">Cancel upload</span>×
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
