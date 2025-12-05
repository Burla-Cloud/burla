import React from "react";
import type { BeforeDownloadEventArgs, FileLoadEventArgs } from "@syncfusion/ej2-filemanager";
import type { FileInfo, SelectedEventArgs } from "@syncfusion/ej2-inputs";
import {
    FileManagerComponent,
    Inject,
    NavigationPane,
    DetailsView,
    Toolbar,
    ContextMenu,
} from "@syncfusion/ej2-react-filemanager";
import { X } from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";

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

function formatFileManagerSize(bytes: number): string {
    if (!Number.isFinite(bytes) || bytes <= 0) {
        return "0 B";
    }
    const units = ["B", "KB", "MB", "GB", "TB", "PB", "EB"];
    let unitIndex = 0;
    let value = bytes;
    while (value >= 1024 && unitIndex < units.length - 1) {
        value /= 1024;
        unitIndex += 1;
    }
    while (value >= 1000 && unitIndex < units.length - 1) {
        value /= 1024;
        unitIndex += 1;
    }
    const decimalsForUnit = (index: number) => {
        if (index <= 1) {
            return 0;
        }
        if (units[index] === "MB") {
            return 1;
        }
        return 2;
    };
    const roundedForUnit = (val: number, index: number) => {
        const decimals = decimalsForUnit(index);
        if (decimals === 0) {
            return Math.round(val);
        }
        const factor = 10 ** decimals;
        return Math.round(val * factor) / factor;
    };
    let rounded = roundedForUnit(value, unitIndex);
    while (rounded >= 1000 && unitIndex < units.length - 1) {
        value /= 1024;
        unitIndex += 1;
        rounded = roundedForUnit(value, unitIndex);
    }
    if (rounded === 0 && bytes > 0) {
        const decimals = decimalsForUnit(unitIndex);
        if (decimals === 0) {
            rounded = 1;
        } else {
            rounded = 1 / 10 ** decimals;
        }
    }
    const decimals = decimalsForUnit(unitIndex);
    if (decimals === 0) {
        return `${Math.round(rounded)} ${units[unitIndex]}`;
    }
    const formatted = rounded.toFixed(decimals).replace(/\.0+$|0+$/, "");
    return `${formatted} ${units[unitIndex]}`;
}

function formatFileManagerDate(value: string | Date | null | undefined): string {
    if (!value) {
        return "—";
    }
    const date = value instanceof Date ? value : new Date(value);
    if (Number.isNaN(date.getTime())) {
        return "—";
    }
    const dayNames = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
    const monthNames = [
        "Jan",
        "Feb",
        "Mar",
        "Apr",
        "May",
        "Jun",
        "Jul",
        "Aug",
        "Sep",
        "Oct",
        "Nov",
        "Dec",
    ];
    const dayName = dayNames[date.getDay()];
    const monthName = monthNames[date.getMonth()];
    const dayOfMonth = date.getDate().toString().padStart(2, "0");
    const year = date.getFullYear();
    const hours = date.getHours();
    const hourValue = hours % 12 || 12;
    const minuteValue = date.getMinutes().toString().padStart(2, "0");
    const period = hours >= 12 ? "PM" : "AM";
    const displayHour = hourValue.toString().padStart(2, "0");
    return `${dayName} ${monthName} ${dayOfMonth} ${year} ${displayHour}:${minuteValue} ${period}`;
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

function storageFolderPrefix(entry: FileManagerEntry, fallbackPath: string): string {
    const directoryPath = normalizeServerPath(entry.path ?? fallbackPath);
    const prefix = directoryPath === "/" ? "" : directoryPath.slice(1);
    if (!entry.name) {
        throw new Error("Entry name is required");
    }
    return `${prefix}${entry.name}/`;
}

function clearUploaderFiles(uploader: FileManagerComponent["uploadObj"] | null | undefined) {
    if (!uploader) {
        return;
    }
    const instance = uploader as unknown as {
        clearData?: () => void;
        clearAll?: () => void;
        filesData?: unknown;
        selectedFiles?: unknown;
    };
    let didInvoke = false;
    if (typeof instance.clearData === "function") {
        instance.clearData();
        didInvoke = true;
    }
    if (typeof instance.clearAll === "function") {
        instance.clearAll();
        didInvoke = true;
    }
    if (Array.isArray(instance.filesData)) {
        instance.filesData.length = 0;
    }
    if (Array.isArray(instance.selectedFiles)) {
        instance.selectedFiles.length = 0;
    }
    if (!didInvoke && typeof uploader === "object" && uploader) {
        try {
            (uploader as unknown as { clearAll?: () => void }).clearAll?.();
        } catch {
            /* noop */
        }
    }
}

function getDropArea(manager: FileManagerComponent | null | undefined): HTMLElement | null {
    const candidate = (manager as unknown as { dropArea?: HTMLElement | null })?.dropArea ?? null;
    return candidate instanceof HTMLElement ? candidate : null;
}

function clearDropHighlight(manager: FileManagerComponent | null | undefined) {
    const elements: HTMLElement[] = [];
    const dropArea = getDropArea(manager);
    if (dropArea) {
        elements.push(dropArea);
    }
    const uploadWrapper =
        (manager as unknown as { uploadWrapper?: HTMLElement | null })?.uploadWrapper ?? null;
    if (uploadWrapper instanceof HTMLElement) {
        elements.push(uploadWrapper);
    }
    const host = (manager?.element as HTMLElement | undefined) ?? null;
    if (host) {
        elements.push(host);
        const hovered = Array.from(host.querySelectorAll<HTMLElement>(".e-upload-drag-hover"));
        elements.push(...hovered);
    }
    for (const element of elements) {
        element.classList.remove("e-upload-drag-hover");
        element.removeAttribute("aria-dropeffect");
    }
}

function ensureWebkitRelativePath(file: File, relativePath: string): FileWithPath {
    const fileWithPath = file as FileWithPath;
    if (fileWithPath.webkitRelativePath === relativePath) {
        return fileWithPath;
    }
    try {
        Object.defineProperty(fileWithPath, "webkitRelativePath", {
            value: relativePath,
            configurable: true,
        });
    } catch {
        (fileWithPath as unknown as { [key: string]: unknown }).webkitRelativePath = relativePath;
    }
    return fileWithPath;
}

function createFileInfoFromSelection(selection: FileSelection): FileInfo {
    const { file, relativePath } = selection;
    const rawFile = ensureWebkitRelativePath(file, relativePath);
    return {
        name: file.name,
        rawFile,
        size: file.size,
        status: "",
        type: file.type,
        validationMessages: { minSize: "", maxSize: "" },
        statusCode: "1",
    } as FileInfo;
}

async function collectSelectionsFromEntry(
    entry: FileSystemEntryLike,
    parentPath: string,
    accumulator: FileSelection[]
): Promise<void> {
    if (entry.isFile) {
        const fileEntry = entry as FileSystemFileEntryLike;
        const file = await new Promise<File>((resolve, reject) => {
            fileEntry.file(resolve, (error) =>
                reject(error ?? new DOMException("Failed to read file"))
            );
        });
        const relativePath = parentPath ? `${parentPath}${file.name}` : file.name;
        accumulator.push({ file, relativePath });
        if (accumulator.length >= MAX_SELECTION_FILES) {
            throw new Error(MAX_SELECTION_ERROR_CODE);
        }
        return;
    }
    if (entry.isDirectory) {
        const directoryEntry = entry as FileSystemDirectoryEntryLike;
        const nextPath = parentPath
            ? `${parentPath}${directoryEntry.name}/`
            : `${directoryEntry.name}/`;
        const reader = directoryEntry.createReader();

        const readBatch = async (): Promise<void> => {
            const entries = await new Promise<FileSystemEntryLike[]>((resolve, reject) => {
                reader.readEntries(
                    (batch) => resolve(batch as FileSystemEntryLike[]),
                    (error) => reject(error ?? new DOMException("Failed to read directory entries"))
                );
            });

            if (!entries.length) {
                return;
            }

            for (const child of entries) {
                await collectSelectionsFromEntry(child, nextPath, accumulator);
                if (accumulator.length >= MAX_SELECTION_FILES) {
                    return;
                }
            }

            if (accumulator.length >= MAX_SELECTION_FILES) {
                return;
            }

            await readBatch();
        };

        await readBatch();
        return;
    }
    return;
}

async function collectSelectionsFromItem(
    item: DataTransferItemWithEntry,
    accumulator: FileSelection[]
): Promise<void> {
    const entry = item.webkitGetAsEntry?.();
    if (entry) {
        await collectSelectionsFromEntry(entry, "", accumulator);
        return;
    }
    const file = item.getAsFile();
    if (file) {
        accumulator.push({ file, relativePath: file.name });
        if (accumulator.length >= MAX_SELECTION_FILES) {
            throw new Error(MAX_SELECTION_ERROR_CODE);
        }
        return;
    }
    return;
}

async function collectSelectionsFromDataTransfer(
    dataTransfer: DataTransfer
): Promise<FileSelection[]> {
    const selections: FileSelection[] = [];
    const items = Array.from(dataTransfer.items ?? []);
    if (items.length) {
        for (const item of items) {
            await collectSelectionsFromItem(item as DataTransferItemWithEntry, selections);
            if (selections.length >= MAX_SELECTION_FILES) {
                break;
            }
        }
        if (selections.length) {
            return selections;
        }
    }

    const files = Array.from(dataTransfer.files ?? []);
    for (const file of files) {
        selections.push({ file, relativePath: file.name });
        if (selections.length >= MAX_SELECTION_FILES) {
            throw new Error(MAX_SELECTION_ERROR_CODE);
        }
    }
    if (selections.length >= MAX_SELECTION_FILES) {
        throw new Error(MAX_SELECTION_ERROR_CODE);
    }
    return selections;
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

function fileInfoSize(file: FileInfo): number {
    if (typeof file.size === "number" && file.size > 0) {
        return file.size;
    }
    const raw = file.rawFile as File | Blob | undefined;
    return raw?.size ?? 0;
}

type FileWithPath = File & { webkitRelativePath?: string };

type QueuedUpload = {
    rawFile: File | Blob;
    relativePath: string;
    displayLabel: string;
    size: number;
    contentType: string;
    directoryPath: string;
};

type FileSystemEntryLike = {
    isFile: boolean;
    isDirectory: boolean;
    name: string;
};
type DataTransferItemWithEntry = DataTransferItem & {
    webkitGetAsEntry?: () => FileSystemEntryLike | null;
};

type FileSystemFileEntryLike = FileSystemEntryLike & {
    isFile: true;
    file: (
        successCallback: (file: File) => void,
        errorCallback?: (error: DOMException) => void
    ) => void;
};

type FileSystemDirectoryReaderLike = {
    readEntries: (
        successCallback: (entries: FileSystemEntryLike[]) => void,
        errorCallback?: (error: DOMException) => void
    ) => void;
};

type FileSystemDirectoryEntryLike = FileSystemEntryLike & {
    isDirectory: true;
    createReader: () => FileSystemDirectoryReaderLike;
};

type FileSelection = {
    file: File;
    relativePath: string;
};

const MAX_SELECTION_FILES = 1000;
const MAX_SELECTION_ERROR_CODE = "MAX_SELECTION_EXCEEDED";

function isMaxSelectionError(error: unknown): boolean {
    return error instanceof Error && error.message === MAX_SELECTION_ERROR_CODE;
}

function notifyMaxSelectionLimit() {
    window.alert(
        "Burla is unable to upload more than 1000 files at once :(\n" +
            "Please email jake@burla.dev if this is annoying!"
    );
}

type PreparedUploads = {
    items: QueuedUpload[];
    selectionToken: string;
};

function relativePathForFile(fileInfo: FileInfo): string {
    const rawFile = fileInfo.rawFile as FileWithPath | undefined;
    const rawPath = rawFile?.webkitRelativePath;
    if (rawPath && rawPath.trim() !== "") {
        return rawPath;
    }
    if (fileInfo.name && fileInfo.name.trim() !== "") {
        return fileInfo.name;
    }
    if (rawFile) {
        return rawFile.name;
    }
    throw new Error("Missing file path");
}

export default function Filesystem() {
    const fmRef = React.useRef<FileManagerComponent | null>(null);
    const maxUploadSizeBytes = 10 * 1024 ** 4;
    const [activeUpload, setActiveUpload] = React.useState<ActiveUploadState | null>(null);
    const [isPreparingBatchDownload, setIsPreparingBatchDownload] = React.useState(false);
    const abortControllerRef = React.useRef<AbortController | null>(null);
    const batchDownloadAbortControllerRef = React.useRef<AbortController | null>(null);
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

    const uploadQueueRef = React.useRef<QueuedUpload[]>([]);
    const isProcessingUploadRef = React.useRef(false);
    const queueTotalBytesRef = React.useRef(0);
    const completedBytesRef = React.useRef(0);
    const totalFilesRef = React.useRef(0);
    const processedFilesRef = React.useRef(0);
    const lastSelectionTokenRef = React.useRef<string | null>(null);

    const uploadFileToStorage = React.useCallback(
        async (
            queuedFile: QueuedUpload,
            objectName: string,
            baseUploadedBytes: number,
            totalQueueBytes: number,
            fileIndex: number,
            totalFiles: number
        ) => {
            abortControllerRef.current?.abort();
            const controller = new AbortController();
            abortControllerRef.current = controller;

            const rawFile = queuedFile.rawFile;
            const contentType = queuedFile.contentType || "application/octet-stream";

            const queueBytes = totalQueueBytes > 0 ? totalQueueBytes : rawFile.size;
            const initialUploadedBytes = Math.min(baseUploadedBytes, queueBytes);
            const label =
                totalFiles > 1
                    ? `${queuedFile.displayLabel} (${fileIndex}/${totalFiles})`
                    : queuedFile.displayLabel;
            let lastUploadedBytes = initialUploadedBytes;

            setActiveUpload({
                name: label,
                uploadedBytes: initialUploadedBytes,
                totalBytes: queueBytes,
                state: "uploading",
            });

            try {
                const signedResponse = await fetch(
                    `/signed-resumable?object_name=${encodeURIComponent(
                        objectName
                    )}&content_type=${encodeURIComponent(contentType)}`,
                    { signal: controller.signal }
                );
                const { url } = await signedResponse.json();

                const sessionResponse = await fetch(url, {
                    method: "POST",
                    headers: {
                        "Content-Length": "0",
                        "x-goog-resumable": "start",
                        "Content-Type": contentType,
                    },
                    signal: controller.signal,
                });
                const sessionUrl = sessionResponse.headers.get("Location");
                if (!sessionUrl) {
                    throw new Error("Missing resumable session URL");
                }

                const chunkSize = 8 * 1024 * 1024;
                let offset = 0;
                while (offset < rawFile.size) {
                    if (controller.signal.aborted) {
                        throw new DOMException("Upload aborted", "AbortError");
                    }

                    const end = Math.min(offset + chunkSize, rawFile.size);
                    const chunk = rawFile.slice(offset, end);
                    const range = `bytes ${offset}-${end - 1}/${rawFile.size}`;

                    const response = await fetch(sessionUrl, {
                        method: "PUT",
                        headers: {
                            "Content-Range": range,
                            "Content-Type": contentType,
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

                        lastUploadedBytes = Math.min(queueBytes, baseUploadedBytes + offset);
                        setActiveUpload((current) =>
                            current && current.state === "uploading"
                                ? {
                                      ...current,
                                      uploadedBytes: lastUploadedBytes,
                                  }
                                : current
                        );
                        continue;
                    }

                    if (!response.ok) {
                        throw new Error(`Chunk failed: ${response.status}`);
                    }

                    offset = end;

                    lastUploadedBytes = Math.min(queueBytes, baseUploadedBytes + end);
                    setActiveUpload((current) =>
                        current && current.state === "uploading"
                            ? {
                                  ...current,
                                  uploadedBytes: lastUploadedBytes,
                              }
                            : current
                    );
                }

                lastUploadedBytes = Math.min(queueBytes, baseUploadedBytes + rawFile.size);
                setActiveUpload((current) =>
                    current && current.state === "uploading"
                        ? {
                              ...current,
                              uploadedBytes: lastUploadedBytes,
                          }
                        : current
                );
            } catch (error) {
                const uploadedBytes = lastUploadedBytes;
                if (isAbortError(error)) {
                    setActiveUpload((current) =>
                        current
                            ? {
                                  ...current,
                                  uploadedBytes,
                                  totalBytes: queueBytes,
                                  state: "cancelled",
                              }
                            : current
                    );
                } else {
                    console.error("Resumable upload failed", error);
                    setActiveUpload((current) =>
                        current
                            ? {
                                  ...current,
                                  uploadedBytes,
                                  totalBytes: queueBytes,
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
        let encounteredError = false;
        let wasCancelled = false;

        try {
            while (uploadQueueRef.current.length > 0) {
                const next = uploadQueueRef.current.shift();
                if (!next) {
                    continue;
                }
                const fileSize = next.size;

                let objectName: string;
                try {
                    objectName = buildObjectName(next.directoryPath, next.relativePath);
                } catch (error) {
                    console.error("Resumable upload failed", error);
                    setActiveUpload({
                        name: next.displayLabel,
                        uploadedBytes: completedBytesRef.current,
                        totalBytes: queueTotalBytesRef.current || fileSize,
                        state: "error",
                    });
                    window.alert("Upload failed. Please try again.");
                    encounteredError = true;
                    break;
                }

                try {
                    const baseUploaded = completedBytesRef.current;
                    const totalQueueBytes = queueTotalBytesRef.current || fileSize;
                    const fileIndex = processedFilesRef.current + 1;
                    const totalFiles = totalFilesRef.current;

                    await uploadFileToStorage(
                        next,
                        objectName,
                        baseUploaded,
                        totalQueueBytes,
                        fileIndex,
                        totalFiles
                    );

                    completedBytesRef.current = baseUploaded + fileSize;
                    processedFilesRef.current = fileIndex;
                    uploadedAny = true;
                } catch (error) {
                    if (isAbortError(error)) {
                        wasCancelled = true;
                    } else {
                        encounteredError = true;
                        window.alert("Upload failed. Please try again.");
                    }
                    break;
                }
            }
        } finally {
            uploadQueueRef.current = [];
            abortControllerRef.current = null;

            if (
                uploadedAny &&
                !encounteredError &&
                !wasCancelled &&
                totalFilesRef.current > 0 &&
                processedFilesRef.current === totalFilesRef.current
            ) {
                const totalBytes =
                    queueTotalBytesRef.current > 0
                        ? queueTotalBytesRef.current
                        : completedBytesRef.current;
                setActiveUpload((current) =>
                    current
                        ? {
                              ...current,
                              name: totalFilesRef.current > 1 ? "Uploads complete" : current.name,
                              uploadedBytes: totalBytes,
                              totalBytes,
                              state: "done",
                          }
                        : current
                );
            }

            if (uploadedAny) {
                fmRef.current?.refreshFiles();
            }
            clearUploaderFiles(fmRef.current?.uploadObj ?? null);
            isProcessingUploadRef.current = false;
            completedBytesRef.current = 0;
            processedFilesRef.current = 0;
            totalFilesRef.current = 0;
            queueTotalBytesRef.current = 0;
            lastSelectionTokenRef.current = null;
        }
    }, [uploadFileToStorage]);

    const prepareQueuedUploads = React.useCallback(
        (filesData: FileInfo[], directoryPath: string): PreparedUploads => {
            const uniqueEntries = new Map<
                string,
                {
                    fileInfo: FileInfo;
                    segments: string[];
                }
            >();
            let hasNestedContent = false;

            for (const fileEntry of filesData) {
                if (!fileEntry || !(fileEntry.rawFile instanceof Blob)) {
                    continue;
                }

                let originalRelativePath: string;
                try {
                    originalRelativePath = relativePathForFile(fileEntry);
                } catch (error) {
                    throw error;
                }

                const normalizedRelativePath = normalizeUploadRelativePath(originalRelativePath);
                const segments = normalizedRelativePath.split("/").filter(Boolean);
                if (segments.length > 1) {
                    hasNestedContent = true;
                }

                if (!uniqueEntries.has(normalizedRelativePath)) {
                    uniqueEntries.set(normalizedRelativePath, {
                        fileInfo: fileEntry,
                        segments,
                    });
                    if (uniqueEntries.size >= MAX_SELECTION_FILES) {
                        throw new Error(MAX_SELECTION_ERROR_CODE);
                    }
                }
            }

            if (!uniqueEntries.size) {
                throw new Error("No files to upload");
            }

            const filteredEntries: Array<[string, { fileInfo: FileInfo; segments: string[] }]> = [];

            if (hasNestedContent) {
                const folderCandidates: Array<
                    [string, { fileInfo: FileInfo; segments: string[] }]
                > = [];
                const fileCandidates: Array<[string, { fileInfo: FileInfo; segments: string[] }]> =
                    [];

                for (const [path, entry] of uniqueEntries.entries()) {
                    if (entry.segments.length > 1) {
                        folderCandidates.push([path, entry]);
                    } else {
                        fileCandidates.push([path, entry]);
                    }
                }

                filteredEntries.push(...fileCandidates);
                filteredEntries.push(...folderCandidates);
            } else {
                for (const entry of uniqueEntries.entries()) {
                    filteredEntries.push(entry);
                }
            }

            if (!filteredEntries.length) {
                throw new Error("No files to upload");
            }

            if (filteredEntries.length >= MAX_SELECTION_FILES) {
                throw new Error(MAX_SELECTION_ERROR_CODE);
            }

            const items = filteredEntries.map(([path, entry]) => {
                const rawFile = entry.fileInfo.rawFile as File | Blob;
                return {
                    rawFile,
                    relativePath: path,
                    displayLabel: path,
                    size: fileInfoSize(entry.fileInfo),
                    contentType: rawFile.type || "application/octet-stream",
                    directoryPath,
                };
            });

            const selectionToken = filteredEntries
                .map(([path]) => path)
                .sort((a, b) => (a < b ? -1 : a > b ? 1 : 0))
                .join("|");

            return {
                items,
                selectionToken,
            };
        },
        []
    );

    const enqueueUploads = React.useCallback(
        (queuedItems: QueuedUpload[]) => {
            if (!queuedItems.length) {
                return;
            }

            const additionalBytes = queuedItems.reduce((total, item) => total + item.size, 0);

            if (isProcessingUploadRef.current) {
                uploadQueueRef.current.push(...queuedItems);
                queueTotalBytesRef.current += additionalBytes;
                totalFilesRef.current += queuedItems.length;
                setActiveUpload((current) =>
                    current && current.state === "uploading"
                        ? {
                              ...current,
                              totalBytes:
                                  queueTotalBytesRef.current > 0
                                      ? queueTotalBytesRef.current
                                      : current.totalBytes,
                          }
                        : current
                );
                return;
            }

            queueTotalBytesRef.current = additionalBytes;
            completedBytesRef.current = 0;
            totalFilesRef.current = queuedItems.length;
            processedFilesRef.current = 0;

            uploadQueueRef.current = queuedItems.slice();
            isProcessingUploadRef.current = true;
            void processUploadQueue();
        },
        [processUploadQueue]
    );

    const processFilesSelection = React.useCallback(
        (filesData: FileInfo[], manager: FileManagerComponent | null) => {
            if (!manager || !filesData.length) {
                return false;
            }

            const basePath = typeof manager.path === "string" ? manager.path : "/";
            const directoryPath = normalizeServerPath(basePath);
            const prepared = prepareQueuedUploads(filesData, directoryPath);

            if (
                prepared.selectionToken &&
                lastSelectionTokenRef.current &&
                prepared.selectionToken === lastSelectionTokenRef.current
            ) {
                return false;
            }

            lastSelectionTokenRef.current = prepared.selectionToken;
            enqueueUploads(prepared.items);
            manager.uploadDialogObj?.hide();
            return true;
        },
        [enqueueUploads, prepareQueuedUploads]
    );

    const handleUploadSelected = React.useCallback(
        (event: SelectedEventArgs) => {
            event.cancel = true;
            const manager = fmRef.current;
            if (!manager) {
                return;
            }

            const filesData = Array.isArray(event.filesData) ? (event.filesData as FileInfo[]) : [];
            if (!filesData.length) {
                clearUploaderFiles(manager.uploadObj ?? null);
                return;
            }

            try {
                if (filesData.length >= MAX_SELECTION_FILES) {
                    throw new Error(MAX_SELECTION_ERROR_CODE);
                }
                const processed = processFilesSelection(filesData, manager);
                clearDropHighlight(manager);
                if (!processed) {
                    manager.uploadDialogObj?.hide();
                }
            } catch (error) {
                if (isMaxSelectionError(error)) {
                    notifyMaxSelectionLimit();
                } else {
                    console.error("Resumable upload failed", error);
                    window.alert("Upload failed. Please try again.");
                }
                clearDropHighlight(manager);
                lastSelectionTokenRef.current = null;
            } finally {
                clearUploaderFiles(manager.uploadObj ?? null);
            }
        },
        [processFilesSelection]
    );

    const handleBeforeSend = React.useCallback(
        (args: any) => {
            const normalizedAction =
                typeof args.action === "string" ? args.action.toLowerCase() : "";

            if (normalizedAction === "search") {
                args.cancel = true;
                return;
            }

            if (normalizedAction === "create") {
                fmRef.current?.dialogObj?.hide();
                return;
            }

            if (normalizedAction === "rename") {
                fmRef.current?.dialogObj?.hide();
                return;
            }

            if (normalizedAction === "upload") {
                args.cancel = true;

                const manager = fmRef.current;
                const uploader = manager?.uploadObj as unknown as {
                    getFilesData?: () => FileInfo[];
                    selectedFiles?: FileInfo[];
                } | null;

                const uploadedFiles = uploader?.getFilesData?.() ?? [];
                const selectedFiles = Array.isArray(uploader?.selectedFiles)
                    ? uploader?.selectedFiles
                    : [];
                const argsWithFilesData = args as { filesData?: FileInfo[] };
                const fallbackFiles = Array.isArray(argsWithFilesData.filesData)
                    ? argsWithFilesData.filesData
                    : [];

                const filesData = uploadedFiles.length
                    ? uploadedFiles
                    : selectedFiles.length
                    ? selectedFiles
                    : fallbackFiles;

                if (filesData.length) {
                    try {
                        if (filesData.length >= MAX_SELECTION_FILES) {
                            throw new Error(MAX_SELECTION_ERROR_CODE);
                        }
                        const processed = processFilesSelection(filesData, manager ?? null);
                        clearDropHighlight(manager ?? null);
                        if (!processed) {
                            manager?.uploadDialogObj?.hide();
                        }
                    } catch (error) {
                        if (isMaxSelectionError(error)) {
                            notifyMaxSelectionLimit();
                        } else {
                            console.error("Resumable upload failed", error);
                            window.alert("Upload failed. Please try again.");
                        }
                        clearDropHighlight(manager ?? null);
                        lastSelectionTokenRef.current = null;
                    } finally {
                        clearUploaderFiles(manager?.uploadObj ?? null);
                    }
                    return;
                }

                manager?.uploadDialogObj?.hide();
                clearUploaderFiles(manager?.uploadObj ?? null);
            }
        },
        [processFilesSelection]
    );

    React.useEffect(() => {
        let isActive = true;
        let retryId: number | null = null;
        let cleanup: (() => void) | null = null;

        const attachSelectedHandler = () => {
            if (!isActive) {
                return;
            }

            const manager = fmRef.current;
            const uploader = manager?.uploadObj as unknown as {
                on?: (event: string, handler: (event: unknown) => void) => void;
                off?: (event: string, handler: (event: unknown) => void) => void;
            } | null;

            if (!manager || !uploader || typeof uploader.on !== "function") {
                retryId = window.setTimeout(attachSelectedHandler, 50);
                return;
            }

            cleanup?.();
            uploader.on("selected", handleUploadSelected);
            cleanup = () => {
                uploader.off?.("selected", handleUploadSelected);
            };
            if (retryId !== null) {
                window.clearTimeout(retryId);
                retryId = null;
            }
        };

        attachSelectedHandler();

        return () => {
            isActive = false;
            if (retryId !== null) {
                window.clearTimeout(retryId);
            }
            cleanup?.();
        };
    }, [handleUploadSelected]);

    React.useEffect(() => {
        const manager = fmRef.current;
        const hostElement = manager?.element as HTMLElement | undefined;
        if (!manager || !hostElement) {
            return;
        }

        const handleDrop = (event: DragEvent) => {
            const dataTransfer = event.dataTransfer;
            if (!dataTransfer) {
                return;
            }

            event.preventDefault();
            event.stopPropagation();

            void (async () => {
                try {
                    const selections = await collectSelectionsFromDataTransfer(dataTransfer);
                    if (!selections.length) {
                        return;
                    }
                    if (selections.length >= MAX_SELECTION_FILES) {
                        throw new Error(MAX_SELECTION_ERROR_CODE);
                    }
                    const fileInfos = selections.map(createFileInfoFromSelection) as FileInfo[];
                    const processed = processFilesSelection(fileInfos, manager);
                    if (!processed) {
                        manager.uploadDialogObj?.hide();
                    }
                } catch (error) {
                    if (isMaxSelectionError(error)) {
                        notifyMaxSelectionLimit();
                    } else {
                        console.error("Resumable upload failed", error);
                        window.alert("Upload failed. Please try again.");
                    }
                    lastSelectionTokenRef.current = null;
                } finally {
                    clearDropHighlight(manager);
                    dataTransfer.clearData();
                }
            })();
        };

        const handleDragLeave = () => {
            clearDropHighlight(manager);
        };

        const handleDragEnd = () => {
            clearDropHighlight(manager);
        };

        hostElement.addEventListener("drop", handleDrop, true);
        hostElement.addEventListener("dragleave", handleDragLeave, true);
        hostElement.addEventListener("dragend", handleDragEnd, true);
        const dropAreaElement = getDropArea(manager);
        if (dropAreaElement && dropAreaElement !== hostElement) {
            dropAreaElement.addEventListener("drop", handleDrop, true);
            dropAreaElement.addEventListener("dragleave", handleDragLeave, true);
            dropAreaElement.addEventListener("dragend", handleDragEnd, true);
        }

        return () => {
            hostElement.removeEventListener("drop", handleDrop, true);
            hostElement.removeEventListener("dragleave", handleDragLeave, true);
            hostElement.removeEventListener("dragend", handleDragEnd, true);
            const dropAreaElementCleanup = getDropArea(manager);
            if (dropAreaElementCleanup && dropAreaElementCleanup !== hostElement) {
                dropAreaElementCleanup.removeEventListener("drop", handleDrop, true);
                dropAreaElementCleanup.removeEventListener("dragleave", handleDragLeave, true);
                dropAreaElementCleanup.removeEventListener("dragend", handleDragEnd, true);
            }
        };
    }, [processFilesSelection]);

    const handleCancelUpload = React.useCallback(() => {
        uploadQueueRef.current = [];
        abortControllerRef.current?.abort();
        abortControllerRef.current = null;
        isProcessingUploadRef.current = false;
        setActiveUpload((current) =>
            current
                ? {
                      ...current,
                      uploadedBytes: Math.min(current.totalBytes, completedBytesRef.current),
                      totalBytes:
                          current.totalBytes ||
                          (queueTotalBytesRef.current > 0
                              ? queueTotalBytesRef.current
                              : current.totalBytes),
                      state: "cancelled",
                  }
                : current
        );
        totalFilesRef.current = 0;
        processedFilesRef.current = 0;
        completedBytesRef.current = 0;
        queueTotalBytesRef.current = 0;
        clearUploaderFiles(fmRef.current?.uploadObj ?? null);
        lastSelectionTokenRef.current = null;
    }, []);

    const handleSuccess = React.useCallback((args: any) => {
        if (!args || args.action !== "move") return;
        fmRef.current?.refreshFiles();
    }, []);

    const handleFileLoad = React.useCallback((args: FileLoadEventArgs) => {
        const fileDetails = args.fileDetails as {
            size?: number;
            isFile?: boolean;
            type?: string;
            dateModified?: string;
            _fm_modified?: string;
        };
        const isFile = fileDetails.isFile ?? fileDetails.type !== "folder";
        const sizeElement = args.element?.querySelector<HTMLElement>(".e-fe-size, .e-size");
        const modifiedElement = args.element?.querySelector<HTMLElement>(
            ".e-fe-date-value, .e-fe-date"
        );
        if (!isFile) {
            if (sizeElement) {
                sizeElement.textContent = "—";
                sizeElement.title = "—";
            }
            if (modifiedElement) {
                modifiedElement.textContent = "—";
                modifiedElement.title = "—";
            }
            return;
        }
        if (typeof fileDetails.size !== "number") {
            if (modifiedElement) {
                const formattedDate = formatFileManagerDate(
                    fileDetails._fm_modified ?? fileDetails.dateModified
                );
                modifiedElement.textContent = formattedDate;
                modifiedElement.title = formattedDate;
            }
            return;
        }
        const formattedSize = formatFileManagerSize(fileDetails.size);
        if (sizeElement) {
            sizeElement.textContent = formattedSize;
            sizeElement.title = formattedSize;
        }
        if (modifiedElement) {
            const formattedDate = formatFileManagerDate(
                fileDetails._fm_modified ?? fileDetails.dateModified
            );
            modifiedElement.textContent = formattedDate;
            modifiedElement.title = formattedDate;
        }
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
            ? rawEntries
            : (payload.names ?? []).map((name) => ({
                  name,
                  path: payload.path,
                  isFile: true,
              }));

        if (!entries.length) {
            window.alert("Select a file to download.");
            return;
        }

        if (entries.length === 1) {
            const entry = entries[0];
            if (isFileEntry(entry)) {
                if (!entry.name) {
                    window.alert("Download failed. Please try again.");
                    return;
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
                }
                return;
            }
        }

        type BatchItem =
            | {
                  type: "file";
                  objectName: string;
                  archivePath: string;
                  name: string;
              }
            | {
                  type: "folder";
                  prefix: string;
                  archivePath: string;
                  name: string;
              };

        const batchItems: BatchItem[] = [];
        for (const entry of entries) {
            if (!entry || !entry.name) {
                continue;
            }
            try {
                if (isFileEntry(entry)) {
                    const objectName = storageObjectName(entry, fallbackPath);
                    batchItems.push({
                        type: "file",
                        objectName,
                        archivePath: entry.name,
                        name: entry.name,
                    });
                } else {
                    const prefix = storageFolderPrefix(entry, fallbackPath);
                    batchItems.push({
                        type: "folder",
                        prefix,
                        archivePath: entry.name,
                        name: entry.name,
                    });
                }
            } catch (error) {
                console.error("Download failed", error);
                window.alert("Download failed. Please try again.");
                return;
            }
        }

        if (!batchItems.length) {
            window.alert("Download failed. Please try again.");
            return;
        }

        const singleEntry = entries.length === 1 ? entries[0] : null;
        const archiveName =
            singleEntry && singleEntry.name ? `${singleEntry.name}.zip` : "files.zip";

        setIsPreparingBatchDownload(true);
        const controller = new AbortController();
        batchDownloadAbortControllerRef.current = controller;
        try {
            const response = await fetch("/batch-download", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ items: batchItems, archiveName }),
                signal: controller.signal,
            });
            if (!response.ok) {
                throw new Error(`Request failed with status ${response.status}`);
            }
            const blob = await response.blob();
            const url = URL.createObjectURL(blob);
            triggerDownload(url, archiveName);
            window.setTimeout(() => URL.revokeObjectURL(url), 60_000);
        } catch (error) {
            if (isAbortError(error)) {
                return;
            }
            console.error("Download failed", error);
            window.alert("Download failed. Please try again.");
        } finally {
            batchDownloadAbortControllerRef.current = null;
            setIsPreparingBatchDownload(false);
        }
    }, []);

    const handleCancelBatchDownload = React.useCallback(() => {
        if (batchDownloadAbortControllerRef.current) {
            batchDownloadAbortControllerRef.current.abort();
            batchDownloadAbortControllerRef.current = null;
        }
        setIsPreparingBatchDownload(false);
    }, []);

    const [showWelcome, setShowWelcome] = React.useState(true);
    React.useEffect(() => {
        const hidden = localStorage.getItem("filesystemWelcomeHidden") === "true";
        setShowWelcome(!hidden);
    }, []);
    const handleDismissWelcome = () => {
        setShowWelcome(false);
        localStorage.setItem("filesystemWelcomeHidden", "true");
    };

    return (
        <div className="flex-1 flex flex-col justify-start px-12 pt-6 pb-12 min-h-0">
            <div className="max-w-7xl mx-auto w-full flex-1 flex flex-col min-h-0">
                {showWelcome && (
                    <div className="spotlight-surface rounded-xl my-8">
                        <Card className="w-full relative rounded-xl shadow-lg shadow-black/5 bg-white/90 backdrop-blur">
                            <button
                                onClick={handleDismissWelcome}
                                className="absolute top-2 right-2 p-1 hover:bg-gray-100 rounded-full"
                                aria-label="Dismiss welcome message"
                            >
                                <X className="h-6 w-6" />
                            </button>
                            <CardHeader className="pb-4">
                                <CardTitle className="text-[1.45rem] font-semibold text-primary">
                                    📂 &nbsp;Welcome to Your Network Filesystem!
                                </CardTitle>
                            </CardHeader>
                            <CardContent className="space-y-3">
                                <p className="text-gray-700">
                                    <ul className="list-disc pl-6 space-y-1 text-left">
                                        <li>
                                            Any files uploaded here will appear in "
                                            <code>/workspace/shared</code>" inside the cluster.
                                        </li>
                                        <li>
                                            Any files you write to "<code>/workspace/shared</code>"{" "}
                                            inside the cluster, will appear here where you can
                                            download them!
                                        </li>
                                    </ul>
                                </p>
                            </CardContent>
                        </Card>
                    </div>
                )}
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
                            directoryUpload: false,
                        }}
                        detailsViewSettings={{
                            columns: detailsViewColumns,
                        }}
                        navigationPaneSettings={{ visible: false }}
                        contextMenuSettings={{
                            file: ["Download", "Delete", "Rename"],
                            folder: ["Open", "Delete", "Rename", "Download"],
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
                        fileLoad={handleFileLoad}
                        height="100%"
                        width="100%"
                    >
                        <Inject services={[NavigationPane, Toolbar, DetailsView, ContextMenu]} />
                    </FileManagerComponent>
                    {isPreparingBatchDownload && (
                        <div className="absolute inset-0 z-30 flex items-center justify-center bg-slate-900/45 backdrop-blur-sm">
                            <div className="pointer-events-auto relative flex w-full max-w-xl flex-col items-center gap-6 rounded-3xl border border-slate-200 bg-white px-8 py-8 text-slate-800 shadow-xl shadow-slate-900/10">
                                <button
                                    type="button"
                                    className="absolute right-5 top-5 rounded-full p-2 text-slate-500 transition hover:bg-slate-100 hover:text-slate-800"
                                    aria-label="Cancel download"
                                    onClick={handleCancelBatchDownload}
                                >
                                    <X className="h-6 w-6" aria-hidden="true" />
                                </button>
                                <span
                                    className="inline-flex h-10 w-10 animate-spin rounded-full border-[3px]"
                                    style={{
                                        borderColor: "rgba(15, 23, 42, 0.12)",
                                        borderTopColor: "hsl(var(--brand))",
                                    }}
                                    aria-hidden="true"
                                />
                                <span className="text-base font-semibold tracking-tight text-slate-700 text-center">
                                    Compressing files for download …
                                </span>
                            </div>
                        </div>
                    )}
                    {activeUpload && (
                        <div className="absolute inset-0 z-20 flex items-center justify-center bg-slate-900/45 backdrop-blur-sm">
                            <div className="pointer-events-auto relative w-full max-w-2xl rounded-2xl bg-white/95 p-8 shadow-2xl">
                                <div className="flex items-start justify-between gap-6">
                                    <p className="flex-1 text-base font-semibold text-gray-700 truncate">
                                        {activeUpload.state === "uploading"
                                            ? `Uploading: ${activeUpload.name}`
                                            : activeUpload.name}
                                    </p>
                                    {activeUpload.state === "uploading" && (
                                        <button
                                            type="button"
                                            className="-mr-2 -mt-2 p-2 text-gray-500 transition hover:bg-gray-100 hover:text-gray-800 rounded-full"
                                            aria-label="Cancel upload"
                                            onClick={handleCancelUpload}
                                        >
                                            <X className="h-6 w-6" aria-hidden="true" />
                                        </button>
                                    )}
                                </div>
                                <div className="mt-5 h-4 w-full overflow-hidden rounded-full bg-gray-200">
                                    <div
                                        className="h-full rounded-full"
                                        style={{
                                            width: `${(() => {
                                                if (activeUpload.totalBytes > 0) {
                                                    return Math.min(
                                                        100,
                                                        Math.floor(
                                                            (activeUpload.uploadedBytes /
                                                                activeUpload.totalBytes) *
                                                                100
                                                        )
                                                    );
                                                }
                                                return activeUpload.state === "done" ? 100 : 0;
                                            })()}%`,
                                            backgroundColor:
                                                activeUpload.state === "error"
                                                    ? "rgb(239 68 68)"
                                                    : activeUpload.state === "cancelled"
                                                    ? "rgb(245 158 11)"
                                                    : "hsl(var(--brand))",
                                        }}
                                    />
                                </div>
                                <p className="mt-4 text-xs font-medium text-gray-500">
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
