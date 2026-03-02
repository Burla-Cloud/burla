import { RefObject } from "react";
import { VariableSizeList as List } from "react-window";
import { Button } from "@/components/ui/button";
import { Copy, CornerDownLeft, WrapText, MoveDown, AlertTriangle } from "lucide-react";
import { Switch } from "@/components/ui/switch";

export type LogRowItem =
    | { type: "divider"; key: string; label: string }
    | { type: "empty"; key: string; label: string }
    | { type: "log"; key: string; id: string; logTimestamp: number; message: string };

type LogViewerProps = {
    selectedIndex: number;
    rows: LogRowItem[];
    listHeight: number;
    listRef: RefObject<List | null>;
    outerListRef: RefObject<HTMLDivElement | null>;
    wrapLines: boolean;
    autoScroll: boolean;
    onWrapToggle: (checked: boolean) => void;
    onAutoScrollToggle: (checked: boolean) => void;
    onJumpToBottom: () => void;
    onCopyAll: () => void;
    onCopyTraceback: () => void;
    isPageLoading: boolean;
    isPanelLoading: boolean;
    isLoadingOlderLogs: boolean;
    getItemSize: (index: number) => number;
    formatTime: (timestamp: number) => string;
    onMeasureRow: (id: string, height: number, fromIndex: number) => void;
    onListScroll: (scrollDirection: "forward" | "backward", scrollOffset: number, scrollUpdateWasRequested: boolean) => void;
};

const isErrorMessage = (message: string) =>
    message.includes("Traceback (most recent call last):") ||
    message.startsWith("Exception:") ||
    message.startsWith("Error:");

const LogViewer = ({
    selectedIndex,
    rows,
    listHeight,
    listRef,
    outerListRef,
    wrapLines,
    autoScroll,
    onWrapToggle,
    onAutoScrollToggle,
    onJumpToBottom,
    onCopyAll,
    onCopyTraceback,
    isPageLoading,
    isPanelLoading,
    isLoadingOlderLogs,
    getItemSize,
    formatTime,
    onMeasureRow,
    onListScroll,
}: LogViewerProps) => {
    return (
        <div className="flex h-full min-h-0 flex-col">
            <div className="sticky top-0 z-20 flex flex-wrap items-center justify-between gap-2 border-b border-slate-200 bg-white px-3 py-2">
                <div className="text-[13px] text-slate-800">Call {selectedIndex.toLocaleString()} Logs</div>
                <div className="flex flex-wrap items-center gap-2">
                    <label className="inline-flex items-center gap-2 rounded-md border border-slate-200 bg-white px-2.5 py-1.5 text-[13px]">
                        <Switch checked={wrapLines} onCheckedChange={onWrapToggle} className="scale-75 origin-left" aria-label="Toggle line wrapping" />
                        <WrapText className="h-4 w-4 text-slate-500" />
                        <span>Wrap</span>
                    </label>
                    <label className="inline-flex items-center gap-2 rounded-md border border-slate-200 bg-white px-2.5 py-1.5 text-[13px]">
                        <Switch checked={autoScroll} onCheckedChange={onAutoScrollToggle} className="scale-75 origin-left" aria-label="Toggle auto scroll" />
                        <span>Auto-scroll</span>
                    </label>
                    <Button variant="outline" size="sm" className="h-9 text-[13px]" onClick={onJumpToBottom}>
                        <MoveDown className="mr-1.5 h-4 w-4" />
                        Jump to bottom
                    </Button>
                    <Button variant="outline" size="sm" className="h-9 text-[13px]" onClick={onCopyTraceback}>
                        <AlertTriangle className="mr-1.5 h-4 w-4" />
                        Copy traceback
                    </Button>
                    <Button variant="outline" size="sm" className="h-9 text-[13px]" onClick={onCopyAll}>
                        <Copy className="mr-1.5 h-4 w-4" />
                        Copy
                    </Button>
                </div>
            </div>

            <div className="relative flex-1 min-h-0 font-mono text-[13px] text-gray-800">
                {isPanelLoading && !isPageLoading && (
                    <div className="absolute inset-0 z-20 flex items-center justify-center bg-white/70">
                        <div className="inline-flex items-center gap-2 rounded-md border border-slate-200 bg-white px-3 py-1.5 text-[13px] text-slate-700">
                            <div className="h-4 w-4 rounded-full border-2 border-slate-300 border-t-primary animate-spin" />
                            Loading logs...
                        </div>
                    </div>
                )}

                {isPageLoading ? (
                    <div className="flex h-full w-full items-center justify-center">
                        <div className="inline-flex items-center gap-2 rounded-md border border-slate-200 bg-white px-3 py-1.5 text-[13px] text-slate-700">
                            <div className="h-4 w-4 rounded-full border-2 border-slate-300 border-t-primary animate-spin" />
                            Loading logs...
                        </div>
                    </div>
                ) : (
                    <div className="h-full">
                        {isLoadingOlderLogs && (
                            <div className="absolute left-0 right-0 top-0 z-10 flex items-center justify-center gap-2 border-b border-slate-200 bg-white/95 py-2 text-[13px] text-slate-700">
                                <div className="h-4 w-4 rounded-full border-2 border-slate-300 border-t-primary animate-spin" />
                                <span>Loading older logs...</span>
                            </div>
                        )}
                        <List
                            height={listHeight}
                            itemCount={rows.length}
                            itemSize={getItemSize}
                            width="100%"
                            ref={listRef}
                            outerRef={outerListRef}
                            itemKey={(index) => rows[index]?.key ?? index}
                            onScroll={({ scrollDirection, scrollOffset, scrollUpdateWasRequested }) =>
                                onListScroll(scrollDirection, scrollOffset, scrollUpdateWasRequested)
                            }
                        >
                            {({ index, style }) => {
                                const row = rows[index];
                                if (!row) return null;

                                if (row.type === "divider") {
                                    return (
                                        <div key={row.key} style={style} className="px-4 py-2" role="separator" aria-label={`Logs for ${row.label}`}>
                                            <div className="flex w-full items-center gap-3 select-none">
                                                <div className="h-px w-full bg-slate-200" />
                                                <span className="shrink-0 text-center text-[13px] tracking-tight text-slate-600">{row.label}</span>
                                                <div className="h-px w-full bg-slate-200" />
                                            </div>
                                        </div>
                                    );
                                }

                                if (row.type === "empty") {
                                    return (
                                        <div key={row.key} style={style} className="px-4 py-4 text-slate-500">
                                            {row.label}
                                        </div>
                                    );
                                }

                                const hasErrorStyle = isErrorMessage(row.message);
                                return (
                                    <div key={row.key} style={style}>
                                        <div
                                            ref={(element) => {
                                                if (!element) return;
                                                requestAnimationFrame(() => {
                                                    if (!element.isConnected) return;
                                                    onMeasureRow(row.id, Math.ceil(element.offsetHeight), index);
                                                });
                                            }}
                                            className={`grid grid-cols-[8rem,1fr] gap-3 border-t border-slate-200 px-4 py-2 ${hasErrorStyle ? "bg-rose-50/50" : index % 2 === 0 ? "bg-slate-50/40" : "bg-white"}`}
                                        >
                                            <div className="tabular-nums text-slate-500">{formatTime(row.logTimestamp)}</div>
                                            <div className={wrapLines ? "whitespace-pre-wrap break-words" : "whitespace-pre overflow-x-auto"}>
                                                {row.message}
                                            </div>
                                        </div>
                                    </div>
                                );
                            }}
                        </List>
                    </div>
                )}
            </div>
        </div>
    );
};

export default LogViewer;
