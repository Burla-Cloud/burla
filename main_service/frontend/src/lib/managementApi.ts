export async function managementJson<T>(
    path: string,
    init: RequestInit = {}
): Promise<T> {
    const response = await fetch(`/v1/management${path}`, {
        credentials: "include",
        ...init,
        headers: {
            Accept: "application/json",
            ...(init.body ? { "Content-Type": "application/json" } : {}),
            ...init.headers,
        },
    });
    if (!response.ok) {
        const body = await response.json().catch(() => null);
        throw new Error(body?.error?.message || `HTTP ${response.status}`);
    }
    return response.json() as Promise<T>;
}

// Streams end server-side every ~50s and EventSource reconnects on its own,
// but a connection that dies silently (laptop sleep, dropped relay tunnel)
// never fires an error, so without a watchdog the page freezes on its last
// received state forever. The server sends keepalive events every 15s; if
// nothing arrives for SSE_STALE_MS the stream is reopened by hand.
const SSE_STALE_MS = 45_000;

export function managementEvents(
    path: string,
    handlers: Record<string, (data: any) => void>
): { close: () => void } {
    let source: EventSource | null = null;
    let lastEventId = "";
    let lastEventAt = Date.now();

    const open = () => {
        source?.close();
        // Only the log streams send event ids; resuming from the last one
        // prevents replayed (duplicated) log lines after a watchdog reopen.
        const separator = path.includes("?") ? "&" : "?";
        const resume = lastEventId ? `${separator}after=${encodeURIComponent(lastEventId)}` : "";
        source = new EventSource(`/v1/management${path}${resume}`);
        lastEventAt = Date.now();
        Object.entries({ keepalive: () => {}, ...handlers }).forEach(([event, handler]) => {
            if (event === "error") {
                source!.addEventListener("error", handler);
                return;
            }
            source!.addEventListener(event, (message) => {
                lastEventAt = Date.now();
                lastEventId = (message as MessageEvent).lastEventId || lastEventId;
                handler(JSON.parse((message as MessageEvent).data));
            });
        });
    };

    const reopenIfStale = () => {
        if (Date.now() - lastEventAt > SSE_STALE_MS) open();
    };
    const staleCheck = window.setInterval(reopenIfStale, 10_000);
    const onVisibilityChange = () => {
        if (document.visibilityState === "visible") reopenIfStale();
    };
    document.addEventListener("visibilitychange", onVisibilityChange);
    open();

    return {
        close: () => {
            window.clearInterval(staleCheck);
            document.removeEventListener("visibilitychange", onVisibilityChange);
            source?.close();
        },
    };
}
