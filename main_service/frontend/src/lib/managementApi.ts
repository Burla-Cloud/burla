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

export function managementEvents(
    path: string,
    handlers: Record<string, (data: any) => void>
): EventSource {
    const source = new EventSource(`/v1/management${path}`);
    Object.entries(handlers).forEach(([event, handler]) => {
        source.addEventListener(event, (message) => {
            handler(JSON.parse((message as MessageEvent).data));
        });
    });
    return source;
}
