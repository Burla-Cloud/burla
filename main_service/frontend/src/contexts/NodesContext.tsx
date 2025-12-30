import { createContext, useContext, useEffect, useState } from "react";
import { BurlaNode, NodeStatus } from "@/types/coreTypes";

interface NodesContextType {
  nodes: BurlaNode[];
  setNodes: React.Dispatch<React.SetStateAction<BurlaNode[]>>;
  loading: boolean;
}

const NodesContext = createContext<NodesContextType>({
  nodes: [],
  setNodes: () => {},
  loading: true,
});

export const NodesProvider = ({ children }: { children: React.ReactNode }) => {
  const [nodes, setNodes] = useState<BurlaNode[]>([]);
  const [loading, setLoading] = useState(true);

  const handleNodeUpdate = (data: any) => {
    setNodes(prevNodes => {
      const nodeId = String(data.nodeId || "");
      if (!nodeId) return prevNodes;

      const existingNode = prevNodes.find(n => n.id === nodeId);

      if (data.deleted) {
        const deletedAt = Date.now();

        if (!existingNode) {
          const tombstone: BurlaNode = {
            id: nodeId,
            name: nodeId,
            status: "DELETED",
            type: data.type || "unknown",
            cpus: data.cpus,
            gpus: data.gpus,
            memory: data.memory,
            age: data.age,
            logs: data.logs,
            started_booting_at:
              typeof data.started_booting_at === "number" ? data.started_booting_at : undefined,
            deletedAt,
          };
          return [...prevNodes, tombstone];
        }

        return prevNodes.map(n =>
          n.id === nodeId
            ? {
                ...n,
                status: "DELETED" as NodeStatus,
                deletedAt,
              }
            : n
        );
      }

      if (!existingNode) {
        return [...prevNodes, createNewNode(data)];
      }

      return prevNodes.map(n =>
        n.id === nodeId
          ? {
              ...n,
              status: (data.status as NodeStatus) ?? n.status,
              type: data.type ?? n.type,
              cpus: data.cpus ?? n.cpus,
              gpus: data.gpus ?? n.gpus,
              memory: data.memory ?? n.memory,
              age: data.age ?? n.age,
              logs: data.logs ?? n.logs,
              started_booting_at:
                typeof data.started_booting_at === "number"
                  ? data.started_booting_at
                  : n.started_booting_at,
            }
          : n
      );
    });
  };

  useEffect(() => {
    let firstMessageSeen = false;
    let stopped = false;
    let source: EventSource | null = null;
    let rotateTimeoutId: number | undefined;
    let closingForRotate = false;

    const ROTATE_MS = 55_000;

    const armRotationTimer = () => {
      if (rotateTimeoutId) window.clearTimeout(rotateTimeoutId);
      rotateTimeoutId = window.setTimeout(() => {
        if (stopped) return;
        closingForRotate = true;
        if (source) source.close();
        window.setTimeout(() => {
          closingForRotate = false;
          open();
        }, 0);
      }, ROTATE_MS);
    };

    const open = () => {
      if (stopped) return;
      if (source) source.close();

      source = new EventSource("/v1/cluster");

      source.onopen = () => {
        armRotationTimer();
      };

      source.onmessage = event => {
        const data = JSON.parse(event.data);

        if (!firstMessageSeen) {
          setLoading(false);
          firstMessageSeen = true;
        }

        if (data.type === "empty") {
          setNodes([]);
          return;
        }

        handleNodeUpdate(data);
      };

      source.onerror = error => {
        if (closingForRotate) return;
        if (rotateTimeoutId) window.clearTimeout(rotateTimeoutId);
        console.error("EventSource /v1/cluster failed", error);
      };
    };

    open();

    return () => {
      stopped = true;
      if (rotateTimeoutId) window.clearTimeout(rotateTimeoutId);
      if (source) source.close();
    };
  }, []);

  return (
    <NodesContext.Provider value={{ nodes, setNodes, loading }}>
      {children}
    </NodesContext.Provider>
  );
};

const createNewNode = (data: any): BurlaNode => ({
  id: String(data.nodeId),
  name: String(data.nodeId),
  status: data.status as NodeStatus,
  type: data.type || "unknown",
  cpus: data.cpus,
  gpus: data.gpus,
  memory: data.memory,
  age: data.age,
  logs: data.logs,
  started_booting_at:
    typeof data.started_booting_at === "number" ? data.started_booting_at : undefined,
  deletedAt: undefined,
});

export const useNodes = () => useContext(NodesContext);

