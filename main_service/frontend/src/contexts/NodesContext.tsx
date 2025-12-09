// import { createContext, useContext, useEffect, useState } from "react";
// import { BurlaNode, NodeStatus } from "@/types/coreTypes";

// interface NodesContextType {
//     nodes: BurlaNode[];
//     setNodes: React.Dispatch<React.SetStateAction<BurlaNode[]>>;
//     loading: boolean;
// }

// const NodesContext = createContext<NodesContextType>({
//     nodes: [],
//     setNodes: () => {},
//     loading: true,
// });

// export const NodesProvider = ({ children }: { children: React.ReactNode }) => {
//     const [nodes, setNodes] = useState<BurlaNode[]>([]);
//     const [loading, setLoading] = useState(true);
//     // const firstEventReceived = useState(false); // not needed anymore

//     const handleNodeUpdate = (data: any) => {
//         setNodes((prevNodes) => {
//             if (data.deleted) {
//                 // Remove the node from the list if deleted
//                 return prevNodes.filter((node) => node.id !== data.nodeId);
//             }
//             const existingNode = prevNodes.find((node) => node.id === data.nodeId);

//             // create a new node entry if one does not exist yet
//             if (!existingNode) {
//                 return [...prevNodes, createNewNode(data)];
//             }

//             // merge updated fields into the existing node (status and logs)
//             return prevNodes.map((node) =>
//                 node.id === data.nodeId
//                     ? {
//                           ...node,
//                           status: data.status as NodeStatus,
//                           // bring in new logs but keep existing logs if the backend removed them
//                           logs: data.logs ?? node.logs,
//                       }
//                     : node
//             );
//         });
//     };

//     useEffect(() => {
//         let firstMessageSeen = false;
//         let stopped = false;
//         let source: EventSource | null = null;
//         let rotateTimeoutId: number | undefined;
//         let closingForRotate = false;
//         let inErrorRecovery = false;

//         const ROTATE_MS = 55_000; // proactively renew before proxy hard timeout

//         const armRotationTimer = () => {
//             if (rotateTimeoutId) window.clearTimeout(rotateTimeoutId);
//             rotateTimeoutId = window.setTimeout(() => {
//                 if (stopped) return;
//                 closingForRotate = true;
//                 if (source) source.close();
//                 // reopen on next tick; suppress any transient onerror caused by close()
//                 window.setTimeout(() => {
//                     closingForRotate = false;
//                     open();
//                 }, 0);
//             }, ROTATE_MS);
//         };

//         const open = () => {
//             if (stopped) return;
//             if (source) source.close();

//             source = new EventSource("/v1/cluster");

//             source.onopen = () => {
//                 inErrorRecovery = false;
//                 armRotationTimer();
//             };

//             source.onmessage = (event) => {
//                 const data = JSON.parse(event.data);
//                 if (!firstMessageSeen) {
//                     setLoading(false);
//                     firstMessageSeen = true;
//                 }
//                 if (data.type === "empty") {
//                     setNodes([]);
//                     setLoading(false);
//                     return;
//                 }
//                 handleNodeUpdate(data);
//             };

//             source.onerror = (error) => {
//                 if (closingForRotate) return; // intentional close: do not log, do not interfere
//                 inErrorRecovery = true;
//                 if (rotateTimeoutId) window.clearTimeout(rotateTimeoutId); // let server-side retry delay dictate reconnect timing
//                 console.error("EventSource failed!", error);
//             };
//         };

//         open();

//         return () => {
//             stopped = true;
//             if (rotateTimeoutId) window.clearTimeout(rotateTimeoutId);
//             if (source) source.close();
//         };
//     }, []);

//     return (
//         <NodesContext.Provider value={{ nodes, setNodes, loading }}>
//             {children}
//         </NodesContext.Provider>
//     );
// };

// const createNewNode = (data: any): BurlaNode => ({
//     id: data.nodeId,
//     name: data.nodeId,
//     status: data.status as NodeStatus,
//     type: data.type || "unknown",
//     cpus: data.cpus,
//     gpus: data.gpus,
//     memory: data.memory,
//     age: data.age,
//     logs: data.logs,
// });

// export const useNodes = () => useContext(NodesContext);


// NodesContext.tsx



import {
  createContext,
  useContext,
  useEffect,
  useState,
} from "react";
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
      const existingNode = prevNodes.find(node => node.id === data.nodeId);

      if (data.deleted) {
        const deletedAt = Date.now();

        if (!existingNode) {
          const tombstone: BurlaNode = {
            id: data.nodeId,
            name: data.nodeId,
            status: "DELETED",
            type: data.type || "unknown",
            cpus: data.cpus,
            gpus: data.gpus,
            memory: data.memory,
            age: data.age,
            logs: data.logs,
            deletedAt,
          };
          return [...prevNodes, tombstone];
        }

        return prevNodes.map(node =>
          node.id === data.nodeId
            ? {
                ...node,
                status: "DELETED" as NodeStatus,
                deletedAt,
              }
            : node
        );
      }

      if (!existingNode) {
        return [...prevNodes, createNewNode(data)];
      }

      return prevNodes.map(node =>
        node.id === data.nodeId
          ? {
              ...node,
              status: data.status as NodeStatus,
              type: data.type ?? node.type,
              cpus: data.cpus ?? node.cpus,
              gpus: data.gpus ?? node.gpus,
              memory: data.memory ?? node.memory,
              age: data.age ?? node.age,
              logs: data.logs ?? node.logs,
            }
          : node
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
          setNodes(prev =>
            prev.filter(
              n => n.status === "DELETED" || n.status === "FAILED"
            )
          );
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
  id: data.nodeId,
  name: data.nodeId,
  status: data.status as NodeStatus,
  type: data.type || "unknown",
  cpus: data.cpus,
  gpus: data.gpus,
  memory: data.memory,
  age: data.age,
  logs: data.logs,
  deletedAt: undefined,
});

export const useNodes = () => useContext(NodesContext);
