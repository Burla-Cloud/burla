import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { cn } from "@/lib/utils";
import { Cpu, Database, Trash2, Microchip } from "lucide-react";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { useState } from "react";

interface Node {
  id: string;
  name: string;
  status: "RUNNING" | "STARTING" | "STOPPING" | null;
  type: string;
  cpus?: number;
  gpus?: number;
  memory?: string;
  age?: string;
}

interface NodesListProps {
  nodes: Node[];
  onDeleteNode: (nodeId: string) => void;
}

export const NodesList = ({ nodes, onDeleteNode }: NodesListProps) => {
  const [parallelism, setParallelism] = useState(0);

  const getStatusDisplay = (status: Node["status"]) => {
    return status === "RUNNING" ? "ON" : status.toLowerCase();
  };

  const handleParallelismChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const value = Math.min(256, Math.max(0, parseInt(e.target.value) || 0));
    setParallelism(value);
  };

  return (
    <div className="space-y-6">
      <Card className="w-full">
        <CardHeader>
          <CardTitle className="text-xl font-semibold" style={{ color: "#3b5a64" }}>Manage the Cluster</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="grid grid-cols-1 md:grid-cols-1 gap-4">
          Click "Start" above to boot the cluster (1-2 minutes).
          Manage node statuses and view configuration details below, from boot-up to shutdown or deletion.
          </div>
        </CardContent>
      </Card>

      <Card className="w-full">
        <CardHeader className="flex flex-row items-center justify-between">
          <CardTitle className="text-xl font-semibold" style={{ color: "#3b5a64" }}>Nodes</CardTitle>
        </CardHeader>
        <CardContent>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Status</TableHead>
                <TableHead>Name</TableHead>
                <TableHead>Type</TableHead>
                <TableHead>CPUs</TableHead>
                <TableHead>Memory</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {nodes.map((node) => (
                <TableRow key={node.id}>
                  <TableCell>
                    <div className="flex items-center space-x-2">
                      <div
                        className={cn(
                          "w-2 h-2 rounded-full",
                          node.status === "RUNNING" ? "bg-green-500" :
                          node.status === "STARTING" ? "bg-yellow-500 animate-pulse" :
                          node.status === "STOPPING" ? "bg-gray-300 animate-pulse" :
                          "bg-gray-300" // Default
                        )}
                      />
                      <span className={cn("text-sm capitalize", node.status)}>
                        {getStatusDisplay(node.status)}
                      </span>
                    </div>
                  </TableCell>
                  <TableCell>{node.name}</TableCell>
                  <TableCell>{node.type}</TableCell>

                  <TableCell>
                    <div className="flex items-center space-x-1">
                      <Cpu className="h-4 w-4" />
                      <span>{node.cpus || 0}</span>
                    </div>
                  </TableCell>
                  <TableCell>
                    <div className="flex items-center space-x-1">
                      <Database className="h-4 w-4" />
                      <span>{node.memory || '0'}</span>
                    </div>
                  </TableCell>
                  <TableCell className="text-right">
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </CardContent>
      </Card>
    </div>
  );
};
