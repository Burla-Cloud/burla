import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { useQuery } from "@tanstack/react-query";

type NodeStatus = "READY" | "RUNNING" | "BOOTING";

interface ClusterNode {
  id: string;
  name: string;
  cpus: number;
  ramGB: number;
  status: NodeStatus;
}

const getStatusColor = (status: NodeStatus) => {
  switch (status) {
    case "READY":
      return "bg-green-500/15 text-green-700 hover:bg-green-500/25";
    case "RUNNING":
      return "bg-blue-500/15 text-blue-700 hover:bg-blue-500/25";
    case "BOOTING":
      return "bg-yellow-500/15 text-yellow-700 hover:bg-yellow-500/25";
    default:
      return "";
  }
};

// Mock data - replace with actual API call
const fetchNodes = async (): Promise<ClusterNode[]> => {
  // Simulating API delay
  await new Promise((resolve) => setTimeout(resolve, 1000));
  
  return [
    { id: "1", name: "node-1", cpus: 4, ramGB: 16, status: "READY" },
    { id: "2", name: "node-2", cpus: 8, ramGB: 32, status: "RUNNING" },
    { id: "3", name: "node-3", cpus: 16, ramGB: 64, status: "BOOTING" },
  ] as const;
};

export function ClusterNodeList() {
  const { data: nodes, isLoading } = useQuery({
    queryKey: ["cluster-nodes"],
    queryFn: fetchNodes,
  });

  if (isLoading) {
    return (
      <div className="space-y-3">
        <Skeleton className="h-4 w-[250px]" />
        <Skeleton className="h-[300px] w-full" />
      </div>
    );
  }

  return (
    <div className="rounded-md border">
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>Node Name</TableHead>
            <TableHead>CPUs</TableHead>
            <TableHead>RAM (GB)</TableHead>
            <TableHead>Status</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {nodes?.map((node) => (
            <TableRow key={node.id}>
              <TableCell className="font-medium">{node.name}</TableCell>
              <TableCell>{node.cpus}</TableCell>
              <TableCell>{node.ramGB}</TableCell>
              <TableCell>
                <Badge className={getStatusColor(node.status)} variant="secondary">
                  {node.status}
                </Badge>
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
  );
}