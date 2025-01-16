import { Button } from "@/components/ui/button";
import { PlayCircle, StopCircle, RotateCw } from "lucide-react";
import { useMutation } from "@tanstack/react-query";
import { useToast } from "@/components/ui/use-toast";

// Mock API functions - replace with actual API calls
const startCluster = async () => {
  await new Promise((resolve) => setTimeout(resolve, 1000));
  return { success: true };
};

const stopCluster = async () => {
  await new Promise((resolve) => setTimeout(resolve, 1000));
  return { success: true };
};

const rebootCluster = async () => {
  await new Promise((resolve) => setTimeout(resolve, 1000));
  return { success: true };
};

export function ClusterControls() {
  const { toast } = useToast();

  const startMutation = useMutation({
    mutationFn: startCluster,
    onSuccess: () => {
      toast({
        title: "Cluster Starting",
        description: "The cluster is now starting up. This may take a few minutes.",
      });
    },
  });

  const stopMutation = useMutation({
    mutationFn: stopCluster,
    onSuccess: () => {
      toast({
        title: "Cluster Stopping",
        description: "The cluster is now shutting down. This may take a few minutes.",
      });
    },
  });

  const rebootMutation = useMutation({
    mutationFn: rebootCluster,
    onSuccess: () => {
      toast({
        title: "Cluster Rebooting",
        description: "The cluster is now rebooting. This may take a few minutes.",
      });
    },
  });

  return (
    <div className="flex flex-wrap gap-4">
      <Button
        onClick={() => startMutation.mutate()}
        disabled={startMutation.isPending}
        className="bg-green-600 hover:bg-green-700"
      >
        <PlayCircle className="mr-2 h-4 w-4" />
        Start Cluster
      </Button>

      <Button
        onClick={() => stopMutation.mutate()}
        disabled={stopMutation.isPending}
        variant="destructive"
      >
        <StopCircle className="mr-2 h-4 w-4" />
        Stop Cluster
      </Button>

      <Button
        onClick={() => rebootMutation.mutate()}
        disabled={rebootMutation.isPending}
        variant="secondary"
      >
        <RotateCw className="mr-2 h-4 w-4" />
        Reboot Cluster
      </Button>
    </div>
  );
}