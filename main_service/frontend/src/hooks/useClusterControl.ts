import { useState } from 'react';
import { useToast } from "@/components/ui/use-toast";

type ClusterStatus = "RUNNING" | "STOPPED" | "STARTING" | "STOPPING";

export const useClusterControl = () => {
  const [status, setStatus] = useState<ClusterStatus>("STOPPED");
  const { toast } = useToast();

  const startCluster = async () => {
    setStatus("STARTING");
    
    try {
      const response = await fetch("http://localhost:5001/v1/cluster/restart", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
      });

      if (!response.ok) {
        throw new Error("Failed to restart the cluster");
      }

      setStatus("RUNNING");
      toast({
        title: "Success",
        description: "Cluster has been started successfully",
      });
      return true;

    } catch (error) {
      setStatus("STOPPED");
      toast({
        variant: "destructive",
        title: "Error",
        description: "Failed to start the cluster. Please try again.",
      });
      return false;
    }
  };

  const stopCluster = async () => {
    setStatus("STOPPING");
    
    try {
      const response = await fetch("http://localhost:5001/v1/cluster/delete", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
      });

      if (!response.ok) {
        throw new Error("Failed to stop the cluster");
      }

      setStatus("STOPPED");
      toast({
        title: "Success",
        description: "Cluster has been stopped successfully",
      });
      return true;

    } catch (error) {
      setStatus("RUNNING");
      toast({
        variant: "destructive",
        title: "Error",
        description: "Failed to stop the cluster. Please try again.",
      });
      return false;
    }
  };

  return {
    status,
    startCluster,
    stopCluster,
  };
}; 