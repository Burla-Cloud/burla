import { zodResolver } from "@hookform/resolvers/zod";
import { useForm } from "react-hook-form";
import { useMutation } from "@tanstack/react-query";
import { useToast } from "@/components/ui/use-toast";
import { z } from "zod";
import {
  Form,
  FormControl,
  FormDescription,
  FormField,
  FormItem,
  FormLabel,
  FormMessage,
} from "@/components/ui/form";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Textarea } from "@/components/ui/textarea";

const clusterConfigSchema = z.object({
  nodeCount: z.coerce
    .number()
    .min(1, "Must have at least 1 node")
    .max(100, "Maximum 100 nodes allowed"),
  cpusPerNode: z.coerce
    .number()
    .min(1, "Must have at least 1 CPU per node")
    .max(128, "Maximum 128 CPUs per node"),
  ramPerNode: z.coerce
    .number()
    .min(1, "Must have at least 1GB RAM per node")
    .max(1024, "Maximum 1024GB RAM per node"),
  containers: z.string().min(1, "Container configuration is required"),
});

type ClusterConfig = z.infer<typeof clusterConfigSchema>;

// Mock API function - replace with actual API call
const updateClusterConfig = async (config: ClusterConfig) => {
  await new Promise((resolve) => setTimeout(resolve, 1000));
  return { success: true, config };
};

export function ClusterConfigForm() {
  const { toast } = useToast();

  const form = useForm<ClusterConfig>({
    resolver: zodResolver(clusterConfigSchema),
    defaultValues: {
      nodeCount: 3,
      cpusPerNode: 4,
      ramPerNode: 16,
      containers: `[
  {
    "name": "app",
    "image": "nginx:latest",
    "port": 80
  }
]`,
    },
  });

  const mutation = useMutation({
    mutationFn: updateClusterConfig,
    onSuccess: (data) => {
      toast({
        title: "Configuration Updated",
        description: "The cluster configuration has been updated successfully.",
      });
    },
  });

  const onSubmit = (data: ClusterConfig) => {
    mutation.mutate(data);
  };

  return (
    <Form {...form}>
      <form onSubmit={form.handleSubmit(onSubmit)} className="space-y-6">
        <div className="grid gap-6 md:grid-cols-3">
          <FormField
            control={form.control}
            name="nodeCount"
            render={({ field }) => (
              <FormItem>
                <FormLabel>Number of Nodes</FormLabel>
                <FormControl>
                  <Input type="number" {...field} />
                </FormControl>
                <FormDescription>
                  Total number of nodes in the cluster
                </FormDescription>
                <FormMessage />
              </FormItem>
            )}
          />

          <FormField
            control={form.control}
            name="cpusPerNode"
            render={({ field }) => (
              <FormItem>
                <FormLabel>CPUs per Node</FormLabel>
                <FormControl>
                  <Input type="number" {...field} />
                </FormControl>
                <FormDescription>
                  Number of CPU cores allocated to each node
                </FormDescription>
                <FormMessage />
              </FormItem>
            )}
          />

          <FormField
            control={form.control}
            name="ramPerNode"
            render={({ field }) => (
              <FormItem>
                <FormLabel>RAM per Node (GB)</FormLabel>
                <FormControl>
                  <Input type="number" {...field} />
                </FormControl>
                <FormDescription>
                  Amount of RAM allocated to each node in GB
                </FormDescription>
                <FormMessage />
              </FormItem>
            )}
          />
        </div>

        <FormField
          control={form.control}
          name="containers"
          render={({ field }) => (
            <FormItem>
              <FormLabel>Container Configuration (JSON)</FormLabel>
              <FormControl>
                <Textarea
                  {...field}
                  className="font-mono"
                  rows={10}
                  placeholder="Enter container configuration in JSON format"
                />
              </FormControl>
              <FormDescription>
                JSON array of container configurations to run on each node
              </FormDescription>
              <FormMessage />
            </FormItem>
          )}
        />

        <Button
          type="submit"
          className="w-full md:w-auto"
          disabled={mutation.isPending}
        >
          {mutation.isPending ? "Updating..." : "Update Configuration"}
        </Button>
      </form>
    </Form>
  );
}