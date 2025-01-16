import { motion } from "framer-motion";
import { ClusterNodeList } from "@/components/ClusterNodeList";
import { ClusterControls } from "@/components/ClusterControls";
import { ClusterConfigForm } from "@/components/ClusterConfigForm";
import { Separator } from "@/components/ui/separator";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { ScrollArea } from "@/components/ui/scroll-area";

const Index = () => {
  return (
    <motion.div
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      transition={{ duration: 0.6, ease: "easeOut" }}
      className="min-h-screen bg-background"
    >
      <div className="flex">
        {/* Main Content */}
        <div className="flex-1 p-6">
          <div className="max-w-4xl space-y-8 page-transition">
            <div className="space-y-4">
              <span className="inline-block px-3 py-1 text-sm tracking-wide bg-secondary rounded-full text-primary/80">
                Cluster Dashboard
              </span>
              <h1 className="text-4xl font-medium tracking-tight sm:text-5xl text-primary">
                Compute Cluster Status
              </h1>
              <p className="text-lg text-muted-foreground max-w-[42rem]">
                Monitor your compute nodes and their current status in real-time
              </p>
            </div>
            
            <ClusterControls />
            <ClusterNodeList />
          </div>
        </div>

        {/* Right Sidebar */}
        <div className="w-[400px] border-l min-h-screen">
          <Tabs defaultValue="dashboard" className="h-full">
            <div className="border-b px-4 py-2">
              <TabsList className="w-full">
                <TabsTrigger value="dashboard" className="flex-1">Dashboard</TabsTrigger>
                <TabsTrigger value="settings" className="flex-1">Settings</TabsTrigger>
              </TabsList>
            </div>

            <ScrollArea className="h-[calc(100vh-56px)]">
              <TabsContent value="dashboard" className="p-4 m-0">
                <div className="space-y-4">
                  <h2 className="text-2xl font-medium tracking-tight text-primary">
                    Quick Overview
                  </h2>
                  <p className="text-muted-foreground">
                    Monitor your cluster's performance and status
                  </p>
                  <ClusterNodeList />
                </div>
              </TabsContent>

              <TabsContent value="settings" className="p-4 m-0">
                <div className="space-y-4">
                  <h2 className="text-2xl font-medium tracking-tight text-primary">
                    Cluster Configuration
                  </h2>
                  <p className="text-muted-foreground">
                    Modify your cluster settings and container configurations
                  </p>
                  <ClusterConfigForm />
                </div>
              </TabsContent>
            </ScrollArea>
          </Tabs>
        </div>
      </div>
    </motion.div>
  );
}

export default Index;