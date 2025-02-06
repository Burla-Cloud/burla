import { Toaster } from "@/components/ui/toaster";
import { TooltipProvider } from "@/components/ui/tooltip";
import { BrowserRouter, Routes, Route } from "react-router-dom";
import Index from "@/pages/Index";
import { NodesProvider } from "@/contexts/NodesContext";
import { ClusterProvider } from "@/contexts/ClusterContext";
import ErrorBoundary from "@/components/ErrorBoundary";

const App = () => (
    <ErrorBoundary>
        <NodesProvider>
            <ClusterProvider>
                <TooltipProvider>
                    <Toaster />
                    <BrowserRouter>
                        <Routes>
                            <Route path="/" element={<Index />} />
                        </Routes>
                    </BrowserRouter>
                </TooltipProvider>
            </ClusterProvider>
        </NodesProvider>
    </ErrorBoundary>
);

export default App;
