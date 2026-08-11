import { BrowserRouter as Router, Routes, Route, Outlet } from "react-router-dom";
import Sidebar from "@/components/Sidebar";
import Dashboard from "@/pages/Index";
import Jobs from "@/pages/Jobs";
import Settings from "@/pages/Settings";
import JobDetails from "@/pages/JobDetails";
import Filesystem from "@/pages/Filesystem";
import { Toaster } from "@/components/ui/toaster";
import { TooltipProvider } from "@/components/ui/tooltip";
import { NodesProvider } from "@/contexts/NodesContext";
import { ClusterProvider } from "@/contexts/ClusterContext";
import { JobsProvider } from "@/contexts/JobsContext";
import { SettingsProvider } from "@/contexts/SettingsContext";
import { LogsProvider } from "@/contexts/LogsContext";
import ErrorBoundary from "@/components/ErrorBoundary";
import ProfilePicture from "@/components/ProfilePicture";
import ThemeToggle from "@/components/ThemeToggle";
import { useState } from "react";

const Layout = () => {
  const [saving, setSaving] = useState(false);

  return (
    <div className="flex h-screen w-full overflow-hidden bg-background">
      <div className="fixed top-6 right-6 z-50 flex items-center gap-3">
        <ThemeToggle />
        <ProfilePicture />
      </div>
      <div className="shrink-0">
        <Sidebar disabled={saving} />
      </div>

      {/* Make the main pane scroll vertically (not the whole window) */}
      <div className="flex-1 min-w-0 overflow-y-auto overflow-x-hidden">
        {/* Global top + bottom padding so pages never kiss the bottom */}
        <div className="min-h-full w-full pt-10 pb-12 px-12 min-w-0 flex items-stretch">
          <Outlet context={{ saving, setSaving }} />
        </div>
      </div>
    </div>
  );
};

const App = () => (
  <ErrorBoundary>
    <NodesProvider>
      <ClusterProvider>
        <TooltipProvider>
          <Toaster />
          <JobsProvider>
            <SettingsProvider>
              <LogsProvider>
                <Router>
                  <Routes>
                    <Route element={<Layout />}>
                      <Route path="/" element={<Dashboard />} />
                      <Route path="/jobs" element={<Jobs />} />
                      <Route path="/jobs/:jobId" element={<JobDetails />} />
                      <Route path="/settings" element={<Settings />} />
                      <Route path="/filesystem" element={<Filesystem />} />
                    </Route>
                  </Routes>
                </Router>
              </LogsProvider>
            </SettingsProvider>
          </JobsProvider>
        </TooltipProvider>
      </ClusterProvider>
    </NodesProvider>
  </ErrorBoundary>
);

export default App;
