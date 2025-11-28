import { BrowserRouter as Router, Routes, Route, Outlet } from "react-router-dom";
import Sidebar from "@/components/Sidebar";
import Dashboard from "@/pages/Index";
import Jobs from "@/pages/Jobs";
import Settings from "@/pages/Settings";
import JobDetails from "@/pages/JobDetails";
import Storage from "@/pages/Storage";
import Filesystem from "@/pages/Filesystem";
import { Toaster } from "@/components/ui/toaster";
import { TooltipProvider } from "@/components/ui/tooltip";
import { NodesProvider } from "@/contexts/NodesContext";
import { ClusterProvider } from "@/contexts/ClusterContext";
import { JobsProvider } from "@/contexts/JobsContext";
import { SettingsProvider } from "@/contexts/SettingsContext";
import { LogsProvider } from "@/contexts/LogsContext";
import ErrorBoundary from "@/components/ErrorBoundary";
import AppDataLoader from "@/components/AppDataLoader";
import ProfilePicture from "@/components/ProfilePicture";
import { useState } from "react";

const Layout = () => {
  const [saving, setSaving] = useState(false);

  return (
    <div className="flex min-h-screen items-stretch bg-gray-50">
      <ProfilePicture />
      <Sidebar disabled={saving} />
      <div className="flex-1 py-10 px-12 flex items-stretch">
        <Outlet context={{ saving, setSaving }} />
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
                <AppDataLoader />
                <Router>
                  <Routes>
                    <Route element={<Layout />}>
                      <Route path="/" element={<Dashboard />} />
                      <Route path="/jobs" element={<Jobs />} />
                      <Route path="/jobs/:jobId" element={<JobDetails />} />
                      <Route path="/settings" element={<Settings />} />
                                                <Route
                                                    path="/filesystem"
                                                    element={<Filesystem />}
                                                />
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

