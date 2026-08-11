import { Link } from "react-router-dom";
import { BrandLockup } from "@/components/BrandLockup";

interface SidebarProps {
  disabled?: boolean;
}

// Injected by main_service's dashboard endpoint; false when the cluster has
// no shared-workspace bucket (the client-hosted default).
declare global {
  interface Window {
    __BURLA_FILESYSTEM_ENABLED__?: boolean;
  }
}

const linkClass =
  "flex items-center space-x-1 font-mono text-[15px] text-sidebar-foreground hover:text-sidebar-accent-foreground hover:bg-sidebar-accent p-2 rounded-md";

const Sidebar = ({ disabled = false }: SidebarProps) => {
  const filesystemEnabled = window.__BURLA_FILESYSTEM_ENABLED__ !== false;
  return (
    <div
      className={`w-60 min-h-screen bg-sidebar border-r border-sidebar-border p-4 flex flex-col transition-opacity duration-200 ${
        disabled ? "opacity-60 pointer-events-none select-none" : ""
      }`}
    >
      {/* Logo */}
      <div className="flex justify-left mt-6 mb-4 ml-2">
        <BrandLockup />
      </div>

      <hr className="border-sidebar-border my-5 w-full" />

      <nav className="space-y-1">
        <Link to="/" className={linkClass}>
          <span>Cluster Status</span>
        </Link>
        <Link to="/jobs" className={linkClass}>
          <span>Jobs</span>
        </Link>
        {filesystemEnabled && (
          <Link to="/filesystem" className={linkClass}>
            <span>Filesystem</span>
          </Link>
        )}
        <Link to="/settings" className={linkClass}>
          <span>Settings</span>
        </Link>
      </nav>
    </div>
  );
};

export default Sidebar;
