import clusterImage from "@/assets/logo.svg";
import { Link } from "react-router-dom";
import ThemeToggle from "@/components/ThemeToggle";

interface SidebarProps {
  disabled?: boolean;
}

const Sidebar = ({ disabled = false }: SidebarProps) => {
  return (
    <div
      className={`w-60 min-h-screen bg-gray-100 dark:bg-gray-900 border-r border-gray-200 dark:border-gray-800 p-4 flex flex-col transition-opacity duration-200 ${
        disabled ? "opacity-60 pointer-events-none select-none" : ""
      }`}
    >
      {/* Logo */}
      <div className="flex justify-left mt-6 mb-4">
        <Link to="/">
          <img
            src={clusterImage}
            style={{ width: "128px", height: "auto" }}
            className="ml-2 dark:invert dark:brightness-95"
          />
        </Link>
      </div>

      <hr className="border-gray-300 dark:border-gray-700 my-5 w-full" />

      <nav className="space-y-1 text-lg font-medium">
        <Link
          to="/"
          className="flex items-center space-x-1 text-gray-700 dark:text-gray-300 hover:text-primary hover:bg-primary/10 p-2 rounded-md"
        >
          <span>Cluster Status</span>
        </Link>
        <Link
          to="/jobs"
          className="flex items-center space-x-1 text-gray-700 dark:text-gray-300 hover:text-primary hover:bg-primary/10 p-2 rounded-md"
        >
          <span>Jobs</span>
        </Link>
        <Link
          to="/filesystem"
          className="flex items-center space-x-1 text-gray-700 dark:text-gray-300 hover:text-primary hover:bg-primary/10 p-2 rounded-md"
        >
          <span>Filesystem</span>
        </Link>
        <Link
          to="/settings"
          className="flex items-center space-x-1 text-gray-700 dark:text-gray-300 hover:text-primary hover:bg-primary/10 p-2 rounded-md"
        >
          <span>Settings</span>
        </Link>
      </nav>

      <div className="mt-auto pt-4 flex justify-center">
        <ThemeToggle />
      </div>
    </div>
  );
};

export default Sidebar;
