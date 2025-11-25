import clusterImage from "@/assets/logo.svg";
import { Link } from "react-router-dom";

interface SidebarProps {
  disabled?: boolean;
}

const Sidebar = ({ disabled = false }: SidebarProps) => {
  return (
    <div
      className={`w-60 min-h-screen bg-gray-100 border-r p-4 flex flex-col transition-opacity duration-200 ${
        disabled ? "opacity-60 pointer-events-none select-none" : ""
      }`}
    >
      {/* Logo */}
      <div className="flex justify-left mt-6 mb-4">
        <Link to="/">
          <img
            src={clusterImage}
            style={{ width: "128px", height: "auto" }}
            className="ml-2"
          />
        </Link>
      </div>

      <hr className="border-gray-300 my-5 w-full" />

      <nav className="space-y-1 text-lg font-medium">
        <Link
          to="/"
          className="flex items-center space-x-1 text-gray-700 hover:text-primary hover:bg-primary/10 p-2 rounded-md"
        >
          <span>Cluster Status</span>
        </Link>
        <Link
          to="/jobs"
          className="flex items-center space-x-1 text-gray-700 hover:text-primary hover:bg-primary/10 p-2 rounded-md"
        >
          <span>Jobs</span>
        </Link>
        <Link
          to="/storage"
          className="flex items-center space-x-1 text-gray-700 hover:text-primary hover:bg-primary/10 p-2 rounded-md"
        >
          <span>Network Storage</span>
        </Link>
                <Link
                    to="/filesystem"
                    className="flex items-center space-x-1 text-gray-700 hover:text-primary hover:bg-primary/10 p-2 rounded-md"
                >
                    <span>Filesystem</span>
                </Link>
        <Link
          to="/settings"
          className="flex items-center space-x-1 text-gray-700 hover:text-primary hover:bg-primary/10 p-2 rounded-md"
        >
          <span>Settings</span>
        </Link>
      </nav>
    </div>
  );
};

export default Sidebar;
