import clusterImage from "@/assets/burla_logo.png";
import { Link } from "react-router-dom";

const Sidebar = () => {
    return (
        <div className="w-60 min-h-screen bg-gray-100 border-r p-4 flex flex-col">
            {/* Logo Container (Lowered a bit using `mt-2`) */}
            <div className="flex justify-left mt-5 mb-4">
                <Link to="/">
                    <img
                        src={clusterImage}
                        style={{ width: "128px", height: "auto" }}
                        className="ml-2"
                    />
                </Link>
            </div>

            {/* Horizontal Divider */}
            <hr className="border-gray-300 my-5 w-full" />

            {/* Navigation Links */}
            <nav className="space-y-1 text-lg font-medium">
                <Link
                    to="/"
                    className="flex items-center space-x-1 text-gray-700 hover:text-black hover:bg-gray-200 p-2 rounded-md"
                >
                    <span>Dashboard</span>
                </Link>
                <Link
                    to="/jobs"
                    className="flex items-center space-x-1 text-gray-700 hover:text-black hover:bg-gray-200 p-2 rounded-md"
                >
                    <span>Jobs</span>
                </Link>
                <Link
                    to="/settings"
                    className="flex items-center space-x-1 text-gray-700 hover:text-black hover:bg-gray-200 p-2 rounded-md"
                >
                    <span>Settings</span>
                </Link>
            </nav>
        </div>
    );
};

export default Sidebar;
