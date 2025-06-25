import { useState, useEffect, useRef } from "react";

export default function ProfilePicture() {
    const [profilePicUrl, setProfilePicUrl] = useState<string | null>(null);
    const [userName, setUserName] = useState<string | null>(null);
    const [userEmail, setUserEmail] = useState<string | null>(null);
    const [isOpen, setIsOpen] = useState<boolean>(false);
    const containerRef = useRef<HTMLDivElement>(null);
    const firstName = userName?.split(" ")[0] || "";

    useEffect(() => {
        const fetchUserInfo = async () => {
            try {
                const response = await fetch("/api/user");
                if (response.ok) {
                    const data = await response.json();
                    setProfilePicUrl(data.profile_pic);
                    setUserName(data.name);
                    setUserEmail(data.email);
                }
            } catch (error) {
                console.error("Failed to fetch user info:", error);
            }
        };

        fetchUserInfo();
    }, []);

    useEffect(() => {
        function handleClickOutside(event: MouseEvent) {
            if (containerRef.current && !containerRef.current.contains(event.target as Node)) {
                setIsOpen(false);
            }
        }
        if (isOpen) {
            document.addEventListener("mousedown", handleClickOutside);
        }
        return () => {
            document.removeEventListener("mousedown", handleClickOutside);
        };
    }, [isOpen]);

    if (!profilePicUrl) return null;

    return (
        <div ref={containerRef} className="fixed top-6 right-6 z-50">
            <img
                src={profilePicUrl}
                alt="User profile"
                className="h-10 w-10 rounded-full border-2 border-gray-200 shadow-md object-cover cursor-pointer"
                onClick={() => setIsOpen(!isOpen)}
            />

            {isOpen && (
                <div className="absolute top-full mt-2 right-0 z-50 bg-white border border-gray-200 rounded-xl shadow-lg px-4 pt-6 pb-2 w-56 text-center">
                    <img
                        src={profilePicUrl}
                        alt="User profile large"
                        className="h-20 w-20 rounded-full mx-auto object-cover"
                    />
                    <p className="mt-2 font-semibold text-gray-800">Hi {firstName} !</p>
                    <p className="text-sm text-gray-600 mt-1">logged in as {userEmail}</p>
                    <hr className="border-t border-gray-200 mt-6 mb-2" />
                    <button className="flex items-center w-full text-left px-2 py-1 hover:bg-gray-100 rounded-md">
                        <svg
                            xmlns="http://www.w3.org/2000/svg"
                            className="h-5 w-5 text-gray-600 mr-2 stroke-current"
                            fill="none"
                            viewBox="0 0 24 24"
                            stroke="currentColor"
                        >
                            <path
                                strokeLinecap="round"
                                strokeLinejoin="round"
                                strokeWidth="2"
                                d="M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4"
                            />
                            <polyline
                                strokeLinecap="round"
                                strokeLinejoin="round"
                                strokeWidth="2"
                                points="16 17 21 12 16 7"
                            />
                            <line
                                strokeLinecap="round"
                                strokeLinejoin="round"
                                strokeWidth="2"
                                x1="21"
                                y1="12"
                                x2="9"
                                y2="12"
                            />
                        </svg>
                        <span className="text-sm text-gray-800">Log out</span>
                    </button>
                </div>
            )}
        </div>
    );
}
