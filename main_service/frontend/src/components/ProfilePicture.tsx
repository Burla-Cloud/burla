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
                <div className="absolute top-full mt-2 right-0 z-50 bg-white border border-gray-200 rounded-xl shadow-lg p-4 pt-6 w-56 text-center">
                    <img
                        src={profilePicUrl}
                        alt="User profile large"
                        className="h-20 w-20 rounded-full mx-auto object-cover"
                    />
                    <p className="mt-2 font-semibold text-gray-800">Hi {firstName} !</p>
                    <p className="text-sm text-gray-600 mt-1">logged in as {userEmail}</p>
                </div>
            )}
        </div>
    );
}
