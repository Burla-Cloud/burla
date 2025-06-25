import { useState, useEffect } from "react";

export default function ProfilePicture() {
    const [profilePicUrl, setProfilePicUrl] = useState<string | null>(null);

    useEffect(() => {
        const fetchUserInfo = async () => {
            try {
                const response = await fetch("/api/user");
                if (response.ok) {
                    const data = await response.json();
                    setProfilePicUrl(data.profile_pic);
                }
            } catch (error) {
                console.error("Failed to fetch user info:", error);
            }
        };

        fetchUserInfo();
    }, []);

    if (!profilePicUrl) return null;

    return (
        <div className="fixed top-6 right-6 z-50">
            <img
                src={profilePicUrl}
                alt="User profile"
                className="h-10 w-10 rounded-full border-2 border-gray-200 shadow-md object-cover"
            />
        </div>
    );
}
