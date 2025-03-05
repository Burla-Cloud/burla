import { JobsList } from "@/components/JobsList";


const Jobs = () => {

    return (
        <div className="flex-1 flex flex-col justify-start px-12 pt-0">
            <div className="max-w-6xl mx-auto w-full">
                {/* Move Dashboard Heading Up Precisely */}
                <h1 className="text-3xl font-bold mt-[-4px] mb-4" style={{ color: "#3b5a64" }}>
                    Jobs
                </h1>

                <div className="space-y-6">
                    < JobsList />
                    <div className="text-center text-sm text-gray-500 mt-8">
                        Need help? Email me!{" "}
                        <a href="mailto:jake@burla.dev" className="text-blue-500 hover:underline">
                            jake@burla.dev
                        </a>
                    </div>
                </div>
            </div>
        </div>
    );
};

export default Jobs;
