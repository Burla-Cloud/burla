import { Component, ErrorInfo, ReactNode } from "react";
import { BrandLockup } from "@/components/BrandLockup";

interface Props {
    children: ReactNode;
}

interface State {
    hasError: boolean;
}

class ErrorBoundary extends Component<Props, State> {
    public state: State = {
        hasError: false,
    };

    public componentDidCatch(error: Error, errorInfo: ErrorInfo) {
        this.setState({ hasError: true });
        console.error("React Error:", error);
        console.error("Error Info:", errorInfo);
    }

    public render() {
        if (this.state.hasError) {
            return (
                <div className="min-h-screen bg-background text-foreground pt-10 text-center">
                    <div className="mb-5 flex justify-center">
                        <BrandLockup />
                    </div>
                    <h2 className="text-lg font-semibold">Oops! Something went wrong.</h2>
                    <p>Please try refreshing the page.</p>
                    <br />
                    <p>If that dosen't work please email me!</p>
                    <p>
                        (&nbsp;{" "}
                        <a href="mailto:jake@burla.dev" className="text-primary hover:underline">
                            jake@burla.dev
                        </a>
                        &nbsp;)
                    </p>
                </div>
            );
        } else {
            return this.props.children;
        }
    }
}
export default ErrorBoundary;
