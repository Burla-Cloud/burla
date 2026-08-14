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
                <div className="flex min-h-screen flex-col items-center justify-center bg-background px-6 text-center text-foreground">
                    <BrandLockup />
                    <h2 className="mt-6 text-base font-semibold">Something went wrong</h2>
                    <p className="mt-1 text-sm text-muted-foreground">
                        Try refreshing the page. If that doesn't work, email{" "}
                        <a href="mailto:jake@burla.dev" className="text-primary hover:underline">
                            jake@burla.dev
                        </a>
                        .
                    </p>
                </div>
            );
        } else {
            return this.props.children;
        }
    }
}
export default ErrorBoundary;
