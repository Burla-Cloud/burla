import { Component, ErrorInfo, ReactNode } from "react";
import clusterImage from "@/assets/logo.svg";

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
                <div style={{ padding: "20px", textAlign: "center" }}>
                    <br />
                    <br />
                    <img
                        src={clusterImage}
                        style={{
                            width: "120px",
                            height: "auto",
                            margin: "0 auto 20px",
                        }}
                    />
                    <h2>Oops! Something went wrong.</h2>
                    <p>Please try refreshing the page.</p>
                    <br />
                    <p>If that dosen't work please email me!</p>
                    <p>
                        (&nbsp;{" "}
                        <a href="mailto:jake@burla.dev" className="text-blue-500 hover:underline">
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
