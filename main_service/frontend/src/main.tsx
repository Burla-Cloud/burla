import { createRoot } from "react-dom/client";
import App from "./App.tsx";
import "./index.css";
import { registerLicense } from "@syncfusion/ej2-base";

// Register Syncfusion license key before any Syncfusion components are used
registerLicense(import.meta.env.VITE_SYNCFUSION_LICENSE_KEY as string);

createRoot(document.getElementById("root")!).render(<App />);
