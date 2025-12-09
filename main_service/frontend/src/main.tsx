import "./syncfusion-license";
import { createRoot } from "react-dom/client";
import App from "./App.tsx";
import "./index.css";
import { registerLicense, Observer } from "@syncfusion/ej2-base";

type SyncfusionObserverPrototype = {
    notExist(prop: string): boolean;
    boundedEvents?: Record<string, unknown[]> | undefined;
};

// Prevent Syncfusion observers from crashing after FileManager teardown leaves bounded events undefined.
const observerPrototype = Observer.prototype as unknown as SyncfusionObserverPrototype;
const originalNotExist =
    typeof observerPrototype.notExist === "function"
        ? (observerPrototype.notExist as unknown as (
              this: SyncfusionObserverPrototype,
              prop: string
          ) => boolean)
        : null;

if (originalNotExist && !Reflect.has(observerPrototype, "__burlaPatchedNotExist")) {
    Reflect.set(observerPrototype, "__burlaPatchedNotExist", true);
    observerPrototype.notExist = function (this: SyncfusionObserverPrototype, prop: string) {
        if (!this.boundedEvents) {
            return true;
        }
        return originalNotExist.call(this, prop);
    };
}

// Register Syncfusion license key before any Syncfusion components are used
registerLicense(import.meta.env.VITE_SYNCFUSION_LICENSE_KEY as string);

createRoot(document.getElementById("root")!).render(<App />);
