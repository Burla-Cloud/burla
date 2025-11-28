import { registerLicense } from "@syncfusion/ej2-base";

registerLicense(import.meta.env.VITE_SYNCFUSION_LICENSE_KEY);

console.log("Syncfusion key from env:", import.meta.env.VITE_SYNCFUSION_LICENSE_KEY);
