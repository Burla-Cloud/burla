import { registerLicense } from "@syncfusion/ej2-base";

declare global {
    interface Window {
        __SYNCFUSION_LICENSE_KEY__?: string;
    }
}

// Served deployments inject the key into index.html at request time (see the
// dashboard route in main_service/__init__.py) so it never lives in this public
// repo. The Vite env var is the fallback for standalone `npm run dev`.
const licenseKey =
    window.__SYNCFUSION_LICENSE_KEY__ || import.meta.env.VITE_SYNCFUSION_LICENSE_KEY;

if (licenseKey) {
    registerLicense(licenseKey);
}
