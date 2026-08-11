import { defineConfig } from "vite";
import react from "@vitejs/plugin-react-swc";
import path from "path";
import { componentTagger } from "lovable-tagger";

// Dev clusters publish the head on a per-worktree port (`make cluster-info`),
// so the proxy target is overridable: BURLA_HEAD_PORT=5798 npm run dev
const head = `http://localhost:${process.env.BURLA_HEAD_PORT ?? "5001"}`;

// https://vitejs.dev/config/
export default defineConfig(({ mode }) => ({
  server: {
    host: "::",
    port: 8080,
    proxy: {
      "/api": head,
      "/v1": head,
      "/v3": head,
    },
  },
  plugins: [react(), mode === "development" && componentTagger()].filter(
    Boolean,
  ),
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src"),
    },
  },
}));
