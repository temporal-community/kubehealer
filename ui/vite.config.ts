import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// Dev: proxy the WebSocket to the gateway (run `python run_dashboard.py` on :8080).
// Prod: the gateway serves dist/ on its own origin, so /ws is same-origin.
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/ws": { target: "ws://127.0.0.1:8080", ws: true },
    },
  },
  build: { outDir: "dist", emptyOutDir: true },
});
