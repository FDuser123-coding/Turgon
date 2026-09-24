import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// Normally the build lands in pkg/console/dist, which the Go binary embeds.
// On Vercel it lands in dist and runs in demo mode with sample data, since
// the Porter API lives inside the customer's environment.
// public/robots.txt is copied there and committed, so the directory exists
// (and the Go build works) before the app is built.
const vercel = !!process.env.VERCEL;
const demo = vercel || process.env.PORTER_CONSOLE_DEMO === "1";

export default defineConfig({
  plugins: [react()],
  define: { __PORTER_DEMO__: JSON.stringify(demo) },
  build: { outDir: vercel ? "dist" : "../pkg/console/dist", emptyOutDir: true },
  server: { proxy: { "/api": "http://127.0.0.1:8080" } },
  test: { environment: "node" },
});
