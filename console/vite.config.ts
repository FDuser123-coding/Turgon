import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// The build lands in pkg/console/dist, which the Go binary embeds.
// public/robots.txt is copied there and committed, so the directory exists
// (and the Go build works) before the app is built.
export default defineConfig({
  plugins: [react()],
  build: { outDir: "../pkg/console/dist", emptyOutDir: true },
  server: { proxy: { "/api": "http://127.0.0.1:8080" } },
  test: { environment: "node" },
});
