import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// Dev server proxies the API to a local cluster started with scripts/cluster.sh (n1..n5 on 8081..8085).
//   /api/...          -> n1
//   /nodes/nX/api/... -> node nX directly (node-local data)
// The Docker image does the same with nginx (docker/nginx.conf).
const nodeProxies = Object.fromEntries(
  [1, 2, 3, 4, 5].map((i) => [
    `/nodes/n${i}/api`,
    { target: `http://localhost:${8080 + i}`, changeOrigin: true, rewrite: (p: string) => p.replace(`/nodes/n${i}`, "") },
  ]),
);

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      ...nodeProxies,
      "/api": { target: process.env.QUORUMDB_API ?? "http://localhost:8081", changeOrigin: true },
    },
  },
  build: { outDir: "dist", sourcemap: false, chunkSizeWarningLimit: 900 },
});
