import { defineConfig, loadEnv } from "vite";
import react from "@vitejs/plugin-react";
import { wardrobeImportApi } from "./scripts/import-job-api.mjs";
import { outfitStudioApi } from "./scripts/outfit-studio-api.mjs";
import { googlePhotosApi } from "./scripts/google-photos-api.mjs";
import { responsiveImageApi } from "./scripts/responsive-image-api.mjs";

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), "");
  // Bind to loopback by default; set WARDROBE_HOST=0.0.0.0 to expose on your LAN.
  const host = env.WARDROBE_HOST || "127.0.0.1";
  const bridge = {};
  return {
    optimizeDeps: {
      include: ["react", "react-dom/client"],
    },
    server: {
      host,
      allowedHosts: ["terminal.local"],
      warmup: {
        clientFiles: ["./src/main.jsx"],
      },
    },
    preview: {
      host,
      port: 4173,
      allowedHosts: ["localhost"],
    },
    plugins: [
      react(),
      responsiveImageApi(),
      wardrobeImportApi({ env, bridge }),
      outfitStudioApi({ env }),
      googlePhotosApi({ env, bridge }),
    ],
  };
});
