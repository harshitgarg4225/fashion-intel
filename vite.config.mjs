import { defineConfig, loadEnv } from "vite";
import react from "@vitejs/plugin-react";
import { authApi } from "./scripts/auth-api.mjs";
import { wardrobeImportApi } from "./scripts/import-job-api.mjs";
import { outfitStudioApi } from "./scripts/outfit-studio-api.mjs";
import { googlePhotosApi } from "./scripts/google-photos-api.mjs";
import { responsiveImageApi } from "./scripts/responsive-image-api.mjs";

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), "");
  const onRailway = Boolean(env.RAILWAY_ENVIRONMENT || env.RAILWAY_PROJECT_ID);
  // Loopback by default; Railway (or WARDROBE_HOST) binds publicly.
  const host = env.WARDROBE_HOST || (onRailway ? "0.0.0.0" : "127.0.0.1");
  const port = Number(env.PORT) || 4173;
  // Behind Railway's proxy the Host header is the public domain.
  const allowedHosts = onRailway || env.WARDROBE_ALLOW_ALL_HOSTS === "true" ? true : ["localhost"];
  const bridge = {};
  return {
    optimizeDeps: {
      include: ["react", "react-dom/client"],
    },
    server: {
      host,
      allowedHosts: allowedHosts === true ? true : ["terminal.local"],
      warmup: {
        clientFiles: ["./src/main.jsx"],
      },
    },
    preview: {
      host,
      port,
      allowedHosts,
    },
    plugins: [
      react(),
      authApi({ env }),
      responsiveImageApi(),
      wardrobeImportApi({ env, bridge }),
      outfitStudioApi({ env }),
      googlePhotosApi({ env, bridge }),
    ],
  };
});
