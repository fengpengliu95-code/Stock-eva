import { resolve } from "node:path";

import { defineConfig } from "vitest/config";

export default defineConfig({
  define: {
    "process.env.NODE_ENV": JSON.stringify("production"),
  },
  build: {
    lib: {
      entry: resolve(__dirname, "src/main.ts"),
      formats: ["es"],
      fileName: () => "security-cockpit.js",
    },
    outDir: resolve(__dirname, "assets"),
    emptyOutDir: true,
    sourcemap: false,
    rollupOptions: {
      output: {
        banner:
          "/*! KLineChart v10.0.0 Copyright (c) 2019 lihu; Apache-2.0. Includes TradingView Lightweight Charts copyright notice. */",
      },
    },
  },
  test: {
    environment: "jsdom",
    include: ["src/**/*.test.ts"],
  },
});
