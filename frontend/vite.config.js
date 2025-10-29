import { defineConfig, loadEnv } from 'vite';
import react from '@vitejs/plugin-react';
import path from 'path';

export default defineConfig(({ mode }) => {
  // ⚠️ Load ALL env vars (not just those starting with VITE_)
  const env = loadEnv(mode, path.resolve(__dirname, '../backend'), '');

  console.log('Loaded PANEL_USER =', env.PANEL_USER); // Debug log

  return {
    plugins: [react()],
    server: {
      proxy: {
        '/api': 'http://localhost:8000',
        '/get_': 'http://localhost:8000',
      },
    },
    define: {
      // ⚠️ Inject PANEL_USER as a frontend variable
      'import.meta.env.VITE_PANEL_USER': JSON.stringify(env.PANEL_USER || ''),
    },
  };
});
