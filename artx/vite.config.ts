import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'
import path from 'path'

export default defineConfig({
  plugins: [react(), tailwindcss()],
  // Important: do NOT override import.meta.env here.
  // VITE_API_URL must come from Vercel/Railway env injection at build time.
  // Overriding it to "" here erases the configured value in production.

  resolve: {

    alias: {
      '@': path.resolve(__dirname, './src'),
    },
  },
  server: {
    port: 5173,
    proxy: {
      // REST API routes
      '/api': { target: 'http://localhost:8000', changeOrigin: true },
      '/foster': { target: 'http://localhost:8000', changeOrigin: true },
      '/swarm': { target: 'http://localhost:8000', changeOrigin: true },
      '/workflow': {
        target: 'http://localhost:8000',
        changeOrigin: true,
        // WebSocket upgrade for /workflow/{id}/stream
        ws: true,
      },
      '/emergent': { target: 'http://localhost:8000', changeOrigin: true },
      '/health': { target: 'http://localhost:8000', changeOrigin: true },
      '/chat': { target: 'http://localhost:8000', changeOrigin: true },
      '/events': { target: 'http://localhost:8000', changeOrigin: true },
      '/agent': { target: 'http://localhost:8000', changeOrigin: true },
      '/dashboard': { target: 'http://localhost:8000', changeOrigin: true },
      '/families': { target: 'http://localhost:8000', changeOrigin: true },
      '/metrics': { target: 'http://localhost:8000', changeOrigin: true },
      // WebSocket endpoints
      '/ws': {
        target: 'ws://localhost:8000',
        changeOrigin: true,
        ws: true,
      },
    },
  },
})
