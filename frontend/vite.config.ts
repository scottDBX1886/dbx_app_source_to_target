import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  build: {
    outDir: '../static',
    assetsDir: 'assets',
    emptyOutDir: true,
    // Generate manifest for Databricks deployment
    manifest: true,
    // Optimize bundle size and loading performance
    rollupOptions: {
      output: {
        // Split vendor libraries into separate chunks
        manualChunks: {
          // React and router
          'react-vendor': ['react', 'react-dom', 'react-router-dom']
        }
      }
    },
    // Increase chunk size warning limit  
    chunkSizeWarningLimit: 600
  },
  server: {
    // Configure for development with FastAPI backend
    proxy: {
      '/api': {
        target: 'http://localhost:8000',
        changeOrigin: true,
      }
    }
  }
})
