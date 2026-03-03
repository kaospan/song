import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vite.dev/config/
export default defineConfig(() => {
  const basePath = process.env.VITE_BASE_PATH || '/'

  return {
    base: basePath,
    plugins: [react()],
    server: {
      proxy: {
        '/api': 'http://127.0.0.1:8000',
      },
    },
  }
})
