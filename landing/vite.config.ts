import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// base is "/burla/" for GitHub project Pages, "/" for local dev.
// https://vite.dev/config/
export default defineConfig({
  base: process.env.VITE_BASE ?? "/",
  plugins: [react()],
})
