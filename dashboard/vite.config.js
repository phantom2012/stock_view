import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'
// 关键：必须导入 VTJ 插件
// import vtj from '@vtj/designer/vite'

export default defineConfig({
  plugins: [
    vue(),
    // vtj() // ✅ 现在 vtj 已经被定义了
  ],
  server: {
    port: 3000,
    proxy: {
      '/api': {
        target: 'http://127.0.0.1:8000',
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/api/, '')
      }
    }
  }
})