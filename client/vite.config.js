import { defineConfig } from 'vite';
import vue from '@vitejs/plugin-vue';
import path from 'path';

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [vue()],
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './src'),
    },
  },
  server: {
    port: 3100,
    host: '0.0.0.0',
    strictPort: true,
    cors: true,
    // 如果后端API在不同端口，可以添加代理配置
    proxy: {
      '/api': {
        target: 'http://localhost:3000',
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/api/, '')
      }
    }
  },
  // 确保处理路由刷新问题
  build: {
    outDir: 'dist',
    assetsDir: 'assets',
  },
  base: '/',
}); 