import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig({
  plugins: [react()],
  clearScreen: false,
  server: {
    port: 5173,
    strictPort: true,
    // vite 不监视 Rust 构建产物（tauri dev 自管 Rust 重编译）；
    // 否则并发 cargo build 锁 DLL 时 chokidar 撞 EBUSY 崩掉整个 dev 进程
    watch: { ignored: ['**/target/**'] },
  },
  build: { outDir: 'dist' },
});
