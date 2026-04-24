import swc from 'unplugin-swc';
import { defineConfig } from 'vitest/config';
import path from 'path';

export default defineConfig({
  test: {
    globals: true,
    root: './',
    include: ['src/**/*.spec.ts'],
    alias: {
      src: path.resolve(__dirname, './src'),
    },
    testTimeout: 10000,
  },
  plugins: [
    swc.vite({
      module: { type: 'es6' },
    }),
  ],
});
