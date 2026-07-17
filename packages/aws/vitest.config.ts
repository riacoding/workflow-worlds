import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    testTimeout: 90_000,
    hookTimeout: 180_000,
    include: ['test/**/*.test.ts'],
    setupFiles: ['test/setup.ts'],
    // Run sequentially in one fork so a single LocalStack container is shared.
    pool: 'forks',
    poolOptions: {
      forks: {
        singleFork: true,
      },
    },
  },
});
