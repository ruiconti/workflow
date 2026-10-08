import { createWorld as createSqliteWorld, type Config } from '../index.js';

export function createWorld(config?: Partial<Config>) {
  return createSqliteWorld({
    tablePrefix: process.env.WORLD_SQLITE_TEST_TABLE_PREFIX,
    ...config,
  });
}
