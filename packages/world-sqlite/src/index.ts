import path from 'node:path';
import type { DatabaseSync } from 'node:sqlite';
import type { WorldWrite } from './writes.js';

export type { WorldWrite } from './writes.js';

import type { QueuePrefix, World } from '@workflow/world';
import {
  getQueueTopicPrefix,
  mintedSpecVersion,
  resolveQueueNamespace,
} from '@workflow/world';
import {
  type DirectHandler,
  instrumentObject,
  type LocalWorldConfig,
} from '@workflow/world-local';
import { createQueue, type QueueDelivery } from './queue.js';

export type { QueueDelivery } from './queue.js';
export { QueueIdempotencyConflictError } from './queue.js';

import { Db, decode } from './db.js';
import { createStorage } from './storage/index.js';
import { createStreamer, type SqliteStreamer } from './streamer.js';

export {
  compareVersions,
  MIN_SQLITE_VERSION,
  SCHEMA_VERSION,
  SqliteUnavailableError,
  SqliteVersionError,
  WORLD_TABLES,
  worldTables,
} from './db.js';
export { UnsafeEntityIdError } from './storage/common.js';
export type { AppendOptions, AppendResult } from './streamer.js';
export { StreamClosedError, StreamOperationConflictError } from './streamer.js';

export type Config = LocalWorldConfig & {
  /**
   * SQLite database file. Defaults to `$WORKFLOW_SQLITE_PATH`, else
   * `<dataDir>/workflow.sqlite`.
   */
  dbPath?: string;
  /** Override HTTP/direct delivery; headers include stable queue message identity. */
  deliver?: (delivery: QueueDelivery) => Promise<Response>;
  /** Per-delivery deadline in milliseconds. Default: 300,000. */
  deliveryTimeoutMs?: number;
  /** Maximum deliveries before a failed message is retained. Default: 48. */
  maxAttempts?: number;
  /** Retain successful idempotency keys this long. Default: 24 hours. */
  idempotencyRetentionMs?: number;
  /** Queue for waking an active run that has no recorded queue (its run was
   * created but never enqueued). Default: the workflow topic under this
   * process's queue namespace, which is wrong when each deployment uses its
   * own namespace. */
  recoveryQueueName?: (run: {
    runId: string;
    workflowName: string;
    deploymentId?: string;
  }) => string;
  /** Concurrent deliveries; defaults to WORKFLOW_LOCAL_QUEUE_CONCURRENCY or 1,000. */
  queueConcurrency?: number;
  /** Already-open host connection. The world never changes pragmas or closes it. */
  database?: DatabaseSync;
  /** Namespace for world tables and indexes. Empty by default for existing stores. */
  tablePrefix?: string;
  /** Synchronous, in-transaction hook. Throwing rolls back the write. Defer
   * external side effects until the host commits (including outer transactions).
   * Mutation notifications cover all changed SQL rows; semantic notifications
   * additionally describe events, runs, and streams. */
  onWrite?: (write: WorldWrite) => void;
};

export type SqliteWorld = World &
  SqliteStreamer & {
    registerHandler(prefix: QueuePrefix, handler: DirectHandler): void;
    clear(): Promise<void>;
    /** Absolute path of the database file. */
    readonly dbPath: string;
  };

function resolveRecoverActiveRuns(config: Partial<Config>): boolean {
  if (config.recoverActiveRuns !== undefined) return config.recoverActiveRuns;
  const raw = process.env.WORKFLOW_LOCAL_RECOVER_ACTIVE_RUNS?.toLowerCase();
  return !(raw === '0' || raw === 'false');
}

const ENTITY_TABLES = [
  'runs',
  'events',
  'steps',
  'hooks',
  'waits',
  'streams',
  'run_streams',
] as const;

export function createWorld(args?: Partial<Config>): SqliteWorld {
  const definedArgs = args
    ? Object.fromEntries(
        Object.entries(args).filter(([, value]) => value !== undefined)
      )
    : {};
  const config: Config = {
    dataDir: process.env.WORKFLOW_LOCAL_DATA_DIR || '.workflow-data',
    baseUrl: process.env.WORKFLOW_LOCAL_BASE_URL,
    ...definedArgs,
  };
  const dbPath = path.resolve(
    config.dbPath ??
      process.env.WORKFLOW_SQLITE_PATH ??
      path.join(config.dataDir, 'workflow.sqlite')
  );
  const tag = config.tag || undefined;
  const tagValue = tag ?? '';

  // Opening checks the SQLite version and creates the schema, so a store
  // this runtime can't safely share fails here, at startup.
  const db = new Db(
    config.database ?? dbPath,
    config.onWrite,
    config.tablePrefix
  );
  const queue = createQueue(db, config);
  const storage = createStorage(db, tag);
  const recoverActiveRuns = resolveRecoverActiveRuns(config);

  function clearTagged(): void {
    db.transaction(() => {
      db.run(
        'DELETE FROM queue_runs WHERE run_id IN (SELECT run_id FROM queue_messages WHERE tag = ?) OR run_id IN (SELECT run_id FROM runs WHERE tag = ?)',
        tagValue,
        tagValue
      );
      db.run('DELETE FROM queue_messages WHERE tag = ?', tagValue);
      const tokens = db.all<{ token: string }>(
        'SELECT token FROM hooks WHERE tag = ?',
        tagValue
      );
      for (const { token } of tokens) {
        db.run('DELETE FROM hook_tokens WHERE token = ?', token);
      }
      db.run(
        'DELETE FROM hook_resumes WHERE run_id IN (SELECT run_id FROM runs WHERE tag = ?)',
        tagValue
      );
      for (const table of ['chunks', 'stream_ops']) {
        db.run(
          `DELETE FROM ${table} WHERE stream_id IN (SELECT id FROM streams WHERE tag = ? AND NOT EXISTS (SELECT 1 FROM run_streams r WHERE r.stream_name = streams.name AND r.tag != ?))`,
          tagValue,
          tagValue
        );
      }
      // Stream contents have one owner, not a tag per chunk. Keep shared names
      // intact while another tag still registers them.
      db.run(
        'DELETE FROM streams WHERE tag = ? AND NOT EXISTS (SELECT 1 FROM run_streams r WHERE r.stream_name = streams.name AND r.tag != ?)',
        tagValue,
        tagValue
      );
      // Transfer ownership when the clearing tag leaves a shared stream.
      db.run(
        'UPDATE streams SET tag = (SELECT min(r.tag) FROM run_streams r WHERE r.stream_name = streams.name AND r.tag != ?) WHERE tag = ? AND EXISTS (SELECT 1 FROM run_streams r WHERE r.stream_name = streams.name AND r.tag != ?)',
        tagValue,
        tagValue,
        tagValue
      );
      for (const table of ENTITY_TABLES.filter(
        (table) => table !== 'streams'
      )) {
        db.run(`DELETE FROM ${table} WHERE tag = ?`, tagValue);
      }
      // Every claim this instance writes carries its `.<tag>` suffix.
      const suffix = `.${tagValue}`;
      db.run(
        'DELETE FROM locks WHERE length(name) > length(?) AND substr(name, -length(?)) = ?',
        suffix,
        suffix,
        suffix
      );
    });
  }

  function clearAll(): void {
    db.transaction(() => {
      for (const table of [
        'queue_messages',
        'queue_runs',
        'chunks',
        'stream_ops',
        ...ENTITY_TABLES,
        'hook_tokens',
        'hook_resumes',
        'locks',
        'snapshots',
      ]) {
        db.run(`DELETE FROM ${table}`);
      }
    });
    // Hand the freed pages back so a cleared store is small again.
    if (!config.database && !db.raw.isTransaction) db.raw.exec('VACUUM');
  }

  return {
    specVersion: mintedSpecVersion(),
    capabilities: {
      hookRetention: { active: true },
      dynamicWorkflowCode: true,
      hookResumeDedup: true,
      hookForceClaim: true,
    },
    dbPath: db.file,
    ...queue,
    ...storage,
    ...instrumentObject('world.streams', {
      ...createStreamer(db, tag),
      ...(config.streamFlushIntervalMs !== undefined && {
        streamFlushIntervalMs: config.streamFlushIntervalMs,
      }),
    }),
    async start() {
      queue.start();
      if (!recoverActiveRuns) return;
      const rows = db.all<{ data: Uint8Array; queue_name: string | null }>(
        "SELECT r.data, q.queue_name FROM runs r LEFT JOIN queue_runs q ON q.run_id = r.run_id WHERE r.tag = ? AND r.status IN ('pending', 'running') AND NOT EXISTS (SELECT 1 FROM queue_messages m WHERE m.run_id = r.run_id AND m.tag = r.tag AND m.status IN ('pending', 'failed'))",
        tagValue
      );
      for (const row of rows) {
        const run = decode(row.data);
        queue.wakeIdleRun(
          row.queue_name ??
            config.recoveryQueueName?.(run) ??
            `${getQueueTopicPrefix('workflow', resolveQueueNamespace())}${run.workflowName}`,
          { runId: run.runId },
          { deploymentId: run.deploymentId, specVersion: run.specVersion }
        );
      }
    },
    async close() {
      await queue.close();
      db.close();
    },
    async clear() {
      if (tag) clearTagged();
      else clearAll();
    },
  };
}

export default createWorld;
