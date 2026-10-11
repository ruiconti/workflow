import { createHash } from 'node:crypto';
import { EventEmitter } from 'node:events';
import type { Streamer, GetChunksOptions } from '@workflow/world';
import type { Db } from './db.js';
import { assertSafeEntityId } from './storage/common.js';
export const STREAM_POLL_INTERVAL_MS = 100;
export class StreamClosedError extends Error {
  constructor(name: string) {
    super(`Stream ${name} is closed`);
    this.name = 'StreamClosedError';
  }
}
export class StreamOperationConflictError extends Error {
  constructor(id: string) {
    super(`Stream operation ${id} has a different payload`);
    this.name = 'StreamOperationConflictError';
  }
}
export interface AppendOptions {
  operationId?: string;
  close?: boolean;
}
export interface AppendResult {
  from: number;
  to: number;
  duplicate: boolean;
  closed: boolean;
}
export type SqliteStreamer = {
  streams: Streamer['streams'] & {
    appendSync(
      runId: string,
      name: string,
      chunks: (string | Uint8Array)[],
      opts?: AppendOptions
    ): AppendResult;
    /** Host transactions must call wake only after committing. */
    wake(name: string): void;
  };
};
const emitters = new WeakMap<Db, EventEmitter>();
const queues = new WeakMap<Db, Map<string, Promise<void>>>();
interface StreamRow {
  id: number;
  next_idx: number;
  closed: number;
}
function toBytes(chunk: string | Uint8Array): Uint8Array {
  return typeof chunk === 'string'
    ? new TextEncoder().encode(chunk)
    : Uint8Array.from(chunk);
}
function runStreamNames(db: Db, runId: string, tag: string): string[] {
  return db
    .all<{ stream_name: string }>(
      `SELECT stream_name FROM run_streams
       WHERE run_id = ? AND tag = (
         SELECT tag FROM run_streams WHERE run_id = ? AND tag IN (?, '')
         ORDER BY tag = '' LIMIT 1
       )
       ORDER BY position, stream_name`,
      runId,
      runId,
      tag
    )
    .map((row) => row.stream_name);
}

export function purgeRunStreamData(db: Db, runId: string, tag: string): void {
  for (const name of runStreamNames(db, runId, tag)) {
    db.run(
      'DELETE FROM chunks WHERE stream_id = (SELECT id FROM streams WHERE name = ?)',
      name
    );
    db.run('UPDATE streams SET closed = 2 WHERE name = ?', name);
  }
}
export function createStreamer(db: Db, tag?: string): SqliteStreamer {
  const tagValue = tag ?? '';
  let emitter = emitters.get(db);
  if (!emitter) {
    emitter = new EventEmitter();
    emitter.setMaxListeners(0);
    emitters.set(db, emitter);
  }
  const wakes = emitter;
  let pending = queues.get(db);
  if (!pending) {
    pending = new Map();
    queues.set(db, pending);
  }
  const order = pending;
  function registerStreamForRun(runId: string, streamName: string): void {
    assertSafeEntityId('runId', runId);
    assertSafeEntityId('streamName', streamName);

    // Copy-on-write, as world-local writes its tagged list: a tagged world's
    // first registration copies the untagged list it was reading, then
    // appends. The untagged list is never modified by a tagged world.
    db.transaction(() => {
      const ownRows = db.get<{ n: number }>(
        'SELECT count(*) AS n FROM run_streams WHERE run_id = ? AND tag = ?',
        runId,
        tagValue
      )!.n;
      if (tagValue !== '' && Number(ownRows) === 0) {
        db.run(
          `INSERT INTO run_streams (run_id, stream_name, tag, position)
           SELECT run_id, stream_name, ?, position FROM run_streams
           WHERE run_id = ? AND tag = ''`,
          tagValue,
          runId
        );
      }
      db.run(
        `INSERT INTO run_streams (run_id, stream_name, tag, position)
         VALUES (?, ?, ?, (
           SELECT coalesce(max(position) + 1, 0) FROM run_streams
           WHERE run_id = ? AND tag = ?
         ))
         ON CONFLICT (run_id, stream_name, tag) DO NOTHING`,
        runId,
        streamName,
        tagValue,
        runId,
        tagValue
      );
    });
  }

  function info(name: string): StreamRow | undefined {
    assertSafeEntityId('streamName', name);
    return db.get<StreamRow>(
      'SELECT id, next_idx, closed FROM streams WHERE name = ?',
      name
    );
  }
  function appendSync(
    runId: string,
    name: string,
    chunks: (string | Uint8Array)[],
    opts: AppendOptions = {}
  ): AppendResult {
    assertSafeEntityId('runId', runId);
    assertSafeEntityId('streamName', name);
    const payloads = chunks.map(toBytes);
    const hash = createHash('sha256');
    for (const data of payloads) {
      const length = Buffer.alloc(8);
      length.writeBigUInt64BE(BigInt(data.length));
      hash.update(length);
      hash.update(data);
    }
    hash.update(Buffer.from([opts.close ? 1 : 0]));
    const digest = hash.digest('hex');
    return db.transaction(() => {
      db.run(
        'INSERT INTO streams (name, run_id, tag) VALUES (?, ?, ?) ON CONFLICT(name) DO NOTHING',
        name,
        runId,
        tagValue
      );
      const row = info(name)!;
      if (opts.operationId !== undefined) {
        const op = db.get<{ from_idx: number; count: number; digest: string }>(
          'SELECT from_idx, count, digest FROM stream_ops WHERE stream_id = ? AND op_id = ?',
          row.id,
          opts.operationId
        );
        if (op) {
          if (op.digest !== digest)
            throw new StreamOperationConflictError(opts.operationId);
          return {
            from: op.from_idx,
            to: op.from_idx + op.count - 1,
            duplicate: true,
            closed: row.closed !== 0,
          };
        }
      }
      const updated = db.get<{ next_idx: number }>(
        'UPDATE streams SET next_idx = next_idx + ?, closed = CASE WHEN ? THEN 1 ELSE closed END WHERE id = ? AND closed = 0 RETURNING next_idx',
        payloads.length,
        opts.close ? 1 : 0,
        row.id
      );
      if (!updated) throw new StreamClosedError(name);
      if (
        !db.get(
          'SELECT 1 FROM run_streams WHERE run_id = ? AND stream_name = ? AND tag = ?',
          runId,
          name,
          tagValue
        )
      )
        registerStreamForRun(runId, name);
      const from = updated.next_idx - payloads.length;
      payloads.forEach((data, i) =>
        db.run(
          'INSERT INTO chunks (stream_id, idx, data) VALUES (?, ?, ?)',
          row.id,
          from + i,
          data
        )
      );
      if (opts.operationId !== undefined)
        db.run(
          'INSERT INTO stream_ops (stream_id, op_id, from_idx, count, digest) VALUES (?, ?, ?, ?, ?)',
          row.id,
          opts.operationId,
          from,
          payloads.length,
          digest
        );
      db.notifyWrite({
        kind: 'stream',
        runId,
        name,
        chunks: payloads.map((data, i) => ({
          index: from + i,
          chunkId: `${row.id}:${from + i}`,
          data,
        })),
        ...(opts.close ? { closed: true } : {}),
      });
      return {
        from,
        to: updated.next_idx - 1,
        duplicate: false,
        closed: !!opts.close,
      };
    });
  }
  function wake(name: string): void {
    wakes.emit(name);
  }
  // Reserve call order before awaiting run ids. Host-owned transactions do not wake until the host commits.
  function enqueue(
    runId: string | Promise<string>,
    name: string,
    chunks: (string | Uint8Array)[],
    close = false
  ): Promise<void> {
    const resolved = Promise.resolve(runId);
    const task = (order.get(name) ?? Promise.resolve()).then(async () => {
      const id = await resolved;
      if (close && info(name)?.closed) return;
      appendSync(id, name, chunks, { close });
      if (!db.raw.isTransaction) wake(name);
    });
    const settled = task.catch(() => {});
    order.set(name, settled);
    void settled.then(() => {
      if (order.get(name) === settled) order.delete(name);
    });
    return task;
  }
  return {
    streams: {
      appendSync,
      wake,
      write(runId, name, chunk) {
        return enqueue(runId, name, [chunk]);
      },
      writeMulti(runId, name, chunks) {
        return chunks.length ? enqueue(runId, name, chunks) : Promise.resolve();
      },
      close(runId, name) {
        return enqueue(runId, name, [], true);
      },
      async list(runId) {
        assertSafeEntityId('runId', runId);
        return runStreamNames(db, runId, tagValue);
      },
      async getInfo(_runId, name) {
        const row = info(name);
        return { tailIndex: (row?.next_idx ?? 0) - 1, done: !!row?.closed };
      },
      async getChunks(
        _runId: string,
        name: string,
        options?: GetChunksOptions
      ) {
        let start = 0;
        try {
          if (options?.cursor) {
            const i = JSON.parse(
              Buffer.from(options.cursor, 'base64').toString()
            ).i;
            if (Number.isSafeInteger(i) && i >= 0) start = i;
          }
        } catch {}
        const row = info(name);
        const limit = Math.max(0, Math.floor(options?.limit ?? 100));
        const rows = row
          ? db.all<{ idx: number; data: Uint8Array }>(
              'SELECT idx, data FROM chunks WHERE stream_id = ? AND idx >= ? ORDER BY idx LIMIT ?',
              row.id,
              start,
              limit + 1
            )
          : [];
        const hasMore = rows.length > limit;
        const data = rows
          .slice(0, limit)
          .map((r) => ({ index: r.idx, data: Uint8Array.from(r.data) }));
        return {
          data,
          hasMore,
          done: !!row?.closed && !hasMore,
          cursor: hasMore
            ? Buffer.from(
                JSON.stringify({
                  i: data.length ? data[data.length - 1].index + 1 : start,
                })
              ).toString('base64')
            : null,
        };
      },
      async get(_runId, name, startIndex = 0) {
        const initial = info(name);
        let cursor =
          startIndex < 0
            ? Math.max(0, (initial?.next_idx ?? 0) + startIndex)
            : startIndex;
        let stopped = false;
        let teardown = () => {};
        return new ReadableStream<Uint8Array>({
          start(controller) {
            let lastVersion = db.dataVersion();
            const drain = () => {
              if (stopped || !db.isOpen) return;
              const row = info(name);
              if (!row) return;
              for (;;) {
                const page = db.all<{ idx: number; data: Uint8Array }>(
                  'SELECT idx, data FROM chunks WHERE stream_id = ? AND idx >= ? ORDER BY idx LIMIT ?',
                  row.id,
                  cursor,
                  100
                );
                for (const chunk of page) {
                  cursor = chunk.idx + 1;
                  if (chunk.data.byteLength)
                    controller.enqueue(Uint8Array.from(chunk.data));
                }
                if (page.length < 100) break;
              }
              if (row.closed && (row.closed === 2 || cursor >= row.next_idx)) {
                stopped = true;
                teardown();
                controller.close();
              }
            };
            wakes.on(name, drain);
            const timer = setInterval(() => {
              if (!db.isOpen) {
                teardown();
                return;
              }
              const version = db.dataVersion();
              if (version !== lastVersion) {
                lastVersion = version;
                drain();
              }
            }, STREAM_POLL_INTERVAL_MS);
            teardown = () => {
              wakes.off(name, drain);
              clearInterval(timer);
            };
            drain();
          },
          cancel() {
            stopped = true;
            teardown();
          },
        });
      },
    },
  };
}
