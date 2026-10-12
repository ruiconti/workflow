import { randomUUID } from 'node:crypto';
import os from 'node:os';
import { deserialize, serialize } from 'node:v8';
import { WorkflowWorldError } from '@workflow/errors';
import {
  createWorkflowBaseUrl,
  createWorkflowHealthEndpoint,
  createWorkflowUrl,
} from '@workflow/utils';
import { getWorkflowPort } from '@workflow/utils/get-port';
import { MessageId, type Queue, type QueuePrefix } from '@workflow/world';
import {
  createQueue as createLocalQueue,
  type DirectHandler,
} from '@workflow/world-local';
import { ulid } from 'ulid';
import type { Db } from './db.js';
import type { Config } from './index.js';

export type QueueDelivery = {
  messageId: string;
  queueName: string;
  attempt: number;
  message: unknown;
  headers: Record<string, string>;
  deploymentId?: string;
  signal: AbortSignal;
};

export class QueueIdempotencyConflictError extends WorkflowWorldError {
  constructor(key: string) {
    super(`Queue idempotency key ${key} is already used for another message`, {
      status: 409,
    });
    this.name = 'QueueIdempotencyConflictError';
  }
}

type MessageRow = {
  id: string;
  queue_name: string;
  run_id: string | null;
  step_id: string | null;
  deployment_id: string | null;
  message: Uint8Array;
  headers: string;
  attempt: number;
  max_attempts: number;
  idempotency_key: string | null;
};

export function createQueue(db: Db, config: Config) {
  const tag = config.tag ?? '';
  const owner = `${os.hostname()}:${process.pid}:${randomUUID()}`;
  const timeoutMs = config.deliveryTimeoutMs ?? 300_000;
  const concurrency =
    config.queueConcurrency ??
    (Number(process.env.WORKFLOW_LOCAL_QUEUE_CONCURRENCY) || 1000);
  const handlers = new Map<QueuePrefix, DirectHandler>();
  // Reuse only protocol parsing and deployment identity, never the local enqueue.
  const local = createLocalQueue(config);
  const inflight = new Map<string, AbortController>();
  const tasks = new Set<Promise<void>>();
  let started = false;
  let closed = false;
  let timer: ReturnType<typeof setTimeout> | undefined;
  let nextPrune = 0;

  async function deliver(d: QueueDelivery): Promise<Response> {
    if (config.deliver) return config.deliver(d);
    const handler = [...handlers].find(([prefix]) =>
      d.queueName.startsWith(prefix)
    )?.[1];
    let baseUrl = config.baseUrl ?? process.env.WORKFLOW_LOCAL_BASE_URL;
    if (!baseUrl && !handler) {
      const port =
        config.port ??
        (process.env.PORT
          ? Number(process.env.PORT)
          : await getWorkflowPort({
              endpoint: createWorkflowHealthEndpoint(),
            }));
      if (!port)
        throw new Error('Unable to resolve base URL for workflow queue.');
      baseUrl = createWorkflowBaseUrl(`http://localhost:${port}`);
    }
    const request = new Request(
      createWorkflowUrl(baseUrl ?? createWorkflowBaseUrl('http://localhost'), {
        type: 'flow',
      }),
      {
        method: 'POST',
        headers: d.headers,
        signal: d.signal,
        body: JSON.stringify(d.message, (_key, value) =>
          value instanceof Uint8Array
            ? {
                __type: 'Uint8Array',
                data: Buffer.from(value).toString('base64'),
              }
            : value
        ),
      }
    );
    return handler ? handler(request) : fetch(request);
  }

  async function dispatch(row: MessageRow, controller: AbortController) {
    const deadline = setTimeout(
      () => controller.abort(new Error('Queue delivery timed out')),
      timeoutMs
    );
    const aborted = new Promise<never>((_, reject) =>
      controller.signal.addEventListener(
        'abort',
        () => reject(controller.signal.reason),
        { once: true }
      )
    );
    const update = (sql: string, ...values: (string | number | null)[]) =>
      db.run(
        `${sql} WHERE id = ? AND lease_owner = ? AND status = 'pending'`,
        ...values,
        row.id,
        owner
      );
    try {
      const response = await Promise.race([
        deliver({
          messageId: row.id,
          queueName: row.queue_name,
          attempt: row.attempt,
          message: deserialize(row.message),
          headers: {
            ...JSON.parse(row.headers),
            'content-type': 'application/json',
            'x-vqs-queue-name': row.queue_name,
            'x-vqs-message-id': row.id,
            'x-vqs-message-attempt': String(row.attempt),
          },
          deploymentId: row.deployment_id ?? undefined,
          signal: controller.signal,
        }),
        aborted,
      ]);
      if (!response.ok)
        throw new Error(`Queue delivery returned HTTP ${response.status}`);
      // The acknowledgement is a JSON object: {} or {ok} acknowledges,
      // {timeoutSeconds: n} asks again in n seconds; anything else (an empty
      // body, invalid JSON, null, an array, a scalar) is a failed attempt.
      const text = await Promise.race([response.text(), aborted]);
      let result: unknown;
      try {
        result = JSON.parse(text);
      } catch {
        throw new Error('Queue delivery returned invalid JSON');
      }
      if (
        result === null ||
        typeof result !== 'object' ||
        Array.isArray(result)
      )
        throw new Error('Queue delivery returned a non-object acknowledgement');
      let timeoutSeconds: number | undefined;
      if ('timeoutSeconds' in result && result.timeoutSeconds !== undefined) {
        if (
          typeof result.timeoutSeconds !== 'number' ||
          !Number.isFinite(result.timeoutSeconds) ||
          result.timeoutSeconds < 0
        )
          throw new Error('Queue delivery returned invalid timeoutSeconds');
        timeoutSeconds = result.timeoutSeconds;
      }
      if (closed || !db.raw.isOpen) return;
      if (timeoutSeconds !== undefined) {
        if (row.attempt >= row.max_attempts) {
          update(
            "UPDATE queue_messages SET status = 'failed', last_error = coalesce(last_error, ?), lease_until = NULL, lease_owner = NULL",
            `exhausted after ${row.attempt} attempts`
          );
        } else {
          update(
            'UPDATE queue_messages SET due_at = ?, lease_until = NULL, lease_owner = NULL',
            Date.now() + timeoutSeconds * 1000
          );
        }
      } else if (row.idempotency_key !== null) {
        update(
          "UPDATE queue_messages SET status = 'done', completed_at = ?, lease_until = NULL, lease_owner = NULL",
          Date.now()
        );
      } else update('DELETE FROM queue_messages');
    } catch (error) {
      if (!closed && db.raw.isOpen)
        update(
          'UPDATE queue_messages SET status = ?, last_error = ?, due_at = ?, lease_until = NULL, lease_owner = NULL',
          row.attempt >= row.max_attempts ? 'failed' : 'pending',
          String(error),
          Date.now() + Math.min(60_000, 1000 * 2 ** (row.attempt - 1))
        );
    } finally {
      clearTimeout(deadline);
      inflight.delete(row.id);
      if (!closed) wake();
    }
  }

  function tick() {
    timer = undefined;
    // A borrowed connection can be closed by its host before close() runs.
    if (closed || !db.raw.isOpen) return;
    const now = Date.now();
    if (now >= nextPrune) {
      db.run(
        "DELETE FROM queue_messages WHERE tag = ? AND status = 'done' AND completed_at < ?",
        tag,
        now - (config.idempotencyRetentionMs ?? 86_400_000)
      );
      nextPrune = now + 60_000;
    }
    const candidates = db.all<{ id: string }>(
      "SELECT id FROM queue_messages WHERE tag = ? AND status = 'pending' AND due_at <= ? AND (lease_until IS NULL OR lease_until <= ?) LIMIT ?",
      tag,
      now,
      now,
      Math.max(0, concurrency - inflight.size)
    );
    for (const candidate of candidates) {
      const row = db.transaction(() => {
        db.run(
          "UPDATE queue_messages SET status = 'failed', last_error = coalesce(last_error, 'exhausted after ' || attempt || ' attempts'), lease_until = NULL, lease_owner = NULL WHERE id = ? AND status = 'pending' AND attempt >= max_attempts AND due_at <= ? AND (lease_until IS NULL OR lease_until <= ?)",
          candidate.id,
          now,
          now
        );
        return db.get<MessageRow>(
          "UPDATE queue_messages SET attempt = attempt + 1, lease_until = ?, lease_owner = ? WHERE id = ? AND status = 'pending' AND attempt < max_attempts AND due_at <= ? AND (lease_until IS NULL OR lease_until <= ?) RETURNING *",
          now + timeoutMs + 30_000,
          owner,
          candidate.id,
          now,
          now
        );
      });
      if (!row) continue;
      const controller = new AbortController();
      inflight.set(row.id, controller);
      const task = dispatch(row, controller);
      tasks.add(task);
      void task.finally(() => tasks.delete(task));
    }
    const next = db.get<{ at: number | null }>(
      "SELECT min(max(due_at, coalesce(lease_until, 0))) AS at FROM queue_messages WHERE tag = ? AND status = 'pending'",
      tag
    )?.at;
    timer = setTimeout(
      tick,
      inflight.size >= concurrency
        ? 1000
        : Math.max(1, Math.min(1000, (next ?? now + 1000) - Date.now()))
    );
    timer.unref();
  }
  function wake() {
    if (closed) return;
    if (!started) start();
    if (timer) clearTimeout(timer);
    timer = setTimeout(tick, 0);
    timer.unref();
  }
  function start() {
    if (started || closed) return;
    started = true;
    db.transaction(() => {
      for (const row of db.all<{ id: string; lease_owner: string }>(
        "SELECT id, lease_owner FROM queue_messages WHERE tag = ? AND status = 'pending' AND lease_owner IS NOT NULL",
        tag
      )) {
        const [host, pidString] = row.lease_owner.split(':');
        if (host !== os.hostname()) continue;
        const pid = Number(pidString);
        let dead = pid === process.pid && row.lease_owner !== owner;
        if (Number.isInteger(pid) && pid > 0 && pid !== process.pid) {
          try {
            process.kill(pid, 0);
          } catch (error) {
            dead = (error as NodeJS.ErrnoException).code === 'ESRCH';
          }
        }
        if (dead)
          db.run(
            'UPDATE queue_messages SET lease_until = NULL, lease_owner = NULL WHERE id = ?',
            row.id
          );
      }
    });
    wake();
  }
  // One write transaction (BEGIN IMMEDIATE, so it also excludes other
  // processes). A recovery wake inserts nothing when the run already has a
  // pending or failed message; for a run with no message at all (its start
  // was never enqueued) it takes the start's idempotency key, so it and the
  // start's own enqueue dedupe whichever lands first.
  function enqueue(
    queueName: string,
    message: unknown,
    opts: Parameters<Queue['queue']>[2],
    recovery?: { startKey?: string }
  ): string | undefined {
    const payload = message as { runId?: string; stepId?: string };
    return db.transaction(() => {
      if (recovery) {
        if (
          db.get(
            "SELECT 1 FROM queue_messages WHERE tag = ? AND run_id = ? AND status IN ('pending', 'failed') LIMIT 1",
            tag,
            payload.runId ?? null
          )
        )
          return undefined;
        if (
          recovery.startKey !== undefined &&
          !db.get(
            'SELECT 1 FROM queue_messages WHERE tag = ? AND run_id = ? LIMIT 1',
            tag,
            payload.runId ?? null
          )
        )
          opts = { ...opts, idempotencyKey: recovery.startKey };
      }
      const previous =
        opts?.idempotencyKey !== undefined
          ? db.get<MessageRow>(
              'SELECT * FROM queue_messages WHERE tag = ? AND idempotency_key = ?',
              tag,
              opts.idempotencyKey
            )
          : undefined;
      if (previous) {
        if (
          previous.queue_name !== queueName ||
          previous.run_id !== (payload.runId ?? null) ||
          previous.step_id !== (payload.stepId ?? null)
        )
          throw new QueueIdempotencyConflictError(opts!.idempotencyKey!);
        return previous.id;
      }
      const id = `msg_${ulid()}`;
      db.run(
        "INSERT INTO queue_messages (id, tag, queue_name, idempotency_key, run_id, step_id, deployment_id, message, headers, attempt, max_attempts, due_at, status, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, 'pending', ?)",
        id,
        tag,
        queueName,
        opts?.idempotencyKey ?? null,
        payload.runId ?? null,
        payload.stepId ?? null,
        opts?.deploymentId ?? null,
        serialize(message),
        JSON.stringify(opts?.headers ?? {}),
        config.maxAttempts ?? 48,
        Date.now() + (opts?.delaySeconds ?? 0) * 1000,
        Date.now()
      );
      if (payload.runId && queueName.includes('_wkf_workflow_'))
        db.run(
          'INSERT INTO queue_runs (run_id, queue_name) VALUES (?, ?) ON CONFLICT(run_id) DO UPDATE SET queue_name = excluded.queue_name',
          payload.runId,
          queueName
        );
      return id;
    });
  }
  const queue: Queue['queue'] = async (queueName, message, opts) => {
    if (closed) throw new Error('Queue is closed');
    const id = enqueue(queueName, message, opts)!;
    wake();
    return { messageId: MessageId.parse(id) };
  };
  /** Wake a run unless it already has a pending or failed message. */
  function wakeIdleRun(
    queueName: string,
    message: { runId: string },
    opts: Parameters<Queue['queue']>[2],
    startKey?: string
  ): boolean {
    if (closed) return false;
    const id = enqueue(queueName, message, opts, { startKey });
    if (id) wake();
    return id !== undefined;
  }
  return {
    queue,
    wakeIdleRun,
    createQueueHandler: local.createQueueHandler,
    getDeploymentId: local.getDeploymentId,
    registerHandler(prefix: QueuePrefix, handler: DirectHandler) {
      handlers.set(prefix, handler);
    },
    start,
    async close() {
      closed = true;
      if (timer) clearTimeout(timer);
      for (const controller of inflight.values())
        controller.abort(new Error('Queue closed'));
      await Promise.all(tasks);
      if (db.raw.isOpen)
        db.run(
          "UPDATE queue_messages SET lease_until = NULL, lease_owner = NULL WHERE lease_owner = ? AND status = 'pending'",
          owner
        );
      await local.close();
    },
  };
}
