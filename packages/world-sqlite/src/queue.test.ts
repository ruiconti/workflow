import { mkdtempSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { Db, encode } from './db.js';
import {
  type Config,
  createWorld,
  QueueIdempotencyConflictError,
  type SqliteWorld,
} from './index.js';

const queueName = '__wkf_workflow_test';
const worlds: SqliteWorld[] = [];
const databases: Db[] = [];
afterEach(async () => {
  for (const world of worlds.splice(0)) await world.close();
  for (const db of databases.splice(0)) db.close();
});
function setup(config: Partial<Config> = {}) {
  const dbPath = path.join(
    mkdtempSync(path.join(os.tmpdir(), 'wsqlite-queue-')),
    'world.sqlite'
  );
  const world = createWorld({ dbPath, recoverActiveRuns: false, ...config });
  worlds.push(world);
  const db = new Db(dbPath);
  databases.push(db);
  return { world, db, dbPath };
}
const row = (db: Db) => db.get<any>('SELECT * FROM queue_messages');

describe('durable queue', () => {
  it('deduplicates any status, ignores attempt-specific payload, and rejects identity conflicts', async () => {
    const { world, db } = setup({ deliver: async () => new Response('{}') });
    const first = await world.queue(
      queueName,
      { runId: 'run', traceCarrier: { first: 'a' } },
      { idempotencyKey: 'key' }
    );
    await vi.waitFor(() => expect(row(db).status).toBe('done'));
    expect(
      await world.queue(
        queueName,
        { runId: 'run', traceCarrier: { second: 'b' } },
        { idempotencyKey: 'key' }
      )
    ).toEqual(first);
    await expect(
      world.queue(queueName, { runId: 'other' }, { idempotencyKey: 'key' })
    ).rejects.toBeInstanceOf(QueueIdempotencyConflictError);
    await expect(
      world.queue(`${queueName}2`, { runId: 'run' }, { idempotencyKey: 'key' })
    ).rejects.toMatchObject({ status: 409 });
    expect(db.all('SELECT * FROM queue_messages')).toHaveLength(1);
  });

  it('honors delay and delivers stable headers before ack', async () => {
    const deliver = vi.fn(async () => new Response('{}'));
    const { world, db } = setup({ deliver });
    const before = Date.now();
    const { messageId } = await world.queue(
      queueName,
      { runId: 'run' },
      {
        delaySeconds: 0.1,
        idempotencyKey: 'key',
        headers: { custom: 'header' },
      }
    );
    expect(row(db).due_at).toBeGreaterThanOrEqual(before + 100);
    expect(deliver).not.toHaveBeenCalled();
    await vi.waitFor(() => expect(row(db).status).toBe('done'));
    expect(deliver.mock.calls[0]?.[0]).toMatchObject({
      messageId,
      attempt: 1,
      headers: {
        custom: 'header',
        'x-vqs-message-id': messageId,
        'x-vqs-message-attempt': '1',
      },
    });
  });

  it('backs off failures and keeps exhausted jobs', async () => {
    const { world, db } = setup({
      maxAttempts: 2,
      deliver: async () => new Response('bad', { status: 500 }),
    });
    await world.queue(queueName, { runId: 'run' });
    await vi.waitFor(() => expect(row(db)?.last_error).toContain('500'));
    expect(row(db)).toMatchObject({
      attempt: 1,
      status: 'pending',
      lease_until: null,
    });
    expect(row(db).due_at).toBeGreaterThan(Date.now() + 500);
    db.run('UPDATE queue_messages SET due_at = 0');
    await vi.waitFor(() => expect(row(db).status).toBe('failed'), {
      timeout: 2000,
    });
    expect(row(db).attempt).toBe(2);
  });

  it('reschedules timeoutSeconds with the same message id', async () => {
    const deliver = vi.fn(async () => new Response('{"timeoutSeconds":0.1}'));
    const { world, db } = setup({ deliver });
    const { messageId } = await world.queue(queueName, { runId: 'run' });
    await vi.waitFor(() => expect(deliver).toHaveBeenCalledTimes(2));
    expect(row(db)).toMatchObject({
      id: messageId,
      status: 'pending',
      attempt: 2,
      lease_until: null,
    });
  });

  it('reclaims dead-pid leases immediately with the next attempt', async () => {
    const { world, db, dbPath } = setup();
    const { messageId } = await world.queue(
      queueName,
      { runId: 'run' },
      { delaySeconds: 60 }
    );
    await world.close();
    worlds.splice(worlds.indexOf(world), 1);
    db.run(
      'UPDATE queue_messages SET attempt = 3, due_at = 0, lease_until = ?, lease_owner = ?',
      Date.now() + 600_000,
      `${os.hostname()}:2147483647:dead`
    );
    const deliver = vi.fn(async () => new Response('{}'));
    const restarted = createWorld({
      dbPath,
      recoverActiveRuns: false,
      deliver,
    });
    worlds.push(restarted);
    await restarted.start();
    await vi.waitFor(() => expect(deliver).toHaveBeenCalled());
    expect(deliver.mock.calls[0]?.[0]).toMatchObject({ messageId, attempt: 4 });
  });

  it('aborts mid-delivery close without failure and redelivers on reopen', async () => {
    const deliver = vi.fn(() => new Promise<Response>(() => {}));
    const { world, db, dbPath } = setup({ deliver });
    const { messageId } = await world.queue(queueName, { runId: 'run' });
    await vi.waitFor(() => expect(deliver).toHaveBeenCalled());
    await world.close();
    worlds.splice(worlds.indexOf(world), 1);
    expect(row(db)).toMatchObject({
      attempt: 1,
      last_error: null,
      lease_until: null,
    });
    const redeliver = vi.fn(async () => new Response('{}'));
    const restarted = createWorld({
      dbPath,
      deliver: redeliver,
      recoverActiveRuns: false,
    });
    worlds.push(restarted);
    await restarted.start();
    await vi.waitFor(() => expect(redeliver).toHaveBeenCalled());
    expect(redeliver.mock.calls[0]?.[0]).toMatchObject({
      messageId,
      attempt: 2,
    });
  });

  it('recovers only own active runs without pending messages, preserving deployment and saved queue', async () => {
    const { world, db, dbPath } = setup({ tag: 'own' });
    await world.queue(
      '__agent_wkf_workflow_test',
      { runId: 'saved' },
      { delaySeconds: 60 }
    );
    db.run('DELETE FROM queue_messages');
    for (const [runId, tag] of [
      ['saved', 'own'],
      ['waiting', 'own'],
      ['foreign', 'other'],
    ]) {
      const run = {
        runId,
        workflowName: 'test',
        deploymentId: 'deployment',
        specVersion: 2,
      };
      db.run(
        "INSERT INTO runs (run_id, tag, status, workflow_name, created_at, data) VALUES (?, ?, 'pending', 'test', ?, ?)",
        runId!,
        tag!,
        Date.now(),
        encode(run)
      );
    }
    await world.queue(queueName, { runId: 'waiting' }, { delaySeconds: 60 });
    await world.close();
    worlds.splice(worlds.indexOf(world), 1);
    const restarted = createWorld({
      dbPath,
      tag: 'own',
      deliver: () => new Promise<Response>(() => {}),
    });
    worlds.push(restarted);
    await restarted.start();
    const messages = db.all<any>(
      'SELECT * FROM queue_messages ORDER BY run_id'
    );
    expect(messages).toHaveLength(2);
    expect(messages[0]).toMatchObject({
      run_id: 'saved',
      queue_name: '__agent_wkf_workflow_test',
      deployment_id: 'deployment',
    });
    expect(messages[1]).toMatchObject({ run_id: 'waiting', attempt: 0 });
  });
  it('wakes a run with no recorded queue on the configured recovery queue', async () => {
    const { world, db, dbPath } = setup();
    const run = {
      runId: 'unqueued',
      workflowName: 'test',
      deploymentId: 'local-bot',
      specVersion: 2,
    };
    db.run(
      "INSERT INTO runs (run_id, tag, status, workflow_name, created_at, data) VALUES (?, '', 'running', 'test', ?, ?)",
      run.runId,
      Date.now(),
      encode(run)
    );
    await world.close();
    worlds.splice(worlds.indexOf(world), 1);
    const restarted = createWorld({
      dbPath,
      deliver: () => new Promise<Response>(() => {}),
      recoveryQueueName: (r) =>
        `__ns${r.deploymentId!.replace(/[^a-z]/g, '')}_wkf_workflow_${r.workflowName}`,
    });
    worlds.push(restarted);
    await restarted.start();
    expect(
      db.all<any>(
        'SELECT run_id, queue_name, deployment_id FROM queue_messages'
      )
    ).toEqual([
      {
        run_id: 'unqueued',
        queue_name: '__nslocalbot_wkf_workflow_test',
        deployment_id: 'local-bot',
      },
    ]);
  });

  it('times out a transport that ignores abort and retains the failed record', async () => {
    const { world, db } = setup({
      deliveryTimeoutMs: 20,
      maxAttempts: 1,
      deliver: () => new Promise<Response>(() => {}),
    });
    await world.queue(queueName, { runId: 'run' });
    await vi.waitFor(() => expect(row(db)?.status).toBe('failed'));
    expect(row(db)).toMatchObject({ attempt: 1, lease_until: null });
    expect(row(db).last_error).toContain('timed out');
  });

  it('isolates tags and prefixed tables, prunes retained completions, and clears queue tables', async () => {
    const { world, dbPath } = setup({
      tablePrefix: 'host_',
      tag: 'one',
      deliver: async () => new Response('{}'),
      idempotencyRetentionMs: 0,
    });
    const db = new Db(dbPath, undefined, 'host_');
    databases.push(db);
    const other = createWorld({
      dbPath,
      tablePrefix: 'host_',
      tag: 'two',
      recoverActiveRuns: false,
    });
    worlds.push(other);
    const first = await world.queue(
      queueName,
      { runId: 'one' },
      { idempotencyKey: 'key' }
    );
    const second = await other.queue(
      queueName,
      { runId: 'two' },
      { idempotencyKey: 'key', delaySeconds: 60 }
    );
    expect(first.messageId).not.toBe(second.messageId);
    await vi.waitFor(() =>
      expect(
        db.get<any>("SELECT * FROM queue_messages WHERE tag = 'one'").status
      ).toBe('done')
    );
    await world.close();
    worlds.splice(worlds.indexOf(world), 1);
    db.run("UPDATE queue_messages SET completed_at = 0 WHERE tag = 'one'");
    const restarted = createWorld({
      dbPath,
      tablePrefix: 'host_',
      tag: 'one',
      recoverActiveRuns: false,
      idempotencyRetentionMs: 0,
    });
    worlds.push(restarted);
    await restarted.start();
    await vi.waitFor(() =>
      expect(
        db.all("SELECT * FROM queue_messages WHERE tag = 'one'")
      ).toHaveLength(0)
    );
    await other.clear();
    expect(db.all('SELECT * FROM queue_messages')).toHaveLength(0);
    expect(
      db.all("SELECT * FROM queue_runs WHERE run_id = 'two'")
    ).toHaveLength(0);
  });
});
