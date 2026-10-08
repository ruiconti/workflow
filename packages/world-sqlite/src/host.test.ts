import { mkdtempSync, realpathSync, rmSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { mintedSpecVersion } from '@workflow/world';
import { expect, it } from 'vitest';
import { createWorld, WORLD_TABLES, type WorldWrite } from './index.js';

const request = () => ({
  eventType: 'run_created' as const,
  specVersion: mintedSpecVersion(),
  eventData: {
    deploymentId: 'deployment',
    workflowName: 'workflow',
    input: [],
  },
});

it('preserves host pragmas, filename and connection ownership', async () => {
  const dir = mkdtempSync(path.join(os.tmpdir(), 'sqlite-host-'));
  const file = path.join(dir, 'host.sqlite');
  const database = new DatabaseSync(file);
  database.exec(
    'PRAGMA auto_vacuum=FULL; PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA locking_mode=EXCLUSIVE; PRAGMA foreign_keys=ON'
  );
  const pragmas = [
    'auto_vacuum',
    'journal_mode',
    'synchronous',
    'locking_mode',
    'foreign_keys',
  ];
  const before = pragmas.map((p) => database.prepare(`PRAGMA ${p}`).get());
  const world = createWorld({ database, dataDir: dir });
  try {
    expect(world.dbPath).toBe(realpathSync(file));
    expect(pragmas.map((p) => database.prepare(`PRAGMA ${p}`).get())).toEqual(
      before
    );
    const tables = database
      .prepare("SELECT name FROM sqlite_master WHERE type='table'")
      .all()
      .map((r) => r.name);
    expect(tables.sort()).toEqual([...WORLD_TABLES].sort());
    await world.close();
    expect(database.prepare('SELECT 1 AS n').get()?.n).toBe(1);
  } finally {
    database.close();
    rmSync(dir, { recursive: true, force: true });
  }
});

it('notifies events, runs and ordered stream chunks in the transaction', async () => {
  const database = new DatabaseSync(':memory:');
  const writes: WorldWrite[] = [];
  const world = createWorld({
    database,
    onWrite(write) {
      expect(database.isTransaction).toBe(true);
      writes.push(write);
      if (write.kind === 'event')
        expect(
          database.prepare('SELECT count(*) AS n FROM events').get()?.n
        ).toBe(1);
    },
  });
  try {
    const result = await world.events.create(null, request());
    const runId = result.run!.runId;
    await world.streams.write(runId, 'host-stream', 'a');
    await world.streams.writeMulti(runId, 'host-stream', ['b', 'c']);
    await world.streams.close(runId, 'host-stream');
    expect(
      writes.some(
        (w) => w.kind === 'event' && w.event.eventId === result.event!.eventId
      )
    ).toBe(true);
    expect(writes.some((w) => w.kind === 'run' && w.run.runId === runId)).toBe(
      true
    );
    const streams = writes.filter((w) => w.kind === 'stream');
    expect(streams.flatMap((w) => w.chunks.map((c) => c.index))).toEqual([
      0, 1, 2,
    ]);
    expect(streams.at(-1)?.closed).toBe(true);
  } finally {
    await world.close();
    database.close();
  }
});

it('rolls back world and host rows when the hook throws, including savepoints', async () => {
  const database = new DatabaseSync(':memory:');
  database.exec('CREATE TABLE host (value TEXT)');
  let fail = true;
  const world = createWorld({
    database,
    onWrite(write) {
      if (write.kind === 'event' || write.kind === 'stream') {
        database.prepare('INSERT INTO host VALUES (?)').run(write.kind);
        if (fail) throw new Error('host rejected');
      }
    },
  });
  try {
    database.exec("BEGIN; INSERT INTO host VALUES ('outer')");
    await expect(world.events.create(null, request())).rejects.toThrow(
      'host rejected'
    );
    expect(database.isTransaction).toBe(true);
    expect(database.prepare('SELECT count(*) AS n FROM runs').get()?.n).toBe(0);
    expect(database.prepare('SELECT count(*) AS n FROM host').get()?.n).toBe(1);
    await expect(
      world.streams.write('wrun_host', 'host-stream', 'bad')
    ).rejects.toThrow('host rejected');
    expect(
      database.prepare('SELECT count(*) AS n FROM stream_chunks').get()?.n
    ).toBe(0);
    expect(await world.streams.list('wrun_host')).toEqual([]);
    fail = false;
    await world.streams.write('wrun_host', 'host-stream', 'good');
    await world.events.create(null, request());
    expect(database.isTransaction).toBe(true);
    expect(await world.streams.list('wrun_host')).toEqual(['host-stream']);
    database.exec('ROLLBACK');
    expect(database.prepare('SELECT count(*) AS n FROM runs').get()?.n).toBe(0);
    expect(database.prepare('SELECT count(*) AS n FROM host').get()?.n).toBe(0);
  } finally {
    await world.close();
    database.close();
  }
});

it('rolls back standalone snapshot writes inside an outer host transaction', async () => {
  const database = new DatabaseSync(':memory:');
  database.exec('CREATE TABLE host (value TEXT)');
  const world = createWorld({
    database,
    onWrite(write) {
      if (write.kind === 'mutation') {
        database.exec("INSERT INTO host VALUES ('hook')");
        throw new Error('rejected mutation');
      }
    },
  });
  try {
    // Seed a row through the host so delete exercises a changed standalone write.
    database
      .prepare('INSERT INTO snapshots VALUES (?, ?)')
      .run('wrun_host', new Uint8Array());
    database.exec('BEGIN');
    await expect(
      world.experimental_snapshots!.delete('wrun_host')
    ).rejects.toThrow('rejected mutation');
    expect(database.isTransaction).toBe(true);
    expect(
      database.prepare('SELECT count(*) AS n FROM snapshots').get()?.n
    ).toBe(1);
    expect(database.prepare('SELECT count(*) AS n FROM host').get()?.n).toBe(0);
    database.exec('ROLLBACK');
  } finally {
    await world.close();
    database.close();
  }
});
