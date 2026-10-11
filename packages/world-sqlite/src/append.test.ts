import { DatabaseSync } from 'node:sqlite';
import { performance } from 'node:perf_hooks';
import { expect, it } from 'vitest';
import { Db } from './db.js';
import {
  createStreamer,
  StreamClosedError,
  StreamOperationConflictError,
} from './streamer.js';
import { createWorld } from './index.js';

function setup() {
  const raw = new DatabaseSync(':memory:');
  const db = new Db(raw);
  return { raw, db, streams: createStreamer(db).streams };
}

it('allocates independent positions and deduplicates operations without notifications', () => {
  const { raw, db, streams } = setup();
  const writes: unknown[] = [];
  const host = new Db(raw, (write) => writes.push(write));
  const s = createStreamer(host).streams;
  expect(s.appendSync('run', 'a', ['a', 'b'], { operationId: 'op' })).toEqual({
    from: 0,
    to: 1,
    duplicate: false,
    closed: false,
  });
  expect(s.appendSync('run', 'b', ['z']).from).toBe(0);
  expect(
    writes
      .filter((w: any) => w.kind === 'stream')
      .map((w: any) => w.chunks.map((c: any) => c.index))
  ).toEqual([[0, 1], [0]]);
  const n = writes.length;
  expect(s.appendSync('run', 'a', ['a', 'b'], { operationId: 'op' })).toEqual({
    from: 0,
    to: 1,
    duplicate: true,
    closed: false,
  });
  expect(writes.length).toBe(n);
  expect(() => s.appendSync('run', 'a', ['x'], { operationId: 'op' })).toThrow(
    StreamOperationConflictError
  );
  s.appendSync('run', 'a', [], { close: true });
  expect(
    s.appendSync('run', 'a', ['a', 'b'], { operationId: 'op' }).duplicate
  ).toBe(true);
  expect(() => streams.appendSync('run', 'a', ['x'])).toThrow(
    StreamClosedError
  );
  expect(() => streams.appendSync('run', 'a', [], { close: true })).toThrow(
    StreamClosedError
  );
  expect(db.get<{ n: number }>('SELECT count(*) n FROM chunks')!.n).toBe(3);
  raw.close();
});

it('orders concurrent calls before awaiting run ids and closes idempotently', async () => {
  const { raw, streams } = setup();
  let resolve!: (id: string) => void;
  const id = new Promise<string>((r) => (resolve = r));
  const a = streams.writeMulti!(id, 'a', ['a', 'b']);
  const b = streams.writeMulti!('run', 'a', ['c', 'd']);
  resolve('run');
  await Promise.all([a, b]);
  await streams.close('run', 'a');
  await streams.close('run', 'a');
  expect(await streams.getInfo('run', 'missing')).toEqual({
    tailIndex: -1,
    done: false,
  });
  expect((await streams.getChunks('run', 'missing')).done).toBe(false);
  const page = await streams.getChunks('run', 'a');
  expect(
    page.data.map((c) => [c.index, Buffer.from(c.data).toString()])
  ).toEqual([
    [0, 'a'],
    [1, 'b'],
    [2, 'c'],
    [3, 'd'],
  ]);
  raw.close();
});

it('rolls back chunks, positions and operation records with the host transaction', () => {
  const { raw, db, streams } = setup();
  streams.appendSync('run', 'a', ['first']);
  raw.exec('BEGIN');
  streams.appendSync('run', 'a', ['second'], { operationId: 'op' });
  raw.exec('ROLLBACK');
  expect(
    db.get<{ next_idx: number }>('SELECT next_idx FROM streams')!.next_idx
  ).toBe(1);
  expect(db.get<{ n: number }>('SELECT count(*) n FROM stream_ops')!.n).toBe(0);
  expect(db.get<{ n: number }>('SELECT count(*) n FROM chunks')!.n).toBe(1);
  raw.close();
});

const oldSchema = `CREATE TABLE stream_chunks (stream_name TEXT NOT NULL, chunk_id TEXT NOT NULL, tag TEXT NOT NULL, eof INTEGER NOT NULL, data BLOB NOT NULL, PRIMARY KEY(stream_name,chunk_id)); CREATE TABLE run_streams (run_id TEXT NOT NULL,stream_name TEXT NOT NULL,tag TEXT NOT NULL,position INTEGER NOT NULL,PRIMARY KEY(run_id,stream_name,tag));`;
function legacy() {
  const raw = new DatabaseSync(':memory:');
  raw.exec(oldSchema);
  const insert = raw.prepare('INSERT INTO stream_chunks VALUES (?,?,?,?,?)');
  for (const [name, id, eof, data] of [
    ['eof', '02', 0, 'b'],
    ['eof', '01', 0, 'a'],
    ['eof', '03', 1, ''],
    ['open', '04', 0, 'c'],
    ['purged', 'chnk_00000000000000000000000000', 1, ''],
    ['purged', 'zz', 0, 'discard'],
  ] as const)
    insert.run(name, id, '', eof, Buffer.from(data));
  raw
    .prepare('INSERT INTO run_streams VALUES (?,?,?,?)')
    .run('run', 'eof', '', 0);
  return raw;
}
it('migrates legacy cursor positions, EOF, purge tombstones and open streams at world open', async () => {
  const raw = legacy();
  const world = createWorld({ database: raw, recoverActiveRuns: false });
  const s = world.streams;
  const first = await s.getChunks('run', 'eof', { limit: 1 });
  expect(first.data[0].index).toBe(0);
  expect(first.hasMore).toBe(true);
  expect(first.done).toBe(false);
  const second = await s.getChunks('run', 'eof', {
    cursor: first.cursor!,
    limit: 1,
  });
  expect(
    second.data.map((c) => [c.index, Buffer.from(c.data).toString()])
  ).toEqual([[1, 'b']]);
  expect(second.done).toBe(true);
  expect(await s.getInfo('run', 'eof')).toEqual({ tailIndex: 1, done: true });
  expect(await s.getInfo('run', 'open')).toEqual({ tailIndex: 0, done: false });
  expect(await s.getInfo('run', 'purged')).toEqual({
    tailIndex: -1,
    done: true,
  });
  expect((await s.getChunks('run', 'purged')).data).toEqual([]);
  expect(
    raw.prepare("SELECT 1 FROM sqlite_master WHERE name='stream_chunks'").get()
  ).toBeUndefined();
  expect(
    raw.prepare("SELECT run_id FROM streams WHERE name='eof'").get()?.run_id
  ).toBe('run');
  await world.close();
  raw.close();
});
it('rolls back a failed migration and reruns it successfully', () => {
  const raw = legacy();
  raw.exec(
    `CREATE TABLE chunks(stream_id INTEGER NOT NULL,idx INTEGER NOT NULL,data BLOB NOT NULL,PRIMARY KEY(stream_id,idx)) WITHOUT ROWID; CREATE TRIGGER reject_chunk BEFORE INSERT ON chunks BEGIN SELECT RAISE(ABORT,'injected'); END;`
  );
  expect(() => new Db(raw)).toThrow('injected');
  expect(raw.prepare('SELECT count(*) n FROM stream_chunks').get()?.n).toBe(6);
  expect(
    raw.prepare("SELECT 1 FROM sqlite_master WHERE name='streams'").get()
  ).toBeUndefined();
  raw.exec('DROP TRIGGER reject_chunk');
  new Db(raw);
  expect(raw.prepare('SELECT count(*) n FROM chunks').get()?.n).toBe(3);
  raw.close();
});
it('keeps append, metadata and tail reads bounded at 100k chunks', async () => {
  const { raw, db, streams } = setup();
  streams.appendSync('run', 'a', ['first']);
  async function measure() {
    const appends: number[] = [];
    const infos: number[] = [];
    for (let i = 0; i < 100; i++) {
      let t = performance.now();
      streams.appendSync('run', 'a', ['x']);
      appends.push(performance.now() - t);
      t = performance.now();
      await streams.getInfo('run', 'a');
      infos.push(performance.now() - t);
    }
    appends.sort((a, b) => a - b);
    infos.sort((a, b) => a - b);
    return { append: appends[50], info: infos[50] };
  }
  const small = await measure();
  streams.appendSync(
    'run',
    'a',
    Array.from({ length: 100000 - 101 }, () => 'x')
  );
  const big = await measure();
  console.log('100k timings (ms, median)', { small, big });
  expect(big.append).toBeLessThan(small.append * 5);
  expect(big.info).toBeLessThan(small.info * 5);
  const page = await streams.getChunks('run', 'a', {
    cursor: Buffer.from(JSON.stringify({ i: 99999 })).toString('base64'),
    limit: 2,
  });
  expect(page.data.map((c) => c.index)).toEqual([99999, 100000]);
  streams.appendSync('run', 'a', [], { close: true });
  const reader = (await streams.get('run', 'a', -2)).getReader();
  expect((await reader.read()).done).toBe(false);
  expect((await reader.read()).done).toBe(false);
  expect((await reader.read()).done).toBe(true);
  expect(
    db.get<{ next_idx: number }>('SELECT next_idx FROM streams')!.next_idx
  ).toBe(100100);
  raw.close();
});

it('does not wake live readers until the host commits and explicitly wakes', async () => {
  const { raw, streams } = setup();
  const reader = (await streams.get('run', 'a')).getReader();
  let received = false;
  const reading = reader.read().then((result) => {
    received = true;
    return result;
  });
  raw.exec('BEGIN');
  await streams.write('run', 'a', 'committed');
  await Promise.resolve();
  expect(received).toBe(false);
  raw.exec('COMMIT');
  streams.wake('a');
  expect(Buffer.from((await reading).value!).toString()).toBe('committed');
  await reader.cancel();
  raw.close();
});

it('migrates a prefixed legacy store without changing host tables', async () => {
  const raw = new DatabaseSync(':memory:');
  raw.exec(
    oldSchema
      .replaceAll('stream_chunks', 'wf_stream_chunks')
      .replaceAll('run_streams', 'wf_run_streams')
  );
  raw
    .prepare('INSERT INTO wf_stream_chunks VALUES (?,?,?,?,?)')
    .run('a', '01', '', 0, Buffer.from('x'));
  raw.exec(
    "CREATE TABLE streams (host TEXT); INSERT INTO streams VALUES ('untouched')"
  );
  const db = new Db(raw, undefined, 'wf_');
  expect(await createStreamer(db).streams.getInfo('run', 'a')).toEqual({
    tailIndex: 0,
    done: false,
  });
  expect(raw.prepare('SELECT host FROM streams').get()?.host).toBe('untouched');
  expect(
    raw
      .prepare("SELECT 1 FROM sqlite_master WHERE name='wf_stream_chunks'")
      .get()
  ).toBeUndefined();
  raw.close();
});
