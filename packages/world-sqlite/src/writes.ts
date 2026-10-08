import type { Event, WorkflowRun } from '@workflow/world';

/** Notifications run synchronously before commit. Hosts must defer external
 * side effects until commit; an outer host transaction may still roll back.
 * A logical write can produce multiple notifications. */
export type WorldWrite =
  | { kind: 'event'; runId: string; event: Event; run?: WorkflowRun }
  | { kind: 'run'; run: WorkflowRun }
  | {
      kind: 'stream';
      runId: string;
      name: string;
      chunks: { index: number; chunkId: string; data: Uint8Array }[];
      closed?: boolean;
    }
  | { kind: 'mutation'; sql: string };
