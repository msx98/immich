import { DecodeToBufferOptions, GenerateThumbhashOptions, GenerateThumbnailOptions } from 'src/types';

/** How an image input crosses the IPC boundary: a path is passed as-is, a Buffer is base64-encoded. */
export type WorkerInputRef = { type: 'path'; value: string } | { type: 'buffer'; value: string };

export const toWorkerInput = (input: string | Buffer): WorkerInputRef =>
  typeof input === 'string' ? { type: 'path', value: input } : { type: 'buffer', value: input.toString('base64') };

export const resolveWorkerInput = (ref: WorkerInputRef): string | Buffer =>
  ref.type === 'path' ? ref.value : Buffer.from(ref.value, 'base64');

type WithRequestId<T> = T & { requestId: string };

export type GenerateThumbnailMessage = WithRequestId<{
  operation: 'generateThumbnail';
  input: WorkerInputRef;
  options: GenerateThumbnailOptions;
  output: string;
}>;
export type GenerateThumbnailResponse = WithRequestId<{ ok: true } | { ok: false; error: string }>;

export type DecodeImageMessage = WithRequestId<{
  operation: 'decodeImage';
  input: WorkerInputRef;
  options: DecodeToBufferOptions;
}>;
export type DecodeImageResponse = WithRequestId<
  | { ok: true; data: string; info: { width: number; height: number; channels: 1 | 2 | 3 | 4 } }
  | { ok: false; error: string }
>;

export type GenerateThumbhashMessage = WithRequestId<{
  operation: 'generateThumbhash';
  input: WorkerInputRef;
  options: GenerateThumbhashOptions;
}>;
export type GenerateThumbhashResponse = WithRequestId<{ ok: true; data: string } | { ok: false; error: string }>;

export type GetImageMetadataMessage = WithRequestId<{
  operation: 'getImageMetadata';
  input: WorkerInputRef;
}>;
export type GetImageMetadataResponse = WithRequestId<
  { ok: true; width: number; height: number; isTransparent: boolean } | { ok: false; error: string }
>;

export type SharpWorkerMessage =
  | GenerateThumbnailMessage
  | DecodeImageMessage
  | GenerateThumbhashMessage
  | GetImageMetadataMessage;

export type SharpWorkerResponse =
  | GenerateThumbnailResponse
  | DecodeImageResponse
  | GenerateThumbhashResponse
  | GetImageMetadataResponse;

/** How long the repository waits for a single sharp op before treating the worker as unresponsive. */
export const SHARP_WORKER_TIMEOUT_MS = 30_000;

/** Consecutive timeouts (across possibly-unrelated requests) before we proactively cycle
 * the worker rather than waiting for it to either finish or crash on its own. Kept small:
 * a single slow image is normal, but several timeouts in a row suggest the process itself
 * is stuck (e.g. an infinite loop in libvips), not just one large file. */
export const SHARP_WORKER_MAX_CONSECUTIVE_TIMEOUTS = 2;

/** How often the worker checks whether its original parent process is still alive, so it can
 * self-terminate instead of surviving as an orphan if the parent is killed ungracefully (e.g. SIGKILL,
 * OOM-kill) without a chance to signal its children. */
export const SHARP_WORKER_PARENT_LIVENESS_CHECK_MS = 5_000;
