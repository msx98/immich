import sharp from 'sharp';
import { ORIENTATION_TO_SHARP_ROTATION } from 'src/constants';
import { AssetEditActionItem } from 'src/dtos/editing.dto';
import { Colorspace } from 'src/enum';
import { DecodeToBufferOptions, GenerateThumbhashOptions, GenerateThumbnailOptions } from 'src/types';
import { createAffineMatrix } from 'src/utils/transform';
import {
  DecodeImageMessage,
  GenerateThumbhashMessage,
  GenerateThumbnailMessage,
  GetImageMetadataMessage,
  resolveWorkerInput,
  SHARP_WORKER_PARENT_LIVENESS_CHECK_MS,
  SharpWorkerMessage,
  SharpWorkerResponse,
} from 'src/workers/sharp-thumbnail.protocol';

type WorkerResponsePayload =
  | { ok: true }
  | { ok: true; data: string }
  | { ok: true; data: string; info: { width: number; height: number; channels: 1 | 2 | 3 | 4 } }
  | { ok: true; width: number; height: number; isTransparent: boolean }
  | { ok: false; error: string };

sharp.concurrency(0);
sharp.cache({ files: 0 });

// Mirrors upstream MediaRepository.applyEdits (server/src/repositories/media.repository.ts)
const applyEdits = (pipeline: sharp.Sharp, edits: AssetEditActionItem[]): sharp.Sharp => {
  const crop = edits.find((edit) => edit.action === 'crop');
  if (crop) {
    pipeline = pipeline.extract({
      left: Math.round(crop.parameters.x),
      top: Math.round(crop.parameters.y),
      width: Math.round(crop.parameters.width),
      height: Math.round(crop.parameters.height),
    });
  }

  const affineEditOperations = edits.filter((edit) => edit.action !== 'crop');
  if (affineEditOperations.length > 0) {
    const { a, b, c, d } = createAffineMatrix(affineEditOperations);
    pipeline = pipeline.affine([
      [a, b],
      [c, d],
    ]);
  }

  return pipeline;
};

// Mirrors upstream MediaRepository.getImageDecodingPipeline
const getImageDecodingPipeline = (input: string | Buffer, options: DecodeToBufferOptions) => {
  let pipeline = sharp(input, {
    failOn: options.processInvalidImages ? 'none' : 'error',
    limitInputPixels: false,
    raw: options.raw,
    unlimited: true,
  })
    .pipelineColorspace(options.colorspace === Colorspace.Srgb ? 'srgb' : 'rgb16')
    .withIccProfile(options.colorspace);

  if (!options.raw) {
    const { angle, flip, flop } = options.orientation ? ORIENTATION_TO_SHARP_ROTATION[options.orientation] : {};
    pipeline = pipeline.rotate(angle);

    if (flip) {
      pipeline = pipeline.flip();
    }

    if (flop) {
      pipeline = pipeline.flop();
    }
  }

  if (options.edits && options.edits.length > 0) {
    pipeline = applyEdits(pipeline, options.edits);
  }

  if (options.size !== undefined) {
    pipeline = pipeline.resize(options.size, options.size, { fit: 'outside', withoutEnlargement: true });
  }

  return pipeline;
};

const send = (response: SharpWorkerResponse): Promise<void> =>
  new Promise((resolve, reject) => {
    if (typeof process.send !== 'function') {
      resolve();
      return;
    }

    process.send(response, (error) => {
      if (error) {
        reject(error);
        return;
      }

      resolve();
    });
  });

const runGenerateThumbnail = async (message: GenerateThumbnailMessage): Promise<WorkerResponsePayload> => {
  const input = resolveWorkerInput(message.input);
  const output = getImageDecodingPipeline(input, message.options).toFormat(message.options.format, {
    quality: message.options.quality,
    // this is default in libvips (except the threshold is 90), but we need to set it manually in sharp
    chromaSubsampling: message.options.quality >= 80 ? '4:4:4' : '4:2:0',
    progressive: message.options.progressive,
  });

  await output.toFile(message.output);
  return { ok: true };
};

const runDecodeImage = async (message: DecodeImageMessage): Promise<WorkerResponsePayload> => {
  const input = resolveWorkerInput(message.input);
  const { data, info } = await getImageDecodingPipeline(input, message.options).raw().toBuffer({
    resolveWithObject: true,
  });

  return {
    ok: true,
    data: data.toString('base64'),
    info: { width: info.width, height: info.height, channels: info.channels as 1 | 2 | 3 | 4 },
  };
};

const runGenerateThumbhash = async (message: GenerateThumbhashMessage): Promise<WorkerResponsePayload> => {
  const { rgbaToThumbHash } = await import('thumbhash');
  const input = resolveWorkerInput(message.input);
  const { data, info } = await getImageDecodingPipeline(input, {
    colorspace: message.options.colorspace,
    processInvalidImages: message.options.processInvalidImages,
    raw: message.options.raw,
    edits: message.options.edits,
  })
    .resize(100, 100, { fit: 'inside', withoutEnlargement: true })
    .raw()
    .ensureAlpha()
    .toBuffer({ resolveWithObject: true });

  return { ok: true, data: Buffer.from(rgbaToThumbHash(info.width, info.height, data)).toString('base64') };
};

const runGetImageMetadata = async (message: GetImageMetadataMessage): Promise<WorkerResponsePayload> => {
  const input = resolveWorkerInput(message.input);
  const { width = 0, height = 0, hasAlpha = false } = await sharp(input).metadata();
  return { ok: true, width, height, isTransparent: hasAlpha };
};

/**
 * This worker is spawned once by MediaRepository's SharpWorkerClient and stays alive for the
 * lifetime of the server, handling every sharp call it's sent (multiplexed by requestId) so
 * multiple thumbnail operations can run concurrently here — just like they did when sharp ran
 * directly in the main process. The only difference is that a native libvips/sharp crash now
 * only takes down this one process, which the client transparently respawns on the next call.
 */
process.on('message', (message: SharpWorkerMessage) => {
  const { requestId } = message;

  (async () => {
    try {
      let payload: WorkerResponsePayload;
      if (message.operation === 'generateThumbnail') {
        payload = await runGenerateThumbnail(message);
      } else if (message.operation === 'decodeImage') {
        payload = await runDecodeImage(message);
      } else if (message.operation === 'getImageMetadata') {
        payload = await runGetImageMetadata(message);
      } else {
        payload = await runGenerateThumbhash(message);
      }

      await send({ requestId, ...payload } as SharpWorkerResponse);
    } catch (error: Error | any) {
      try {
        await send({ requestId, ok: false, error: error?.stack || error?.message || String(error) });
      } catch {
        // best effort error reporting over IPC
      }
      // Note: we deliberately don't process.exit() here — a caught, non-fatal JS error (bad
      // input, sharp throwing cleanly) shouldn't kill the persistent worker. Only an actual
      // native crash (segfault, bypassing this try/catch entirely) ends this process, which
      // is exactly the isolation SharpWorkerClient is designed to detect and recover from.
    }
  })();
});

// Exit cleanly when the parent closes the IPC channel — this fires on a graceful parent
// shutdown (normal exit, or SIGTERM handled by Nest's OnModuleDestroy calling child.disconnect()),
// avoiding a lingering worker process after the main server has already gone away.
process.on('disconnect', () => process.exit(0));

// Backstop against becoming an orphaned/zombie process if the parent dies ungracefully
// (SIGKILL, OOM-kill) without a chance to disconnect the IPC channel or signal this worker.
// If our original parent pid stops existing, there is no server left to serve, so exit.
const originalParentPid = process.ppid;
setInterval(() => {
  try {
    // signal 0 doesn't send a signal, it just tests whether the process exists
    process.kill(originalParentPid, 0);
  } catch {
    process.exit(0);
  }
}, SHARP_WORKER_PARENT_LIVENESS_CHECK_MS).unref();
