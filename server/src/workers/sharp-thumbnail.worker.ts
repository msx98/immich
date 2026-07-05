import sharp from 'sharp';
import { ORIENTATION_TO_SHARP_ROTATION } from 'src/constants';
import { AssetEditActionItem } from 'src/dtos/editing.dto';
import { Colorspace } from 'src/enum';
import { DecodeToBufferOptions, GenerateThumbhashOptions, GenerateThumbnailOptions } from 'src/types';
import { createAffineMatrix } from 'src/utils/transform';

type WorkerInput = {
  input: { type: 'path'; value: string } | { type: 'buffer'; value: string };
};

type GenerateThumbnailMessage = WorkerInput & {
  operation: 'generateThumbnail';
  options: GenerateThumbnailOptions;
  output: string;
};

type DecodeImageMessage = WorkerInput & {
  operation: 'decodeImage';
  options: DecodeToBufferOptions;
};

type GenerateThumbhashMessage = WorkerInput & {
  operation: 'generateThumbhash';
  options: GenerateThumbhashOptions;
};

type GetImageMetadataMessage = WorkerInput & {
  operation: 'getImageMetadata';
};

type WorkerMessage = GenerateThumbnailMessage | DecodeImageMessage | GenerateThumbhashMessage | GetImageMetadataMessage;

type WorkerResponse =
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

const send = (response: WorkerResponse): Promise<void> =>
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

const resolveInput = (message: WorkerInput) =>
  message.input.type === 'path' ? message.input.value : Buffer.from(message.input.value, 'base64');

const runGenerateThumbnail = async (message: GenerateThumbnailMessage): Promise<WorkerResponse> => {
  const input = resolveInput(message);
  const output = getImageDecodingPipeline(input, message.options).toFormat(message.options.format, {
    quality: message.options.quality,
    // this is default in libvips (except the threshold is 90), but we need to set it manually in sharp
    chromaSubsampling: message.options.quality >= 80 ? '4:4:4' : '4:2:0',
    progressive: message.options.progressive,
  });

  await output.toFile(message.output);
  return { ok: true };
};

const runDecodeImage = async (message: DecodeImageMessage): Promise<WorkerResponse> => {
  const input = resolveInput(message);
  const { data, info } = await getImageDecodingPipeline(input, message.options).raw().toBuffer({
    resolveWithObject: true,
  });

  return {
    ok: true,
    data: data.toString('base64'),
    info: { width: info.width, height: info.height, channels: info.channels as 1 | 2 | 3 | 4 },
  };
};

const runGenerateThumbhash = async (message: GenerateThumbhashMessage): Promise<WorkerResponse> => {
  const { rgbaToThumbHash } = await import('thumbhash');
  const input = resolveInput(message);
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

const runGetImageMetadata = async (message: GetImageMetadataMessage): Promise<WorkerResponse> => {
  const input = resolveInput(message);
  const { width = 0, height = 0, hasAlpha = false } = await sharp(input).metadata();
  return { ok: true, width, height, isTransparent: hasAlpha };
};

process.once('message', async (message: WorkerMessage) => {
  try {
    let response: WorkerResponse;
    if (message.operation === 'generateThumbnail') {
      response = await runGenerateThumbnail(message);
    } else if (message.operation === 'decodeImage') {
      response = await runDecodeImage(message);
    } else if (message.operation === 'getImageMetadata') {
      response = await runGetImageMetadata(message);
    } else {
      response = await runGenerateThumbhash(message);
    }

    await send(response);
    process.exit(0);
  } catch (error: Error | any) {
    try {
      await send({ ok: false, error: error?.stack || error?.message || String(error) });
    } catch {
      // best effort error reporting over IPC
    }
    process.exit(1);
  }
});
