import { Injectable } from '@nestjs/common';
import { ExifDateTime, exiftool, WriteTags } from 'exiftool-vendored';
import ffmpeg, { FfprobeData } from 'fluent-ffmpeg';
import { Duration } from 'luxon';
import { fork } from 'node:child_process';
import fs from 'node:fs/promises';
import { join } from 'node:path';
import { Writable } from 'node:stream';
import { Exif } from 'src/database';
import { LogLevel, RawExtractedFormat } from 'src/enum';
import { LoggingRepository } from 'src/repositories/logging.repository';
import {
  DecodeToBufferOptions,
  GenerateThumbhashOptions,
  GenerateThumbnailOptions,
  ImageDimensions,
  ProbeOptions,
  TranscodeCommand,
  VideoInfo,
} from 'src/types';
import { handlePromiseError } from 'src/utils/misc';

const probe = (input: string, options: string[]): Promise<FfprobeData> =>
  new Promise((resolve, reject) =>
    ffmpeg.ffprobe(input, options, (error, data) => (error ? reject(error) : resolve(data))),
  );

type ProgressEvent = {
  frames: number;
  currentFps: number;
  currentKbps: number;
  targetSize: number;
  timemark: string;
  percent?: number;
};

export type ExtractResult = {
  buffer: Buffer;
  format: RawExtractedFormat;
};

type GenerateThumbnailMessage = {
  operation: 'generateThumbnail';
  input: { type: 'path'; value: string } | { type: 'buffer'; value: string };
  options: GenerateThumbnailOptions;
  output: string;
};

type GenerateThumbnailResponse = { ok: true } | { ok: false; error: string };

type DecodeImageMessage = {
  operation: 'decodeImage';
  input: { type: 'path'; value: string } | { type: 'buffer'; value: string };
  options: DecodeToBufferOptions;
};

type DecodeImageResponse =
  | {
    ok: true;
    data: string;
    info: {
      width: number;
      height: number;
      channels: 1 | 2 | 3 | 4;
    };
  }
  | { ok: false; error: string };

type GenerateThumbhashMessage = {
  operation: 'generateThumbhash';
  input: { type: 'path'; value: string } | { type: 'buffer'; value: string };
  options: GenerateThumbhashOptions;
};

type GenerateThumbhashResponse = { ok: true; data: string } | { ok: false; error: string };

type GetImageMetadataMessage = {
  operation: 'getImageMetadata';
  input: { type: 'path'; value: string } | { type: 'buffer'; value: string };
};

type GetImageMetadataResponse =
  | { ok: true; width: number; height: number; isTransparent: boolean }
  | { ok: false; error: string };

@Injectable()
export class MediaRepository {
  constructor(private logger: LoggingRepository) {
    this.logger.setContext(MediaRepository.name);
  }

  /**
   *
   * @param input file path to the input image
   * @returns ExtractResult if succeeded, or null if failed
   */
  async extract(input: string): Promise<ExtractResult | null> {
    try {
      const buffer = await exiftool.extractBinaryTagToBuffer('JpgFromRaw2', input);
      return { buffer, format: RawExtractedFormat.Jpeg };
    } catch (error: any) {
      this.logger.debug(`Could not extract JpgFromRaw2 buffer from image, trying JPEG from RAW next: ${error}`);
    }

    try {
      const buffer = await exiftool.extractBinaryTagToBuffer('JpgFromRaw', input);
      return { buffer, format: RawExtractedFormat.Jpeg };
    } catch (error: any) {
      this.logger.debug(`Could not extract JPEG buffer from image, trying PreviewJXL next: ${error}`);
    }

    try {
      const buffer = await exiftool.extractBinaryTagToBuffer('PreviewJXL', input);
      return { buffer, format: RawExtractedFormat.Jxl };
    } catch (error: any) {
      this.logger.debug(`Could not extract PreviewJXL buffer from image, trying PreviewImage next: ${error}`);
    }

    try {
      const buffer = await exiftool.extractBinaryTagToBuffer('PreviewImage', input);
      return { buffer, format: RawExtractedFormat.Jpeg };
    } catch (error: any) {
      this.logger.debug(`Could not extract preview buffer from image: ${error}`);
      return null;
    }
  }

  async writeExif(tags: Partial<Exif>, output: string): Promise<boolean> {
    try {
      const tagsToWrite: WriteTags = {
        ExifImageWidth: tags.exifImageWidth,
        ExifImageHeight: tags.exifImageHeight,
        DateTimeOriginal: tags.dateTimeOriginal && ExifDateTime.fromMillis(tags.dateTimeOriginal.getTime()),
        ModifyDate: tags.modifyDate && ExifDateTime.fromMillis(tags.modifyDate.getTime()),
        TimeZone: tags.timeZone,
        GPSLatitude: tags.latitude,
        GPSLongitude: tags.longitude,
        ProjectionType: tags.projectionType,
        City: tags.city,
        Country: tags.country,
        Make: tags.make,
        Model: tags.model,
        LensModel: tags.lensModel,
        Fnumber: tags.fNumber?.toFixed(1),
        FocalLength: tags.focalLength?.toFixed(1),
        ISO: tags.iso,
        ExposureTime: tags.exposureTime,
        ProfileDescription: tags.profileDescription,
        ColorSpace: tags.colorspace,
        Rating: tags.rating === null ? 0 : tags.rating,
        // specially convert Orientation to numeric Orientation# for exiftool
        'Orientation#': tags.orientation ? Number(tags.orientation) : undefined,
      };

      await exiftool.write(output, tagsToWrite, {
        ignoreMinorErrors: true,
        writeArgs: ['-overwrite_original'],
      });
      return true;
    } catch (error: any) {
      this.logger.warn(`Could not write exif data to image: ${error.message}`);
      return false;
    }
  }

  async copyTagGroup(tagGroup: string, source: string, target: string): Promise<boolean> {
    try {
      await exiftool.write(
        target,
        {},
        {
          ignoreMinorErrors: true,
          writeArgs: ['-TagsFromFile', source, `-${tagGroup}:all>${tagGroup}:all`, '-overwrite_original'],
        },
      );
      return true;
    } catch (error: any) {
      this.logger.warn(`Could not copy tag data to image: ${error.message}`);
      return false;
    }
  }

  async decodeImage(input: string | Buffer, options: DecodeToBufferOptions) {
    const message: DecodeImageMessage = {
      operation: 'decodeImage',
      input: typeof input === 'string' ? { type: 'path', value: input } : { type: 'buffer', value: input.toString('base64') },
      options,
    };

    const response = await this.runDecodeImageProcess(message);
    return {
      data: Buffer.from(response.data, 'base64'),
      info: response.info,
    };
  }

  async generateThumbnail(input: string | Buffer, options: GenerateThumbnailOptions, output: string): Promise<void> {
    const message: GenerateThumbnailMessage = {
      operation: 'generateThumbnail',
      input: typeof input === 'string' ? { type: 'path', value: input } : { type: 'buffer', value: input.toString('base64') },
      options,
      output,
    };

    await this.runGenerateThumbnailProcess(message);
  }

  private async runGenerateThumbnailProcess(message: GenerateThumbnailMessage): Promise<void> {
    // eslint-disable-next-line unicorn/prefer-module
    const workerPath = join(__dirname, '..', 'workers', 'sharp-thumbnail.worker.js');

    const child = fork(workerPath, {
      stdio: ['ignore', 'ignore', 'ignore', 'ipc'],
    });

    return new Promise<void>((resolve, reject) => {
      let settled = false;

      const cleanup = () => {
        child.removeAllListeners('error');
        child.removeAllListeners('exit');
        child.removeAllListeners('message');
      };

      const finish = (error?: Error) => {
        if (settled) {
          return;
        }

        settled = true;
        cleanup();
        if (error) {
          reject(error);
        } else {
          resolve();
        }
      };

      child.once('error', (error) => finish(error));
      child.once('exit', (code, signal) => {
        if (settled) {
          return;
        }

        if (code === 0) {
          finish();
          return;
        }

        finish(new Error(`Thumbnail worker exited unexpectedly (code: ${code ?? 'null'}, signal: ${signal ?? 'none'})`));
      });

      child.once('message', (response: GenerateThumbnailResponse) => {
        if (response.ok) {
          finish();
          return;
        }

        finish(new Error(`Thumbnail worker failed: ${response.error}`));
      });

      child.send(message, (error) => {
        if (error) {
          finish(error);
        }
      });
    });
  }

  async generateThumbhash(input: string | Buffer, options: GenerateThumbhashOptions): Promise<Buffer> {
    const message: GenerateThumbhashMessage = {
      operation: 'generateThumbhash',
      input: typeof input === 'string' ? { type: 'path', value: input } : { type: 'buffer', value: input.toString('base64') },
      options,
    };

    const response = await this.runGenerateThumbhashProcess(message);
    return Buffer.from(response.data, 'base64');
  }

  private async runDecodeImageProcess(message: DecodeImageMessage): Promise<Extract<DecodeImageResponse, { ok: true }>> {
    // eslint-disable-next-line unicorn/prefer-module
    const workerPath = join(__dirname, '..', 'workers', 'sharp-thumbnail.worker.js');

    const child = fork(workerPath, {
      stdio: ['ignore', 'ignore', 'ignore', 'ipc'],
    });

    return new Promise((resolve, reject) => {
      let settled = false;

      const cleanup = () => {
        child.removeAllListeners('error');
        child.removeAllListeners('exit');
        child.removeAllListeners('message');
      };

      const finish = (error?: Error, response?: Extract<DecodeImageResponse, { ok: true }>) => {
        if (settled) {
          return;
        }

        settled = true;
        cleanup();

        if (error) {
          reject(error);
          return;
        }

        resolve(response as Extract<DecodeImageResponse, { ok: true }>);
      };

      child.once('error', (error) => finish(error));
      child.once('exit', (code, signal) => {
        if (settled) {
          return;
        }

        if (code === 0) {
          finish(new Error('Decode worker exited before returning a response'));
          return;
        }

        finish(new Error(`Decode worker exited unexpectedly (code: ${code ?? 'null'}, signal: ${signal ?? 'none'})`));
      });

      child.once('message', (response: DecodeImageResponse) => {
        if (response.ok) {
          finish(undefined, response);
          return;
        }

        finish(new Error(`Decode worker failed: ${response.error}`));
      });

      child.send(message, (error) => {
        if (error) {
          finish(error);
        }
      });
    });
  }

  private async runGenerateThumbhashProcess(
    message: GenerateThumbhashMessage,
  ): Promise<Extract<GenerateThumbhashResponse, { ok: true }>> {
    // eslint-disable-next-line unicorn/prefer-module
    const workerPath = join(__dirname, '..', 'workers', 'sharp-thumbnail.worker.js');

    const child = fork(workerPath, {
      stdio: ['ignore', 'ignore', 'ignore', 'ipc'],
    });

    return new Promise((resolve, reject) => {
      let settled = false;

      const cleanup = () => {
        child.removeAllListeners('error');
        child.removeAllListeners('exit');
        child.removeAllListeners('message');
      };

      const finish = (error?: Error, response?: Extract<GenerateThumbhashResponse, { ok: true }>) => {
        if (settled) {
          return;
        }

        settled = true;
        cleanup();

        if (error) {
          reject(error);
          return;
        }

        resolve(response as Extract<GenerateThumbhashResponse, { ok: true }>);
      };

      child.once('error', (error) => finish(error));
      child.once('exit', (code, signal) => {
        if (settled) {
          return;
        }

        if (code === 0) {
          finish(new Error('Thumbhash worker exited before returning a response'));
          return;
        }

        finish(new Error(`Thumbhash worker exited unexpectedly (code: ${code ?? 'null'}, signal: ${signal ?? 'none'})`));
      });

      child.once('message', (response: GenerateThumbhashResponse) => {
        if (response.ok) {
          finish(undefined, response);
          return;
        }

        finish(new Error(`Thumbhash worker failed: ${response.error}`));
      });

      child.send(message, (error) => {
        if (error) {
          finish(error);
        }
      });
    });
  }

  async probe(input: string, options?: ProbeOptions): Promise<VideoInfo> {
    const results = await probe(input, options?.countFrames ? ['-count_packets'] : []); // gets frame count quickly: https://stackoverflow.com/a/28376817
    return {
      format: {
        formatName: results.format.format_name,
        formatLongName: results.format.format_long_name,
        duration: this.parseFloat(results.format.duration),
        bitrate: this.parseInt(results.format.bit_rate),
      },
      videoStreams: results.streams
        .filter((stream) => stream.codec_type === 'video' && !stream.disposition?.attached_pic)
        .map((stream) => {
          const height = this.parseInt(stream.height);
          const dar = this.getDar(stream.display_aspect_ratio);
          return {
            index: stream.index,
            height,
            width: dar ? Math.round(height * dar) : this.parseInt(stream.width),
            codecName: stream.codec_name === 'h265' ? 'hevc' : stream.codec_name,
            codecType: stream.codec_type,
            frameCount: this.parseInt(options?.countFrames ? stream.nb_read_packets : stream.nb_frames),
            rotation: this.parseInt(stream.rotation),
            isHDR: stream.color_transfer === 'smpte2084' || stream.color_transfer === 'arib-std-b67',
            bitrate: this.parseInt(stream.bit_rate),
            pixelFormat: stream.pix_fmt || 'yuv420p',
            colorPrimaries: stream.color_primaries,
            colorSpace: stream.color_space,
            colorTransfer: stream.color_transfer,
          };
        }),
      audioStreams: results.streams
        .filter((stream) => stream.codec_type === 'audio')
        .map((stream) => ({
          index: stream.index,
          codecType: stream.codec_type,
          codecName: stream.codec_name,
          bitrate: this.parseInt(stream.bit_rate),
        })),
    };
  }

  transcode(input: string, output: string | Writable, options: TranscodeCommand): Promise<void> {
    if (!options.twoPass) {
      return new Promise((resolve, reject) => {
        this.configureFfmpegCall(input, output, options)
          .on('error', reject)
          .on('end', () => resolve())
          .run();
      });
    }

    if (typeof output !== 'string') {
      throw new TypeError('Two-pass transcoding does not support writing to a stream');
    }

    // two-pass allows for precise control of bitrate at the cost of running twice
    // recommended for vp9 for better quality and compression
    return new Promise((resolve, reject) => {
      // first pass output is not saved as only the .log file is needed
      this.configureFfmpegCall(input, '/dev/null', options)
        .addOptions('-pass', '1')
        .addOptions('-passlogfile', output)
        .addOptions('-f null')
        .on('error', reject)
        .on('end', () => {
          // second pass
          this.configureFfmpegCall(input, output, options)
            .addOptions('-pass', '2')
            .addOptions('-passlogfile', output)
            .on('error', reject)
            .on('end', () => handlePromiseError(fs.unlink(`${output}-0.log`), this.logger))
            .on('end', () => handlePromiseError(fs.rm(`${output}-0.log.mbtree`, { force: true }), this.logger))
            .on('end', () => resolve())
            .run();
        })
        .run();
    });
  }

  async getImageMetadata(input: string | Buffer): Promise<ImageDimensions & { isTransparent: boolean }> {
    const message: GetImageMetadataMessage = {
      operation: 'getImageMetadata',
      input: typeof input === 'string' ? { type: 'path', value: input } : { type: 'buffer', value: input.toString('base64') },
    };

    const response = await this.runGetImageMetadataProcess(message);
    return {
      width: response.width,
      height: response.height,
      isTransparent: response.isTransparent,
    };
  }

  private async runGetImageMetadataProcess(
    message: GetImageMetadataMessage,
  ): Promise<Extract<GetImageMetadataResponse, { ok: true }>> {
    // eslint-disable-next-line unicorn/prefer-module
    const workerPath = join(__dirname, '..', 'workers', 'sharp-thumbnail.worker.js');

    const child = fork(workerPath, {
      stdio: ['ignore', 'ignore', 'ignore', 'ipc'],
    });

    return new Promise((resolve, reject) => {
      let settled = false;

      const cleanup = () => {
        child.removeAllListeners('error');
        child.removeAllListeners('exit');
        child.removeAllListeners('message');
      };

      const finish = (error?: Error, response?: Extract<GetImageMetadataResponse, { ok: true }>) => {
        if (settled) {
          return;
        }

        settled = true;
        cleanup();

        if (error) {
          reject(error);
          return;
        }

        resolve(response as Extract<GetImageMetadataResponse, { ok: true }>);
      };

      child.once('error', (error) => finish(error));
      child.once('exit', (code, signal) => {
        if (settled) {
          return;
        }

        if (code === 0) {
          finish(new Error('Image metadata worker exited before returning a response'));
          return;
        }

        finish(
          new Error(`Image metadata worker exited unexpectedly (code: ${code ?? 'null'}, signal: ${signal ?? 'none'})`),
        );
      });

      child.once('message', (response: GetImageMetadataResponse) => {
        if (response.ok) {
          finish(undefined, response);
          return;
        }

        finish(new Error(`Image metadata worker failed: ${response.error}`));
      });

      child.send(message, (error) => {
        if (error) {
          finish(error);
        }
      });
    });
  }

  private configureFfmpegCall(input: string, output: string | Writable, options: TranscodeCommand) {
    const ffmpegCall = ffmpeg(input, { niceness: 10 })
      .inputOptions(options.inputOptions)
      .outputOptions(options.outputOptions)
      .output(output)
      .on('start', (command: string) => this.logger.debug(command))
      .on('error', (error, _, stderr) => this.logger.error(stderr || error));

    const { frameCount, percentInterval } = options.progress;
    const frameInterval = Math.ceil(frameCount / (100 / percentInterval));
    if (this.logger.isLevelEnabled(LogLevel.Debug) && frameCount && frameInterval) {
      let lastProgressFrame: number = 0;
      ffmpegCall.on('progress', (progress: ProgressEvent) => {
        if (progress.frames - lastProgressFrame < frameInterval) {
          return;
        }

        lastProgressFrame = progress.frames;
        const percent = ((progress.frames / frameCount) * 100).toFixed(2);
        const ms = progress.currentFps ? Math.floor((frameCount - progress.frames) / progress.currentFps) * 1000 : 0;
        const duration = ms ? Duration.fromMillis(ms).rescale().toHuman({ unitDisplay: 'narrow' }) : '';
        const outputText = output instanceof Writable ? 'stream' : output.split('/').pop();
        this.logger.debug(
          `Transcoding ${percent}% done${duration ? `, estimated ${duration} remaining` : ''} for output ${outputText}`,
        );
      });
    }

    return ffmpegCall;
  }

  private parseInt(value: string | number | undefined): number {
    return Number.parseInt(value as string) || 0;
  }

  private parseFloat(value: string | number | undefined): number {
    return Number.parseFloat(value as string) || 0;
  }

  private getDar(dar: string | undefined): number {
    if (dar) {
      const [darW, darH] = dar.split(':').map(Number);
      if (darW && darH) {
        return darW / darH;
      }
    }

    return 0;
  }
}
