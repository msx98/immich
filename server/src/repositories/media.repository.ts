import { Injectable, OnModuleDestroy } from '@nestjs/common';
import { ExifDateTime, exiftool, WriteTags } from 'exiftool-vendored';
import ffmpeg, { FfprobeData, FfprobeStream } from 'fluent-ffmpeg';
import _ from 'lodash';
import { Duration } from 'luxon';
import { ChildProcess, execFile as execFileCb } from 'node:child_process';
import fs from 'node:fs/promises';
import { randomUUID } from 'node:crypto';
import { dirname, join } from 'node:path';
import { Writable } from 'node:stream';
import { promisify } from 'node:util';
import { Exif } from 'src/database';
import {
  AacProfile,
  Av1Profile,
  ColorMatrix,
  ColorPrimaries,
  ColorTransfer,
  DvProfile,
  DvSignalCompatibility,
  H264Profile,
  HevcProfile,
  LogLevel,
  RawExtractedFormat,
} from 'src/enum';
import { LoggingRepository } from 'src/repositories/logging.repository';
import { ProcessRepository } from 'src/repositories/process.repository';
import {
  DecodeToBufferOptions,
  GenerateThumbhashOptions,
  GenerateThumbnailOptions,
  ImageDimensions,
  ProbeOptions,
  TranscodeCommand,
  VideoInfo,
  VideoPacketInfo,
} from 'src/types';
import { handlePromiseError } from 'src/utils/misc';
import {
  DecodeImageMessage,
  DecodeImageResponse,
  GenerateThumbhashMessage,
  GenerateThumbhashResponse,
  GenerateThumbnailMessage,
  GenerateThumbnailResponse,
  GetImageMetadataMessage,
  GetImageMetadataResponse,
  SHARP_WORKER_MAX_CONSECUTIVE_TIMEOUTS,
  SHARP_WORKER_TIMEOUT_MS,
  SharpWorkerMessage,
  SharpWorkerResponse,
  toWorkerInput,
} from 'src/workers/sharp-thumbnail.protocol';

const probe = (input: string, options: string[]): Promise<FfprobeData> =>
  new Promise((resolve, reject) =>
    ffmpeg.ffprobe(input, options, (error, data) => (error ? reject(error) : resolve(data))),
  );

const execFile = promisify(execFileCb);

const pascalCase = (str: string) => _.upperFirst(_.camelCase(str.toLowerCase()));

/**
 * A single, long-lived thumbnail-generation process shared by every sharp call in this
 * repository. All requests are multiplexed onto one child over IPC (matching how sharp calls
 * used to run concurrently in-process, e.g. via the ThumbnailGeneration job queue's concurrency
 * setting) — the difference is that a native libvips/sharp crash now only takes down this one
 * worker process, which is transparently respawned on the next call, instead of crashing the
 * entire server/microservices process.
 */
class SharpWorkerClient {
  private child: ChildProcess | null = null;
  private consecutiveTimeouts = 0;
  private readonly pending = new Map<
    string,
    {
      resolve: (response: Extract<SharpWorkerResponse, { ok: true }>) => void;
      reject: (error: Error) => void;
      timer: NodeJS.Timeout;
    }
  >();

  constructor(
    private processRepository: ProcessRepository,
    private logger: LoggingRepository,
  ) {}

  send<M extends SharpWorkerMessage, R extends SharpWorkerResponse>(
    message: Omit<M, 'requestId'>,
    timeoutMs = SHARP_WORKER_TIMEOUT_MS,
  ): Promise<Extract<R, { ok: true }>> {
    const requestId = randomUUID();
    const child = this.ensureChild();

    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => this.onTimeout(requestId), timeoutMs);
      this.pending.set(requestId, {
        resolve: resolve as (response: Extract<SharpWorkerResponse, { ok: true }>) => void,
        reject,
        timer,
      });

      child.send({ ...message, requestId }, (error) => {
        if (error && this.pending.delete(requestId)) {
          clearTimeout(timer);
          reject(error);
        }
      });
    });
  }

  /** Kills the current worker (if any) so future calls spawn a fresh one; used on graceful app shutdown. */
  shutdown() {
    if (this.child) {
      this.child.kill('SIGTERM');
      this.child = null;
    }
  }

  private onTimeout(requestId: string) {
    const request = this.pending.get(requestId);
    if (!request) {
      return;
    }
    this.pending.delete(requestId);
    request.reject(new Error('Thumbnail worker timed out waiting for a response'));

    this.consecutiveTimeouts++;
    if (this.consecutiveTimeouts < SHARP_WORKER_MAX_CONSECUTIVE_TIMEOUTS) {
      return;
    }

    // Several timeouts in a row suggest the worker process itself is stuck (e.g. an infinite
    // loop in libvips), not just one slow image — proactively cycle it rather than waiting
    // for it to either finish or crash on its own.
    this.consecutiveTimeouts = 0;
    if (this.child) {
      this.logger.warn(
        `Thumbnail worker had ${SHARP_WORKER_MAX_CONSECUTIVE_TIMEOUTS} consecutive timeouts; restarting it`,
      );
      this.child.kill('SIGTERM');
      this.child = null;
    }
  }

  private ensureChild(): ChildProcess {
    if (this.child) {
      return this.child;
    }

    // eslint-disable-next-line unicorn/prefer-module
    const workerPath = join(dirname(__filename), '..', 'workers', 'sharp-thumbnail.worker.js');
    const child = this.processRepository.fork(workerPath, [], {
      // avoid every forked worker trying to bind the same debugger inspect port
      execArgv: process.execArgv.filter((arg) => !arg.startsWith('--inspect')),
      stdio: ['ignore', 'ignore', 'ignore', 'ipc'],
    });
    this.child = child;

    child.on('message', (response: SharpWorkerResponse) => {
      this.consecutiveTimeouts = 0;
      const request = this.pending.get(response.requestId);
      if (!request) {
        return;
      }
      this.pending.delete(response.requestId);
      clearTimeout(request.timer);
      if (response.ok) {
        request.resolve(response as Extract<SharpWorkerResponse, { ok: true }>);
      } else {
        request.reject(new Error(`Thumbnail worker failed: ${response.error}`));
      }
    });

    const onChildDeath = (error: Error) => {
      if (this.child === child) {
        // next call lazily spawns a fresh worker
        this.child = null;
      }

      // any request already in flight to this (now-dead) child can never be answered
      for (const [requestId, request] of this.pending) {
        clearTimeout(request.timer);
        request.reject(error);
        this.pending.delete(requestId);
      }
    };

    child.once('error', (error) => onChildDeath(error));
    child.once('exit', (code, signal) => {
      if (code === 0) {
        return;
      }
      onChildDeath(
        new Error(
          `Thumbnail worker process exited unexpectedly (code: ${code ?? 'null'}, signal: ${signal ?? 'none'}); it will be restarted for subsequent requests`,
        ),
      );
    });

    return child;
  }
}

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

@Injectable()
export class MediaRepository implements OnModuleDestroy {
  private readonly sharpWorker: SharpWorkerClient;

  constructor(
    private logger: LoggingRepository,
    private processRepository: ProcessRepository,
  ) {
    this.logger.setContext(MediaRepository.name);
    this.sharpWorker = new SharpWorkerClient(this.processRepository, this.logger);
  }

  onModuleDestroy() {
    // avoid leaving an orphaned thumbnail worker process behind when the server shuts down
    this.sharpWorker.shutdown();
  }


  /**
   *
   * @param input file path to the input image
   * @returns ExtractResult if succeeded, or null if failed
   */
  async extract(input: string): Promise<ExtractResult | null> {
    for (const { tag, format } of [
      { tag: 'JpgFromRaw2', format: RawExtractedFormat.Jpeg },
      { tag: 'JpgFromRaw', format: RawExtractedFormat.Jpeg },
      { tag: 'PreviewJXL', format: RawExtractedFormat.Jxl },
      { tag: 'PreviewImage', format: RawExtractedFormat.Jpeg },
    ]) {
      try {
        const buffer = await exiftool.extractBinaryTagToBuffer(tag, input);
        return { buffer, format };
      } catch (error: any) {
        this.logger.debug(`Could not extract ${tag} buffer from image: ${error}`);
      }
    }
    return null;
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
    const response = await this.sharpWorker.send<DecodeImageMessage, DecodeImageResponse>({
      operation: 'decodeImage',
      input: toWorkerInput(input),
      options,
    });
    return { data: Buffer.from(response.data, 'base64'), info: response.info };
  }

  async generateThumbnail(input: string | Buffer, options: GenerateThumbnailOptions, output: string): Promise<void> {
    await this.sharpWorker.send<GenerateThumbnailMessage, GenerateThumbnailResponse>({
      operation: 'generateThumbnail',
      input: toWorkerInput(input),
      options,
      output,
    });
  }

  async generateThumbhash(input: string | Buffer, options: GenerateThumbhashOptions): Promise<Buffer> {
    const response = await this.sharpWorker.send<GenerateThumbhashMessage, GenerateThumbhashResponse>({
      operation: 'generateThumbhash',
      input: toWorkerInput(input),
      options,
    });
    return Buffer.from(response.data, 'base64');
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
        .sort((a, b) => this.compareStreams(a, b))
        .map((stream) => {
          const height = this.parseInt(stream.height);
          const dar = this.getDar(stream.display_aspect_ratio);
          return {
            index: stream.index,
            height,
            width: dar ? Math.round(height * dar) : this.parseInt(stream.width),
            codecName: stream.codec_name === 'h265' ? 'hevc' : (stream.codec_name ?? null),
            profile: this.parseVideoProfile(stream.codec_name, stream.profile as string | undefined) ?? null,
            level: this.parseOptionalInt(stream.level),
            frameCount: this.parseInt(options?.countFrames ? stream.nb_read_packets : stream.nb_frames),
            frameRate: this.parseFrameRate(stream.avg_frame_rate ?? stream.r_frame_rate),
            timeBase: this.parseRational(stream.time_base)?.den ?? null,
            rotation: this.parseInt(stream.rotation),
            bitrate: this.parseInt(stream.bit_rate),
            pixelFormat: stream.pix_fmt || 'yuv420p',
            colorPrimaries: this.parseEnum(ColorPrimaries, stream.color_primaries) ?? ColorPrimaries.Unknown,
            colorMatrix: this.parseEnum(ColorMatrix, stream.color_space) ?? ColorMatrix.Unknown,
            colorTransfer: this.parseEnum(ColorTransfer, stream.color_transfer) ?? ColorTransfer.Unknown,
            dvProfile: this.parseOptionalInt(stream.dv_profile) as DvProfile | null,
            dvLevel: this.parseOptionalInt(stream.dv_level),
            dvBlSignalCompatibilityId: this.parseOptionalInt(
              stream.dv_bl_signal_compatibility_id,
            ) as DvSignalCompatibility | null,
          };
        }),
      audioStreams: results.streams
        .filter((stream) => stream.codec_type === 'audio')
        .sort((a, b) => this.compareStreams(a, b))
        .map((stream) => ({
          index: stream.index,
          codecName: stream.codec_name ?? null,
          profile:
            stream.codec_name === 'aac' ? this.parseEnum(AacProfile, stream.profile as string | undefined) : null,
          bitrate: this.parseInt(stream.bit_rate),
        })),
    };
  }

  /**
   * Needed for accurate segments, especially when remuxing, seeking and/or VFR is involved.
   * Scanning packets for keyframes in JS is much faster than -skip_frame nokey since it avoids decoding the video.
   */
  async probePackets(input: string, streamIndex: number): Promise<VideoPacketInfo | null> {
    const { stdout } = await execFile('ffprobe', [
      '-v',
      'error',
      '-select_streams',
      String(streamIndex),
      '-show_entries',
      'packet=pts,duration,flags',
      '-of',
      'csv=p=0',
      input,
    ]);

    let totalDuration = 0;
    const keyframePts: number[] = [];
    const keyframeAccDuration: number[] = [];
    const keyframeOwnDuration: number[] = [];
    const postDiscard: { pts: number; duration: number }[] = [];
    for (const line of stdout.split('\n')) {
      if (!line) {
        continue;
      }
      const [ptsStr, durationStr, flags] = line.split(',');
      const pts = Number.parseInt(ptsStr);
      const duration = Number.parseInt(durationStr);
      if (Number.isNaN(pts) || Number.isNaN(duration)) {
        continue;
      }
      // Discarded packets don't contribute to packet count, but still contribute to video duration
      totalDuration += duration;
      if (flags[1] !== 'D') {
        postDiscard.push({ pts, duration });
      }
      if (flags[0] === 'K') {
        keyframePts.push(pts);
        keyframeAccDuration.push(totalDuration);
        // VFR content can have variable duration keyframes,
        // so we need to track their duration separately for accurate segment boundaries.
        // Non-keyframes are accounted for in totalDuration.
        keyframeOwnDuration.push(duration);
      }
    }

    if (postDiscard.length === 0) {
      return null;
    }

    return {
      totalDuration,
      packetCount: postDiscard.length,
      outputFrames: this.cfrOutputFrames(postDiscard, postDiscard.length / totalDuration),
      keyframePts,
      keyframeAccDuration,
      keyframeOwnDuration,
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
    const response = await this.sharpWorker.send<GetImageMetadataMessage, GetImageMetadataResponse>({
      operation: 'getImageMetadata',
      input: toWorkerInput(input),
    });
    return { width: response.width, height: response.height, isTransparent: response.isTransparent };
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

  private parseOptionalInt(value: string | number | undefined): number | null {
    const parsed = Number.parseInt(value as string);
    return Number.isNaN(parsed) ? null : parsed;
  }

  private parseEnum<E extends Record<string, number | string>>(enumObj: E, value?: string) {
    return value ? ((enumObj[pascalCase(value)] as Extract<E[keyof E], number> | undefined) ?? null) : null;
  }

  /** Parse a rational like "60000/1001" or "1/600" into `{ num, den }`. */
  private parseRational(value: string | undefined): { num: number; den: number } | null {
    if (value) {
      const [num, den = 1] = value.split('/').map(Number);
      if (num && den) {
        return { num, den };
      }
    }
    return null;
  }

  private parseFrameRate(value: string | undefined): number | null {
    const r = this.parseRational(value);
    return r ? r.num / r.den : null;
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

  private parseVideoProfile(codec?: string, profile?: string) {
    switch (codec) {
      case 'h264': {
        return this.parseEnum(H264Profile, profile);
      }
      case 'h265':
      case 'hevc': {
        return this.parseEnum(HevcProfile, profile);
      }
      case 'av1': {
        return this.parseEnum(Av1Profile, profile);
      }
      default: {
        return null;
      }
    }
  }

  private compareStreams(a: FfprobeStream, b: FfprobeStream): number {
    const d = (b.disposition?.default ?? 0) - (a.disposition?.default ?? 0);
    if (d !== 0) {
      return d;
    }
    return this.parseInt(b.bit_rate) - this.parseInt(a.bit_rate);
  }

  /* Ported from https://code.ffmpeg.org/FFmpeg/FFmpeg/src/commit/5c44245878e235ae64fe87fb9877644856d33d1d/fftools/ffmpeg_filter.c
   * SPDX-License-Identifier: LGPL-2.1-or-later
   * Copyright (c) FFmpeg authors and contributors — https://ffmpeg.org/
   * Modifications: TS port operating on probe-derived packet metadata rather than decoded AVFrames. */
  private cfrOutputFrames(packets: { pts: number; duration: number }[], slotsPerTick: number) {
    packets.sort((a, b) => a.pts - b.pts);
    const firstPts = packets[0].pts;
    let outputFrames = 0;
    let nextPts = 0;
    const history = [0, 0, 0];
    for (const pkt of packets) {
      const syncIpts = (pkt.pts - firstPts) * slotsPerTick;
      const duration = pkt.duration * slotsPerTick;
      let delta0 = syncIpts - nextPts;
      const delta = delta0 + duration;

      if (delta0 < 0 && delta > 0) {
        delta0 = 0;
      }

      let nb = 1;
      let nbPrev = 0;
      if (delta < -1.1) {
        nb = 0;
      } else if (delta > 1.1) {
        nb = Math.round(delta);
        if (delta0 > 1.1) {
          nbPrev = Math.round(delta0 - 0.6);
        }
      }
      outputFrames += nb;
      nextPts += nb;
      history[2] = history[1];
      history[1] = history[0];
      history[0] = nbPrev;
    }
    const median = history.sort((a, b) => a - b)[1];
    return outputFrames + median;
  }
}
