import { spawn } from "node:child_process";
import type { JobController } from "./jobController";
import { CancellationError } from "./jobController";
import { createWriteStream, promises as fsp } from "node:fs";
import path from "node:path";
import { pipeline } from "node:stream/promises";
import got from "got";
import contentDisposition from "content-disposition";
import mimeTypes from "mime-types";
import filenamify from "filenamify";
import { logger } from "../lib/logger";

const ARIA2_BIN = "aria2c";

const ARIA2_EXIT_CODES: Record<number, string> = {
  1: "unknown error",
  2: "timeout",
  3: "resource not found",
  4: "too many 'not found' errors",
  5: "download too slow (lowest-speed-limit)",
  6: "network problem",
  7: "unfinished downloads",
  8: "server doesn't support resume",
  9: "not enough disk space",
  10: "piece length mismatch",
  11: "downloading same file already",
  13: "file already exists",
  14: "file rename failed",
  15: "could not open existing file",
  16: "could not create or truncate file",
  17: "file I/O error",
  18: "could not create directory",
  19: "name resolution failed",
  22: "bad HTTP response header",
  23: "too many redirects",
  24: "HTTP authorization failed",
  29: "server overloaded (HTTP 503)",
  32: "checksum validation failed",
};

// A realistic browser UA — many CDNs reject default Node/aria2 UAs.
const BROWSER_UA =
  "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";

const BROWSER_HEADERS: Record<string, string> = {
  "user-agent": BROWSER_UA,
  accept: "*/*",
  "accept-language": "en-US,en;q=0.9",
};

export type ProgressCallback = (transferred: number, total: number) => void;

export interface DownloadResult {
  filePath: string;
  fileName: string;
  size: number;
}

export interface ProbeResult {
  fileName: string;
  size: number;
  contentType?: string;
}

function extractFilenameFromHeaders(
  headers: Record<string, string | string[] | undefined>,
): string | undefined {
  const cd = headers["content-disposition"];
  if (typeof cd !== "string") return undefined;
  try {
    const parsed = contentDisposition.parse(cd);
    const fn = parsed.parameters?.["filename"];
    if (typeof fn === "string" && fn.length > 0) return fn;
  } catch {
    // ignore
  }
  return undefined;
}

function fileNameFromUrl(url: string): string | undefined {
  try {
    const u = new URL(url);
    const last = u.pathname.split("/").filter(Boolean).pop();
    if (!last) return undefined;
    return decodeURIComponent(last);
  } catch {
    return undefined;
  }
}

function maybeAddExtension(name: string, contentType?: string): string {
  if (path.extname(name)) return name;
  if (!contentType) return name;
  const ext = mimeTypes.extension(contentType);
  if (!ext) return name;
  return `${name}.${ext}`;
}

export async function probeUrl(url: string): Promise<ProbeResult> {
  let size = 0;
  let contentType: string | undefined;
  let fileName: string | undefined;
  let lastError: string | undefined;

  // Try HEAD first
  try {
    const head = await got.head(url, {
      followRedirect: true,
      throwHttpErrors: false,
      timeout: { request: 30_000 },
      headers: BROWSER_HEADERS,
    });
    if (head.statusCode < 400) {
      const lenHeader = head.headers["content-length"];
      if (lenHeader) size = Number(lenHeader);
      const ct = head.headers["content-type"];
      if (typeof ct === "string") contentType = ct.split(";")[0]!.trim();
      fileName = extractFilenameFromHeaders(head.headers);
    } else {
      lastError = `HEAD returned ${head.statusCode}`;
    }
  } catch (err) {
    lastError = err instanceof Error ? err.message : String(err);
    logger.warn({ err, url }, "HEAD failed, will try GET range");
  }

  // If HEAD didn't give us a size or filename, try GET with a Range probe (1 byte)
  if (size === 0 || !fileName) {
    try {
      const res = await got(url, {
        method: "GET",
        followRedirect: true,
        throwHttpErrors: false,
        timeout: { request: 30_000 },
        headers: { ...BROWSER_HEADERS, range: "bytes=0-0" },
      });
      if (res.statusCode < 400) {
        const range = res.headers["content-range"];
        if (typeof range === "string") {
          // bytes 0-0/12345
          const slash = range.lastIndexOf("/");
          if (slash !== -1) {
            const total = Number(range.slice(slash + 1));
            if (Number.isFinite(total)) size = total;
          }
        } else {
          const lenHeader = res.headers["content-length"];
          if (lenHeader && size === 0) size = Number(lenHeader);
        }
        const ct = res.headers["content-type"];
        if (typeof ct === "string" && !contentType)
          contentType = ct.split(";")[0]!.trim();
        if (!fileName) fileName = extractFilenameFromHeaders(res.headers);
      } else if (!lastError) {
        lastError = `GET range returned ${res.statusCode}`;
      }
    } catch (err) {
      logger.warn({ err, url }, "GET range probe failed");
      if (!lastError) lastError = err instanceof Error ? err.message : String(err);
    }
  }

  if (!fileName) fileName = fileNameFromUrl(url);
  if (!fileName || fileName.length === 0) fileName = "download";
  fileName = maybeAddExtension(fileName, contentType);
  fileName = filenamify(fileName, { replacement: "_" }).slice(0, 200);

  if (!Number.isFinite(size) || size < 0) size = 0;

  if (size === 0 && lastError) {
    // Surface the probe error in the result so callers can warn the user;
    // it's still legal to attempt the download with unknown size.
    logger.info({ url, lastError }, "probe completed without size");
  }

  return { fileName, size, contentType };
}

export async function hasAria2(): Promise<boolean> {
  return new Promise((resolve) => {
    const p = spawn(ARIA2_BIN, ["--version"], { stdio: "ignore" });
    p.once("error", () => resolve(false));
    p.once("exit", (code) => resolve(code === 0));
  });
}

/**
 * High-speed download using aria2c with 16 parallel connections.
 * Sends a browser-like User-Agent so picky CDNs don't reject the request.
 */
export async function downloadWithAria2(opts: {
  url: string;
  destDir: string;
  fileName: string;
  expectedSize: number;
  onProgress: ProgressCallback;
  controller?: JobController;
}): Promise<DownloadResult> {
  const { url, destDir, fileName, expectedSize, onProgress, controller } = opts;
  if (controller) controller.throwIfCancelled();
  const finalPath = path.join(destDir, fileName);

  const args = [
    "--file-allocation=none",
    "--summary-interval=1",
    "--console-log-level=notice",
    "--download-result=full",
    "--check-certificate=true",
    // Maximum parallelism — 16 connections per server, 16 splits per file.
    "-x",
    "16",
    "-s",
    "16",
    "-j",
    "16",
    "--max-connection-per-server=16",
    "--split=16",
    "--min-split-size=4M",
    "--piece-length=4M",
    // Be very patient with retries — large downloads on flaky CDNs benefit
    // from many retry attempts. We do NOT set --lowest-speed-limit because
    // it makes aria2 abort with code 5 the moment a CDN throttles briefly,
    // which then forces the slow single-stream fallback.
    "--max-tries=30",
    "--retry-wait=5",
    "--max-file-not-found=10",
    "--connect-timeout=60",
    "--timeout=300",
    "--allow-overwrite=true",
    "--auto-file-renaming=false",
    "--continue=true",
    "--remote-time=true",
    "--enable-http-keep-alive=true",
    "--enable-http-pipelining=false",
    "--http-accept-gzip=false",
    "--reuse-uri=true",
    `--user-agent=${BROWSER_UA}`,
    "--header=Accept: */*",
    "--header=Accept-Language: en-US,en;q=0.9",
    "--dir",
    destDir,
    "--out",
    fileName,
    url,
  ];

  return new Promise<DownloadResult>((resolve, reject) => {
    const proc = spawn(ARIA2_BIN, args, { stdio: ["ignore", "pipe", "pipe"] });
    let stderr = "";
    let stdoutBuf = "";
    let cancelled = false;
    // Last accurate "completed bytes" parsed from aria2c stdout; preferred over
    // fs.stat.size because aria2c writes to multiple offsets via pwrite, which
    // makes the file appear at near-full size immediately (sparse holes).
    let parsedCompleted = 0;
    let parsedTotal = 0;

    if (controller) {
      controller.onCancel(() => {
        cancelled = true;
        try {
          proc.kill("SIGKILL");
        } catch {
          // ignore
        }
      });
    }

    let pollTimer: NodeJS.Timeout | null = null;
    const poll = async () => {
      try {
        const stat = await fsp.stat(finalPath);
        // Prefer aria2c's parsed "completed" counter when we have one.
        // Otherwise fall back to actual on-disk allocation (blocks * 512),
        // which is sparse-aware. stat.size is unreliable here because aria2
        // pwrites at high offsets early, inflating apparent size.
        const actualOnDisk = (stat.blocks ?? 0) * 512;
        const transferred =
          parsedCompleted > 0
            ? parsedCompleted
            : Math.min(actualOnDisk, expectedSize || actualOnDisk);
        const total = parsedTotal > 0 ? parsedTotal : expectedSize;
        onProgress(transferred, total);
      } catch {
        // file may not exist yet
      }
    };
    pollTimer = setInterval(() => {
      void poll();
    }, 500);

    proc.stdout.on("data", (chunk: Buffer) => {
      stdoutBuf += chunk.toString();
      // Keep only the last few KB to avoid unbounded growth on long downloads.
      if (stdoutBuf.length > 8192) stdoutBuf = stdoutBuf.slice(-8192);
      // aria2c summary line example:
      //   [#abc123 1.2GiB/7.4GiB(16%) CN:16 DL:50MiB ETA:2m]
      // We scan ALL matches in the buffer and take the latest.
      const re =
        /\[#[^\s]+\s+([\d.]+)([KMGTP]?i?B)\/([\d.]+)([KMGTP]?i?B)/g;
      let m: RegExpExecArray | null;
      let last: RegExpExecArray | null = null;
      while ((m = re.exec(stdoutBuf)) !== null) last = m;
      if (last) {
        const completed = parseAriaSize(last[1]!, last[2]!);
        const total = parseAriaSize(last[3]!, last[4]!);
        if (completed > 0) parsedCompleted = completed;
        if (total > 0) parsedTotal = total;
      }
    });
    proc.stderr.on("data", (chunk: Buffer) => {
      stderr += chunk.toString();
    });

    proc.once("error", (err) => {
      if (pollTimer) clearInterval(pollTimer);
      reject(err);
    });

    proc.once("exit", async (code) => {
      if (pollTimer) clearInterval(pollTimer);
      if (cancelled) {
        reject(new CancellationError());
        return;
      }
      if (code !== 0) {
        const tail = (stderr.trim() || stdoutBuf.trim()).slice(-800);
        const meaning = ARIA2_EXIT_CODES[code ?? -1] ?? "unknown error";
        reject(
          new Error(
            `aria2c exited with code ${code} (${meaning}): ${tail || "no output"}`,
          ),
        );
        return;
      }
      try {
        const stat = await fsp.stat(finalPath);
        onProgress(stat.size, stat.size);
        resolve({ filePath: finalPath, fileName, size: stat.size });
      } catch (err) {
        reject(err as Error);
      }
    });
  });
}

function parseAriaSize(num: string, unit: string): number {
  const n = Number(num);
  if (!Number.isFinite(n)) return 0;
  const u = unit.toUpperCase();
  const mult: Record<string, number> = {
    B: 1,
    KIB: 1024,
    MIB: 1024 ** 2,
    GIB: 1024 ** 3,
    TIB: 1024 ** 4,
    PIB: 1024 ** 5,
    KB: 1000,
    MB: 1000 ** 2,
    GB: 1000 ** 3,
    TB: 1000 ** 4,
    PB: 1000 ** 5,
  };
  return n * (mult[u] ?? 1);
}

/**
 * Streaming download using got. Reliable fallback when aria2c is unavailable
 * or the URL refuses parallel range requests.
 *
 * Resumes from the existing file size if a partial file is present (e.g. when
 * aria2 failed partway through a multi-GB download). Requires the server to
 * support HTTP Range requests; if not, we restart from byte 0.
 */
export async function downloadWithGot(opts: {
  url: string;
  destDir: string;
  fileName: string;
  expectedSize: number;
  onProgress: ProgressCallback;
  controller?: JobController;
}): Promise<DownloadResult> {
  const { url, destDir, fileName, expectedSize, onProgress, controller } = opts;
  if (controller) controller.throwIfCancelled();
  const finalPath = path.join(destDir, fileName);

  // Resume support: if a partial file already exists, try to continue from
  // that offset using HTTP Range. Falls back to a fresh download (truncating
  // the partial) if the server returns 200 instead of 206.
  //
  // Safety: aria2 multi-connection downloads can leave SPARSE files where
  // apparent size is much greater than actual on-disk data (chunks pwritten
  // at high offsets create holes). Naively resuming from apparent size would
  // produce a corrupted file with zero-filled gaps. So we only resume when
  // the file is contiguously written (blocks*512 ≈ size). Otherwise we wipe
  // it and re-download from byte 0.
  let resumeFrom = 0;
  try {
    const existing = await fsp.stat(finalPath);
    if (existing.isFile() && existing.size > 0) {
      const allocated = (existing.blocks ?? 0) * 512;
      const isContiguous = allocated >= existing.size * 0.98;
      if (isContiguous) {
        resumeFrom = existing.size;
      } else {
        logger.warn(
          {
            apparentSize: existing.size,
            allocated,
            fileName,
          },
          "partial file is sparse — discarding and restarting download to avoid corruption",
        );
        await fsp.unlink(finalPath);
      }
    }
  } catch {
    // no partial file
  }
  // Always clean up aria2's control file before a single-stream download —
  // it would otherwise mislead a future aria2 retry.
  try {
    await fsp.unlink(`${finalPath}.aria2`);
  } catch {
    // ignore
  }

  const headers: Record<string, string> = { ...BROWSER_HEADERS };
  if (resumeFrom > 0) headers.range = `bytes=${resumeFrom}-`;

  // Note: no `request` timeout — large downloads must be allowed to take hours.
  // Newer got versions treat `request: 0` as "0ms = immediate timeout" instead
  // of "no timeout", so we just omit the field.
  const stream = got.stream(url, {
    retry: { limit: 3 },
    headers,
  });

  if (controller) {
    controller.onCancel(() => {
      try {
        stream.destroy(new CancellationError());
      } catch {
        // ignore
      }
    });
  }

  let total = expectedSize;
  let appendMode = false;
  let resumeOffset = 0;
  stream.on("response", (res) => {
    if (resumeFrom > 0 && res.statusCode === 206) {
      // Server honored our resume request — append to the existing file.
      appendMode = true;
      resumeOffset = resumeFrom;
      const range = res.headers["content-range"];
      if (typeof range === "string") {
        const slash = range.lastIndexOf("/");
        if (slash !== -1) {
          const t = Number(range.slice(slash + 1));
          if (Number.isFinite(t)) total = t;
        }
      } else {
        const len = res.headers["content-length"];
        if (len) total = Number(len) + resumeFrom;
      }
      logger.info(
        { resumeFrom, total, fileName },
        "resuming download from partial file",
      );
    } else {
      // No partial, or server ignored Range — start fresh.
      const len = res.headers["content-length"];
      if (len) total = Number(len);
    }
  });
  stream.on("downloadProgress", (p: { transferred: number; total?: number }) => {
    if (!appendMode && p.total) total = p.total;
    onProgress(resumeOffset + p.transferred, total);
  });

  // We can't decide the write mode synchronously (need to see the response),
  // so wait for the response event first, then pipe.
  await new Promise<void>((resolve, reject) => {
    stream.once("response", () => resolve());
    stream.once("error", (err) => reject(err));
  });

  const writeStream = createWriteStream(finalPath, {
    flags: appendMode ? "a" : "w",
  });
  await pipeline(stream, writeStream);

  const stat = await fsp.stat(finalPath);
  return { filePath: finalPath, fileName, size: stat.size };
}

/**
 * Confirms whether a URL supports HTTP Range requests. Required for the
 * range-streaming pipeline (download a slice → upload → delete). Most CDNs
 * do; some live-stream / chunked-transfer endpoints do not.
 */
export async function supportsRange(url: string): Promise<boolean> {
  try {
    const res = await got(url, {
      method: "GET",
      followRedirect: true,
      throwHttpErrors: false,
      timeout: { request: 30_000 },
      headers: { ...BROWSER_HEADERS, range: "bytes=0-0" },
    });
    return res.statusCode === 206;
  } catch {
    return false;
  }
}

/**
 * Downloads a single byte range [start, end) of a remote file to destPath.
 * Internally splits the range into sub-ranges and fetches them in parallel,
 * writing each to its correct offset in the destination file. This gives
 * aria2-like parallelism without needing aria2 to ever see the whole file.
 *
 * Critically, the destination file is exactly (end - start) bytes — we never
 * touch disk for any byte outside this slice. That's what lets us stream
 * 16 GB files through a 6 GB disk quota.
 */
export async function downloadRange(opts: {
  url: string;
  destPath: string;
  start: number;
  end: number; // exclusive
  parallelism: number;
  /**
   * Size in bytes of each individual HTTP Range request. The slice is split
   * into `ceil(sliceSize / chunkBytes)` chunks and `parallelism` workers
   * pull from that queue. Smaller chunkBytes ⇒ TCP connections are recycled
   * more often, which avoids the per-connection bandwidth throttling many
   * CDNs apply once a connection has transferred a few hundred MB.
   * Defaults to 32 MiB.
   */
  chunkBytes?: number;
  onProgress: ProgressCallback;
  controller?: JobController;
}): Promise<void> {
  const { url, destPath, start, end, parallelism, onProgress, controller } = opts;
  const chunkBytes = opts.chunkBytes ?? 32 * 1024 * 1024;
  const sliceSize = end - start;
  if (sliceSize <= 0) throw new Error(`Invalid range: ${start}-${end}`);
  if (controller) controller.throwIfCancelled();

  // Build a queue of fixed-size chunks. Workers pull from the head of the
  // queue, fetch one chunk over a fresh HTTP connection, write it at the
  // correct offset in the destination file, then loop. This naturally
  // rotates connections and rebalances load when one connection slows down.
  interface Chunk {
    subStart: number;
    subEnd: number;
    writeOffset: number;
  }
  const chunks: Chunk[] = [];
  for (let off = 0; off < sliceSize; off += chunkBytes) {
    const subStart = start + off;
    const subEnd = Math.min(start + off + chunkBytes, end);
    chunks.push({ subStart, subEnd, writeOffset: off });
  }
  const workerCount = Math.max(1, Math.min(parallelism, chunks.length));

  const fd = await fsp.open(destPath, "w");
  let transferred = 0;
  let aborted = false;
  let failError: Error | null = null;
  const fail = (err: Error) => {
    if (aborted) return;
    aborted = true;
    failError = err;
  };

  let cursor = 0;
  const nextChunk = (): Chunk | null => {
    if (cursor >= chunks.length) return null;
    return chunks[cursor++]!;
  };

  // Minimum throughput we tolerate per active connection. Anything slower
  // than this for ~6 s in a row → the CDN throttled the connection; we
  // abort it and re-issue the chunk on a fresh TCP socket. This is what
  // keeps the overall download speed flat instead of decaying over time.
  const MIN_BYTES_PER_SECOND = 256 * 1024; // 256 KB/s
  const SLOW_WINDOW_MS = 6_000;

  const fetchOneChunk = async (chunk: Chunk): Promise<void> => {
    const expectedBytes = chunk.subEnd - chunk.subStart;
    const maxAttempts = 8;
    let attempt = 0;
    let chunkOffsetWithinSlice = chunk.writeOffset;
    let remainingStart = chunk.subStart;
    let remainingEnd = chunk.subEnd;
    while (true) {
      attempt++;
      if (aborted) return;
      if (controller) controller.throwIfCancelled();
      let writtenThisAttempt = 0;
      let watchdog: NodeJS.Timeout | null = null;
      let streamRef: ReturnType<typeof got.stream> | null = null;
      try {
        const stream = got.stream(url, {
          headers: {
            ...BROWSER_HEADERS,
            range: `bytes=${remainingStart}-${remainingEnd - 1}`,
          },
          retry: { limit: 0 },
          // Hard "no bytes at all" stall ceiling. Aggressive — we'd much
          // rather rotate to a fresh connection than ride a slow one.
          timeout: { socket: 10_000 },
        });
        streamRef = stream;
        const onCancel = () => {
          try { stream.destroy(new CancellationError()); } catch { /* ignore */ }
        };
        if (controller) controller.onCancel(onCancel);

        await new Promise<void>((resolve, reject) => {
          stream.once("response", (res) => {
            if (res.statusCode === 206) resolve();
            else if (res.statusCode === 200) reject(new Error("server returned 200 — Range not honored"));
            else reject(new Error(`unexpected status ${res.statusCode} for range request`));
          });
          stream.once("error", reject);
        });

        // Slow-progress watchdog: every SLOW_WINDOW_MS we measure how many
        // bytes arrived; if it's below MIN_BYTES_PER_SECOND × window we kill
        // this connection so the worker grabs a fresh one.
        let bytesAtLastTick = 0;
        watchdog = setInterval(() => {
          const delta = writtenThisAttempt - bytesAtLastTick;
          bytesAtLastTick = writtenThisAttempt;
          const minExpected = (MIN_BYTES_PER_SECOND * SLOW_WINDOW_MS) / 1000;
          if (delta < minExpected) {
            try {
              stream.destroy(new Error("connection too slow — rotating"));
            } catch { /* ignore */ }
          }
        }, SLOW_WINDOW_MS);

        for await (const buf of stream) {
          if (aborted) return;
          if (controller) controller.throwIfCancelled();
          const chunkBuf = buf as Buffer;
          await fd.write(
            chunkBuf,
            0,
            chunkBuf.length,
            chunkOffsetWithinSlice + writtenThisAttempt,
          );
          writtenThisAttempt += chunkBuf.length;
          transferred += chunkBuf.length;
          onProgress(transferred, sliceSize);
        }

        if (writtenThisAttempt < expectedBytes) {
          throw new Error(
            `chunk short read: got ${writtenThisAttempt} of ${expectedBytes} bytes`,
          );
        }
        return; // success
      } catch (err) {
        if (err instanceof CancellationError) throw err;
        // We keep what we already wrote on disk and just resume the rest of
        // the chunk on a brand new TCP connection. This means a slow / dead
        // connection costs us at most SLOW_WINDOW_MS of stalled bandwidth,
        // never a re-download of bytes already on disk.
        chunkOffsetWithinSlice += writtenThisAttempt;
        remainingStart += writtenThisAttempt;
        if (remainingStart >= remainingEnd) return; // all bytes accounted for
        if (attempt >= maxAttempts) {
          fail(err instanceof Error ? err : new Error(String(err)));
          return;
        }
        const backoff = Math.min(5_000, 250 * 2 ** Math.min(attempt - 1, 4));
        logger.warn(
          {
            err: err instanceof Error ? err.message : err,
            attempt,
            sub: `${remainingStart}-${remainingEnd}`,
            wrote: writtenThisAttempt,
          },
          "chunk fetch failed/slow; resuming on fresh connection",
        );
        await new Promise((r) => setTimeout(r, backoff));
      } finally {
        if (watchdog) clearInterval(watchdog);
        if (streamRef) {
          try { streamRef.destroy(); } catch { /* ignore */ }
        }
      }
    }
  };

  const runWorker = async (): Promise<void> => {
    while (!aborted) {
      const chunk = nextChunk();
      if (!chunk) return;
      await fetchOneChunk(chunk);
    }
  };

  try {
    await Promise.all(Array.from({ length: workerCount }, () => runWorker()));
    if (failError) throw failError;
  } finally {
    await fd.close();
  }
}

/**
 * Returns true when an incoming size is "close enough" to the probed/expected size.
 * Some servers report slightly different Content-Length than the actual byte count
 * (e.g. transfer-encoding shenanigans), so we allow a 1% tolerance.
 *
 * If the expected size is unknown (0), we accept anything > 0.
 */
function isCompleteSize(actual: number, expected: number): boolean {
  if (actual <= 0) return false;
  if (expected <= 0) return true;
  return actual >= expected * 0.99;
}

export async function downloadFile(opts: {
  url: string;
  destDir: string;
  fileName: string;
  expectedSize: number;
  onProgress: ProgressCallback;
  preferAria2: boolean;
  controller?: JobController;
}): Promise<DownloadResult> {
  const errors: string[] = [];
  const ARIA2_ATTEMPTS = 5;

  // Up to ARIA2_ATTEMPTS aria2 attempts. Each attempt resumes from the
  // partial file via aria2's --continue=true, so it picks up where the last
  // left off instead of restarting from byte 0.
  if (opts.preferAria2) {
    for (let attempt = 1; attempt <= ARIA2_ATTEMPTS; attempt++) {
      if (opts.controller) opts.controller.throwIfCancelled();
      try {
        const r = await downloadWithAria2(opts);
        if (isCompleteSize(r.size, opts.expectedSize)) return r;
        const msg = `aria2c attempt ${attempt} finished but file is ${r.size} bytes (expected ~${opts.expectedSize}).`;
        logger.warn(msg);
        errors.push(msg);
        if (attempt < ARIA2_ATTEMPTS) {
          // brief backoff before retrying — gives flaky CDNs a moment
          await new Promise((res) => setTimeout(res, 2000));
          continue;
        }
      } catch (err) {
        if (err instanceof CancellationError) throw err;
        const msg = err instanceof Error ? err.message : String(err);
        logger.warn({ err, attempt }, "aria2c download attempt failed");
        errors.push(`aria2 attempt ${attempt}: ${msg}`);
        if (attempt < ARIA2_ATTEMPTS) {
          await new Promise((res) => setTimeout(res, 2000));
          continue;
        }
      }
    }
  }

  // Final fallback: single-stream got with HTTP Range resume from whatever
  // bytes aria2 already wrote. This is much slower than aria2 (single TCP
  // stream, no parallelism), so we only reach here if aria2 truly couldn't
  // finish. Avoid wiping the partial file here.
  try {
    const r = await downloadWithGot(opts);
    if (isCompleteSize(r.size, opts.expectedSize)) return r;
    errors.push(
      `got: returned ${r.size} bytes, expected ~${opts.expectedSize}. The URL may be expired or the server is returning an error.`,
    );
  } catch (err) {
    if (err instanceof CancellationError) throw err;
    const msg = err instanceof Error ? err.message : String(err);
    errors.push(`got: ${msg}`);
  }

  throw new Error(
    `Download incomplete after all attempts.\n${errors.join("\n")}`,
  );
}
