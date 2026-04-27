import { spawn } from "node:child_process";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { promises as fsp } from "node:fs";
import { logger } from "../lib/logger";

export interface VideoMeta {
  duration: number;
  width: number;
  height: number;
}

/**
 * Probes a remote URL with ffprobe (it issues HTTP Range requests under the
 * hood) to discover whether it's a video and, if so, its duration / dims.
 * Returns null for non-video sources or when probing fails.
 */
export async function probeVideoFromUrl(url: string): Promise<VideoMeta | null> {
  return new Promise((resolve) => {
    const proc = spawn(
      "ffprobe",
      [
        "-v",
        "error",
        "-headers",
        "User-Agent: Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36\\r\\n",
        "-select_streams",
        "v:0",
        "-show_entries",
        "format=duration:stream=width,height,duration",
        "-of",
        "json",
        url,
      ],
      { stdio: ["ignore", "pipe", "pipe"] },
    );

    let stdout = "";
    let stderr = "";
    proc.stdout.on("data", (c: Buffer) => (stdout += c.toString()));
    proc.stderr.on("data", (c: Buffer) => (stderr += c.toString()));

    // Cap probe at 60s — some servers/CDNs take a while on first range request.
    const timer = setTimeout(() => {
      try {
        proc.kill("SIGKILL");
      } catch {
        // ignore
      }
    }, 60_000);

    proc.once("error", (err) => {
      clearTimeout(timer);
      logger.warn({ err }, "ffprobe URL spawn failed");
      resolve(null);
    });

    proc.once("exit", (code) => {
      clearTimeout(timer);
      if (code !== 0) {
        logger.info(
          { code, stderr: stderr.slice(-200), url },
          "ffprobe URL exit non-zero — not treating as video",
        );
        resolve(null);
        return;
      }
      try {
        const data = JSON.parse(stdout) as {
          streams?: Array<{ width?: number; height?: number; duration?: string }>;
          format?: { duration?: string };
        };
        const stream = data.streams?.[0];
        if (!stream || !stream.width || !stream.height) {
          resolve(null);
          return;
        }
        const durStr = data.format?.duration ?? stream.duration;
        const duration = durStr !== undefined ? Number(durStr) : NaN;
        if (!Number.isFinite(duration) || duration <= 0) {
          resolve({ duration: 0, width: stream.width, height: stream.height });
          return;
        }
        resolve({ duration, width: stream.width, height: stream.height });
      } catch (err) {
        logger.warn({ err }, "ffprobe URL parse failed");
        resolve(null);
      }
    });
  });
}

/**
 * Extracts video duration and dimensions using ffprobe.
 * Returns null when the file is not a video or ffprobe fails.
 */
export async function probeVideo(filePath: string): Promise<VideoMeta | null> {
  return new Promise((resolve) => {
    const proc = spawn(
      "ffprobe",
      [
        "-v",
        "error",
        "-select_streams",
        "v:0",
        "-show_entries",
        "format=duration:stream=width,height,duration",
        "-of",
        "json",
        filePath,
      ],
      { stdio: ["ignore", "pipe", "pipe"] },
    );

    let stdout = "";
    let stderr = "";
    proc.stdout.on("data", (chunk: Buffer) => {
      stdout += chunk.toString();
    });
    proc.stderr.on("data", (chunk: Buffer) => {
      stderr += chunk.toString();
    });

    proc.once("error", (err) => {
      logger.warn({ err }, "ffprobe spawn failed");
      resolve(null);
    });

    proc.once("exit", (code) => {
      if (code !== 0) {
        logger.warn({ code, stderr: stderr.slice(-200) }, "ffprobe exit non-zero");
        resolve(null);
        return;
      }
      try {
        const data = JSON.parse(stdout) as {
          streams?: Array<{ width?: number; height?: number; duration?: string }>;
          format?: { duration?: string };
        };
        const stream = data.streams?.[0];
        if (!stream || !stream.width || !stream.height) {
          resolve(null);
          return;
        }
        const durStr = data.format?.duration ?? stream.duration;
        const duration = durStr !== undefined ? Number(durStr) : NaN;
        if (!Number.isFinite(duration) || duration <= 0) {
          resolve({ duration: 0, width: stream.width, height: stream.height });
          return;
        }
        resolve({
          duration,
          width: stream.width,
          height: stream.height,
        });
      } catch (err) {
        logger.warn({ err }, "ffprobe parse failed");
        resolve(null);
      }
    });
  });
}

/**
 * Generates a JPEG thumbnail directly from a remote URL using ffmpeg's HTTP
 * range support — no full download needed. Used for sliced video parts so
 * each one shows the same poster frame in Telegram.
 */
export async function extractThumbnailFromUrl(opts: {
  url: string;
  duration: number;
  destDir: string;
}): Promise<string | null> {
  const { url, duration, destDir } = opts;
  const seek = duration > 2 ? Math.max(1, Math.floor(duration * 0.1)) : 0;
  const thumbPath = path.join(destDir, `thumb-${randomUUID()}.jpg`);

  const ok = await new Promise<boolean>((resolve) => {
    const proc = spawn(
      "ffmpeg",
      [
        "-y",
        "-user_agent",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "-ss",
        String(seek),
        "-i",
        url,
        "-frames:v",
        "1",
        "-vf",
        "scale='min(320,iw)':-2",
        "-q:v",
        "5",
        "-f",
        "mjpeg",
        thumbPath,
      ],
      { stdio: ["ignore", "ignore", "pipe"] },
    );
    let stderr = "";
    proc.stderr.on("data", (c: Buffer) => {
      stderr += c.toString();
    });
    // Cap at 30s — pulling a single frame should be quick.
    const timer = setTimeout(() => {
      try { proc.kill("SIGKILL"); } catch { /* ignore */ }
    }, 30_000);
    proc.once("error", () => { clearTimeout(timer); resolve(false); });
    proc.once("exit", (code) => {
      clearTimeout(timer);
      if (code !== 0) {
        logger.info({ code, stderr: stderr.slice(-200) }, "URL thumb ffmpeg failed");
        resolve(false);
        return;
      }
      resolve(true);
    });
  });

  if (!ok) {
    try { await fsp.unlink(thumbPath); } catch { /* ignore */ }
    return null;
  }
  try {
    const stat = await fsp.stat(thumbPath);
    if (stat.size > 200 * 1024) {
      // Telegram caps thumbs at 200 KB. Re-encode lower if we overshot.
      await new Promise<void>((resolve) => {
        const proc = spawn("ffmpeg", ["-y", "-i", thumbPath, "-vf", "scale='min(320,iw)':-2", "-q:v", "10", thumbPath + ".small.jpg"], { stdio: "ignore" });
        proc.once("error", () => resolve());
        proc.once("exit", () => resolve());
      });
      try { await fsp.rename(thumbPath + ".small.jpg", thumbPath); } catch { /* ignore */ }
    }
  } catch {
    return null;
  }
  return thumbPath;
}

/**
 * Generates a JPEG thumbnail at ~10% into the video, scaled to max 320px wide.
 * Returns the absolute path to the thumb on success, or null on failure.
 *
 * Telegram requires JPEG, ≤200KB, and recommends ≤320×320 for video thumbs.
 */
export async function generateThumbnail(opts: {
  videoPath: string;
  duration: number;
  destDir: string;
}): Promise<string | null> {
  const { videoPath, duration, destDir } = opts;
  const seek = duration > 2 ? Math.max(1, Math.floor(duration * 0.1)) : 0;
  const thumbPath = path.join(destDir, `thumb-${randomUUID()}.jpg`);

  const ok = await new Promise<boolean>((resolve) => {
    const proc = spawn(
      "ffmpeg",
      [
        "-y",
        "-ss",
        String(seek),
        "-i",
        videoPath,
        "-frames:v",
        "1",
        "-vf",
        "scale='min(320,iw)':-2",
        "-q:v",
        "5",
        "-f",
        "mjpeg",
        thumbPath,
      ],
      { stdio: ["ignore", "ignore", "pipe"] },
    );
    let stderr = "";
    proc.stderr.on("data", (c: Buffer) => {
      stderr += c.toString();
    });
    proc.once("error", () => resolve(false));
    proc.once("exit", (code) => {
      if (code !== 0) {
        logger.warn({ code, stderr: stderr.slice(-200) }, "thumb ffmpeg failed");
        resolve(false);
        return;
      }
      resolve(true);
    });
  });

  if (!ok) {
    try {
      await fsp.unlink(thumbPath);
    } catch {
      // ignore
    }
    return null;
  }

  try {
    const stat = await fsp.stat(thumbPath);
    // Hard cap at 200KB to satisfy Telegram.
    if (stat.size > 200 * 1024) {
      // Re-encode at lower quality if needed.
      await new Promise<void>((resolve) => {
        const proc = spawn(
          "ffmpeg",
          [
            "-y",
            "-i",
            thumbPath,
            "-vf",
            "scale='min(320,iw)':-2",
            "-q:v",
            "10",
            thumbPath + ".small.jpg",
          ],
          { stdio: "ignore" },
        );
        proc.once("error", () => resolve());
        proc.once("exit", () => resolve());
      });
      try {
        await fsp.rename(thumbPath + ".small.jpg", thumbPath);
      } catch {
        // ignore
      }
    }
  } catch {
    return null;
  }

  return thumbPath;
}
