import { spawn } from "node:child_process";
import { promises as fsp } from "node:fs";
import path from "node:path";
import { logger } from "../lib/logger";
import { JobController, CancellationError } from "./jobController";

const YTDLP_BIN = "yt-dlp";

const BROWSER_UA =
  "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";

let cachedAvailable: boolean | null = null;

export async function hasYtDlp(): Promise<boolean> {
  if (cachedAvailable !== null) return cachedAvailable;
  cachedAvailable = await new Promise<boolean>((resolve) => {
    const p = spawn(YTDLP_BIN, ["--version"], { stdio: "ignore" });
    p.once("error", () => resolve(false));
    p.once("exit", (code) => resolve(code === 0));
  });
  return cachedAvailable;
}

export async function extractDirectUrl(pageUrl: string) {
  const info = await listFormats(pageUrl);
  if (!info) return null;
  const direct = info.formats.find((f) => f.kind === "video" && f.code === "best") ?? info.formats[0];
  if (!direct) return null;
  return {
    url: pageUrl,
    fileName: `${sanitizeBaseName(info.title)}.mp4`,
    extractor: info.extractor,
  };
}

interface YtDlpFormat {
  format_id?: string;
  ext?: string;
  height?: number;
  width?: number;
  vcodec?: string;
  acodec?: string;
  filesize?: number;
  filesize_approx?: number;
  protocol?: string;
  tbr?: number;
  abr?: number;
}

interface YtDlpJson {
  title?: string;
  extractor?: string;
  extractor_key?: string;
  thumbnail?: string;
  formats?: YtDlpFormat[];
  duration?: number;
}

export interface FormatOption {
  /** Short code suitable for callback data (e.g. "144", "720", "best", "audio"). */
  code: string;
  /** Human label (e.g. "1080p · ~245 MB"). */
  label: string;
  /** yt-dlp format selector string. */
  selector: string;
  /** "video" or "audio". */
  kind: "video" | "audio";
}

export interface ExtractInfo {
  title: string;
  extractor?: string;
  thumbnail?: string;
  duration?: number;
  formats: FormatOption[];
}

const PRESET_HEIGHTS = [144, 240, 360, 480, 720, 1080, 1440, 2160];

function sanitizeBaseName(name: string): string {
  return name.replace(/[\\/:*?"<>|\x00-\x1f]/g, "_").slice(0, 150).trim() || "video";
}

function approxBytes(value?: number): string {
  if (!value || value <= 0) return "";
  const mb = value / 1024 / 1024;
  if (mb < 1) return ` · ~${(value / 1024).toFixed(0)} KB`;
  if (mb < 1024) return ` · ~${mb.toFixed(0)} MB`;
  return ` · ~${(mb / 1024).toFixed(1)} GB`;
}

/**
 * Calls yt-dlp -J to enumerate available formats, then collapses them into a
 * short list of user-facing quality presets backed by yt-dlp format selectors.
 *
 * Returns null when yt-dlp does not recognize the URL (no extractor matched).
 */
export async function listFormats(pageUrl: string): Promise<ExtractInfo | null> {
  if (!(await hasYtDlp())) return null;

  const json = await runYtDlpJson(pageUrl);
  if (!json) return null;

  const formats = json.formats ?? [];
  const heightsWithVideo = new Set<number>();
  let bestVideoFilesize = 0;
  let hasAudio = false;
  let bestAudioFilesize = 0;

  for (const f of formats) {
    const fs = f.filesize ?? f.filesize_approx ?? 0;
    if (f.vcodec && f.vcodec !== "none" && f.height) {
      heightsWithVideo.add(f.height);
      if (fs > bestVideoFilesize) bestVideoFilesize = fs;
    }
    if (f.acodec && f.acodec !== "none") {
      hasAudio = true;
      if (fs > bestAudioFilesize && (!f.vcodec || f.vcodec === "none")) {
        bestAudioFilesize = fs;
      }
    }
  }

  if (heightsWithVideo.size === 0 && !hasAudio) {
    return null;
  }

  const maxHeight = Math.max(0, ...heightsWithVideo);
  const offered: FormatOption[] = [];

  // Always include "Best" for video sources.
  if (maxHeight > 0) {
    offered.push({
      code: "best",
      label: `Best${approxBytes(bestVideoFilesize)}`,
      selector: "bv*+ba/b",
      kind: "video",
    });
  }

  // For each preset height that the source actually offers (or close to it),
  // pick the best <= preset selector.
  const seen = new Set<number>();
  for (let i = PRESET_HEIGHTS.length - 1; i >= 0; i--) {
    const p = PRESET_HEIGHTS[i]!;
    if (p > maxHeight) continue;
    // Find the actual height in source closest at or below this preset.
    let actualHeight = 0;
    for (const h of heightsWithVideo) {
      if (h <= p && h > actualHeight) actualHeight = h;
    }
    if (actualHeight === 0) continue;
    if (seen.has(actualHeight)) continue;
    seen.add(actualHeight);

    // Approximate filesize: pick the largest format at this height.
    let approx = 0;
    for (const f of formats) {
      if (f.height === actualHeight && f.vcodec && f.vcodec !== "none") {
        const fs = f.filesize ?? f.filesize_approx ?? 0;
        if (fs > approx) approx = fs;
      }
    }
    offered.push({
      code: String(actualHeight),
      label: `${actualHeight}p${approxBytes(approx)}`,
      selector: `bv*[height<=${actualHeight}]+ba/b[height<=${actualHeight}]/b`,
      kind: "video",
    });
  }

  if (hasAudio) {
    offered.push({
      code: "audio",
      label: `🎵 Audio only${approxBytes(bestAudioFilesize)}`,
      selector: "ba/b",
      kind: "audio",
    });
  }

  return {
    title: json.title ?? json.extractor_key ?? "video",
    extractor: json.extractor_key ?? json.extractor,
    thumbnail: json.thumbnail,
    duration: json.duration,
    formats: offered,
  };
}

async function runYtDlpJson(pageUrl: string): Promise<YtDlpJson | null> {
  return new Promise((resolve) => {
    const args = [
      "--no-warnings",
      "--no-playlist",
      "--no-call-home",
      "--quiet",
      "-J",
      "--skip-download",
      "--user-agent",
      BROWSER_UA,
      pageUrl,
    ];
    const proc = spawn(YTDLP_BIN, args, { stdio: ["ignore", "pipe", "pipe"] });
    let stdout = "";
    let stderr = "";
    const killer = setTimeout(() => proc.kill("SIGKILL"), 60_000);
    proc.stdout.on("data", (c: Buffer) => {
      stdout += c.toString();
    });
    proc.stderr.on("data", (c: Buffer) => {
      stderr += c.toString();
    });
    proc.once("error", () => {
      clearTimeout(killer);
      resolve(null);
    });
    proc.once("exit", (code) => {
      clearTimeout(killer);
      if (code !== 0) {
        if (!/Unsupported URL/i.test(stderr)) {
          logger.warn({ code, stderr: stderr.slice(-200) }, "yt-dlp -J failed");
        }
        resolve(null);
        return;
      }
      try {
        const data = JSON.parse(stdout) as YtDlpJson;
        resolve(data);
      } catch (err) {
        logger.warn({ err }, "yt-dlp -J parse failed");
        resolve(null);
      }
    });
  });
}

/**
 * Downloads using yt-dlp directly (handles HLS/DASH, video+audio merging, etc.).
 * Returns the resulting file path. yt-dlp picks the actual extension based on
 * the chosen format / merger output.
 */
export async function downloadWithYtDlp(opts: {
  pageUrl: string;
  selector: string;
  kind: "video" | "audio";
  destDir: string;
  baseName: string;
  controller: JobController;
  onProgress: (transferred: number, total: number) => void;
}): Promise<{ filePath: string; fileName: string; size: number }> {
  const { pageUrl, selector, kind, destDir, baseName, controller, onProgress } =
    opts;
  controller.throwIfCancelled();

  const safeBase = sanitizeBaseName(baseName);
  const outputTemplate = path.join(destDir, `${safeBase}.%(ext)s`);

  const args = [
    "--no-warnings",
    "--no-playlist",
    "--no-call-home",
    "--user-agent",
    BROWSER_UA,
    "-f",
    selector,
    "-o",
    outputTemplate,
    "--newline",
    "--progress",
    "--progress-template",
    "DLPROG %(progress.downloaded_bytes)s/%(progress.total_bytes_estimate)s/%(progress.total_bytes)s",
    "--restrict-filenames",
    "--no-mtime",
    "--no-part",
    "--concurrent-fragments",
    "8",
  ];

  if (kind === "video") {
    args.push("--merge-output-format", "mp4");
  } else {
    args.push("-x", "--audio-format", "mp3", "--audio-quality", "0");
  }

  args.push(pageUrl);

  const beforeFiles = new Set(await fsp.readdir(destDir).catch(() => []));

  return new Promise((resolve, reject) => {
    const proc = spawn(YTDLP_BIN, args, { stdio: ["ignore", "pipe", "pipe"] });
    let stderr = "";
    let stdoutBuf = "";

    let cancelled = false;
    controller.onCancel(() => {
      cancelled = true;
      try {
        proc.kill("SIGKILL");
      } catch {
        // ignore
      }
    });

    proc.stdout.on("data", (chunk: Buffer) => {
      stdoutBuf += chunk.toString();
      let idx: number;
      while ((idx = stdoutBuf.indexOf("\n")) !== -1) {
        const line = stdoutBuf.slice(0, idx);
        stdoutBuf = stdoutBuf.slice(idx + 1);
        const m = /^DLPROG (\S+)\/(\S+)\/(\S+)/.exec(line.trim());
        if (m) {
          const transferred = Number(m[1]);
          const totalEstimate = Number(m[2]);
          const totalReal = Number(m[3]);
          const total = Number.isFinite(totalReal) && totalReal > 0
            ? totalReal
            : Number.isFinite(totalEstimate) && totalEstimate > 0
              ? totalEstimate
              : 0;
          if (Number.isFinite(transferred)) {
            onProgress(transferred, total || transferred);
          }
        }
      }
    });
    proc.stderr.on("data", (c: Buffer) => {
      stderr += c.toString();
    });

    proc.once("error", (err) => {
      reject(err);
    });

    proc.once("exit", async (code) => {
      if (cancelled) {
        reject(new CancellationError());
        return;
      }
      if (code !== 0) {
        const tail = stderr.slice(-600).trim() || "no stderr";
        reject(new Error(`yt-dlp exited with code ${code}: ${tail}`));
        return;
      }
      try {
        const after = await fsp.readdir(destDir);
        const newFiles = after.filter((f) => !beforeFiles.has(f));
        if (newFiles.length === 0) {
          reject(new Error("yt-dlp produced no output file"));
          return;
        }
        // Pick the largest produced file (in case sidecars were created).
        let largest: { name: string; size: number } | null = null;
        for (const name of newFiles) {
          try {
            const st = await fsp.stat(path.join(destDir, name));
            if (!largest || st.size > largest.size) {
              largest = { name, size: st.size };
            }
          } catch {
            // ignore
          }
        }
        if (!largest) {
          reject(new Error("yt-dlp output file disappeared"));
          return;
        }
        // Cleanup any other auxiliary files (subtitles, etc.).
        for (const name of newFiles) {
          if (name === largest.name) continue;
          try {
            await fsp.unlink(path.join(destDir, name));
          } catch {
            // ignore
          }
        }
        resolve({
          filePath: path.join(destDir, largest.name),
          fileName: largest.name,
          size: largest.size,
        });
      } catch (err) {
        reject(err as Error);
      }
    });
  });
}
