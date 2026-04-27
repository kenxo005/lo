import { promises as fsp } from "node:fs";
import os from "node:os";
import path from "node:path";
import { randomBytes, randomUUID } from "node:crypto";
import { Api, TelegramClient } from "telegram";
import { CustomFile } from "telegram/client/uploads.js";
import type { NewMessageEvent } from "telegram/events/index.js";
import type { CallbackQueryEvent } from "telegram/events/CallbackQuery.js";
import { Button } from "telegram/tl/custom/button.js";
import prettyBytes from "pretty-bytes";
import mimeTypes from "mime-types";
import filenamify from "filenamify";
import { logger } from "../lib/logger";
import {
  downloadFile,
  downloadRange,
  hasAria2,
  probeUrl,
  supportsRange,
  type DownloadResult,
  type ProbeResult,
} from "./download";
import {
  extractThumbnailFromUrl,
  generateThumbnail,
  probeVideo,
  probeVideoFromUrl,
} from "./videoMeta";
import { extractDirectUrl, hasYtDlp } from "./extractor";
import { escapeHtml, formatProgress, throttle } from "./progress";
import { JobController, isCancellation } from "./jobController";

// Telegram caps a single bot upload at ~2 GB (FILE_PARTS_INVALID kicks in
// past that). We only ever split when the file is bigger than this, and each
// slice itself fits in the same limit. 1.95 GiB stays comfortably under the
// server-side ceiling and the 8000-part / 512KB-chunk MTProto quirks.
const MAX_PART_BYTES = 1950 * 1024 * 1024;
// Hard ceiling — disk safety guard.
const ABSOLUTE_MAX_BYTES = 16 * 1024 * 1024 * 1024;
const URL_REGEX_GLOBAL = /(https?:\/\/[^\s<>"']+)/gi;
const URL_REGEX_SINGLE = /https?:\/\/[^\s<>"']+/i;
const PROGRESS_EDIT_INTERVAL_MS = 2500;
// MTProto protocol cap: each chunk must be ≤ 512KB (524288 bytes). gramjs
// already uses 512KB for any file ≥ 750MB — this is the hard ceiling, not a
// gramjs setting we can raise.
//
// To go faster we instead pump more 512KB chunks in parallel:
//   workers × 512KB = "in flight per part"
//   workers × 512KB × concurrency = "in flight per job"
// gramjs documents 16 workers as stable. Many forks safely run 24–32 on
// large files; above that you start hitting FLOOD_WAIT_X. Both knobs are
// env-configurable so you can tune for your DC / network.
const UPLOAD_WORKERS = (() => {
  const raw = process.env["UPLOAD_WORKERS"];
  const n = raw ? Number(raw) : 96;
  if (!Number.isFinite(n) || n < 1) return 96;
  return Math.min(256, Math.max(1, Math.floor(n))); // safety clamp 1..256
})();
const UPLOAD_PART_CONCURRENCY = (() => {
  const raw = process.env["UPLOAD_PART_CONCURRENCY"];
  const n = raw ? Number(raw) : 6;
  if (!Number.isFinite(n) || n < 1) return 6;
  return Math.min(12, Math.max(1, Math.floor(n))); // safety clamp 1..12
})();
// How many parallel sub-range fetchers a single slice download uses, and how
// big each sub-range request is. Smaller sub-ranges = more frequent fresh TCP
// connections, which sidesteps the per-connection throttling most CDNs apply
// once a connection has moved a few hundred MB. 16 MB per request × 32
// parallel = ~512 MB in flight per slice, every connection retired in 5–15 s.
const DOWNLOAD_PARALLELISM = (() => {
  const raw = process.env["DOWNLOAD_PARALLELISM"];
  const n = raw ? Number(raw) : 32;
  if (!Number.isFinite(n) || n < 1) return 32;
  return Math.min(128, Math.max(1, Math.floor(n)));
})();
const DOWNLOAD_SUBRANGE_BYTES = (() => {
  const raw = process.env["DOWNLOAD_SUBRANGE_MB"];
  const n = raw ? Number(raw) : 16;
  if (!Number.isFinite(n) || n < 1) return 16 * 1024 * 1024;
  return Math.min(256, Math.max(1, Math.floor(n))) * 1024 * 1024;
})();
const PENDING_TTL_MS = 10 * 60 * 1000;

let aria2Available: boolean | null = null;

async function getAria2Available(): Promise<boolean> {
  if (aria2Available === null) {
    aria2Available = await hasAria2();
    logger.info({ aria2Available }, "aria2 availability checked");
  }
  return aria2Available;
}

interface PendingPrompt {
  id: string;
  url: string; // resolved direct URL (post yt-dlp if needed)
  originalUrl: string;
  defaultName: string;
  size: number;
  contentType?: string;
  promptMsgId: number;
  chatKey: string;
  createdAt: number;
}

interface PendingRename {
  url: string;
  defaultName: string;
  promptMsgId: number;
  createdAt: number;
}

interface QueueJob {
  url: string;
  fileName: string;
  withThumbnail: boolean;
}

interface ChatQueue {
  jobs: QueueJob[];
  processing: boolean;
}

interface ActiveJob {
  controller: JobController;
  fileName: string;
}

interface PendingThumbChoice {
  url: string;
  fileName: string;
  promptMsgId: number;
  createdAt: number;
}

const promptsById = new Map<string, PendingPrompt>();
const renameByChat = new Map<string, PendingRename>();
const queueByChat = new Map<string, ChatQueue>();
const activeByChat = new Map<string, ActiveJob>();
const thumbChoiceById = new Map<string, PendingThumbChoice>();

function makeId(): string {
  return randomBytes(6).toString("hex");
}

function chatKey(event: NewMessageEvent | CallbackQueryEvent): string | null {
  const id =
    "message" in event && event.message
      ? event.message.chatId
      : (event as CallbackQueryEvent).chatId;
  if (!id) return null;
  return id.toString();
}

function sanitizeFilename(input: string): string {
  const cleaned = filenamify(input.trim(), { replacement: "_" }).slice(0, 200);
  return cleaned.length > 0 ? cleaned : "download";
}

function pruneExpired(): void {
  const now = Date.now();
  for (const [k, v] of promptsById) {
    if (now - v.createdAt > PENDING_TTL_MS) promptsById.delete(k);
  }
  for (const [k, v] of renameByChat) {
    if (now - v.createdAt > PENDING_TTL_MS) renameByChat.delete(k);
  }
  for (const [k, v] of thumbChoiceById) {
    if (now - v.createdAt > PENDING_TTL_MS) thumbChoiceById.delete(k);
  }
}

function isVideoMime(contentType?: string, fileName?: string): boolean {
  if (contentType?.toLowerCase().startsWith("video/")) return true;
  if (fileName) {
    const m = mimeTypes.lookup(fileName);
    if (m && m.startsWith("video/")) return true;
  }
  return false;
}

function isHtmlLike(contentType?: string): boolean {
  if (!contentType) return false;
  const ct = contentType.toLowerCase();
  return ct.includes("text/html") || ct.includes("application/xhtml");
}

/**
 * Many premium-link CDNs encode an expiry timestamp in the URL query string.
 * Common parameter names are: expiry, expires, exp, e, t, validuntil, ttl_end.
 * Values may be in seconds or milliseconds since epoch.
 *
 * Returns the expired-at Date when the URL is *clearly* past its expiry,
 * or null when no expiry is detected or it's still valid.
 */
function detectUrlExpiry(rawUrl: string): Date | null {
  let u: URL;
  try {
    u = new URL(rawUrl);
  } catch {
    return null;
  }
  const candidates = ["expiry", "expires", "exp", "e", "validuntil", "ttl_end"];
  for (const name of candidates) {
    const v = u.searchParams.get(name);
    if (!v) continue;
    const num = Number(v);
    if (!Number.isFinite(num) || num <= 0) continue;
    // Heuristic: > 1e12 means milliseconds; otherwise seconds.
    const ms = num > 1e12 ? num : num * 1000;
    // Sanity: must be in the year 2001..2100 range to count as a real epoch.
    if (ms < 1e12 || ms > 4e12) continue;
    if (ms < Date.now()) {
      return new Date(ms);
    }
    return null; // valid expiry, not expired
  }
  return null;
}

export function getWelcomeMessage(): string {
  return [
    "👋 <b>URL → Telegram Uploader</b>",
    "",
    "Send any direct download URL or a video page URL — I'll grab the file at full speed and post it back here.",
    "",
    "<b>Commands</b>",
    "• <code>/start</code> — show this help",
    "• <code>/help</code> — same as /start",
    "• <code>/queue</code> — show your queued downloads",
    "• <code>/cancel</code> — clear the queue and any pending rename",
    "• <code>/cancelupload</code> — abort the current download/upload",
    "",
    "<b>How it works</b>",
    "1. Paste a URL (or several at once — they'll queue up)",
    "2. Tap <b>✅ Use this name</b>, <b>✏️ Rename</b>, or <b>❌ Cancel</b>",
    "3. For videos, choose <b>🖼 With thumbnail</b> or <b>⚡ Skip thumbnail</b> (faster)",
    "4. I download with up to 16 parallel connections (aria2)",
    "5. I upload with 16 parallel workers — videos get duration + streaming support",
    "",
    "<b>Page URLs</b>",
    "If you send a video page (YouTube, Vimeo, and 1000+ other sites supported by yt-dlp), I'll auto-extract the direct media URL.",
    "",
    "<b>Big files</b>",
    `Files over ~2 GB are split automatically (each part ≤ ${prettyBytes(MAX_PART_BYTES)}). Re-join with <code>cat file.part*</code> on Linux/Mac or 7-Zip on Windows.`,
    "",
    `Hard ceiling per URL: <b>${prettyBytes(ABSOLUTE_MAX_BYTES)}</b>.`,
  ].join("\n");
}

function buildPromptButtons(id: string, isVideo: boolean) {
  if (isVideo) {
    return [
      [
        Button.inline("🖼 With thumbnail", Buffer.from(`kw:${id}`)),
        Button.inline("⚡ Skip thumbnail", Buffer.from(`ks:${id}`)),
      ],
      [
        Button.inline("✏️ Rename", Buffer.from(`rn:${id}`)),
        Button.inline("❌ Cancel", Buffer.from(`cn:${id}`)),
      ],
    ];
  }
  return [
    [
      Button.inline("✅ Use this name", Buffer.from(`ok:${id}`)),
      Button.inline("✏️ Rename", Buffer.from(`rn:${id}`)),
    ],
    [Button.inline("❌ Cancel", Buffer.from(`cn:${id}`))],
  ];
}

function buildThumbChoiceButtons(id: string) {
  return [
    [
      Button.inline("🖼 With thumbnail", Buffer.from(`tw:${id}`)),
      Button.inline("⚡ Skip thumbnail", Buffer.from(`ts:${id}`)),
    ],
    [Button.inline("❌ Cancel", Buffer.from(`tc:${id}`))],
  ];
}

function buildPromptText(opts: {
  fileName: string;
  size: number;
  contentType?: string;
  originalUrl: string;
  resolvedUrl: string;
  extractor?: string;
}): string {
  const sizeText = opts.size > 0 ? prettyBytes(opts.size) : "unknown";
  const splitNote =
    opts.size > MAX_PART_BYTES
      ? `\n\n📦 Larger than 2 GB — will be split into ${Math.ceil(
          opts.size / MAX_PART_BYTES,
        )} parts.`
      : "";
  const extractorNote = opts.extractor
    ? `\n🛠 Extracted via <code>${escapeHtml(opts.extractor)}</code>`
    : "";
  return [
    `📥 <b>Detected</b>`,
    `<code>${escapeHtml(opts.fileName)}</code>`,
    `Size: <b>${sizeText}</b>`,
    opts.contentType ? `Type: <code>${escapeHtml(opts.contentType)}</code>` : "",
    `🔗 <a href="${escapeHtml(opts.originalUrl)}">source</a>${extractorNote}${splitNote}`,
  ]
    .filter(Boolean)
    .join("\n");
}

export async function handleMessage(
  client: TelegramClient,
  event: NewMessageEvent,
): Promise<void> {
  pruneExpired();
  const message = event.message;
  if (!message || message.out) return;
  const text = (message.text ?? "").trim();
  if (!text) return;

  const peerEntity = await message.getInputChat();
  if (!peerEntity) return;
  const peer = peerEntity as unknown as Api.TypeInputPeer;
  const key = chatKey(event);

  // Awaiting a typed-in new filename?
  if (key) {
    const pending = renameByChat.get(key);
    if (pending) {
      const cmd = text.toLowerCase();
      if (cmd === "/cancel") {
        renameByChat.delete(key);
        await client.sendMessage(peer, { message: "❌ Cancelled." });
        return;
      }
      if (URL_REGEX_SINGLE.test(text)) {
        renameByChat.delete(key);
      } else if (text.startsWith("/")) {
        await client.sendMessage(peer, {
          message:
            "I'm waiting for a new filename. Send the filename (with extension) or <code>/cancel</code>.",
          parseMode: "html",
        });
        return;
      } else {
        let newName = sanitizeFilename(text);
        if (!path.extname(newName)) {
          const defaultExt = path.extname(pending.defaultName);
          if (defaultExt) newName += defaultExt;
        }
        renameByChat.delete(key);
        await proceedAfterFilename(client, peer, key, pending.url, newName);
        return;
      }
    }
  }

  if (text === "/start" || text === "/help") {
    await client.sendMessage(peer, {
      message: getWelcomeMessage(),
      parseMode: "html",
      linkPreview: false,
    });
    return;
  }

  if (text === "/queue") {
    await sendQueueStatus(client, peer, key);
    return;
  }

  if (text === "/cancel") {
    if (key) {
      renameByChat.delete(key);
      const q = queueByChat.get(key);
      if (q) {
        const cleared = q.jobs.length;
        q.jobs.length = 0;
        await client.sendMessage(peer, {
          message: cleared
            ? `🛑 Cleared ${cleared} queued job(s). Current upload (if any) will finish — use /cancelupload to abort it too.`
            : "Nothing pending.",
        });
        return;
      }
    }
    await client.sendMessage(peer, { message: "Nothing pending." });
    return;
  }

  if (text === "/cancelupload") {
    if (key) {
      const active = activeByChat.get(key);
      if (active) {
        active.controller.cancel("user requested /cancelupload");
        await client.sendMessage(peer, {
          message: `🛑 Cancelling current job: <code>${escapeHtml(active.fileName)}</code>`,
          parseMode: "html",
        });
        return;
      }
    }
    await client.sendMessage(peer, {
      message: "Nothing actively running. Use /cancel to clear the queue.",
    });
    return;
  }

  const matches = Array.from(text.matchAll(URL_REGEX_GLOBAL)).map((m) => m[1]!);
  if (matches.length === 0) {
    await client.sendMessage(peer, {
      message:
        "❓ Please send a valid http(s) URL — either a direct file link or a video page URL.",
      parseMode: "html",
    });
    return;
  }

  // De-duplicate
  const seen = new Set<string>();
  const urls = matches.filter((u) => {
    if (seen.has(u)) return false;
    seen.add(u);
    return true;
  });

  for (const url of urls) {
    await promptForUrl(client, event, url);
  }
}

async function sendQueueStatus(
  client: TelegramClient,
  peer: Api.TypeInputPeer,
  key: string | null,
): Promise<void> {
  const q = key ? queueByChat.get(key) : undefined;
  if (!q || (!q.processing && q.jobs.length === 0)) {
    await client.sendMessage(peer, { message: "📭 Queue is empty." });
    return;
  }
  const lines: string[] = [`📋 <b>Queue</b>`];
  if (q.processing) lines.push(`▶️ Processing: 1 job in flight`);
  q.jobs.forEach((j, i) => {
    lines.push(`${i + 1}. <code>${escapeHtml(j.fileName)}</code>`);
  });
  await client.sendMessage(peer, {
    message: lines.join("\n"),
    parseMode: "html",
    linkPreview: false,
  });
}

async function promptForUrl(
  client: TelegramClient,
  event: NewMessageEvent,
  originalUrl: string,
): Promise<void> {
  const message = event.message;
  const peerEntity = await message.getInputChat();
  if (!peerEntity) return;
  const peer = peerEntity as unknown as Api.TypeInputPeer;
  const key = chatKey(event);
  if (!key) return;

  const probing = await client.sendMessage(peer, {
    message: `🔍 Inspecting URL...`,
    parseMode: "html",
    linkPreview: false,
  });

  // Catch obviously-dead premium-link CDN URLs before wasting time on probe.
  const expiredAt = detectUrlExpiry(originalUrl);
  if (expiredAt) {
    await client.editMessage(peer, {
      message: probing.id,
      text: [
        `⌛ <b>URL is expired</b>`,
        `The link's <code>expiry</code> timestamp is <b>${escapeHtml(expiredAt.toISOString())}</b>, which has already passed.`,
        ``,
        `Premium-link CDNs sign their URLs with a short-lived token; once it's past the expiry no client can download from it. Open the source page again and copy a fresh link.`,
      ].join("\n"),
      parseMode: "html",
      linkPreview: false,
    });
    return;
  }

  let probe: ProbeResult | null = null;
  let probeError: string | undefined;
  try {
    probe = await probeUrl(originalUrl);
  } catch (err) {
    probeError = err instanceof Error ? err.message : String(err);
  }

  // If the URL looks like a webpage (or probe failed), try yt-dlp.
  let resolvedUrl = originalUrl;
  let extractor: string | undefined;
  const looksLikePage =
    !probe ||
    isHtmlLike(probe.contentType) ||
    (probe.size === 0 && !!probeError);

  if (looksLikePage && (await hasYtDlp())) {
    await client.editMessage(peer, {
      message: probing.id,
      text: `🛠 Looks like a webpage — trying to extract a direct video URL...`,
      parseMode: "html",
    });
    const extracted = await extractDirectUrl(originalUrl);
    if (extracted) {
      resolvedUrl = extracted.url;
      extractor = extracted.extractor;
      // Re-probe the direct URL.
      try {
        const reprobe = await probeUrl(resolvedUrl);
        // Prefer yt-dlp's filename (it has the title).
        reprobe.fileName = extracted.fileName;
        probe = reprobe;
        probeError = undefined;
      } catch (err) {
        probeError = err instanceof Error ? err.message : String(err);
      }
    }
  }

  if (!probe) {
    await client.editMessage(peer, {
      message: probing.id,
      text: `❌ Could not inspect the URL.\n<code>${escapeHtml((probeError ?? "unknown").slice(0, 300))}</code>`,
      parseMode: "html",
    });
    return;
  }

  if (probe.size > 0 && probe.size > ABSOLUTE_MAX_BYTES) {
    await client.editMessage(peer, {
      message: probing.id,
      text: `❌ File is too large.\n\nDetected: <b>${prettyBytes(probe.size)}</b>\nMax: <b>${prettyBytes(ABSOLUTE_MAX_BYTES)}</b>`,
      parseMode: "html",
    });
    return;
  }

  const id = makeId();
  promptsById.set(id, {
    id,
    url: resolvedUrl,
    originalUrl,
    defaultName: probe.fileName,
    size: probe.size,
    contentType: probe.contentType,
    promptMsgId: probing.id,
    chatKey: key,
    createdAt: Date.now(),
  });

  const isVideo = isVideoMime(probe.contentType, probe.fileName);
  await client.editMessage(peer, {
    message: probing.id,
    text: buildPromptText({
      fileName: probe.fileName,
      size: probe.size,
      contentType: probe.contentType,
      originalUrl,
      resolvedUrl,
      extractor,
    }),
    parseMode: "html",
    linkPreview: false,
    buttons: buildPromptButtons(id, isVideo),
  });
}

export async function handleCallback(
  client: TelegramClient,
  event: CallbackQueryEvent,
): Promise<void> {
  pruneExpired();
  const data = event.data;
  if (!data) return;
  const dataStr = data.toString("utf8");
  const sep = dataStr.indexOf(":");
  if (sep === -1) return;
  const action = dataStr.slice(0, sep);
  const id = dataStr.slice(sep + 1);

  const peerEntity = await event.getInputChat();
  if (!peerEntity) return;
  const peer = peerEntity as unknown as Api.TypeInputPeer;
  const key = chatKey(event);

  // Thumbnail-choice prompt actions (post-rename, when source is video).
  if (action === "tw" || action === "ts" || action === "tc") {
    const choice = thumbChoiceById.get(id);
    if (!choice) {
      await event.answer({ message: "This prompt has expired.", alert: true });
      return;
    }
    thumbChoiceById.delete(id);
    if (action === "tc") {
      await event.answer({ message: "Cancelled" });
      try {
        await client.editMessage(peer, {
          message: choice.promptMsgId,
          text: `❌ Cancelled.`,
          parseMode: "html",
          buttons: [],
        });
      } catch {
        // ignore
      }
      return;
    }
    const withThumbnail = action === "tw";
    await event.answer({ message: withThumbnail ? "With thumbnail" : "Skipping thumbnail" });
    try {
      await client.editMessage(peer, {
        message: choice.promptMsgId,
        text: `📥 <b>Queued</b> ${withThumbnail ? "🖼" : "⚡"}\n<code>${escapeHtml(choice.fileName)}</code>`,
        parseMode: "html",
        linkPreview: false,
        buttons: [],
      });
    } catch {
      // ignore
    }
    if (!key) return;
    await enqueueAndStart(client, peer, key, {
      url: choice.url,
      fileName: choice.fileName,
      withThumbnail,
    });
    return;
  }

  const prompt = promptsById.get(id);
  if (!prompt) {
    await event.answer({ message: "This prompt has expired.", alert: true });
    return;
  }

  switch (action) {
    case "ok":
    case "kw":
    case "ks": {
      promptsById.delete(id);
      const withThumbnail = action === "kw"; // "ok" non-video, "ks" video skip
      const noteIcon = action === "kw" ? "🖼" : action === "ks" ? "⚡" : "";
      await event.answer({ message: "Queued" });
      try {
        await client.editMessage(peer, {
          message: prompt.promptMsgId,
          text: `📥 <b>Queued</b> ${noteIcon}\n<code>${escapeHtml(prompt.defaultName)}</code>`,
          parseMode: "html",
          linkPreview: false,
          buttons: [],
        });
      } catch {
        // ignore
      }
      if (!key) return;
      await enqueueAndStart(client, peer, key, {
        url: prompt.url,
        fileName: prompt.defaultName,
        withThumbnail,
      });
      return;
    }
    case "rn": {
      promptsById.delete(id);
      if (key) {
        renameByChat.set(key, {
          url: prompt.url,
          defaultName: prompt.defaultName,
          promptMsgId: prompt.promptMsgId,
          createdAt: Date.now(),
        });
      }
      await event.answer({ message: "Send the new filename" });
      try {
        await client.editMessage(peer, {
          message: prompt.promptMsgId,
          text: [
            `✏️ <b>Send the new filename</b>`,
            `Default: <code>${escapeHtml(prompt.defaultName)}</code>`,
            ``,
            `Include the extension you want (e.g. <code>movie.mp4</code>).`,
            `If you skip the extension I'll keep the original.`,
            `Send <code>/cancel</code> to abort.`,
          ].join("\n"),
          parseMode: "html",
          linkPreview: false,
          buttons: [],
        });
      } catch {
        // ignore
      }
      return;
    }
    case "cn": {
      promptsById.delete(id);
      if (key) renameByChat.delete(key);
      await event.answer({ message: "Cancelled" });
      try {
        await client.editMessage(peer, {
          message: prompt.promptMsgId,
          text: `❌ Cancelled.`,
          parseMode: "html",
          buttons: [],
        });
      } catch {
        // ignore
      }
      return;
    }
    default:
      await event.answer({});
      return;
  }
}

/**
 * After the user has settled on a filename (either default or renamed), decide
 * whether to ask the thumbnail question (for videos) or queue immediately.
 */
async function proceedAfterFilename(
  client: TelegramClient,
  peer: Api.TypeInputPeer,
  key: string,
  url: string,
  fileName: string,
): Promise<void> {
  if (!isVideoMime(undefined, fileName)) {
    await enqueueAndStart(client, peer, key, {
      url,
      fileName,
      withThumbnail: false,
    });
    return;
  }
  const id = makeId();
  const prompt = await client.sendMessage(peer, {
    message: [
      `🎬 <b>Thumbnail?</b>`,
      `<code>${escapeHtml(fileName)}</code>`,
      ``,
      `🖼 <b>With thumbnail</b> — generates a preview frame (~1–5s extra)`,
      `⚡ <b>Skip thumbnail</b> — faster, no preview image`,
    ].join("\n"),
    parseMode: "html",
    linkPreview: false,
    buttons: buildThumbChoiceButtons(id),
  });
  thumbChoiceById.set(id, {
    url,
    fileName,
    promptMsgId: prompt.id,
    createdAt: Date.now(),
  });
}

async function enqueueAndStart(
  client: TelegramClient,
  peer: Api.TypeInputPeer,
  key: string,
  job: QueueJob,
): Promise<void> {
  let q = queueByChat.get(key);
  if (!q) {
    q = { jobs: [], processing: false };
    queueByChat.set(key, q);
  }
  q.jobs.push(job);
  if (q.processing) {
    const position = q.jobs.length; // job we just pushed
    await client.sendMessage(peer, {
      message: `🕐 <b>Queued</b> (position ${position})\n<code>${escapeHtml(job.fileName)}</code>\nWill start when current job finishes.`,
      parseMode: "html",
      linkPreview: false,
    });
    return;
  }
  // Start the worker
  void runQueue(client, peer, key);
}

async function runQueue(
  client: TelegramClient,
  peer: Api.TypeInputPeer,
  key: string,
): Promise<void> {
  const q = queueByChat.get(key);
  if (!q || q.processing) return;
  q.processing = true;
  try {
    while (q.jobs.length > 0) {
      const job = q.jobs.shift()!;
      const controller = new JobController();
      activeByChat.set(key, { controller, fileName: job.fileName });
      try {
        await processUrl(
          client,
          peer,
          job.url,
          job.fileName,
          job.withThumbnail,
          controller,
        );
      } catch (err) {
        logger.error({ err, job }, "queue job failed");
      } finally {
        if (activeByChat.get(key)?.controller === controller) {
          activeByChat.delete(key);
        }
      }
    }
  } finally {
    q.processing = false;
  }
}

async function processUrl(
  client: TelegramClient,
  peer: Api.TypeInputPeer,
  url: string,
  fileName: string,
  withThumbnail: boolean,
  controller: JobController,
): Promise<void> {
  const status = await client.sendMessage(peer, {
    message: [
      `📥 <b>Starting download</b>`,
      `<code>${escapeHtml(fileName)}</code>`,
    ].join("\n"),
    parseMode: "html",
    linkPreview: false,
  });

  const editStatus = async (html: string) => {
    try {
      await client.editMessage(peer, {
        message: status.id,
        text: html,
        parseMode: "html",
        linkPreview: false,
      });
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      if (!msg.includes("MESSAGE_NOT_MODIFIED")) {
        logger.warn({ err }, "edit status failed");
      }
    }
  };

  const tempDir = path.join(os.tmpdir(), `tgbot-${randomUUID()}`);
  await fsp.mkdir(tempDir, { recursive: true });

  let downloaded: DownloadResult | null = null;
  try {
    const probe = await probeUrl(url);
    const expectedSize = probe.size;

    if (expectedSize > ABSOLUTE_MAX_BYTES) {
      await editStatus(
        `❌ File is too large (${prettyBytes(expectedSize)}). Hard ceiling is ${prettyBytes(ABSOLUTE_MAX_BYTES)}.`,
      );
      return;
    }

    // STREAMING PIPELINE — for files larger than the Telegram per-upload cap
    // (~2 GB) we slice on the fly so the whole file never lands on disk
    // (the container has only ~6 GB of write quota). Each slice is fetched
    // via HTTP Range, uploaded, deleted, then the next one starts.
    // Peak disk = 2 slices (~4 GB) thanks to look-ahead.
    if (expectedSize > MAX_PART_BYTES) {
      const rangeOk = await supportsRange(url);
      if (rangeOk) {
        await streamDownloadAndUploadInParts(client, peer, editStatus, {
          url,
          fileName,
          totalSize: expectedSize,
          tempDir,
          controller,
        });
        return;
      }
      logger.warn(
        { url, expectedSize },
        "server does not support Range — falling back to whole-file download (may exceed disk quota)",
      );
    }

    const downloadStartedAt = Date.now();
    const downloadThrottle = throttle<{ transferred: number; total: number }>(
      async (v) => {
        await editStatus(
          formatProgress({
            phase: "Downloading",
            fileName,
            transferred: v.transferred,
            total: v.total,
            startedAt: downloadStartedAt,
            url,
          }),
        );
      },
      PROGRESS_EDIT_INTERVAL_MS,
    );

    const useAria2 = await getAria2Available();
    downloaded = await downloadFile({
      url,
      destDir: tempDir,
      fileName,
      expectedSize,
      preferAria2: useAria2,
      controller,
      onProgress: (transferred, total) => {
        downloadThrottle.schedule({ transferred, total });
      },
    });
    await downloadThrottle.flush();

    controller.throwIfCancelled();

    if (downloaded.size > ABSOLUTE_MAX_BYTES) {
      await editStatus(
        `❌ Downloaded file is over the hard ceiling (${prettyBytes(downloaded.size)}).`,
      );
      return;
    }

    const downloadElapsed = (Date.now() - downloadStartedAt) / 1000;
    const downloadSpeed = downloaded.size / Math.max(0.001, downloadElapsed);

    if (downloaded.size > MAX_PART_BYTES) {
      await uploadInParts(client, peer, editStatus, downloaded, {
        downloadSpeed,
        downloadElapsed,
        tempDir,
        controller,
      });
    } else {
      await uploadSingle(client, peer, editStatus, downloaded, {
        downloadSpeed,
        downloadElapsed,
        tempDir,
        controller,
        withThumbnail,
      });
    }
  } catch (err) {
    if (isCancellation(err)) {
      logger.info({ url, fileName }, "job cancelled by user");
      await editStatus(
        `🛑 <b>Cancelled</b>\n<code>${escapeHtml(fileName)}</code>`,
      );
      return;
    }
    const errMsg = err instanceof Error ? err.message : String(err);
    logger.error({ err, url, fileName }, "processUrl failed");
    await editStatus(
      `❌ <b>Failed</b>\n<code>${escapeHtml(errMsg.slice(0, 600))}</code>`,
    );
  } finally {
    try {
      await fsp.rm(tempDir, { recursive: true, force: true });
    } catch (err) {
      logger.warn({ err, tempDir }, "tempDir cleanup failed");
    }
  }
}

async function uploadSingle(
  client: TelegramClient,
  peer: Api.TypeInputPeer,
  editStatus: (html: string) => Promise<void>,
  downloaded: DownloadResult,
  meta: {
    downloadSpeed: number;
    downloadElapsed: number;
    tempDir: string;
    controller: JobController;
    withThumbnail: boolean;
  },
): Promise<void> {
  const { controller, withThumbnail } = meta;
  const resolvedMime =
    mimeTypes.lookup(downloaded.fileName) || "application/octet-stream";
  const isVideo = resolvedMime.startsWith("video/");
  const isImage = resolvedMime.startsWith("image/");
  const isAudio = resolvedMime.startsWith("audio/");

  let videoMeta = null;
  let thumbPath: string | null = null;
  if (isVideo) {
    await editStatus(
      [
        `✅ <b>Download complete</b>`,
        `<code>${escapeHtml(downloaded.fileName)}</code>`,
        `Size: ${prettyBytes(downloaded.size)}`,
        `Avg: ${prettyBytes(meta.downloadSpeed)}/s in ${meta.downloadElapsed.toFixed(1)}s`,
        ``,
        withThumbnail
          ? `🎬 Reading video metadata + generating thumbnail...`
          : `🎬 Reading video metadata (skipping thumbnail)...`,
      ].join("\n"),
    );
    videoMeta = await probeVideo(downloaded.filePath);
    controller.throwIfCancelled();
    if (withThumbnail && videoMeta && videoMeta.duration > 0) {
      thumbPath = await generateThumbnail({
        videoPath: downloaded.filePath,
        duration: videoMeta.duration,
        destDir: meta.tempDir,
      });
      controller.throwIfCancelled();
    }
  }

  await editStatus(
    [
      `✅ <b>Download complete</b>`,
      `<code>${escapeHtml(downloaded.fileName)}</code>`,
      `Size: ${prettyBytes(downloaded.size)}`,
      `Avg: ${prettyBytes(meta.downloadSpeed)}/s in ${meta.downloadElapsed.toFixed(1)}s`,
      ``,
      `📤 Preparing upload (${UPLOAD_WORKERS} parallel workers)...`,
    ].join("\n"),
  );

  const uploadStartedAt = Date.now();
  const uploadThrottle = throttle<{ transferred: number; total: number }>(
    async (v) => {
      await editStatus(
        formatProgress({
          phase: "Uploading",
          fileName: downloaded.fileName,
          transferred: v.transferred,
          total: v.total,
          startedAt: uploadStartedAt,
        }),
      );
    },
    PROGRESS_EDIT_INTERVAL_MS,
  );

  const customFile = new CustomFile(
    downloaded.fileName,
    downloaded.size,
    downloaded.filePath,
  );

  const attributes: Api.TypeDocumentAttribute[] = [
    new Api.DocumentAttributeFilename({ fileName: downloaded.fileName }),
  ];
  if (isVideo && videoMeta) {
    attributes.push(
      new Api.DocumentAttributeVideo({
        duration: Math.max(1, Math.round(videoMeta.duration)),
        w: videoMeta.width,
        h: videoMeta.height,
        supportsStreaming: true,
      }),
    );
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const sendOpts: Record<string, any> = {
    file: customFile,
    caption: `<code>${escapeHtml(downloaded.fileName)}</code>\n${prettyBytes(downloaded.size)}`,
    parseMode: "html",
    forceDocument: !isVideo && !isImage && !isAudio,
    supportsStreaming: isVideo,
    attributes,
    workers: UPLOAD_WORKERS,
    progressCallback: (progress: number) => {
      if (controller.cancelled) {
        // Throwing inside the gramjs progressCallback aborts the upload.
        throw new Error("__CANCELLED__");
      }
      const transferred = Math.round(progress * downloaded.size);
      uploadThrottle.schedule({
        transferred,
        total: downloaded.size,
      });
    },
  };
  if (thumbPath) {
    try {
      await fsp.access(thumbPath);
      // gramjs's thumb handling only accepts a string path, browser File, or
      // Buffer — passing a CustomFile triggers "Could not create file from
      // [object Object]". Pass the path directly.
      sendOpts["thumb"] = thumbPath;
    } catch {
      // ignore
    }
  }

  try {
    await client.sendFile(peer, sendOpts as unknown as Parameters<typeof client.sendFile>[1]);
  } catch (err) {
    if (controller.cancelled || (err instanceof Error && err.message === "__CANCELLED__")) {
      controller.cancel();
      controller.throwIfCancelled();
    }
    throw err;
  }
  await uploadThrottle.flush();

  // Upload succeeded — free disk immediately rather than waiting for the
  // tempDir cleanup at the end of processUrl.
  try {
    await fsp.unlink(downloaded.filePath);
  } catch (err) {
    logger.warn({ err, filePath: downloaded.filePath }, "failed to unlink uploaded file");
  }
  if (thumbPath) {
    try {
      await fsp.unlink(thumbPath);
    } catch {
      // ignore
    }
  }

  const uploadElapsed = (Date.now() - uploadStartedAt) / 1000;
  const uploadSpeed = downloaded.size / Math.max(0.001, uploadElapsed);

  const summary = [
    `✅ <b>Done!</b>`,
    `<code>${escapeHtml(downloaded.fileName)}</code>`,
    `Size: ${prettyBytes(downloaded.size)}`,
    ``,
    `⬇️ Download: ${prettyBytes(meta.downloadSpeed)}/s (${meta.downloadElapsed.toFixed(1)}s)`,
    `⬆️ Upload: ${prettyBytes(uploadSpeed)}/s (${uploadElapsed.toFixed(1)}s)`,
  ];
  if (isVideo && videoMeta) {
    summary.push(
      `🎬 Video: ${Math.round(videoMeta.duration)}s · ${videoMeta.width}×${videoMeta.height}${thumbPath ? " · 🖼 thumb" : ""}`,
    );
  }
  await editStatus(summary.join("\n"));
}

async function uploadInParts(
  client: TelegramClient,
  peer: Api.TypeInputPeer,
  editStatus: (html: string) => Promise<void>,
  downloaded: DownloadResult,
  meta: {
    downloadSpeed: number;
    downloadElapsed: number;
    tempDir: string;
    controller: JobController;
  },
): Promise<void> {
  const { controller } = meta;
  const partCount = Math.ceil(downloaded.size / MAX_PART_BYTES);

  // Pre-compute part metadata (offsets, names) without touching disk yet.
  const ext = path.extname(downloaded.fileName);
  const stem = path.basename(downloaded.fileName, ext);
  const pad = String(partCount).length;
  const partsMeta = Array.from({ length: partCount }, (_, i) => {
    const start = i * MAX_PART_BYTES;
    const end = Math.min(start + MAX_PART_BYTES, downloaded.size);
    const size = end - start;
    const fileName = `${stem}.part${String(i + 1).padStart(pad, "0")}of${String(partCount).padStart(pad, "0")}${ext}`;
    return { index: i + 1, total: partCount, start, end, size, fileName };
  });

  const concurrency = Math.min(UPLOAD_PART_CONCURRENCY, partCount);

  // Try to extract one thumbnail from the source so every part shows the
  // same poster frame in Telegram. Skipped silently for non-video files.
  let thumbPath: string | null = null;
  try {
    const v = await probeVideo(downloaded.filePath);
    if (v && v.duration > 0 && v.width > 0) {
      thumbPath = await generateThumbnail({
        videoPath: downloaded.filePath,
        duration: v.duration,
        destDir: meta.tempDir,
      });
    }
  } catch (err) {
    logger.warn({ err }, "thumbnail extraction failed for sliced upload");
    thumbPath = null;
  }

  await editStatus(
    [
      `✅ <b>Download complete</b>`,
      `<code>${escapeHtml(downloaded.fileName)}</code>`,
      `Size: ${prettyBytes(downloaded.size)}`,
      `Avg: ${prettyBytes(meta.downloadSpeed)}/s in ${meta.downloadElapsed.toFixed(1)}s`,
      ``,
      `📦 Will upload as ${partCount} parts (≤ ${prettyBytes(MAX_PART_BYTES)} each)`,
      thumbPath ? `🖼 Thumbnail attached to every part` : `(non-video — no thumbnail)`,
      `🧹 Each part is extracted, uploaded, then immediately deleted to save disk.`,
      `📤 Starting multi-part upload (${concurrency} part${concurrency > 1 ? "s" : ""} in parallel × ${UPLOAD_WORKERS} workers)...`,
    ].join("\n"),
  );

  const uploadStartedAt = Date.now();
  let totalUploaded = 0;
  let completedParts = 0;
  const inFlight = new Map<number, { fileName: string; transferred: number; total: number }>();

  // Shared progress reporter — aggregates state across all parallel parts.
  const progressThrottle = throttle<void>(async () => {
    const inFlightTransferred = Array.from(inFlight.values()).reduce(
      (sum, p) => sum + p.transferred,
      0,
    );
    const cumulative = totalUploaded + inFlightTransferred;
    const elapsed = (Date.now() - uploadStartedAt) / 1000;
    const speed = cumulative / Math.max(0.001, elapsed);
    const lines = [
      `⬆️ <b>Multi-part upload</b> (${completedParts}/${partCount} done, ${inFlight.size} in flight)`,
      `<code>${escapeHtml(downloaded.fileName)}</code>`,
      `Total: ${prettyBytes(cumulative)} / ${prettyBytes(downloaded.size)} · ${prettyBytes(speed)}/s`,
      ``,
    ];
    for (const [idx, p] of Array.from(inFlight.entries()).sort((a, b) => a[0] - b[0])) {
      const pct = p.total > 0 ? ((p.transferred / p.total) * 100).toFixed(1) : "0.0";
      lines.push(
        `· part ${idx}/${partCount}: ${prettyBytes(p.transferred)} / ${prettyBytes(p.total)} (${pct}%)`,
      );
    }
    await editStatus(lines.join("\n"));
  }, PROGRESS_EDIT_INTERVAL_MS);

  // Worker-pool scheduler: spawn `concurrency` workers, each pulls the next
  // part index from a shared cursor.
  let cursor = 0;
  const runWorker = async (): Promise<void> => {
    while (true) {
      controller.throwIfCancelled();
      const myIndex = cursor++;
      if (myIndex >= partsMeta.length) return;
      const part = partsMeta[myIndex];
      if (!part) return;
      const partPath = path.join(meta.tempDir, part.fileName);

      // Step 1: slice this part out of the original to a temp file.
      await extractRange({
        sourcePath: downloaded.filePath,
        destPath: partPath,
        start: part.start,
        end: part.end,
      });
      controller.throwIfCancelled();

      // Step 2: upload it. Track per-part progress in the shared map.
      inFlight.set(part.index, { fileName: part.fileName, transferred: 0, total: part.size });
      progressThrottle.schedule();

      const customFile = new CustomFile(part.fileName, part.size, partPath);
      try {
        const sendOpts: Record<string, unknown> = {
          file: customFile,
          caption: `<code>${escapeHtml(part.fileName)}</code>\nPart ${part.index} / ${part.total} · ${prettyBytes(part.size)}`,
          parseMode: "html",
          forceDocument: true,
          attributes: [
            new Api.DocumentAttributeFilename({ fileName: part.fileName }),
          ],
          workers: UPLOAD_WORKERS,
          progressCallback: ((progress: number) => {
            if (controller.cancelled) {
              throw new Error("__CANCELLED__");
            }
            const transferred = Math.round(progress * part.size);
            const slot = inFlight.get(part.index);
            if (slot) slot.transferred = transferred;
            progressThrottle.schedule();
          }) as never,
        };
        if (thumbPath) sendOpts.thumb = thumbPath;
        await client.sendFile(peer, sendOpts as unknown as Parameters<typeof client.sendFile>[1]);
      } catch (err) {
        // Make sure a failed part doesn't leave a stale temp file behind.
        try { await fsp.unlink(partPath); } catch { /* ignore */ }
        inFlight.delete(part.index);
        if (controller.cancelled || (err instanceof Error && err.message === "__CANCELLED__")) {
          controller.cancel();
          controller.throwIfCancelled();
        }
        throw err;
      }

      // Step 3: ack — free the slice immediately, update aggregate counters.
      inFlight.delete(part.index);
      totalUploaded += part.size;
      completedParts += 1;
      try {
        await fsp.unlink(partPath);
      } catch (err) {
        logger.warn({ err, partPath }, "failed to unlink uploaded part");
      }
      progressThrottle.schedule();
    }
  };

  const workers = Array.from({ length: concurrency }, () => runWorker());
  try {
    await Promise.all(workers);
  } finally {
    await progressThrottle.flush();
  }

  // All parts uploaded — now we can safely delete the original.
  try {
    await fsp.unlink(downloaded.filePath);
  } catch (err) {
    logger.warn({ err, filePath: downloaded.filePath }, "failed to unlink original");
  }
  if (thumbPath) {
    try { await fsp.unlink(thumbPath); } catch { /* ignore */ }
  }

  const uploadElapsed = (Date.now() - uploadStartedAt) / 1000;
  const uploadSpeed = downloaded.size / Math.max(0.001, uploadElapsed);

  await editStatus(
    [
      `✅ <b>Done!</b>`,
      `<code>${escapeHtml(downloaded.fileName)}</code>`,
      `Total: ${prettyBytes(downloaded.size)} in ${partCount} parts`,
      ``,
      `⬇️ Download: ${prettyBytes(meta.downloadSpeed)}/s (${meta.downloadElapsed.toFixed(1)}s)`,
      `⬆️ Upload: ${prettyBytes(uploadSpeed)}/s (${uploadElapsed.toFixed(1)}s)`,
      ``,
      `💡 Re-join with: <code>cat ${escapeHtml(path.basename(downloaded.fileName, path.extname(downloaded.fileName)))}.part*</code>`,
    ].join("\n"),
  );
}

/**
 * Streaming pipeline for files larger than one slice.
 *
 * Uses HTTP Range requests to fetch one slice (≤ MAX_PART_BYTES) at a time,
 * uploads it to Telegram, deletes it, and moves to the next. With a 1-slice
 * look-ahead the next download runs in parallel with the current upload, so
 * the network is kept busy in both directions while peak disk usage stays
 * around 2 slices (~3 GB).
 *
 * This bypasses the container's ~6 GB write quota that otherwise breaks any
 * file > ~6 GB during the whole-file aria2 download.
 */
async function streamDownloadAndUploadInParts(
  client: TelegramClient,
  peer: Api.TypeInputPeer,
  editStatus: (html: string) => Promise<void>,
  opts: {
    url: string;
    fileName: string;
    totalSize: number;
    tempDir: string;
    controller: JobController;
  },
): Promise<void> {
  const { url, fileName, totalSize, tempDir, controller } = opts;
  const partCount = Math.ceil(totalSize / MAX_PART_BYTES);
  const ext = path.extname(fileName);
  const stem = path.basename(fileName, ext);
  const pad = String(partCount).length;
  const partsMeta = Array.from({ length: partCount }, (_, i) => {
    const start = i * MAX_PART_BYTES;
    const end = Math.min(start + MAX_PART_BYTES, totalSize);
    const size = end - start;
    const partFileName = `${stem}.part${String(i + 1).padStart(pad, "0")}of${String(partCount).padStart(pad, "0")}${ext}`;
    return {
      index: i + 1,
      total: partCount,
      start,
      end,
      size,
      fileName: partFileName,
    };
  });

  await editStatus(
    [
      `🔁 <b>Streaming pipeline</b>`,
      `<code>${escapeHtml(fileName)}</code>`,
      `Size: ${prettyBytes(totalSize)} → ${partCount} parts (≤ ${prettyBytes(MAX_PART_BYTES)} each)`,
      ``,
      `Each slice is downloaded → uploaded to Telegram → deleted before the`,
      `next one starts. The whole file never lands on disk, so size is only`,
      `limited by your network and Telegram's ${prettyBytes(ABSOLUTE_MAX_BYTES)} ceiling.`,
      ``,
      `🖼 Generating thumbnail from source...`,
    ].join("\n"),
  );

  // Probe the URL once. If it's a video, grab a single frame via ffmpeg
  // (HTTP Range — no full download needed) and reuse the same thumb on
  // every part so they all show the same poster in Telegram.
  let thumbPath: string | null = null;
  try {
    const meta = await probeVideoFromUrl(url);
    if (meta && meta.duration > 0 && meta.width > 0) {
      thumbPath = await extractThumbnailFromUrl({
        url,
        duration: meta.duration,
        destDir: tempDir,
      });
      if (thumbPath) {
        logger.info({ thumbPath }, "thumbnail ready for sliced parts");
      }
    }
  } catch (err) {
    logger.warn({ err }, "thumbnail extraction failed — continuing without");
    thumbPath = null;
  }

  await editStatus(
    [
      `🔁 <b>Streaming pipeline</b>`,
      `<code>${escapeHtml(fileName)}</code>`,
      `Size: ${prettyBytes(totalSize)} → ${partCount} parts (≤ ${prettyBytes(MAX_PART_BYTES)} each)`,
      thumbPath ? `🖼 Thumbnail attached to every part` : `(non-video — no thumbnail)`,
      ``,
      `📥 Fetching part 1/${partCount}...`,
    ].join("\n"),
  );

  type Slice = (typeof partsMeta)[number];
  type Ready = { part: Slice; partPath: string };
  const overallStarted = Date.now();
  let totalDownloaded = 0;
  let totalUploaded = 0;
  let completedParts = 0;
  let currentDlBytes = 0;
  let currentDlTotal = 0;

  // Throttled status renderer — single source of truth for what the user sees.
  let phase: "downloading" | "uploading" = "downloading";
  let activePartIndex = 1;
  let uploadBytes = 0;
  let uploadTotal = 0;
  const renderThrottle = throttle<void>(async () => {
    const dlMb = totalDownloaded + currentDlBytes;
    const upMb = totalUploaded + uploadBytes;
    const elapsed = (Date.now() - overallStarted) / 1000;
    const dlRate = dlMb / Math.max(0.001, elapsed);
    const upRate = upMb / Math.max(0.001, elapsed);
    const lines = [
      `🔁 <b>Streaming part ${activePartIndex}/${partCount}</b>`,
      `<code>${escapeHtml(fileName)}</code>`,
      ``,
      `📥 Total downloaded: ${prettyBytes(dlMb)} / ${prettyBytes(totalSize)} · avg ${prettyBytes(dlRate)}/s`,
      `📤 Total uploaded: ${prettyBytes(upMb)} / ${prettyBytes(totalSize)} · avg ${prettyBytes(upRate)}/s`,
      `✅ Parts done: ${completedParts}/${partCount}`,
      ``,
    ];
    if (phase === "downloading" && currentDlTotal > 0) {
      const pct = ((currentDlBytes / currentDlTotal) * 100).toFixed(1);
      lines.push(
        `⬇️ Current slice: ${prettyBytes(currentDlBytes)} / ${prettyBytes(currentDlTotal)} (${pct}%)`,
      );
    } else if (phase === "uploading" && uploadTotal > 0) {
      const pct = ((uploadBytes / uploadTotal) * 100).toFixed(1);
      lines.push(
        `⬆️ Current slice: ${prettyBytes(uploadBytes)} / ${prettyBytes(uploadTotal)} (${pct}%)`,
      );
    }
    await editStatus(lines.join("\n"));
  }, PROGRESS_EDIT_INTERVAL_MS);

  // Helper: download one slice. Returns when the file is fully on disk.
  const startDownload = async (part: Slice): Promise<Ready> => {
    const partPath = path.join(tempDir, part.fileName);
    currentDlBytes = 0;
    currentDlTotal = part.size;
    phase = "downloading";
    activePartIndex = part.index;
    renderThrottle.schedule();
    await downloadRange({
      url,
      destPath: partPath,
      start: part.start,
      end: part.end,
      parallelism: DOWNLOAD_PARALLELISM,
      chunkBytes: DOWNLOAD_SUBRANGE_BYTES,
      controller,
      onProgress: (transferred) => {
        currentDlBytes = transferred;
        renderThrottle.schedule();
      },
    });
    totalDownloaded += part.size;
    currentDlBytes = 0;
    currentDlTotal = 0;
    return { part, partPath };
  };

  // Kick off slice 1 download.
  let nextDownload: Promise<Ready> | null = startDownload(partsMeta[0]!);

  for (let i = 0; i < partsMeta.length; i++) {
    controller.throwIfCancelled();
    const ready = await nextDownload!;
    nextDownload = null;

    // Start downloading the NEXT slice in parallel with this slice's upload.
    if (i + 1 < partsMeta.length) {
      const nextPart = partsMeta[i + 1]!;
      nextDownload = startDownload(nextPart).catch((err) => {
        // Re-throw on consume to fail the whole job
        throw err;
      });
    }

    // Upload the ready slice.
    const { part, partPath } = ready;
    phase = "uploading";
    activePartIndex = part.index;
    uploadBytes = 0;
    uploadTotal = part.size;
    renderThrottle.schedule();

    const customFile = new CustomFile(part.fileName, part.size, partPath);
    try {
      const sendOpts: Record<string, unknown> = {
        file: customFile,
        caption: `<code>${escapeHtml(part.fileName)}</code>\nPart ${part.index} / ${part.total} · ${prettyBytes(part.size)}`,
        parseMode: "html",
        forceDocument: true,
        attributes: [
          new Api.DocumentAttributeFilename({ fileName: part.fileName }),
        ],
        workers: UPLOAD_WORKERS,
        progressCallback: ((progress: number) => {
          if (controller.cancelled) {
            throw new Error("__CANCELLED__");
          }
          uploadBytes = Math.round(progress * part.size);
          renderThrottle.schedule();
        }) as never,
      };
      if (thumbPath) sendOpts.thumb = thumbPath;
      await client.sendFile(peer, sendOpts as unknown as Parameters<typeof client.sendFile>[1]);
    } catch (err) {
      try { await fsp.unlink(partPath); } catch { /* ignore */ }
      if (controller.cancelled || (err instanceof Error && err.message === "__CANCELLED__")) {
        controller.cancel();
        controller.throwIfCancelled();
      }
      throw err;
    }

    // Free this slice's disk immediately.
    totalUploaded += part.size;
    completedParts += 1;
    uploadBytes = 0;
    uploadTotal = 0;
    try {
      await fsp.unlink(partPath);
    } catch (err) {
      logger.warn({ err, partPath }, "failed to unlink uploaded streaming part");
    }
    renderThrottle.schedule();
  }

  await renderThrottle.flush();

  // Clean up the shared thumbnail now that all parts are uploaded.
  if (thumbPath) {
    try { await fsp.unlink(thumbPath); } catch { /* ignore */ }
  }

  const elapsed = (Date.now() - overallStarted) / 1000;
  const avgRate = totalSize / Math.max(0.001, elapsed);
  await editStatus(
    [
      `✅ <b>Done!</b>`,
      `<code>${escapeHtml(fileName)}</code>`,
      `Total: ${prettyBytes(totalSize)} in ${partCount} parts (streaming pipeline)`,
      ``,
      `⏱ Total time: ${elapsed.toFixed(1)}s · avg ${prettyBytes(avgRate)}/s end-to-end`,
      ``,
      `💡 Re-join with: <code>cat ${escapeHtml(stem)}.part*</code>`,
    ].join("\n"),
  );
}

/**
 * Stream-copy the byte range [start, end) from `sourcePath` to `destPath`.
 * Uses createReadStream with explicit start/end so we never load the whole
 * file into memory and never copy bytes outside the requested slice.
 */
async function extractRange(opts: {
  sourcePath: string;
  destPath: string;
  start: number;
  end: number; // exclusive
}): Promise<void> {
  const { createReadStream, createWriteStream } = await import("node:fs");
  const { pipeline } = await import("node:stream/promises");
  const reader = createReadStream(opts.sourcePath, {
    start: opts.start,
    end: opts.end - 1, // createReadStream's `end` is inclusive
  });
  const writer = createWriteStream(opts.destPath);
  await pipeline(reader, writer);
}
