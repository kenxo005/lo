import prettyBytes from "pretty-bytes";

export function makeProgressBar(percent: number, width = 20): string {
  const clamped = Math.max(0, Math.min(1, percent));
  const filled = Math.round(clamped * width);
  const empty = width - filled;
  return "█".repeat(filled) + "░".repeat(empty);
}

export function formatProgress(opts: {
  phase: "Downloading" | "Uploading";
  fileName: string;
  transferred: number;
  total: number;
  startedAt: number;
  url?: string;
}): string {
  const { phase, fileName, transferred, total, startedAt, url } = opts;
  const elapsed = Math.max(0.001, (Date.now() - startedAt) / 1000);
  const speed = transferred / elapsed;
  const percent = total > 0 ? transferred / total : 0;
  const bar = makeProgressBar(percent);
  const pctText = total > 0 ? `${(percent * 100).toFixed(1)}%` : "—";
  const sizeText =
    total > 0
      ? `${prettyBytes(transferred)} / ${prettyBytes(total)}`
      : `${prettyBytes(transferred)} / ?`;
  const speedText = `${prettyBytes(speed)}/s`;
  const etaText =
    total > 0 && speed > 0
      ? formatDuration((total - transferred) / speed)
      : "—";

  const lines = [
    `${phase === "Downloading" ? "⬇️" : "⬆️"} <b>${escapeHtml(phase)}</b>`,
    `<code>${escapeHtml(fileName)}</code>`,
    `[${bar}] ${pctText}`,
    `📦 ${sizeText}`,
    `🚀 ${speedText}   ⏱ ${etaText}`,
  ];
  if (url) {
    lines.push(`🔗 <a href="${escapeHtml(url)}">source</a>`);
  }
  return lines.join("\n");
}

export function formatDuration(seconds: number): string {
  if (!Number.isFinite(seconds) || seconds < 0) return "—";
  const s = Math.round(seconds);
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  const rem = s % 60;
  if (m < 60) return `${m}m ${rem}s`;
  const h = Math.floor(m / 60);
  const remM = m % 60;
  return `${h}h ${remM}m`;
}

export function escapeHtml(text: string): string {
  return text
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}

/**
 * Throttles an async update function so it runs at most once per `intervalMs`.
 * Always runs the most recent value once the in-flight call completes.
 * Telegram limits message edits to about 1 per second per chat.
 */
export function throttle<T>(
  fn: (value: T) => Promise<void> | void,
  intervalMs: number,
): {
  schedule: (value: T) => void;
  flush: () => Promise<void>;
} {
  let lastRun = 0;
  let pending: T | null = null;
  let pendingScheduled = false;
  let inflight: Promise<void> | null = null;

  const run = async (value: T) => {
    lastRun = Date.now();
    inflight = Promise.resolve(fn(value)).then(
      () => {
        inflight = null;
      },
      () => {
        inflight = null;
      },
    );
    await inflight;
  };

  const schedule = (value: T) => {
    pending = value;
    if (pendingScheduled || inflight) return;
    const since = Date.now() - lastRun;
    const wait = Math.max(0, intervalMs - since);
    pendingScheduled = true;
    setTimeout(() => {
      pendingScheduled = false;
      const v = pending;
      pending = null;
      if (v !== null) void run(v);
    }, wait);
  };

  const flush = async () => {
    if (inflight) await inflight;
    if (pending !== null) {
      const v = pending;
      pending = null;
      await run(v);
    }
  };

  return { schedule, flush };
}
