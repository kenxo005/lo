import { promises as fsp } from "node:fs";
import path from "node:path";
import { TelegramClient } from "telegram";
import { StringSession } from "telegram/sessions/index.js";
import { NewMessage } from "telegram/events/index.js";
import { CallbackQuery } from "telegram/events/CallbackQuery.js";
import { logger } from "../lib/logger";
import { handleCallback, handleMessage } from "./handlers";

const SESSION_FILE = path.join(process.cwd(), ".telegram-bot-session");

async function loadSession(): Promise<string> {
  try {
    const data = await fsp.readFile(SESSION_FILE, "utf8");
    return data.trim();
  } catch {
    return "";
  }
}

async function saveSession(value: string): Promise<void> {
  try {
    await fsp.writeFile(SESSION_FILE, value, "utf8");
  } catch (err) {
    logger.warn({ err }, "Failed to persist Telegram session");
  }
}

export async function startTelegramBot(): Promise<TelegramClient | null> {
  const apiIdRaw = process.env["TELEGRAM_API_ID"];
  const apiHash = process.env["TELEGRAM_API_HASH"];
  const botToken = process.env["TELEGRAM_BOT_TOKEN"];

  if (!apiIdRaw || !apiHash || !botToken) {
    logger.warn(
      "Telegram bot disabled — TELEGRAM_API_ID, TELEGRAM_API_HASH, or TELEGRAM_BOT_TOKEN missing",
    );
    return null;
  }

  const apiId = Number(apiIdRaw);
  if (!Number.isFinite(apiId) || apiId <= 0) {
    logger.error({ apiIdRaw }, "Invalid TELEGRAM_API_ID");
    return null;
  }

  const sessionString = await loadSession();
  const session = new StringSession(sessionString);

  // Optional MTProto proxy — set MTPROTO_PROXY_HOST + MTPROTO_PROXY_PORT
  // (and MTPROTO_PROXY_SECRET for an MTProto-secret proxy) to route the
  // bot's Telegram connection through it. Useful when the default route to
  // Telegram DCs is slow. If no proxy is configured we'll auto-discover a
  // fast public MTProto proxy (unless AUTO_MTPROTO_PROXY=0).
  const proxyHost = process.env["MTPROTO_PROXY_HOST"];
  const proxyPortRaw = process.env["MTPROTO_PROXY_PORT"];
  const proxySecret = process.env["MTPROTO_PROXY_SECRET"];
  let proxy: Record<string, unknown> | undefined;
  if (proxyHost && proxyPortRaw) {
    const proxyPort = Number(proxyPortRaw);
    if (Number.isFinite(proxyPort) && proxyPort > 0) {
      proxy = {
        ip: proxyHost,
        port: proxyPort,
        MTProxy: true,
        secret: proxySecret ?? "",
      };
      logger.info(
        { proxyHost, proxyPort, hasSecret: !!proxySecret },
        "Using MTProto proxy (from env)",
      );
    } else {
      logger.warn({ proxyPortRaw }, "Invalid MTPROTO_PROXY_PORT, skipping proxy");
    }
  } else if (process.env["AUTO_MTPROTO_PROXY"] === "1") {
    const auto = await pickFastPublicMtprotoProxy();
    if (auto) {
      proxy = {
        ip: auto.host,
        port: auto.port,
        MTProxy: true,
        secret: auto.secret,
      };
      logger.info(
        { proxyHost: auto.host, proxyPort: auto.port, country: auto.country, ping: auto.ping },
        "Using auto-selected public MTProto proxy",
      );
    }
  }

  const client = new TelegramClient(session, apiId, apiHash, {
    connectionRetries: 10,
    requestRetries: 5,
    autoReconnect: true,
    useWSS: false,
    maxConcurrentDownloads: 16,
    ...(proxy ? { proxy: proxy as never } : {}),
  });

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  (client as unknown as { setLogLevel?: (lvl: string) => void }).setLogLevel?.(
    "error",
  );

  await client.start({ botAuthToken: botToken });

  const saved = client.session.save();
  if (typeof saved === "string" && saved !== sessionString) {
    await saveSession(saved);
  }

  const me = await client.getMe();
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const username = (me as any)?.username as string | undefined;
  logger.info({ username }, "Telegram bot logged in");

  client.addEventHandler(async (event) => {
    try {
      await handleMessage(client, event);
    } catch (err) {
      logger.error({ err }, "NewMessage handler failed");
    }
  }, new NewMessage({}));

  client.addEventHandler(async (event) => {
    try {
      await handleCallback(client, event);
    } catch (err) {
      logger.error({ err }, "CallbackQuery handler failed");
    }
  }, new CallbackQuery({}));

  return client;
}

interface MtprotoProxyEntry {
  host: string;
  port: number;
  secret: string;
  country?: string;
  ping?: number;
  uptime?: number;
}

/**
 * Auto-discover a fast public MTProto proxy. Queries mtpro.xyz's directory,
 * picks entries with high uptime + low ping, and returns the best one.
 * Returns null on any failure — caller will then fall back to a direct
 * connection to Telegram.
 *
 * Public proxies are NOT a security boundary: MTProto is end-to-end encrypted
 * with the DC, so a proxy can only see traffic timings and drop the
 * connection — it cannot decrypt or impersonate.
 */
async function pickFastPublicMtprotoProxy(): Promise<MtprotoProxyEntry | null> {
  const directories = [
    "https://mtpro.xyz/api/?type=mtproto",
    "https://mtpro.xyz/api?type=mtproto",
  ];
  for (const url of directories) {
    try {
      const ctrl = new AbortController();
      const t = setTimeout(() => ctrl.abort(), 8000);
      const res = await fetch(url, { signal: ctrl.signal });
      clearTimeout(t);
      if (!res.ok) continue;
      const raw = (await res.json()) as unknown;
      if (!Array.isArray(raw)) continue;
      const entries: MtprotoProxyEntry[] = [];
      for (const item of raw) {
        if (!item || typeof item !== "object") continue;
        const o = item as Record<string, unknown>;
        const host = typeof o["host"] === "string" ? (o["host"] as string)
          : typeof o["ip"] === "string" ? (o["ip"] as string) : null;
        const port = Number(o["port"]);
        const secret = typeof o["secret"] === "string" ? (o["secret"] as string) : null;
        if (!host || !Number.isFinite(port) || port <= 0 || !secret) continue;
        entries.push({
          host,
          port,
          secret,
          country: typeof o["country"] === "string" ? (o["country"] as string) : undefined,
          ping: typeof o["ping"] === "number" ? (o["ping"] as number) : undefined,
          uptime: typeof o["uptime"] === "number" ? (o["uptime"] as number) : undefined,
        });
      }
      if (entries.length === 0) continue;
      // Prefer high uptime, then low ping. Then prefer well-connected
      // regions (NL/DE/SE/FR/US) which usually have the best Telegram peering.
      const goodCountries = new Set(["NL", "DE", "SE", "FR", "US", "GB", "FI"]);
      entries.sort((a, b) => {
        const ua = a.uptime ?? 0;
        const ub = b.uptime ?? 0;
        if (ub !== ua) return ub - ua;
        const ga = goodCountries.has(a.country ?? "") ? 1 : 0;
        const gb = goodCountries.has(b.country ?? "") ? 1 : 0;
        if (gb !== ga) return gb - ga;
        const pa = a.ping ?? 9999;
        const pb = b.ping ?? 9999;
        return pa - pb;
      });
      const best = entries[0];
      if (best) {
        logger.info(
          { candidates: entries.length, host: best.host, country: best.country, ping: best.ping, uptime: best.uptime },
          "Selected public MTProto proxy",
        );
        return best;
      }
    } catch (err) {
      logger.warn(
        { err: err instanceof Error ? err.message : err, url },
        "Public MTProto proxy directory unreachable",
      );
    }
  }
  logger.warn("No public MTProto proxy available — using direct connection");
  return null;
}
