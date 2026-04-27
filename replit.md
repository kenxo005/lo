# Workspace

## Overview

pnpm workspace monorepo using TypeScript. Each package manages its own dependencies.

## Stack

- **Monorepo tool**: pnpm workspaces
- **Node.js version**: 24
- **Package manager**: pnpm
- **TypeScript version**: 5.9
- **API framework**: Express 5
- **Database**: PostgreSQL + Drizzle ORM
- **Validation**: Zod (`zod/v4`), `drizzle-zod`
- **API codegen**: Orval (from OpenAPI spec)
- **Build**: esbuild (CJS bundle)

## Key Commands

- `pnpm run typecheck` — full typecheck across all packages
- `pnpm run build` — typecheck + build all packages
- `pnpm --filter @workspace/api-spec run codegen` — regenerate API hooks and Zod schemas from OpenAPI spec
- `pnpm --filter @workspace/db run push` — push DB schema changes (dev only)
- `pnpm --filter @workspace/api-server run dev` — run API server + Telegram bot locally

See the `pnpm-workspace` skill for workspace structure, TypeScript setup, and package details.

## Telegram URL → Telegram Bot

The `api-server` artifact also runs a Telegram bot that downloads any direct
URL and sends the file back to the user.

- Entry point: `artifacts/api-server/src/bot/index.ts` (started from `src/index.ts`)
- Modules:
  - `bot/handlers.ts` — message routing and the download → upload pipeline
  - `bot/download.ts` — `aria2c -x16` for max throughput, `got` stream fallback
  - `bot/progress.ts` — progress-bar formatter and edit-message throttle
- Required secrets: `TELEGRAM_BOT_TOKEN`, `TELEGRAM_API_ID`, `TELEGRAM_API_HASH`
- Uses `gramjs` (MTProto) so the bot can upload files up to ~2 GB (the regular
  Bot API caps uploads at 50 MB).
- System dep: `aria2` (multi-connection downloader) — installed via Nix.
- Session is persisted to `.telegram-bot-session` (gitignored).
- Files are downloaded into `os.tmpdir()` and removed after upload.

### Streaming pipeline for big files (>1.95 GB)

The Replit container has a hard write quota around 6 GB, well below the 10+ GB
files the user wants to forward, and Telegram caps a single bot upload at
~2 GB. Files at or below the cap are uploaded as a single document. Files
above the cap go through `streamDownloadAndUploadInParts()`:

- Each slice (≤ 1.95 GiB) is fetched via HTTP Range, uploaded to Telegram,
  then deleted before the next one starts. With a 1-slice look-ahead the
  next download runs in parallel with the current upload.
- Peak disk usage stays around 4 GB regardless of source size, so 10+ GB
  videos stream cleanly.
- Re-join with `cat file.part*` on Linux/Mac or 7-Zip on Windows.

If the source is a video, a single thumbnail is extracted once (from the
URL via ffmpeg HTTP Range, or from the downloaded file with ffprobe +
ffmpeg) and attached to every uploaded part so each one shows the same
poster frame in Telegram.

### Tunables (env vars)

- `UPLOAD_WORKERS` (default 24, max 64) — gramjs file upload workers.
- `UPLOAD_PART_CONCURRENCY` (default 3, max 6) — parallel slice uploads.
- `MTPROTO_PROXY_HOST` / `MTPROTO_PROXY_PORT` / `MTPROTO_PROXY_SECRET` —
  optional MTProto proxy (often the only way to push past Replit→Telegram-DC
  bandwidth ceilings).
