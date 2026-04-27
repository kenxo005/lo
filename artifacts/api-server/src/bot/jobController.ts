import { logger } from "../lib/logger";

export class CancellationError extends Error {
  readonly isCancellation = true;
  constructor(message = "Cancelled by user") {
    super(message);
    this.name = "CancellationError";
  }
}

export function isCancellation(err: unknown): boolean {
  return (
    err instanceof CancellationError ||
    (err instanceof Error && (err as { isCancellation?: boolean }).isCancellation === true)
  );
}

/**
 * Lightweight cancellation token. Long-running operations register an onCancel
 * callback (kill child process, destroy stream, abort upload, etc.).
 * Calling cancel() once invokes every registered callback exactly once.
 */
export class JobController {
  cancelled = false;
  reason?: string;
  private listeners: Array<() => void> = [];

  onCancel(fn: () => void): void {
    if (this.cancelled) {
      try {
        fn();
      } catch (err) {
        logger.warn({ err }, "onCancel handler threw");
      }
      return;
    }
    this.listeners.push(fn);
  }

  cancel(reason?: string): void {
    if (this.cancelled) return;
    this.cancelled = true;
    this.reason = reason;
    const ls = this.listeners.slice();
    this.listeners.length = 0;
    for (const fn of ls) {
      try {
        fn();
      } catch (err) {
        logger.warn({ err }, "cancel handler threw");
      }
    }
  }

  throwIfCancelled(): void {
    if (this.cancelled) {
      throw new CancellationError(this.reason);
    }
  }
}
