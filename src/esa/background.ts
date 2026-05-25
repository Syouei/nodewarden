/**
 * ESA Background Task Utilities
 *
 * These utilities handle background task execution in the ESA environment,
 * replacing Cloudflare's waitUntil() pattern with ESA-compatible alternatives.
 */

/**
 * Schedule a background task to run asynchronously.
 * Errors are caught and logged but do not propagate.
 */
export async function scheduleBackgroundTask(task: () => Promise<void>): Promise<void> {
  task().catch((err: unknown) => {
    console.error('Background task failed:', err);
  });
}

/**
 * Wait for a promise to resolve in a way compatible with ESA's background
 * task handling. In ESA, detached promises are handled differently from
 * Cloudflare's waitUntil().
 *
 * If the promise supports detach(), it is called to run in the background.
 * Otherwise, errors are caught and logged.
 */
export function esawaitUntil(promise: Promise<void>): void {
  if ('detach' in promise && typeof (promise as unknown as { detach: () => void }).detach === 'function') {
    (promise as unknown as { detach: () => void }).detach();
  } else {
    promise.catch((err: unknown) => {
      console.error('Background task failed:', err);
    });
  }
}