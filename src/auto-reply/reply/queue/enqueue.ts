import { resolveGlobalDedupeCache } from "../../../infra/dedupe.js";
import { applyQueueDropPolicy, shouldSkipQueueItem } from "../../../utils/queue-helpers.js";
import { kickFollowupDrainIfIdle, rememberFollowupDrainCallback } from "./drain.js";
import { getExistingFollowupQueue, getFollowupQueue } from "./state.js";
import {
  getFollowupAgentPrompt,
  getFollowupSummaryLine,
  type FollowupRun,
  type QueueDedupeMode,
  type QueueSettings,
} from "./types.js";

const RECENT_QUEUE_MESSAGE_IDS_KEY = Symbol.for("openclaw.recentQueueMessageIds");

const RECENT_QUEUE_MESSAGE_IDS = resolveGlobalDedupeCache(RECENT_QUEUE_MESSAGE_IDS_KEY, {
  ttlMs: 5 * 60 * 1000,
  maxSize: 10_000,
});

function buildRecentMessageIdKey(run: FollowupRun, queueKey: string): string | undefined {
  const messageId = run.messageId?.trim();
  if (!messageId) {
    return undefined;
  }
  return JSON.stringify([
    "queue",
    queueKey,
    run.originatingChannel ?? "",
    run.originatingTo ?? "",
    run.originatingAccountId ?? "",
    run.originatingThreadId == null ? "" : String(run.originatingThreadId),
    messageId,
  ]);
}

function isRunAlreadyQueued(
  run: FollowupRun,
  items: FollowupRun[],
  allowPromptFallback = false,
): boolean {
  const hasSameRouting = (item: FollowupRun) =>
    item.originatingChannel === run.originatingChannel &&
    item.originatingTo === run.originatingTo &&
    item.originatingAccountId === run.originatingAccountId &&
    item.originatingThreadId === run.originatingThreadId;

  const messageId = run.messageId?.trim();
  if (messageId) {
    return items.some((item) => item.messageId?.trim() === messageId && hasSameRouting(item));
  }
  if (!allowPromptFallback) {
    return false;
  }
  return items.some(
    (item) => getFollowupAgentPrompt(item) === getFollowupAgentPrompt(run) && hasSameRouting(item),
  );
}

export function enqueueFollowupRun(
  key: string,
  run: FollowupRun,
  settings: QueueSettings,
  dedupeMode: QueueDedupeMode = "message-id",
  runFollowup?: (run: FollowupRun) => Promise<void>,
  restartIfIdle = true,
): boolean {
  const queue = getFollowupQueue(key, settings);
  const recentMessageIdKey = dedupeMode !== "none" ? buildRecentMessageIdKey(run, key) : undefined;
  if (recentMessageIdKey && RECENT_QUEUE_MESSAGE_IDS.peek(recentMessageIdKey)) {
    return false;
  }

  const dedupe =
    dedupeMode === "none"
      ? undefined
      : (item: FollowupRun, items: FollowupRun[]) =>
          isRunAlreadyQueued(item, items, dedupeMode === "prompt");

  if (shouldSkipQueueItem({ item: run, items: queue.items, dedupe })) {
    return false;
  }

  queue.lastEnqueuedAt = Date.now();
  queue.lastRun = run.run;

  const shouldEnqueue = applyQueueDropPolicy({
    queue,
    summarize: (item) =>
      getFollowupSummaryLine(item) ?? getFollowupAgentPrompt(item) ?? "[Hidden message]", 
  });
  if (!shouldEnqueue) {
    return false;
  }

  if (run.messageId?.trim()) {
    const sameMessageTarget = queue.items.find(
      (item) =>
        item.messageId?.trim() === run.messageId?.trim() &&
        item.originatingChannel === run.originatingChannel &&
        item.originatingTo === run.originatingTo &&
        item.originatingAccountId === run.originatingAccountId &&
        item.originatingThreadId === run.originatingThreadId,
    );
    if (sameMessageTarget) {
      sameMessageTarget.execution = run.execution;
      sameMessageTarget.display = run.display;
      sameMessageTarget.enqueuedAt = run.enqueuedAt;
      sameMessageTarget.run = run.run;
      return true;
    }
  }

  queue.items.push(run);
  if (recentMessageIdKey) {
    RECENT_QUEUE_MESSAGE_IDS.check(recentMessageIdKey);
  }
  if (runFollowup) {
    rememberFollowupDrainCallback(key, runFollowup);
  }
  if (restartIfIdle && !queue.draining) {
    kickFollowupDrainIfIdle(key);
  }
  return true;
}

export function getFollowupQueueDepth(key: string): number {
  const queue = getExistingFollowupQueue(key);
  if (!queue) {
    return 0;
  }
  return queue.items.length;
}

export function resetRecentQueuedMessageIdDedupe(): void {
  RECENT_QUEUE_MESSAGE_IDS.clear();
}
