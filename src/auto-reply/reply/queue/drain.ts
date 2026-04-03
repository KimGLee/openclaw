import { defaultRuntime } from "../../../runtime.js";
import { resolveGlobalMap } from "../../../shared/global-singleton.js";
import {
  beginQueueDrain,
  buildCollectPrompt,
  clearQueueSummaryState,
  drainCollectQueueStep,
  drainNextQueueItem,
  hasCrossChannelItems,
  previewQueueSummaryPrompt,
  waitForQueueDebounce,
} from "../../../utils/queue-helpers.js";
import { isRoutableChannel } from "../route-reply.js";
import { FOLLOWUP_QUEUES } from "./state.js";
import { type FollowupRun } from "./types.js";

const FOLLOWUP_DRAIN_CALLBACKS_KEY = Symbol.for("openclaw.followupDrainCallbacks");

const FOLLOWUP_RUN_CALLBACKS = resolveGlobalMap<string, (run: FollowupRun) => Promise<void>>(
  FOLLOWUP_DRAIN_CALLBACKS_KEY,
);

export function rememberFollowupDrainCallback(
  key: string,
  runFollowup: (run: FollowupRun) => Promise<void>,
): void {
  FOLLOWUP_RUN_CALLBACKS.set(key, runFollowup);
}

export function clearFollowupDrainCallback(key: string): void {
  FOLLOWUP_RUN_CALLBACKS.delete(key);
}

export function kickFollowupDrainIfIdle(key: string): void {
  const cb = FOLLOWUP_RUN_CALLBACKS.get(key);
  if (!cb) {
    return;
  }
  scheduleFollowupDrain(key, cb);
}

type OriginRoutingMetadata = Pick<
  FollowupRun,
  "originatingChannel" | "originatingTo" | "originatingAccountId" | "originatingThreadId"
>;

function resolveOriginRoutingMetadata(items: FollowupRun[]): OriginRoutingMetadata {
  return {
    originatingChannel: items.find((item) => item.originatingChannel)?.originatingChannel,
    originatingTo: items.find((item) => item.originatingTo)?.originatingTo,
    originatingAccountId: items.find((item) => item.originatingAccountId)?.originatingAccountId,
    originatingThreadId: items.find(
      (item) => item.originatingThreadId != null && item.originatingThreadId !== "",
    )?.originatingThreadId,
  };
}

function resolveCrossChannelKey(item: FollowupRun): { cross?: true; key?: string } {
  const { originatingChannel: channel, originatingTo: to, originatingAccountId: accountId } = item;
  const threadId = item.originatingThreadId;
  if (item.messageId?.trim()) {
    return { cross: true };
  }
  if (!channel && !to && !accountId && (threadId == null || threadId === "")) {
    return {};
  }
  if (!isRoutableChannel(channel) || !to) {
    return { cross: true };
  }
  const threadKey = threadId != null && threadId !== "" ? String(threadId) : "";
  return {
    key: [channel, to, accountId || "", threadKey].join("|"),
  };
}

export function scheduleFollowupDrain(
  key: string,
  runFollowup: (run: FollowupRun) => Promise<void>,
): void {
  const queue = beginQueueDrain(FOLLOWUP_QUEUES, key);
  if (!queue) {
    return;
  }
  const effectiveRunFollowup = FOLLOWUP_RUN_CALLBACKS.get(key) ?? runFollowup;
  rememberFollowupDrainCallback(key, effectiveRunFollowup);
  void (async () => {
    try {
      const collectState = { forceIndividualCollect: queue.collectForceIndividual };
      while (queue.items.length > 0 || queue.droppedCount > 0) {
        await waitForQueueDebounce(queue);
        if (queue.mode === "collect") {
          const isCrossChannel = hasCrossChannelItems(queue.items, resolveCrossChannelKey);

          const collectDrainResult = await drainCollectQueueStep({
            collectState,
            isCrossChannel,
            items: queue.items,
            run: effectiveRunFollowup,
          });
          if (collectDrainResult === "empty") {
            break;
          }
          if (collectDrainResult === "drained") {
            continue;
          }

          const items = queue.items.slice();
          const summary = previewQueueSummaryPrompt({ state: queue, noun: "message" });
          const run = items.at(-1)?.run ?? queue.lastRun;
          if (!run) {
            break;
          }

          const routing = resolveOriginRoutingMetadata(items);

          const prompt = buildCollectPrompt({
            title: "[Queued messages while agent was busy]",
            items,
            summary,
            renderItem: (item, idx) => `---
Queued #${idx + 1}
${item.execution.agentPrompt}`.trim(),
          });
          await effectiveRunFollowup({
            execution: { visibility: "internal", agentPrompt: prompt },
            display: { visibility: "summary-only", summaryLine: summary },
            run,
            enqueuedAt: Date.now(),
            ...routing,
          });
          queue.items.splice(0, items.length);
          if (summary) {
            clearQueueSummaryState(queue);
          }
          continue;
        }

        const summaryPrompt = previewQueueSummaryPrompt({ state: queue, noun: "message" });
        if (summaryPrompt) {
          const run = queue.lastRun;
          if (!run) {
            break;
          }
          if (
            !(await drainNextQueueItem(queue.items, async (item) => {
              await effectiveRunFollowup({
                execution: { visibility: "internal", agentPrompt: summaryPrompt },
                display: {
                  visibility: "user-visible",
                  text: summaryPrompt,
                  summaryLine: summaryPrompt,
                },
                run,
                enqueuedAt: Date.now(),
                originatingChannel: item.originatingChannel,
                originatingTo: item.originatingTo,
                originatingAccountId: item.originatingAccountId,
                originatingThreadId: item.originatingThreadId,
              });
            }))
          ) {
            break;
          }
          clearQueueSummaryState(queue);
          continue;
        }

        if (!(await drainNextQueueItem(queue.items, effectiveRunFollowup))) {
          break;
        }
      }
    } catch (err) {
      queue.lastEnqueuedAt = Date.now();
      defaultRuntime.error?.(`followup queue drain failed for ${key}: ${String(err)}`);
    } finally {
      queue.draining = false;
      if (queue.items.length === 0 && queue.droppedCount === 0) {
        queue.collectForceIndividual = false;
        FOLLOWUP_QUEUES.delete(key);
        clearFollowupDrainCallback(key);
      } else {
        scheduleFollowupDrain(key, effectiveRunFollowup);
      }
    }
  })();
}
