import { createHash } from "node:crypto";
import { and, eq, sql } from "drizzle-orm";
import { agentWakeupRequests, type Db } from "@paperclipai/db";

import { notifyDeferredExecutionWork } from "./deferred-execution-notifications.js";

type WakeRequest = typeof agentWakeupRequests.$inferInsert;

export const STAGE_HANDOFF_RETRY_DELAYS_MS = [30_000, 60_000, 120_000, 240_000, 300_000] as const;

export function stageHandoffRetryAttempts(payload: Record<string, unknown> | null | undefined) {
  const state = payload?.stageHandoffRetry as { attempts?: unknown } | undefined;
  return typeof state?.attempts === "number" && Number.isInteger(state.attempts)
    ? Math.max(0, Math.min(STAGE_HANDOFF_RETRY_DELAYS_MS.length, state.attempts)) : 0;
}

/** null means event-only recovery: no further timed polling. */
export function stageHandoffRetryDueAt(payload: Record<string, unknown> | null | undefined, updatedAt: Date) {
  const delay = STAGE_HANDOFF_RETRY_DELAYS_MS[stageHandoffRetryAttempts(payload)];
  return delay === undefined ? null : updatedAt.getTime() + delay;
}

export function stageHandoffRetryDuePredicate(now: Date) {
  return sql`${agentWakeupRequests.updatedAt} + (case
    ${agentWakeupRequests.payload}#>>'{stageHandoffRetry,attempts}'
    when '1' then 60 when '2' then 120 when '3' then 240 when '4' then 300
    when '5' then 0 else 30 end) * interval '1 second' <= ${now.toISOString()}::timestamptz`;
}

/**
 * Record a known gate without inventing an execution attempt. The caller holds
 * the company-scoped issue row lock. Only replaceable automatic signals may
 * coalesce; messages and authorized interaction receipts keep their identity.
 * These receipts are diagnostics, never authority to suppress a future wake:
 * admission must read the current gate again before calling this function.
 */
export async function recordExecutionWait(
  tx: Db,
  input: {
    issueId: string;
    request: WakeRequest;
    condition: Record<string, unknown>;
    coalesce: boolean;
  },
): Promise<{ created: boolean }> {
  const { request, issueId, condition } = input;
  const executionStage = request.payload?.executionStage as { stageId?: unknown } | undefined;
  const stageId = typeof executionStage?.stageId === "string" ? executionStage.stageId : null;
  const stageHandoff = request.reason === "execution_reconciliation_required" && stageId !== null;
  // Register the durable stage receipt before either its insert or coalesced update.
  // The existing deadline worker reconstructs the same obligation after restart.
  if (request.reason === "execution_reconciliation_required" &&
      request.payload?.executionStage && !condition.recoveryActionId) {
    await notifyDeferredExecutionWork(tx, { companyId: request.companyId });
  }
  const digest = createHash("sha256")
    .update(JSON.stringify([request.companyId, request.agentId, issueId, request.reason, condition,
      ...(stageHandoff ? [stageId] : [])]))
    .digest("hex");
  const key = `execution-wait:${digest}`;
  if (input.coalesce) {
    const [existing] = await tx.select({ id: agentWakeupRequests.id })
      .from(agentWakeupRequests)
      .where(and(
        eq(agentWakeupRequests.companyId, request.companyId),
        eq(agentWakeupRequests.agentId, request.agentId),
        eq(agentWakeupRequests.status, "skipped"),
        eq(agentWakeupRequests.idempotencyKey, key),
        sql`${agentWakeupRequests.payload}->>'issueId' = ${issueId}`,
        // A settled stage receipt is history, not the next round's obligation.
        stageHandoff ? sql`${agentWakeupRequests.payload}#>>'{executionWait,readmittedAt}' is null` : undefined,
      )).limit(1);
    if (existing) {
      await tx.update(agentWakeupRequests).set({
        coalescedCount: sql`${agentWakeupRequests.coalescedCount} + 1`,
        updatedAt: new Date(),
      }).where(and(
        eq(agentWakeupRequests.companyId, request.companyId),
        eq(agentWakeupRequests.id, existing.id),
      ));
      return { created: false };
    }
  }
  await tx.insert(agentWakeupRequests).values({
    ...request,
    status: "skipped",
    runId: null,
    finishedAt: new Date(),
    idempotencyKey: input.coalesce ? key : request.idempotencyKey,
    payload: {
      ...request.payload,
      issueId,
      executionWait: { ...condition, requestedIdempotencyKey: request.idempotencyKey ?? null },
    },
  });
  return { created: true };
}
