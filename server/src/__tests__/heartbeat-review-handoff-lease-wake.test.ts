import { agentService } from "../services/agents.js";
import { environmentService } from "../services/environments.js";
import { issueRecoveryActionService } from "../services/issue-recovery-actions.js";
import { passiveBoardRecovery } from "../services/idle-work-proof.js";
import { setTimeout as realDelay } from "node:timers/promises";
import * as blockerModule from "../services/execution-blocker.js";
import { createDeliveryWorkCoordinator } from "../services/delivery-work-coordinator.js";
import { issueService } from "../services/issues.js";
import { getServerAdapter } from "../adapters/index.js";
import { notifyExecutionRunWork } from "../services/execution-control-notifications.js";
import { randomUUID } from "node:crypto";
import { and, eq, sql } from "drizzle-orm";
import { beforeAll, afterAll, beforeEach, afterEach, describe, it, expect, vi } from "vitest";
import {
  agentWakeupRequests, agents, authUsers, companyMemberships, companies, createDb, environmentLeases, heartbeatRuns, issueComments, issueRecoveryActions, issues,
} from "@paperclipai/db";
import { startEmbeddedPostgresTestDatabase, getEmbeddedPostgresTestSupport } from "./helpers/embedded-postgres.js";
import { getExecutionBlocker } from "../services/execution-blocker.js";
import { heartbeatService } from "../services/heartbeat.js";

const support = await getEmbeddedPostgresTestSupport();

// A sandboxed reviewer approves, and the review handoff cancels its run with
// `issue_reassigned`. Its environment lease is released a moment later. The
// next participant's wake arrives while the lease is still held, so admission
// records it as a `skipped` execution wait. After the lease is released, the
// participant must still get a run.
(support.supported ? describe : describe.skip)("review handoff wake after a held environment lease", () => {
  let database: Awaited<ReturnType<typeof startEmbeddedPostgresTestDatabase>>;
  let db: ReturnType<typeof createDb>;
  beforeAll(async () => {
    database = await startEmbeddedPostgresTestDatabase("review-handoff-lease-");
    db = createDb(database.connectionString, { maxConnections: 6, idleTimeoutSeconds: 0, maxLifetimeSeconds: 0 });
    db.$client.options.onnotice = () => {};
    await Promise.all(Array.from({ length: 6 }, () => db.execute(sql`select pg_sleep(0.01)`)));
  }, 30000);
  afterAll(async () => { await db?.$client.end({ timeout: 1 }); await database?.cleanup(); });

  async function seed() {
    const companyId = randomUUID(), reviewerId = randomUUID(), participantId = randomUUID();
    const issueId = randomUUID(), reviewerRunId = randomUUID();
    await db.insert(companies).values({ id: companyId, name: "Handoff", defaultResponsibleUserId: "board",
      issuePrefix: `H${companyId.slice(0, 6)}` });
    await db.insert(agents).values([
      { id: reviewerId, companyId, name: "Reviewer", role: "engineer", adapterType: "claude_local", status: "idle",
        runtimeConfig: { heartbeat: { maxConcurrentRuns: 1 } } },
      { id: participantId, companyId, name: "Next reviewer", role: "engineer", adapterType: "claude_local",
        status: "idle", runtimeConfig: { heartbeat: { maxConcurrentRuns: 1 } } },
    ]);
    const stageId = randomUUID();
    await db.insert(issues).values({ id: issueId, companyId, title: "Two-stage review", status: "in_review",
      assigneeAgentId: participantId, executionState: { status: "pending", currentStageId: stageId,
        currentStageType: "review", currentParticipant: { type: "agent", agentId: participantId, userId: null } } });
    // The approving run was stopped by the handoff; no process remains.
    await db.insert(heartbeatRuns).values({ id: reviewerRunId, companyId, agentId: reviewerId,
      runtimeMode: "legacy", status: "cancelled", errorCode: "issue_reassigned",
      runnerProfileJson: { adapterDispatch: { adapterType: "claude_local" } },
      contextSnapshot: { issueId }, finishedAt: new Date() });
    // Its sandbox lease is still being cleaned up.
    const [lease] = await db.insert(environmentLeases).values({ companyId, issueId, heartbeatRunId: reviewerRunId,
      status: "active", provider: "kubernetes" }).returning();
    // Saturate the participant so an admitted wake stops at `queued`.
    await db.insert(heartbeatRuns).values({ companyId, agentId: participantId, status: "running" });
    return { companyId, reviewerId, participantId, issueId, reviewerRunId, stageId, leaseId: lease.id };
  }

  // The wake shape an approval produces for the next review participant.
  const stageWake = (f: Awaited<ReturnType<typeof seed>>) => {
    const executionStage = { stageId: f.stageId, stageType: "review", wakeRole: "reviewer",
      currentParticipant: { type: "agent", agentId: f.participantId, userId: null } };
    return { source: "assignment" as const, triggerDetail: "system" as const, reason: "execution_review_requested",
      payload: { issueId: f.issueId, mutation: "update", executionStage },
      contextSnapshot: { issueId: f.issueId, taskId: f.issueId, wakeReason: "execution_review_requested",
        source: "issue.execution_stage", executionStage } };
  };

  const queuedRuns = (companyId: string, agentId: string) => db.select().from(heartbeatRuns)
    .where(and(eq(heartbeatRuns.companyId, companyId), eq(heartbeatRuns.agentId, agentId),
      eq(heartbeatRuns.status, "queued")));

  it("starts the next participant after the finishing run's lease is released", async () => {
    const f = await seed();
    expect(await getExecutionBlocker(db, f.companyId, f.issueId)).toMatchObject({
      cause: "execution_owner_active", runId: f.reviewerRunId,
    });
    await heartbeatService(db).wakeup(f.participantId, stageWake(f));
    const [wait] = await db.select().from(agentWakeupRequests)
      .where(and(eq(agentWakeupRequests.companyId, f.companyId), eq(agentWakeupRequests.agentId, f.participantId)));
    expect(wait).toMatchObject({ status: "skipped", reason: "execution_reconciliation_required" });
    expect(await queuedRuns(f.companyId, f.participantId)).toHaveLength(0);

    // Lease cleanup finishes; the gate is gone.
    await db.update(environmentLeases).set({ status: "released", releasedAt: new Date(), cleanupStatus: "succeeded" })
      .where(eq(environmentLeases.id, f.leaseId));
    expect(await getExecutionBlocker(db, f.companyId, f.issueId)).toBeNull();

    // Let the cleanup delay elapse, then run the ordinary periodic sweep twice
    // concurrently: exactly one run starts, and a later pass adds none.
    await db.update(agentWakeupRequests).set({ updatedAt: new Date(0) }).where(eq(agentWakeupRequests.id, wait.id));
    await Promise.all([heartbeatService(db).resumeQueuedRuns(), heartbeatService(db).resumeQueuedRuns()]);
    const runs = await queuedRuns(f.companyId, f.participantId);
    expect(runs).toHaveLength(1);
    expect(runs[0].contextSnapshot).toMatchObject({ issueId: f.issueId, wakeReason: "execution_review_requested",
      executionStage: { stageId: f.stageId } });
    await heartbeatService(db).resumeQueuedRuns();
    expect(await queuedRuns(f.companyId, f.participantId)).toHaveLength(1);

    // An interruption after admission but before retirement leaves the receipt
    // unretired. The next pass finds the keyed replacement and only retires it.
    await db.update(agentWakeupRequests).set({ updatedAt: new Date(0),
      payload: sql`${agentWakeupRequests.payload} #- '{executionWait,readmittedAt}'` })
      .where(eq(agentWakeupRequests.id, wait.id));
    await heartbeatService(db).resumeQueuedRuns();
    expect(await queuedRuns(f.companyId, f.participantId)).toHaveLength(1);
    const [retired] = await db.select().from(agentWakeupRequests).where(eq(agentWakeupRequests.id, wait.id));
    expect((retired.payload as { executionWait?: { readmittedAt?: string } }).executionWait?.readmittedAt)
      .toBeTruthy();
  });

  it("refuses at admission a stage wake whose stage changed after selection", async () => {
    const f = await seed();
    await db.update(environmentLeases).set({ status: "released", releasedAt: new Date(), cleanupStatus: "succeeded" })
      .where(eq(environmentLeases.id, f.leaseId));
    // The task moved to another stage with the same participant and status.
    await db.update(issues).set({ executionState: { status: "pending", currentStageId: randomUUID(),
      currentStageType: "review", currentParticipant: { type: "agent", agentId: f.participantId, userId: null } } })
      .where(eq(issues.id, f.issueId));
    await heartbeatService(db).wakeup(f.participantId, { ...stageWake(f),
      issueStateGuard: { assigneeAgentId: f.participantId, statuses: ["in_review"], executionStageId: f.stageId } });
    const [wake] = await db.select().from(agentWakeupRequests)
      .where(and(eq(agentWakeupRequests.companyId, f.companyId), eq(agentWakeupRequests.agentId, f.participantId)));
    expect(wake).toMatchObject({ status: "skipped", reason: "issue_state_guard_mismatch" });
    expect(await queuedRuns(f.companyId, f.participantId)).toHaveLength(0);
  });

  it("leaves a parked task alone and admits the handoff once it waits again", async () => {
    const f = await seed();
    await heartbeatService(db).wakeup(f.participantId, stageWake(f));
    const [wait] = await db.select().from(agentWakeupRequests)
      .where(and(eq(agentWakeupRequests.companyId, f.companyId), eq(agentWakeupRequests.agentId, f.participantId)));
    await db.update(environmentLeases).set({ status: "released", releasedAt: new Date(), cleanupStatus: "succeeded" })
      .where(eq(environmentLeases.id, f.leaseId));
    // A status admission refuses: repeated passes add no receipts and keep the handoff.
    await db.update(issues).set({ status: "backlog" }).where(eq(issues.id, f.issueId));
    for (let i = 0; i < 3; i += 1) {
      await db.update(agentWakeupRequests).set({ updatedAt: new Date(0) }).where(eq(agentWakeupRequests.id, wait.id));
      await heartbeatService(db).resumeQueuedRuns();
    }
    expect(await queuedRuns(f.companyId, f.participantId)).toHaveLength(0);
    const wakes = await db.select().from(agentWakeupRequests)
      .where(and(eq(agentWakeupRequests.companyId, f.companyId), eq(agentWakeupRequests.agentId, f.participantId)));
    expect(wakes).toHaveLength(1);
    expect((wakes[0].payload as { executionWait?: { readmittedAt?: string } }).executionWait?.readmittedAt).toBeUndefined();

    // The task waits on the stage again; the next pass admits the handoff.
    await db.update(issues).set({ status: "in_review" }).where(eq(issues.id, f.issueId));
    await db.update(agentWakeupRequests).set({ updatedAt: new Date(0) }).where(eq(agentWakeupRequests.id, wait.id));
    await heartbeatService(db).resumeQueuedRuns();
    expect(await queuedRuns(f.companyId, f.participantId)).toHaveLength(1);
  });

  it("does not let a page of still-blocked handoffs starve one whose gate cleared", async () => {
    // Fifty older handoffs whose leases stay held.
    for (let i = 0; i < 50; i += 1) {
      const blocked = await seed();
      await heartbeatService(db).wakeup(blocked.participantId, stageWake(blocked));
      await db.update(agentWakeupRequests).set({ updatedAt: new Date(1000 + i) })
        .where(eq(agentWakeupRequests.agentId, blocked.participantId));
    }
    const f = await seed();
    await heartbeatService(db).wakeup(f.participantId, stageWake(f));
    await db.update(environmentLeases).set({ status: "released", releasedAt: new Date(), cleanupStatus: "succeeded" })
      .where(eq(environmentLeases.id, f.leaseId));
    await db.update(agentWakeupRequests).set({ updatedAt: new Date(5000) })
      .where(eq(agentWakeupRequests.agentId, f.participantId));
    // The first pass rotates the blocked page; the next reaches the cleared handoff.
    await heartbeatService(db).resumeQueuedRuns();
    await heartbeatService(db).resumeQueuedRuns();
    expect(await queuedRuns(f.companyId, f.participantId)).toHaveLength(1);
  }, 120000);

  it("leaves a handoff alone while the gate is up or the stage has moved on", async () => {
    const f = await seed();
    await heartbeatService(db).wakeup(f.participantId, stageWake(f));
    const [wait] = await db.select().from(agentWakeupRequests)
      .where(and(eq(agentWakeupRequests.companyId, f.companyId), eq(agentWakeupRequests.agentId, f.participantId)));
    await db.update(agentWakeupRequests).set({ updatedAt: new Date(0) }).where(eq(agentWakeupRequests.id, wait.id));
    // Gate still up: nothing starts.
    await heartbeatService(db).resumeQueuedRuns();
    expect(await queuedRuns(f.companyId, f.participantId)).toHaveLength(0);
    // Gate gone, but the task no longer waits on this stage: nothing starts.
    await db.update(environmentLeases).set({ status: "released", releasedAt: new Date(), cleanupStatus: "succeeded" })
      .where(eq(environmentLeases.id, f.leaseId));
    await db.update(issues).set({ executionState: { status: "completed", currentStageId: null } })
      .where(eq(issues.id, f.issueId));
    await heartbeatService(db).resumeQueuedRuns();
    expect(await queuedRuns(f.companyId, f.participantId)).toHaveLength(0);
  });
  // Exercise production notifications and the deadline worker, not resumeQueuedRuns.
  describe("deadline worker route", () => {
    let coordinator: ReturnType<typeof createDeliveryWorkCoordinator>;
    let heartbeat: ReturnType<typeof heartbeatService>;
    let errors: unknown[];
    const passes = new Set<Promise<unknown>>();
    beforeEach(async () => {
      await db.execute(sql`truncate companies cascade`);
      db.$client.options.onnotice = () => {};
      vi.useFakeTimers({ toFake: ["Date", "setTimeout", "clearTimeout"], shouldAdvanceTime: true });
      errors = []; heartbeat = heartbeatService(db);
    });
    afterEach(async () => {
      await coordinator?.stop(); await Promise.allSettled([...passes]); await heartbeat.drainActiveRunExecutions();
      vi.restoreAllMocks(); vi.useRealTimers();
      expect(errors).toEqual([]);
    });
    async function start() {
      coordinator = createDeliveryWorkCoordinator({ owner: db, canRun: () => true,
        canReconcile: () => true, onError: error => errors.push(error) });
      const register = coordinator.register.bind(coordinator);
      coordinator.register = (queue, task) => register(queue, { ...task, run: signal => {
        const pass = Promise.resolve(task.run(signal)); passes.add(pass);
        void pass.then(() => passes.delete(pass), () => passes.delete(pass)); return pass;
      } });
      await heartbeat.registerDeferredReceiptWork(coordinator, () => true).ready;
      await tick(0);
    }
    async function tick(ms = 31_000) {
      await vi.advanceTimersByTimeAsync(ms);
      for (let i = 0; i < 5; i++) {
        await realDelay(10); await Promise.allSettled([...passes]);
        await vi.advanceTimersByTimeAsync(0);
      }
    }
    async function waitFor(assertion: () => Promise<void> | void) {
      let failure: unknown;
      for (let i = 0; i < 40; i++) {
        try { await assertion(); return; } catch (error) { failure = error; }
        await tick(1_000);
      }
      throw failure;
    }
    async function idle() {
      await waitFor(() => expect(coordinator.nextWakeAt()).toBeNull());
      const reads = vi.spyOn(db, "select"); const count = reads.mock.calls.length;
      await tick(5 * 60_000); expect(reads).toHaveBeenCalledTimes(count); reads.mockRestore();
    }
    async function pending(f: Awaited<ReturnType<typeof seed>>, actor: "system" | "agent" = "system") {
      await heartbeat.wakeup(f.participantId, { ...stageWake(f), requestedByActorType: actor,
        ...(actor === "agent" ? { requestedByActorId: f.reviewerId } : {}) });
      const [wait] = await db.select().from(agentWakeupRequests).where(and(
        eq(agentWakeupRequests.companyId, f.companyId), eq(agentWakeupRequests.reason, "execution_reconciliation_required")));
      expect(wait.status).toBe("skipped"); return wait;
    }
    async function release(f: Awaited<ReturnType<typeof seed>>) {
      await db.update(environmentLeases).set({ status: "released", releasedAt: new Date(), cleanupStatus: "succeeded" })
        .where(eq(environmentLeases.id, f.leaseId));
    }
    async function retired(id: string) {
      const [row] = await db.select().from(agentWakeupRequests).where(eq(agentWakeupRequests.id, id));
      return Boolean((row.payload?.executionWait as Record<string, unknown>)?.readmittedAt);
    }
    it.each([
      { actor: "system" as const, restart: false },
      { actor: "system" as const, restart: true },
      { actor: "agent" as const, restart: false },
    ])("producer/worker admits once and becomes idle (actor: $actor, restart: $restart)", async ({ actor, restart }) => {
      const f = await seed(); await start(); await idle();
      const wait = await pending(f, actor); await tick(0);
      if (restart) { await coordinator.stop(); await start(); }
      expect(coordinator.nextWakeAt()).not.toBeNull();
      expect(await queuedRuns(f.companyId, f.participantId)).toHaveLength(0);
      await release(f); await tick();
      await waitFor(async () => {
        expect(await queuedRuns(f.companyId, f.participantId)).toHaveLength(1);
        expect(await retired(wait.id)).toBe(true);
      });
      await idle();
    });
    it.each(["parked", "execution lock"])("reconstructs after temporary %s clears", async gate => {
      const f = await seed(); await start(); const wait = await pending(f);
      await issueService(db).update(f.issueId, gate === "parked" ? { status: "backlog" }
        : { executionRunId: f.reviewerRunId });
      await tick(); await idle();
      expect(await queuedRuns(f.companyId, f.participantId)).toHaveLength(0);
      await release(f);
      await issueService(db).update(f.issueId, gate === "parked" ? { status: "in_review" }
        : { executionRunId: null });
      await tick();
      await waitFor(async () => {
        expect(await queuedRuns(f.companyId, f.participantId)).toHaveLength(1);
        expect(await retired(wait.id)).toBe(true);
      });
      await idle();
    });
    it("does not retire a receipt rewritten by reblocking inside admission", async () => {
      const f = await seed(); await start(); const wait = await pending(f); await release(f);
      const original = blockerModule.getExecutionBlocker; let injected = false;
      const spy = vi.spyOn(blockerModule, "getExecutionBlocker").mockImplementation(async (...args) => {
        const result = await original(...args);
        if (!injected && args[1] === f.companyId && args[2] === f.issueId && result === null) {
          injected = true;
          await db.update(environmentLeases).set({ status: "active", releasedAt: null, cleanupStatus: null })
            .where(eq(environmentLeases.id, f.leaseId));
        }
        return result;
      });
      await tick(); spy.mockRestore();
      expect(injected).toBe(true);
      expect(await queuedRuns(f.companyId, f.participantId)).toHaveLength(0);
      const [rewritten] = await db.select().from(agentWakeupRequests).where(eq(agentWakeupRequests.id, wait.id));
      expect(rewritten.coalescedCount).toBeGreaterThan(0);
      expect(await retired(wait.id)).toBe(false);
      await release(f); await tick();
      await waitFor(async () => expect(await queuedRuns(f.companyId, f.participantId)).toHaveLength(1));
      await idle();
    });
    it("carries an agent-actor reblock into one new durable worker receipt", async () => {
      const f = await seed(); await start(); const wait = await pending(f, "agent"); await release(f);
      const original = blockerModule.getExecutionBlocker; let injected = false;
      const spy = vi.spyOn(blockerModule, "getExecutionBlocker").mockImplementation(async (...args) => {
        const result = await original(...args);
        if (!injected && args[1] === f.companyId && args[2] === f.issueId && result === null) {
          injected = true;
          await db.update(environmentLeases).set({ status: "active", releasedAt: null, cleanupStatus: null })
            .where(eq(environmentLeases.id, f.leaseId));
        }
        return result;
      });
      await tick(); spy.mockRestore();
      expect(injected).toBe(true);
      expect(await queuedRuns(f.companyId, f.participantId)).toHaveLength(0);
      expect(await retired(wait.id)).toBe(true);
      const rows = await db.select().from(agentWakeupRequests).where(and(
        eq(agentWakeupRequests.companyId, f.companyId), eq(agentWakeupRequests.reason, "execution_reconciliation_required")));
      const replacements = rows.filter(row => row.id !== wait.id);
      expect(replacements).toHaveLength(1);
      expect(replacements[0]).toMatchObject({ status: "skipped", runId: null,
        idempotencyKey: `stage-handoff-readmit:${wait.id}`, requestedByActorType: "agent", requestedByActorId: f.reviewerId });
      expect(replacements[0].payload).toMatchObject({ stageHandoffRetry: { attempts: 1, rootRequestId: wait.id } });
      expect(await retired(replacements[0].id)).toBe(false);
      expect(coordinator.nextWakeAt()).not.toBeNull();
      await release(f); await tick();
      await waitFor(async () => {
        expect(await queuedRuns(f.companyId, f.participantId)).toHaveLength(1);
        expect(await retired(replacements[0].id)).toBe(true);
      });
      await idle();
    });
    it("bounds blocked checks, retains one visible hold across restart, and recovers on lease-release notification", async () => {
      const f = await seed(); await start(); const wait = await pending(f, "agent"); await tick(1_100);
      const readWait = async () => (await db.select().from(agentWakeupRequests).where(eq(agentWakeupRequests.id, wait.id)))[0];
      const delays = [30_000, 60_000, 120_000, 240_000, 300_000];
      for (let attempt = 0; attempt < delays.length; attempt++) {
        const before = await readWait();
        const due = before.updatedAt.getTime() + delays[attempt];
        expect(coordinator.nextWakeAt()).toBe(due);
        await tick(Math.max(0, due - Date.now() - 1_000));
        expect((await readWait()).payload?.stageHandoffRetry ?? {}).toMatchObject(attempt ? { attempts: attempt } : {});
        await tick(Math.max(0, due - Date.now()) + 100);
        await waitFor(async () => expect((await readWait()).payload).toMatchObject({ stageHandoffRetry: { attempts: attempt + 1 } }));
        expect(await queuedRuns(f.companyId, f.participantId)).toHaveLength(0);
        if (attempt === 1) {
          await coordinator.stop(); heartbeat = heartbeatService(db); await start();
          expect((await readWait()).payload).toMatchObject({ stageHandoffRetry: { attempts: 2 } });
        }
      }
      await idle();
      const [hold] = await db.select().from(issueRecoveryActions).where(eq(issueRecoveryActions.sourceIssueId, f.issueId));
      expect(hold).toMatchObject({ status: "active", ownerType: "board", cause: "stage_handoff_retry_exhausted",
        attemptCount: 1, maxAttempts: null, returnOwnerAgentId: f.participantId,
        evidence: { stageHandoffRetry: { state: "exhausted" } } });
      const [issue] = await db.select().from(issues).where(eq(issues.id, f.issueId));
      expect(issue).toMatchObject({ status: "in_review", assigneeAgentId: f.participantId });
      expect(await retired(wait.id)).toBe(false);
      await coordinator.stop(); heartbeat = heartbeatService(db); await start(); await idle();
      // Unrelated eligibility signals do not reset or renew the exhausted budget.
      await db.transaction(tx => notifyExecutionRunWork(tx, { companyId: f.companyId })); await tick(1_100); await idle();
      expect((await readWait()).payload).toMatchObject({ stageHandoffRetry: { attempts: 5 } });
      expect(await db.select().from(issueRecoveryActions).where(eq(issueRecoveryActions.sourceIssueId, f.issueId))).toHaveLength(1);
      expect(await queuedRuns(f.companyId, f.participantId)).toHaveLength(0);
      await db.transaction(async tx => {
        await tx.update(environmentLeases).set({ status: "released", releasedAt: new Date(), cleanupStatus: "succeeded" })
          .where(eq(environmentLeases.id, f.leaseId));
        await notifyExecutionRunWork(tx, { companyId: f.companyId, runId: f.reviewerRunId, issueId: f.issueId });
      });
      await tick(1_100);
      await waitFor(async () => {
        expect(await queuedRuns(f.companyId, f.participantId)).toHaveLength(1);
        expect(await retired(wait.id)).toBe(true);
      });
      const [resolved] = await db.select().from(issueRecoveryActions).where(eq(issueRecoveryActions.id, hold.id));
      expect(resolved).toMatchObject({ status: "resolved", outcome: "restored" });
      await idle();
    });
    async function exhausted() {
      const f = await seed(); await start(); const wait = await pending(f, "agent"); await tick(1_100);
      for (const delay of [30_000, 60_000, 120_000, 240_000, 300_000]) await tick(delay + 100);
      await idle();
      const [hold] = await db.select().from(issueRecoveryActions).where(eq(issueRecoveryActions.sourceIssueId, f.issueId));
      expect(hold?.cause).toBe("stage_handoff_retry_exhausted");
      return { f, wait, hold };
    }
    it("production exhaustion: real lease release resumes the retained handoff", async () => {
      const { f, wait } = await exhausted();
      await environmentService(db).releaseLease(f.leaseId, "released", { cleanupStatus: "success" });
      await tick(1_100);
      await waitFor(async () => expect(await queuedRuns(f.companyId, f.participantId)).toHaveLength(1));
      expect(await retired(wait.id)).toBe(true); await idle();
    });
    it("production exhaustion: later reconciliation supersedes the passive notice", async () => {
      const { f } = await exhausted();
      await issueRecoveryActionService(db).upsertSourceScoped({ companyId: f.companyId, sourceIssueId: f.issueId,
        kind: "active_run_watchdog", ownerType: "board", cause: "uncertain_external_action", fingerprint: "later-failure",
        evidence: {}, nextAction: "Reconcile the external action.", maxAttempts: 3 });
      const action = await issueRecoveryActionService(db).getActiveForIssue(f.companyId, f.issueId);
      expect(action?.cause).toBe("uncertain_external_action");
      await environmentService(db).releaseLease(f.leaseId, "released", { cleanupStatus: "success" });
      await tick(1_100);
      expect(await getExecutionBlocker(db, f.companyId, f.issueId)).not.toBeNull();
      expect(await queuedRuns(f.companyId, f.participantId)).toHaveLength(0); await idle();
    });
    it("production exhaustion: refusal preserves the notice and handoff", async () => {
      const { f, wait, hold } = await exhausted();
      await db.update(agents).set({ status: "paused" }).where(eq(agents.id, f.participantId));
      await release(f);
      await db.transaction(tx => notifyExecutionRunWork(tx, { companyId: f.companyId })); await tick(1_100);
      const [action] = await db.select().from(issueRecoveryActions).where(eq(issueRecoveryActions.id, hold.id));
      expect(action.status).toBe("active"); expect(await retired(wait.id)).toBe(false);
      expect(await queuedRuns(f.companyId, f.participantId)).toHaveLength(0); await idle();
    });
    it.each(["done", "reassigned"])("production exhaustion: service %s cancels obsolete hold", async change => {
      const { f, hold } = await exhausted();
      await issueService(db).update(f.issueId, change === "done" ? { status: "done" }
        : { assigneeAgentId: f.reviewerId }); await tick(1_100);
      await waitFor(async () => {
        const [action] = await db.select().from(issueRecoveryActions).where(eq(issueRecoveryActions.id, hold.id));
        expect(action.status).toBe("cancelled");
      });
      expect(await queuedRuns(f.companyId, f.participantId)).toHaveLength(0); await idle();
    });
    it("production exhaustion: notice is passive without hiding an execution owner", async () => {
      const { f, hold } = await exhausted();
      const passive = async () => (await db.execute<{ passive: boolean }>(sql`select ${sql.raw(passiveBoardRecovery("a"))} as passive
        from issue_recovery_actions a where a.id = ${hold.id}`))[0].passive;
      expect(await passive()).toBe(true);
      await issueService(db).update(f.issueId, { executionRunId: f.reviewerRunId });
      expect(await passive()).toBe(false);
    });
    it("targeted hold lifecycle: deleting the reviewer cancels its obsolete hold", async () => {
      const { f, hold } = await exhausted();
      // Fixture shutdown, not an interruption: the saturated run is synthetic.
      await db.update(heartbeatRuns).set({ status: "succeeded", finishedAt: new Date() })
        .where(eq(heartbeatRuns.agentId, f.participantId));
      await db.update(agents).set({ status: "terminated", lifecycleState: "terminated" }).where(eq(agents.id, f.participantId));
      await agentService(db).remove(f.participantId); await tick(1_100);
      const [action] = await db.select().from(issueRecoveryActions).where(eq(issueRecoveryActions.id, hold.id));
      expect(action.status).toBe("cancelled");
      const [task] = await db.select().from(issues).where(eq(issues.id, f.issueId));
      expect(task.assigneeAgentId).toBeNull(); await idle();
    });
    it.each(["participant", "stage"])("targeted hold lifecycle: changing only %s clears the notice", async change => {
      const { f, hold } = await exhausted();
      await issueService(db).update(f.issueId, { executionState: { status: "pending",
        currentStageId: change === "stage" ? randomUUID() : f.stageId, currentStageType: "review",
        currentParticipant: { type: "agent", agentId: change === "participant" ? f.reviewerId : f.participantId, userId: null } } });
      await tick(1_100);
      const [action] = await db.select().from(issueRecoveryActions).where(eq(issueRecoveryActions.id, hold.id));
      expect(action.status).toBe("cancelled");
      expect(await queuedRuns(f.companyId, f.participantId)).toHaveLength(0); await idle();
    });
    it("targeted hold lifecycle: disabled on-demand exhaustion is visible and re-enabling admits once", async () => {
      const f = await seed(); await start(); const wait = await pending(f, "agent");
      await agentService(db).update(f.participantId, { runtimeConfig: { heartbeat: {
        enabled: true, intervalSec: 0, wakeOnDemand: false, maxConcurrentRuns: 1 } } });
      await environmentService(db).releaseLease(f.leaseId, "released", { cleanupStatus: "success" });
      await tick(1_100);
      for (const delay of [30_000, 60_000, 120_000, 240_000, 300_000]) await tick(delay + 100);
      await idle();
      const rows = await db.select().from(issueRecoveryActions).where(eq(issueRecoveryActions.sourceIssueId, f.issueId));
      expect(rows).toHaveLength(1); expect(rows[0].status).toBe("active");
      expect(await queuedRuns(f.companyId, f.participantId)).toHaveLength(0);
      await agentService(db).update(f.participantId, { runtimeConfig: { heartbeat: {
        enabled: true, intervalSec: 0, wakeOnDemand: true, maxConcurrentRuns: 1 } } });
      await tick(1_100);
      await waitFor(async () => expect(await queuedRuns(f.companyId, f.participantId)).toHaveLength(1));
      expect(await retired(wait.id)).toBe(true);
      const [resolved] = await db.select().from(issueRecoveryActions).where(eq(issueRecoveryActions.id, rows[0].id));
      expect(resolved.status).toBe("resolved"); await idle();
    });
    it.each(["retired same-stage", "pending different-stage"])("records a fresh system handoff after a %s receipt", async previous => {
      const f = await seed(); await start(); const wait = await pending(f);
      if (previous === "retired same-stage") {
        await release(f); await tick();
        const [run] = await queuedRuns(f.companyId, f.participantId);
        expect(run).toBeTruthy(); expect(await retired(wait.id)).toBe(true);
        await db.update(heartbeatRuns).set({ status: "succeeded", finishedAt: new Date() }).where(eq(heartbeatRuns.id, run.id));
      }
      await issueService(db).update(f.issueId, { executionRunId: null,
        executionState: { status: "changes_requested", currentStageId: f.stageId } });
      await tick(); await idle();
      if (previous === "pending different-stage") f.stageId = randomUUID();
      await db.update(environmentLeases).set({ status: "active", releasedAt: null, cleanupStatus: null })
        .where(eq(environmentLeases.id, f.leaseId));
      await issueService(db).update(f.issueId, { executionState: { status: "pending", currentStageId: f.stageId,
        currentStageType: "review", currentParticipant: { type: "agent", agentId: f.participantId, userId: null } } });
      await heartbeat.wakeup(f.participantId, { ...stageWake(f), requestedByActorType: "system" });
      const rows = await db.select().from(agentWakeupRequests).where(and(
        eq(agentWakeupRequests.companyId, f.companyId), eq(agentWakeupRequests.reason, "execution_reconciliation_required")));
      const fresh = rows.filter(row => row.id !== wait.id);
      expect(fresh).toHaveLength(1);
      expect(fresh[0].payload).toMatchObject({ executionStage: { stageId: f.stageId } });
      expect(await retired(fresh[0].id)).toBe(false);
      await release(f); await tick();
      await waitFor(async () => {
        expect(await queuedRuns(f.companyId, f.participantId)).toHaveLength(1);
        expect(await retired(fresh[0].id)).toBe(true);
      });
      await idle();
    });
    it.each([
      { manual: false, reentry: false },
      { manual: true, reentry: false },
      { manual: true, reentry: true },
    ])("retires sibling receipts after one admission (manual: $manual, stage re-entry: $reentry)", async ({ manual, reentry }) => {
      const f = await seed(); await start(); const wait = await pending(f);
      // Distinct diagnostic conditions can leave multiple receipts for one stage.
      await db.insert(agentWakeupRequests).values({ ...wait, id: randomUUID(),
        idempotencyKey: `other-condition:${randomUUID()}`, payload: { ...wait.payload } });
      await release(f);
      if (manual) {
        expect(await heartbeat.wakeup(f.participantId, { ...stageWake(f), requestedByActorType: "user" })).toBeTruthy();
      } else {
        await tick();
      }
      const [admitted] = await queuedRuns(f.companyId, f.participantId);
      expect(admitted).toBeTruthy();
      // Simulate the admitted run finishing without a decision; do not execute an adapter.
      await db.update(heartbeatRuns).set({ status: "succeeded", finishedAt: new Date() }).where(eq(heartbeatRuns.id, admitted.id));
      await issueService(db).update(f.issueId, { executionRunId: null });
      if (reentry) {
        // Review policy stage IDs are reused across rounds. Leaving and re-entering
        // the same stage must not turn a satisfied old receipt into a new wake.
        await issueService(db).update(f.issueId, { executionState: { status: "changes_requested",
          currentStageId: f.stageId, currentStageType: "review",
          currentParticipant: { type: "agent", agentId: f.participantId, userId: null } } });
        await tick(); await idle();
        await issueService(db).update(f.issueId, { executionState: { status: "pending",
          currentStageId: f.stageId, currentStageType: "review",
          currentParticipant: { type: "agent", agentId: f.participantId, userId: null } } });
      }
      await tick();
      expect(await queuedRuns(f.companyId, f.participantId)).toHaveLength(0);
      const rows = await db.select().from(agentWakeupRequests).where(and(eq(agentWakeupRequests.companyId, f.companyId),
        eq(agentWakeupRequests.reason, "execution_reconciliation_required")));
      expect(rows).toHaveLength(2);
      expect(rows.every(row => Boolean((row.payload?.executionWait as Record<string, unknown>)?.readmittedAt))).toBe(true);
      await idle();
    });
    it("dispatches exactly one harmless process run through the worker and settles idle", async () => {
      const f = await seed();
      const actorId = randomUUID();
      await db.insert(authUsers).values({ id: actorId, name: "Test board", email: `${actorId}@example.test`,
        createdAt: new Date(), updatedAt: new Date() });
      await db.insert(companyMemberships).values({ companyId: f.companyId, principalId: actorId,
        principalType: "user", status: "active", membershipRole: "owner" });
      await db.update(companies).set({ defaultResponsibleUserId: actorId }).where(eq(companies.id, f.companyId));
      await db.update(issues).set({ responsibleUserId: actorId }).where(eq(issues.id, f.issueId));
      await db.update(agents).set({ adapterType: "process",
        adapterConfig: { command: process.execPath, args: ["-e", "console.log('handoff-worker-proof')"], cwd: process.cwd() },
        runtimeConfig: { heartbeat: { enabled: true, intervalSec: 0, wakeOnDemand: true, maxConcurrentRuns: 1 } } })
        .where(eq(agents.id, f.participantId));
      await start();
      const wait = await pending(f); await release(f); await tick();
      const [admitted] = await queuedRuns(f.companyId, f.participantId);
      expect(admitted).toBeTruthy();
      // Supply the unrelated comment-policy receipt before enabling dispatch.
      // The real process below only prints a marker; it does not run a model or
      // contact an API. Without this receipt the normal comment-repair feature
      // correctly schedules another run, obscuring handoff deduplication.
      await db.insert(issueComments).values({ companyId: f.companyId, issueId: f.issueId,
        authorAgentId: f.participantId, authorType: "agent", createdByRunId: admitted.id,
        body: "Disposable handoff fixture completion receipt." });
      // Simulate the process's review disposition at the adapter boundary. Keep
      // the real process execution; only its business decision is a fixture.
      const adapter = getServerAdapter("process");
      const execute = adapter.execute;
      vi.spyOn(adapter, "execute").mockImplementation(async context => {
        const result = await execute(context);
        await issueService(db).update(f.issueId, { status: "done",
          executionState: { status: "completed", currentStageId: null } });
        return result;
      });
      await db.update(heartbeatRuns).set({ status: "succeeded", finishedAt: new Date() })
        .where(and(eq(heartbeatRuns.agentId, f.participantId), eq(heartbeatRuns.status, "running")));
      await heartbeat.registerRunDispatchWork(coordinator, () => true).ready;
      await heartbeat.registerScheduledRetryCancellationWork(coordinator, () => true).ready;
      await waitFor(async () => {
        await heartbeat.drainActiveRunExecutions();
        const runs = await db.select().from(heartbeatRuns).where(and(eq(heartbeatRuns.agentId, f.participantId),
          sql`${heartbeatRuns.contextSnapshot}->>'issueId' = ${f.issueId}`));
        expect(runs, JSON.stringify(runs.map(run => ({ status: run.status, error: run.error,
          code: run.errorCode, reason: run.contextSnapshot?.wakeReason })))).toHaveLength(1);
        expect(runs[0].status, JSON.stringify({ code: runs[0].errorCode, error: runs[0].error })).toBe("succeeded");
        expect(runs[0].stdoutExcerpt).toContain("handoff-worker-proof");
      });
      expect(await retired(wait.id)).toBe(true);
      await tick(); await idle();
    });
  });

});
