package com.gameops.craft.service;

import com.gameops.craft.common.ApiException;
import com.gameops.craft.common.DocNumbers;
import com.gameops.craft.common.Json;
import com.gameops.craft.domain.CraftOrder;
import com.gameops.craft.domain.RecipeVersion;
import com.gameops.craft.repo.OrderRepository;
import com.gameops.craft.repo.RecipeRepository;
import com.gameops.craft.repo.RecallBatchRepository;
import com.gameops.craft.repo.RecallBatchRepository.BatchOrderRow;
import com.gameops.craft.repo.RecallBatchRepository.BatchRow;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Emergency version recall facade:
 *  - initiates a recall batch (idempotent: same key / repeat on version -> original batch);
 *  - freezes the affected order scope (version + cutoff) and flags the version atomically;
 *  - drives per-order liquidation in independent transactions, resumable after crash/restart;
 *  - exposes batch progress, per-order reasons, and retryable exceptions.
 */
@Service
public class RecallService {

    private static final Logger log = LoggerFactory.getLogger(RecallService.class);

    /** Test hook: crash the run after the nth order's tx committed (before counters refresh loop end). */
    public volatile Predicate<CrashContext> crashAfterOrder = ctx -> false;

    public record CrashContext(String batchNo, int ordersCommitted) {}

    private final RecallBatchRepository batches;
    private final RecipeRepository recipes;
    private final OrderRepository orders;
    private final RecallTxService tx;
    private final TransactionTemplate txTemplate;
    private final Clock clock;

    public RecallService(RecallBatchRepository batches, RecipeRepository recipes, OrderRepository orders,
                         RecallTxService tx, PlatformTransactionManager txManager, Clock clock) {
        this.batches = batches;
        this.recipes = recipes;
        this.orders = orders;
        this.tx = tx;
        this.clock = clock;
        this.txTemplate = new TransactionTemplate(txManager);
        this.txTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
    }

    public record InitiateResult(String batchNo, boolean created, int totalOrders, String status) {}

    /**
     * Freeze scope and flag the version in one transaction.
     * Idempotency: same key, or a second recall for the same version, returns the existing batch.
     */
    public InitiateResult initiate(long versionId, String idempotencyKey, String reason, long operatorId) {
        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            throw ApiException.badRequest("IDEMPOTENCY_KEY_REQUIRED", "召回必须携带 Idempotency-Key");
        }
        if (idempotencyKey.length() > 80) {
            throw ApiException.badRequest("IDEMPOTENCY_KEY_TOO_LONG", "幂等键长度不能超过 80");
        }
        Instant now = Instant.now(clock);
        try {
            return txTemplate.execute(status -> createBatch(versionId, idempotencyKey, reason, operatorId, now));
        } catch (RuntimeException e) {
            if (batches.isDuplicate(e)) {
                BatchRow existing = batches.findByIdempotencyKey(idempotencyKey)
                        .or(() -> batches.findByVersionId(versionId))
                        .orElseThrow(() -> e);
                return new InitiateResult(existing.batchNo(), false, existing.totalOrders(), existing.status());
            }
            throw e;
        }
    }

    private InitiateResult createBatch(long versionId, String key, String reason, long operatorId, Instant now) {
        // Idempotency before any write: same key, or a repeat recall on a version already
        // covered by a batch, must return the ORIGINAL batch (never liquidate twice).
        var byKey = batches.findByIdempotencyKey(key);
        if (byKey.isPresent()) {
            BatchRow b = byKey.get();
            return new InitiateResult(b.batchNo(), false, b.totalOrders(), b.status());
        }
        var byVersion = batches.findByVersionId(versionId);
        if (byVersion.isPresent()) {
            BatchRow b = byVersion.get();
            return new InitiateResult(b.batchNo(), false, b.totalOrders(), b.status());
        }

        RecipeVersion version = recipes.findVersionById(versionId)
                .orElseThrow(() -> ApiException.notFound("VERSION_NOT_FOUND", "配方版本不存在"));

        // Cutoff = this instant. Every order on the version created up to here is in scope;
        // the version flag + row locks below stop any later preoccupy from joining it.
        Instant cutoff = now;
        String batchNo = DocNumbers.next("RB", clock);

        int flagged = recipes.markRecalled(versionId, batchNo, now);
        if (flagged == 0) {
            // Concurrent initiator just flagged it; its batch wins — re-read and return that one.
            RecipeVersion current = recipes.findVersionById(versionId).orElseThrow();
            BatchRow winner = batches.findByNo(current.recallBatchNo())
                    .orElseThrow(() -> ApiException.conflict("VERSION_ALREADY_RECALLED",
                            "版本 v" + current.versionNo() + " 召回处理中，请稍后查询批次 "
                                    + current.recallBatchNo()));
            return new InitiateResult(winner.batchNo(), false, winner.totalOrders(), winner.status());
        }

        // Snapshot locks every affected order row; a racing preoccupy cannot insert because it
        // waits on the version row (flagged) — racing commit/cancel/timeout wait on their order row.
        List<CraftOrder> affected = orders.lockOrdersForRecall(versionId, cutoff);

        // Unique idempotency key + unique version are the final anti-duplicate anchors.
        batches.insertBatch(batchNo, key, version.recipeId(), versionId, version.versionNo(),
                cutoff, reason, operatorId, affected.size(), now);
        BatchRow row = batches.findByNo(batchNo).orElseThrow();
        for (CraftOrder o : affected) {
            batches.insertSnapshotRow(row.id(), batchNo, o.id(), o.orderNo(), o.playerId(),
                    o.status(), now);
        }
        return new InitiateResult(batchNo, true, affected.size(), "PENDING");
    }

    public record RunSummary(String batchNo, String status, int total, int processed,
                             int released, int reversed, int skipped, int exception,
                             boolean interrupted) {}

    /**
     * Process outstanding snapshot rows. Each order is its own transaction; rows already DONE
     * (non-exception) are skipped, so repeated calls / post-restart continuation never produce
     * duplicate compensation ledger rows.
     */
    public RunSummary runBatch(String batchNo) {
        return runBatch(batchNo, Integer.MAX_VALUE);
    }

    public RunSummary runBatch(String batchNo, int maxOrders) {
        BatchRow batch = batches.findByNo(batchNo)
                .orElseThrow(() -> ApiException.notFound("RECALL_BATCH_NOT_FOUND", "召回批次不存在"));
        int committed = 0;
        boolean interrupted = false;
        List<BatchOrderRow> outstanding = batches.listOutstanding(batch.id(), 1000);
        for (BatchOrderRow row : outstanding) {
            if (committed >= maxOrders) {
                interrupted = true;
                break;
            }
            try {
                txTemplate.executeWithoutResult(s ->
                        tx.liquidate(batchNo, batch.id(), row.orderId(), "EXCEPTION".equals(row.result())));
                committed++;
                if (crashAfterOrder.test(new CrashContext(batchNo, committed))) {
                    // Simulate process kill AFTER this order's transaction committed, BEFORE more work.
                    log.warn("recall crash simulation after {} orders of {}", committed, batchNo);
                    throw new SimulatedCrashException(batchNo);
                }
            } catch (SimulatedCrashException crash) {
                interrupted = true;
                return summarize(batchNo, committed, true);
            } catch (RuntimeException e) {
                // One order's failure must not abort the batch; it stays PENDING and is retried later.
                log.warn("recall liquidation failed for order {} of batch {}: {}",
                        row.orderNo(), batchNo, e.getMessage());
            }
        }
        return summarize(batchNo, committed, interrupted);
    }

    /** Operator-triggered retry of a single parked exception order (e.g. after granting outputs). */
    public Map<String, Object> retryOrder(String batchNo, String orderNo, long operatorId) {
        BatchRow batch = batches.findByNo(batchNo)
                .orElseThrow(() -> ApiException.notFound("RECALL_BATCH_NOT_FOUND", "召回批次不存在"));
        CraftOrder order = orders.findByNo(orderNo)
                .orElseThrow(() -> ApiException.notFound("ORDER_NOT_FOUND", "合成单不存在"));
        if (order.recipeVersionId() != batch.versionId()) {
            throw ApiException.badRequest("ORDER_NOT_IN_BATCH", "合成单不属于该召回版本");
        }
        RecallTxService.OrderOutcome outcome = txTemplate.execute(s ->
                tx.liquidate(batchNo, batch.id(), order.id(), true));
        // Re-read: a successful retry moves RECALL_EXCEPTION -> RECALLED inside that transaction.
        CraftOrder settled = orders.findByNo(orderNo).orElseThrow();
        return orderResultBody(batchNo, settled, outcome == null ? null : outcome.result(),
                outcome == null ? Map.of() : outcome.detail());
    }

    private RunSummary summarize(String batchNo, int processedNow, boolean interrupted) {
        BatchRow b = batches.findByNo(batchNo).orElseThrow();
        return new RunSummary(batchNo, b.status(), b.totalOrders(), b.processedOrders(),
                b.resultReleased(), b.resultReversed(), b.resultSkipped(), b.resultException(),
                interrupted);
    }

    // ---- reads -------------------------------------------------------------

    public Map<String, Object> batchReport(String batchNo) {
        BatchRow b = batches.findByNo(batchNo)
                .orElseThrow(() -> ApiException.notFound("RECALL_BATCH_NOT_FOUND", "召回批次不存在"));
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("batchNo", b.batchNo());
        m.put("recipeId", b.recipeId());
        m.put("versionId", b.versionId());
        m.put("versionNo", b.versionNo());
        m.put("cutoffAt", b.cutoffAt());
        m.put("status", b.status());
        m.put("reason", b.reason());
        m.put("operatorId", b.operatorId());
        m.put("createdAt", b.createdAt());
        m.put("startedAt", b.startedAt());
        m.put("finishedAt", b.finishedAt());
        m.put("totalOrders", b.totalOrders());
        m.put("processedOrders", b.processedOrders());
        Map<String, Object> counts = new LinkedHashMap<>();
        counts.put("RELEASED", b.resultReleased());
        counts.put("REVERSED", b.resultReversed());
        counts.put("SKIPPED", b.resultSkipped());
        counts.put("EXCEPTION", b.resultException());
        m.put("resultCounts", counts);
        m.put("orders", listBatchOrders(b));
        return m;
    }

    public List<Map<String, Object>> listBatches() {
        return batches.listRecent(100).stream().map(b -> {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("batchNo", b.batchNo());
            m.put("versionId", b.versionId());
            m.put("versionNo", b.versionNo());
            m.put("status", b.status());
            m.put("cutoffAt", b.cutoffAt());
            m.put("totalOrders", b.totalOrders());
            m.put("processedOrders", b.processedOrders());
            m.put("resultReleased", b.resultReleased());
            m.put("resultReversed", b.resultReversed());
            m.put("resultSkipped", b.resultSkipped());
            m.put("resultException", b.resultException());
            m.put("createdAt", b.createdAt());
            m.put("finishedAt", b.finishedAt());
            return m;
        }).toList();
    }

    private List<Map<String, Object>> listBatchOrders(BatchRow b) {
        List<Map<String, Object>> out = new ArrayList<>();
        for (BatchOrderRow r : batches.listOrdersByBatch(b.id())) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("orderNo", r.orderNo());
            m.put("playerId", r.playerId());
            m.put("snapshotStatus", r.snapshotStatus());
            m.put("status", r.status());
            m.put("result", r.result());
            m.put("detail", parseDetail(r.detailJson()));
            m.put("attempts", r.attempts());
            m.put("settledAt", r.settledAt());
            m.put("retryable", "EXCEPTION".equals(r.result()));
            out.add(m);
        }
        return out;
    }

    /** Retryable exceptions queue for the operator console. */
    public List<Map<String, Object>> listExceptions(String batchNo) {
        List<BatchRow> targets = batchNo == null || batchNo.isBlank()
                ? batches.listRecent(100)
                : batches.findByNo(batchNo).map(List::of).orElse(List.of());
        List<Map<String, Object>> out = new ArrayList<>();
        for (BatchRow b : targets) {
            for (BatchOrderRow r : batches.listOrdersByBatch(b.id())) {
                if ("EXCEPTION".equals(r.result())) {
                    Map<String, Object> m = new LinkedHashMap<>();
                    m.put("batchNo", b.batchNo());
                    m.put("versionNo", b.versionNo());
                    m.put("orderNo", r.orderNo());
                    m.put("playerId", r.playerId());
                    m.put("detail", parseDetail(r.detailJson()));
                    m.put("attempts", r.attempts());
                    m.put("retryable", true);
                    out.add(m);
                }
            }
        }
        return out;
    }

    public List<Map<String, Object>> orderEvents(String batchNo, String orderNo) {
        CraftOrder order = orders.findByNo(orderNo).orElse(null);
        if (order == null) {
            return List.of();
        }
        return batches.listEventsByOrder(batchNo, order.id()).stream().map(e -> {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("eventType", e.eventType());
            m.put("fromStatus", e.fromStatus());
            m.put("toStatus", e.toStatus());
            m.put("result", e.result());
            m.put("message", e.message());
            m.put("createdAt", e.createdAt());
            return m;
        }).toList();
    }

    private Map<String, Object> orderResultBody(String batchNo, CraftOrder order, String result,
                                                Map<String, Object> detail) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("batchNo", batchNo);
        m.put("orderNo", order.orderNo());
        m.put("status", order.status());
        m.put("result", result);
        m.put("detail", detail);
        return m;
    }

    private static Map<String, Object> parseDetail(String json) {
        if (json == null || json.isBlank()) {
            return Map.of();
        }
        try {
            return Json.MAPPER.readValue(json,
                    Json.MAPPER.getTypeFactory().constructMapType(LinkedHashMap.class, String.class, Object.class));
        } catch (Exception e) {
            return Map.of();
        }
    }

    /** Raised at a crash injection point; tests catch it to model a hard process kill. */
    public static class SimulatedCrashException extends RuntimeException {
        public SimulatedCrashException(String batchNo) {
            super("simulated crash during recall batch " + batchNo);
        }
    }
}
