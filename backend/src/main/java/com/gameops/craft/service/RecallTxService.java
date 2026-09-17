package com.gameops.craft.service;

import com.gameops.craft.common.ApiException;
import com.gameops.craft.common.DocNumbers;
import com.gameops.craft.common.Json;
import com.gameops.craft.domain.CraftOrder;
import com.gameops.craft.domain.ItemQty;
import com.gameops.craft.domain.LedgerEntry;
import com.gameops.craft.domain.RecipeVersion;
import com.gameops.craft.repo.HoldRepository;
import com.gameops.craft.repo.InventoryRepository;
import com.gameops.craft.repo.LedgerRepository;
import com.gameops.craft.repo.OrderRepository;
import com.gameops.craft.repo.RecipeRepository;
import com.gameops.craft.repo.RecallBatchRepository;
import com.gameops.craft.repo.RecallBatchRepository.BatchOrderRow;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Per-order liquidation inside one transaction per order, so a crash/restart can
 * resume from the next unfinished snapshot row without ever partially committing.
 *
 * Exactly one state migration wins per order (row lock + CAS on craft_order):
 *  - PREOCCUPIED        -> RECALLED           : release held materials (RELEASE pair)
 *  - COMMITTED, outputs  -> RECALLED           : claw back ALL outputs + return ORIGINAL inputs
 *                          fully present         (paired RECALL_CLAWBACK / RECALL_RETURN rows)
 *  - COMMITTED, any     -> RECALL_EXCEPTION    : no partial liquidation, retryable
 *                          output short
 *  - CANCELLED/TIMEOUT/REVOKED                 : judgement recorded only, history untouched
 *
 * A concurrent commit/cancel/timeout/revoke that commits first moves the order to
 * another status; the order row lock is held for the whole liquidation, so the
 * branch decision and its CAS are atomic and the order lands in exactly one result.
 */
@Service
public class RecallTxService {

    public record OrderOutcome(String result, Map<String, Object> detail) {}

    private final RecallBatchRepository batches;
    private final OrderRepository orders;
    private final HoldRepository holds;
    private final InventoryRepository inventory;
    private final LedgerRepository ledger;
    private final RecipeRepository recipes;
    private final Clock clock;

    public RecallTxService(RecallBatchRepository batches, OrderRepository orders, HoldRepository holds,
                           InventoryRepository inventory, LedgerRepository ledger,
                           RecipeRepository recipes, Clock clock) {
        this.batches = batches;
        this.orders = orders;
        this.holds = holds;
        this.inventory = inventory;
        this.ledger = ledger;
        this.recipes = recipes;
        this.clock = clock;
    }

    /**
     * Liquidate one snapshot order in a single transaction: order CAS, inventory moves,
     * ledger pairs, snapshot result, event and batch counters commit or roll back together.
     *
     * @return RELEASED / REVERSED / SKIPPED / EXCEPTION
     */
    @Transactional
    public OrderOutcome liquidate(String batchNo, long batchId, long orderId, boolean isRetry) {
        Instant now = Instant.now(clock);

        // Lock the snapshot row first: two workers / two retries for the same order serialise here.
        BatchOrderRow snap = batches.findSnapshotRowForUpdate(batchId, orderId)
                .orElseThrow(() -> ApiException.notFound("RECALL_ROW_NOT_FOUND", "清算快照行不存在"));
        if ("DONE".equals(snap.status()) && !"EXCEPTION".equals(snap.result()) && !isRetry) {
            // Previous run finished this order; never compensate twice.
            return new OrderOutcome(snap.result(), Map.of("alreadySettled", true));
        }
        if (isRetry && "DONE".equals(snap.status()) && "EXCEPTION".equals(snap.result())) {
            // Explicit retry of a parked order: reopen for this attempt (still one tx).
            batches.markExceptionRetrying(snap.id(), Instant.now(clock));
            snap = batches.findSnapshotRowForUpdate(batchId, orderId).orElseThrow();
        } else if ("DONE".equals(snap.status()) && !"EXCEPTION".equals(snap.result())) {
            return new OrderOutcome(snap.result(), Map.of("alreadySettled", true));
        }

        // Lock the order: the arbitration point against player commit/cancel/timeout/revoke.
        CraftOrder order = orders.findByIdForUpdate(orderId)
                .orElseThrow(() -> ApiException.notFound("ORDER_NOT_FOUND", "合成单不存在"));

        batches.insertEvent(batchNo, order.id(), order.orderNo(), isRetry ? "RETRY" : "ATTEMPT",
                order.status(), null, null,
                (isRetry ? "第 " + (snap.attempts() + 1) + " 次清算尝试" : "开始清算"), now);

        // Per-order settlement document no: an order's clawback/return pair shares it.
        // Generated inside the tx; a rolled-back attempt leaves no ledger rows behind.
        String settleNo = DocNumbers.next("RC", clock);
        OrderOutcome outcome = dispatch(order, batchNo, settleNo, now);

        Instant doneAt = Instant.now(clock);
        String detailJson = writeJson(outcome.detail());
        // Every attempted order is DONE after this transaction. An EXCEPTION is a finished
        // verdict ("parked, retryable"), not unfinished work — it keeps the batch from being
        // stuck PENDING and is retried only via an explicit retry action.
        batches.markOrderDone(snap.id(), outcome.result(), detailJson, doneAt);
        batches.insertEvent(batchNo, order.id(), order.orderNo(), "SETTLED",
                order.status(), order.status(), outcome.result(),
                String.valueOf(outcome.detail().getOrDefault("reason", "")), doneAt);
        batches.refreshBatchCounters(batchId, doneAt);
        return outcome;
    }

    /** Dispatch by the (locked, stable) current order status. */
    private OrderOutcome dispatch(CraftOrder order, String batchNo, String settleNo, Instant now) {
        return switch (order.status()) {
            case "PREOCCUPIED" -> releasePreoccupied(order, batchNo, settleNo, now);
            case "COMMITTED" -> reverseCommitted(order, batchNo, settleNo, now, false);
            case "RECALL_EXCEPTION" -> retryException(order, batchNo, settleNo, now);
            case "CANCELLED", "TIMEOUT", "REVOKED", "RECALLED" -> alreadyClosed(order, batchNo, now);
            default -> new OrderOutcome("SKIPPED",
                    Map.of("reason", "UNEXPECTED_STATUS:" + order.status()));
        };
    }

    // ---- branches ----------------------------------------------------------

    /** PREOCCUPIED at recall time: give every held material line back, once. */
    private OrderOutcome releasePreoccupied(CraftOrder order, String batchNo, String settleNo, Instant now) {
        int cas = orders.casRecallRelease(order.id(), batchNo, "版本紧急召回，释放预占材料", now);
        if (cas == 0) {
            // Defense in depth: state changed before our lock. Re-dispatch on the new status.
            CraftOrder moved = orders.findByIdForUpdate(order.id()).orElseThrow();
            return dispatch(moved, batchNo, settleNo, now);
        }
        List<Map<String, Object>> returned = new ArrayList<>();
        List<HoldRepository.HoldRow> rows = holds.lockByOrder(order.id());
        for (HoldRepository.HoldRow h : rows) {
            if (holds.markReleased(h.id(), now) == 1) {
                inventory.credit(h.playerId(), h.itemCode(), h.qty(), now);
                // Positive pair of the original CONSUME; related_ref ties it to the order.
                ledger.insert(settleNo, h.playerId(), h.itemCode(), "RELEASE", h.qty(),
                        order.orderNo(), "POSTED", "召回释放预占原材料（批次 " + batchNo + "）", now);
                returned.add(Map.of("itemCode", h.itemCode(), "qty", h.qty()));
            }
        }
        Map<String, Object> detail = new LinkedHashMap<>();
        detail.put("reason", "预占中订单：释放全部原材料");
        detail.put("settleRefNo", settleNo);
        detail.put("returnedMaterials", returned);
        return new OrderOutcome("RELEASED", detail);
    }

    /**
     * COMMITTED at recall time: all-or-nothing reversal.
     * Outputs fully available -> claw back outputs, then return the ORIGINAL inputs
     * from the bound version snapshot, as paired ledger rows in this one transaction.
     * Any output already consumed -> EXCEPTION, nothing moves.
     */
    private OrderOutcome reverseCommitted(CraftOrder order, String batchNo, String settleNo,
                                          Instant now, boolean fromRetry) {
        RecipeVersion version = recipes.findVersionById(order.recipeVersionId())
                .orElseThrow(() -> new IllegalStateException("bound version missing"));
        List<LedgerEntry> produces = ledger.findProducesByOrderNo(order.orderNo());
        if (produces.isEmpty()) {
            throw new IllegalStateException("committed order without PRODUCE rows: " + order.orderNo());
        }

        // Lock every output row (sorted item order = same global lock order as crafting/revoke).
        List<LedgerEntry> sortedOutputs = produces.stream()
                .sorted(Comparator.comparing(LedgerEntry::itemCode)).toList();
        List<Map<String, Object>> shortage = new ArrayList<>();
        boolean complete = true;
        for (LedgerEntry p : sortedOutputs) {
            long have = inventory.lockAndGet(p.playerId(), p.itemCode());
            if (have < p.qtyDelta()) {
                complete = false;
                Map<String, Object> s = new LinkedHashMap<>();
                s.put("itemCode", p.itemCode());
                s.put("need", p.qtyDelta());
                s.put("have", have);
                s.put("short", p.qtyDelta() - have);
                shortage.add(s);
            }
        }

        if (!complete) {
            // No partial liquidation: park the whole order. PENDING rows never touch a balance;
            // every attempt leaves its own dated record (history preserved across retries).
            String pendingNo = DocNumbers.next("RP", clock);
            for (LedgerEntry p : sortedOutputs) {
                ledger.insert(pendingNo, p.playerId(), p.itemCode(), "RECALL_PENDING", -p.qtyDelta(),
                        order.orderNo(), "PENDING",
                        "召回挂账：产出已使用，等待补足后重试（批次 " + batchNo + "）", now);
            }
            int cas = orders.casRecallException(order.id(), batchNo,
                    "召回清算：产出已被使用，整单待处理，补足后可重试", now);
            if (cas == 0 && !fromRetry) {
                CraftOrder moved = orders.findByIdForUpdate(order.id()).orElseThrow();
                if (!"RECALL_EXCEPTION".equals(moved.status())) {
                    return dispatch(moved, batchNo, settleNo, now);
                }
            }
            Map<String, Object> detail = new LinkedHashMap<>();
            detail.put("reason", "产出不足，整单进入待处理异常，未做部分清算");
            detail.put("pendingRefNo", pendingNo);
            detail.put("shortage", shortage);
            return new OrderOutcome("EXCEPTION", detail);
        }

        // All outputs still present: one-shot full reversal.
        // 1) claw back the outputs (negative pair of PRODUCE)
        for (LedgerEntry p : sortedOutputs) {
            int changed = inventory.deductIfEnough(p.playerId(), p.itemCode(), p.qtyDelta(), now);
            if (changed == 0) {
                throw new IllegalStateException("recall clawback failed after lock: " + p.itemCode());
            }
            ledger.insert(settleNo, p.playerId(), p.itemCode(), "RECALL_CLAWBACK", -p.qtyDelta(),
                    order.orderNo(), "POSTED",
                    "版本召回：收回全部产出（批次 " + batchNo + "）", now);
        }
        // 2) return the ORIGINAL inputs per the captured version snapshot (positive pair of CONSUME)
        List<Map<String, Object>> returned = new ArrayList<>();
        for (ItemQty in : version.inputs()) {
            inventory.credit(order.playerId(), in.getItemCode(), in.getQty(), now);
            ledger.insert(settleNo, order.playerId(), in.getItemCode(), "RECALL_RETURN", in.getQty(),
                    order.orderNo(), "POSTED",
                    "版本召回：返还原始材料 v" + version.versionNo(), now);
            returned.add(Map.of("itemCode", in.getItemCode(), "qty", in.getQty()));
        }
        int cas = orders.casRecallReverse(order.id(), batchNo, now);
        if (cas == 0 && !fromRetry) {
            throw new IllegalStateException("recall reverse CAS failed after locks: " + order.orderNo());
        }
        Map<String, Object> detail = new LinkedHashMap<>();
        detail.put("reason", "已完成订单：一次性收回全部产出并返还原始材料");
        detail.put("clawedBackOutputs", sortedOutputs.stream()
                .map(p -> Map.of("itemCode", p.itemCode(), "qty", p.qtyDelta())).toList());
        detail.put("returnedMaterials", returned);
        detail.put("settleRefNo", settleNo);
        return new OrderOutcome("REVERSED", detail);
    }

    /** A previously parked order retried after the missing outputs were replenished. */
    private OrderOutcome retryException(CraftOrder order, String batchNo, String settleNo, Instant now) {
        OrderOutcome outcome = reverseCommitted(order, batchNo, settleNo, now, true);
        if ("REVERSED".equals(outcome.result())) {
            int cas = orders.casRecallExceptionToReversed(order.id(),
                    "召回异常补足后重试成功", Instant.now(clock));
            if (cas == 0) {
                throw new IllegalStateException("exception->reversed CAS failed: " + order.orderNo());
            }
            Map<String, Object> detail = new LinkedHashMap<>(outcome.detail());
            detail.put("retried", true);
            return new OrderOutcome("REVERSED", detail);
        }
        // Still short: stays RECALL_EXCEPTION; earlier PENDING records are preserved.
        return outcome;
    }

    /** Cancelled / timed out / manually revoked / already recalled: judgement only. */
    private OrderOutcome alreadyClosed(CraftOrder order, String batchNo, Instant now) {
        // Record batch ownership for player-facing visibility; never rewrite history rows.
        orders.markRecallBatchOnly(order.id(), batchNo, now);
        String reason = switch (order.status()) {
            case "CANCELLED" -> "已取消订单：仅记录判定结果，历史流水不变";
            case "TIMEOUT" -> "已超时订单：仅记录判定结果，历史流水不变";
            case "REVOKED" -> "已撤销订单：仅记录判定结果，历史流水不变";
            case "RECALLED" -> "订单此前已完成召回清算";
            default -> "订单已结束：仅记录判定结果";
        };
        Map<String, Object> detail = new LinkedHashMap<>();
        detail.put("reason", reason);
        detail.put("snapshotStatus", order.status());
        return new OrderOutcome("SKIPPED", detail);
    }

    private static String writeJson(Object value) {
        try {
            return Json.MAPPER.writeValueAsString(value);
        } catch (Exception e) {
            throw new IllegalStateException("failed to serialize recall detail", e);
        }
    }
}
