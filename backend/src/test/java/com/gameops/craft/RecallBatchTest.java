package com.gameops.craft;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.gameops.craft.common.ApiException;
import com.gameops.craft.domain.LedgerEntry;
import com.gameops.craft.repo.InventoryRepository;
import com.gameops.craft.repo.LedgerRepository;
import com.gameops.craft.repo.OrderRepository;
import com.gameops.craft.repo.RecallBatchRepository;
import com.gameops.craft.service.CraftService;
import com.gameops.craft.service.CraftTxService;
import com.gameops.craft.service.RecallService;
import com.gameops.craft.service.RecallTxService;
import com.gameops.craft.support.TestDataResetter;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;

/**
 * Emergency version recall & liquidation batch coverage:
 *  1. mixed statuses liquidate correctly with paired compensation rows;
 *  2. recall blocks new preoccupies immediately;
 *  3. recall vs player commit race: exactly one legal result chain per order;
 *  4. crash mid-batch + restart continuation + duplicate requests -> no duplicate ledger;
 *  5. insufficient output -> retryable EXCEPTION; replenish -> retry succeeds, history kept;
 *  6. non-recalled versions and the original preoccupy/timeout/revoke/last-material flows stay alive.
 */
@SpringBootTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class RecallBatchTest {

    @Autowired private CraftService craft;
    @Autowired private RecallService recall;
    @Autowired private RecallTxService recallTx;
    @Autowired private InventoryRepository inventory;
    @Autowired private LedgerRepository ledger;
    @Autowired private OrderRepository orders;
    @Autowired private RecallBatchRepository batchRepo;
    @Autowired private TestDataResetter resetter;

    // player3 (id 4) owns plenty of materials for several FIRE_SWORD (v1) crafts.
    private static final long P = 4L;
    private static final long RECIPE = 1L;
    private static final long VERSION_V1 = 1L;

    @BeforeEach
    void resetData() {
        recall.crashAfterOrder = ctx -> false;
        resetter.reset();
        // Plenty for 6 FIRE_SWORD crafts: 18 IRON / 12 CORE / 6 SHARD (covers the race test).
        grant(P, "MAT_IRON", 18);
        grant(P, "MAT_MAGIC_CORE", 12);
        grant(P, "MAT_FIRE_SHARD", 6);
    }

    private void grant(long player, String item, long qty) {
        inventory.upsertBalance(player, item, qty, Instant.now());
    }

    private String preoccupy() {
        return (String) craft.preoccupy(P, RECIPE, UUID.randomUUID().toString()).get("orderNo");
    }

    private void commit(String orderNo) {
        craft.commit(P, orderNo, UUID.randomUUID().toString());
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> counts(String batchNo) {
        return (Map<String, Object>) recall.batchReport(batchNo).get("resultCounts");
    }

    private long countType(List<LedgerEntry> es, String type) {
        return es.stream().filter(e -> type.equals(e.entryType())).count();
    }

    // ---------------------------------------------------------------- (1)

    @Test
    void mixed_status_orders_liquidate_with_paired_compensation_and_consistent_balances() {
        String pre = preoccupy();                                   // stays PREOCCUPIED
        String committed = preoccupy(); commit(committed);          // COMMITTED, outputs held
        String cancelled = preoccupy(); craft.cancel(P, cancelled, "test cancel");
        String timedOut = makeExpiredPreoccupy();                   // TIMEOUT (deadline forced)
        String revoked = preoccupy(); commit(revoked);
        // Operator revokes this one BEFORE the recall batch runs: it ends REVOKED.
        craft.forceTimeout(timedOut); // ensure expired via helper below (already TIMEOUT)
        var revokeResult = revokeCommitted(revoked);
        assertThat(revokeResult).isEqualTo("REVERSED");

        // Snapshot balances before recall.
        long ironBefore = inventory.getQty(P, "MAT_IRON");
        long coreBefore = inventory.getQty(P, "MAT_MAGIC_CORE");
        long shardBefore = inventory.getQty(P, "MAT_FIRE_SHARD");
        long swordBefore = inventory.getQty(P, "EQP_FIRE_SWORD");
        long goldBefore = inventory.getQty(P, "GOLD");

        var init = recall.initiate(VERSION_V1, UUID.randomUUID().toString(), "配置错误紧急召回", 1L);
        String batchNo = init.batchNo();
        assertThat(init.created()).isTrue();
        // 5 orders in scope across every lifecycle state.
        assertThat(init.totalOrders()).isEqualTo(5);

        var summary = recall.runBatch(batchNo);
        assertThat(summary.status()).isEqualTo("COMPLETED");
        Map<String, Object> c = counts(batchNo);
        assertThat(c.get("RELEASED")).isEqualTo(1);   // pre
        assertThat(c.get("REVERSED")).isEqualTo(1);   // committed
        assertThat(c.get("SKIPPED")).isEqualTo(3);    // cancelled + timeout + revoked
        assertThat(c.get("EXCEPTION")).isEqualTo(0);

        // The revoked order was already reversed by the operator BEFORE the recall
        // (outputs clawed back, original materials NOT returned by revoke — original semantics).
        // Recall changes only the one committed order: -1 sword/-100 gold, +3/+2/+1 materials.
        // The preoccupied order is released: +3/+2/+1. Cancelled/timed-out/revoked: no change.
        assertThat(inventory.getQty(P, "MAT_IRON")).isEqualTo(ironBefore + 3 + 3);
        assertThat(inventory.getQty(P, "MAT_MAGIC_CORE")).isEqualTo(coreBefore + 2 + 2);
        assertThat(inventory.getQty(P, "MAT_FIRE_SHARD")).isEqualTo(shardBefore + 1 + 1);
        assertThat(inventory.getQty(P, "EQP_FIRE_SWORD")).isEqualTo(swordBefore - 1);
        assertThat(inventory.getQty(P, "GOLD")).isEqualTo(goldBefore - 100);

        // Player-facing statuses.
        assertThat(orders.findByNo(pre).orElseThrow().status()).isEqualTo("RECALLED");
        assertThat(orders.findByNo(committed).orElseThrow().status()).isEqualTo("RECALLED");
        assertThat(orders.findByNo(cancelled).orElseThrow().status()).isEqualTo("CANCELLED");
        assertThat(orders.findByNo(timedOut).orElseThrow().status()).isEqualTo("TIMEOUT");
        assertThat(orders.findByNo(revoked).orElseThrow().status()).isEqualTo("REVOKED");
        assertThat(orders.findByNo(committed).orElseThrow().recallBatchNo()).isEqualTo(batchNo);
        // Already-terminal orders still carry the batch marker for player visibility.
        assertThat(orders.findByNo(revoked).orElseThrow().recallBatchNo()).isEqualTo(batchNo);

        // Paired compensation rows on the committed order: clawback + return, related to the order.
        var committedLedger = ledger.listByRef(committed);
        assertThat(countType(committedLedger, "RECALL_CLAWBACK")).isEqualTo(2); // sword + gold
        assertThat(countType(committedLedger, "RECALL_RETURN")).isEqualTo(3);   // iron + core + shard
        assertThat(committedLedger).filteredOn(e -> "RECALL_CLAWBACK".equals(e.entryType()))
                .allMatch(e -> e.qtyDelta() < 0 && committed.equals(e.relatedRef()));
        assertThat(committedLedger).filteredOn(e -> "RECALL_RETURN".equals(e.entryType()))
                .allMatch(e -> e.qtyDelta() > 0 && committed.equals(e.relatedRef()));
        // One order's clawback & return pair share a single settle doc ref_no.
        var settleRefs = committedLedger.stream()
                .filter(e -> e.entryType().equals("RECALL_CLAWBACK") || e.entryType().equals("RECALL_RETURN"))
                .map(LedgerEntry::refNo).distinct().toList();
        assertThat(settleRefs).hasSize(1);

        // Preoccupied order got exactly 3 recall RELEASE rows (one per held material).
        assertThat(countType(ledger.listByRef(pre), "RELEASE")).isEqualTo(3);

        // Cancelled/timed-out orders keep their original history; no recall rows appended.
        for (String closed : new String[]{cancelled, timedOut, revoked}) {
            var rows = ledger.listByRef(closed);
            assertThat(rows).noneMatch(e -> e.entryType().startsWith("RECALL_"));
        }
        // Revoked order already has its operator REVOKE rows, untouched.
        assertThat(countType(ledger.listByRef(revoked), "REVOKE")).isEqualTo(2);

        // Persisted per-order results.
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> orderRows =
                (List<Map<String, Object>>) recall.batchReport(batchNo).get("orders");
        assertThat(orderRows).allSatisfy(o -> assertThat(o.get("status")).isEqualTo("DONE"));
    }

    /** Create a preoccupy whose deadline is already in the past, then let it time out. */
    private String makeExpiredPreoccupy() {
        String orderNo = preoccupy();
        // Push the deadline into the past (test-only direct SQL) so the sweeper CAS applies.
        ordersJdbcUpdateDeadline(orderNo);
        boolean timed = craft.forceTimeout(orderNo);
        assertThat(timed).isTrue();
        return orderNo;
    }

    @Autowired private org.springframework.jdbc.core.JdbcTemplate jdbc;

    private void ordersJdbcUpdateDeadline(String orderNo) {
        jdbc.update("UPDATE craft_order SET preoccupy_deadline = ? WHERE order_no = ?",
                java.sql.Timestamp.from(Instant.now().minusSeconds(60)), orderNo);
    }

    @Autowired private CraftTxService craftTx;

    private String revokeCommitted(String orderNo) {
        return craftTx.revoke(orderNo, 1L).result();
    }

    // ---------------------------------------------------------------- (2)

    @Test
    void recall_blocks_new_preoccupies_immediately_and_unrecalled_recipe_still_works() {
        preoccupy(); // one order on v1 in scope
        var init = recall.initiate(VERSION_V1, UUID.randomUUID().toString(), "召回", 1L);
        recall.runBatch(init.batchNo());

        // New preoccupy on the recalled version is refused by the server transaction.
        assertThatThrownBy(() -> craft.preoccupy(P, RECIPE, UUID.randomUUID().toString()))
                .isInstanceOf(ApiException.class)
                .extracting(e -> ((ApiException) e).getCode())
                .isEqualTo("VERSION_RECALLED");

        // Preview reports the server-side recall verdict.
        Map<String, Object> preview = craft.preview(P, RECIPE);
        assertThat(preview.get("craftable")).isEqualTo(false);
        assertThat(preview.get("reason")).isEqualTo("VERSION_RECALLED");

        // A DIFFERENT recipe/version (THUNDER_BOW v1, id 2) is unaffected: normal flow works.
        long p3 = 3L; // player2 owns 5 WOOD / 1 CORE
        String o = (String) craft.preoccupy(p3, 2L, UUID.randomUUID().toString()).get("orderNo");
        craft.commit(p3, o, UUID.randomUUID().toString());
        assertThat(inventory.getQty(p3, "EQP_THUNDER_BOW")).isEqualTo(1);
    }

    // ---------------------------------------------------------------- (3)

    @Test
    void recall_and_commit_race_only_one_legal_chain_per_order() throws Exception {
        int pairs = 6;
        List<String> orderNos = new ArrayList<>();
        for (int i = 0; i < pairs; i++) {
            orderNos.add(preoccupy()); // 6 PREOCCUPIED orders
        }
        var init = recall.initiate(VERSION_V1, UUID.randomUUID().toString(), "并发召回", 1L);
        String batchNo = init.batchNo();
        assertThat(init.totalOrders()).isEqualTo(pairs);

        // Race: for every order a player COMMIT and the recall LIQUIDATION fire together.
        ExecutorService pool = Executors.newFixedThreadPool(pairs * 2);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger commitWon = new AtomicInteger();
        AtomicInteger recallReleased = new AtomicInteger();
        AtomicInteger recallReversed = new AtomicInteger();
        AtomicInteger commitRejected = new AtomicInteger();
        long batchId = batchRepo.findByNo(batchNo).orElseThrow().id();
        for (String orderNo : orderNos) {
            pool.submit(() -> {
                try {
                    start.await();
                    commit(orderNo);
                    commitWon.incrementAndGet();
                } catch (ApiException e) {
                    // Lost to recall: order already closed / recall-pending.
                    commitRejected.incrementAndGet();
                } catch (Exception ignored) {
                }
            });
            long orderId = orders.findByNo(orderNo).orElseThrow().id();
            pool.submit(() -> {
                try {
                    start.await();
                    var out = recallTx.liquidate(batchNo, batchId, orderId, false);
                    if ("RELEASED".equals(out.result())) {
                        recallReleased.incrementAndGet();
                    } else if ("REVERSED".equals(out.result())) {
                        recallReversed.incrementAndGet();
                    }
                } catch (Exception ignored) {
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertThat(pool.awaitTermination(60, TimeUnit.SECONDS)).isTrue();

        // Finish any order the recall did not get to in the racy direct calls.
        recall.runBatch(batchNo);
        Map<String, Object> c = counts(batchNo);
        assertThat(((Number) c.get("RELEASED")).intValue() + ((Number) c.get("REVERSED")).intValue())
                .isEqualTo(pairs);
        assertThat(c.get("SKIPPED")).isEqualTo(0);
        assertThat(c.get("EXCEPTION")).isEqualTo(0);

        // Exactly one terminal outcome per order: COMMITTED (commit won) then REVERSED by recall,
        // or RECALLED (recall won) and the player commit was rejected.
        for (String orderNo : orderNos) {
            assertThat(orders.findByNo(orderNo).orElseThrow().status()).isEqualTo("RECALLED");
        }
        // The two racer win counts partition the order set at the PREOCCUPIED boundary.
        assertThat(recallReleased.get()).isEqualTo(commitRejected.get());
        assertThat(recallReleased.get() + commitWon.get()).isEqualTo(pairs);
        // Orders the commit won are then fully reversed by the recall batch (not released).
        assertThat(((Number) c.get("REVERSED")).intValue()).isEqualTo(commitWon.get());

        // Global material invariant for every touched item: posted ledger deltas of the craft
        // lifecycle net to zero (consume/release; produce/clawback; returned inputs remain).
        assertCraftLifecycleNetsToConsumedReturnOnly(P);
        assertNoDuplicateCompensation(batchNo);
    }

    // ---------------------------------------------------------------- (4)

    @Test
    void crash_mid_batch_restart_resumes_and_duplicate_calls_do_not_double_compensate() {
        String o1 = preoccupy();
        String o2 = preoccupy(); commit(o2);
        String o3 = preoccupy();
        var init = recall.initiate(VERSION_V1, UUID.randomUUID().toString(), "中断测试", 1L);
        String batchNo = init.batchNo();
        long ironBefore = inventory.getQty(P, "MAT_IRON");

        // Kill the (simulated) process right after the FIRST order's transaction committed.
        recall.crashAfterOrder = ctx -> ctx.ordersCommitted() == 1;
        var partial = recall.runBatch(batchNo, 10);
        assertThat(partial.interrupted()).isTrue();
        recall.crashAfterOrder = ctx -> false;

        // Exactly one order settled before the kill.
        Map<String, Object> mid = counts(batchNo);
        int doneMid = ((Number) recall.batchReport(batchNo).get("processedOrders")).intValue();
        assertThat(doneMid).isEqualTo(1);

        // "Restart": run again. Outstanding rows resume; the done one is never repeated.
        var after = recall.runBatch(batchNo);
        assertThat(after.status()).isEqualTo("COMPLETED");
        assertThat(after.processed()).isEqualTo(3);

        // Repeated runs (duplicate requests / another sweeper tick) change nothing.
        var again = recall.runBatch(batchNo);
        assertThat(again.processed()).isEqualTo(3);
        Map<String, Object> c = counts(batchNo);
        assertThat(c.get("RELEASED")).isEqualTo(2);
        assertThat(c.get("REVERSED")).isEqualTo(1);

        // o1/o3 released once each (+3 IRON), o2 reversed (+3 IRON return): total +9.
        assertThat(inventory.getQty(P, "MAT_IRON")).isEqualTo(ironBefore + 9);

        // Ledger: o1/o3 each have exactly their 3 recall RELEASE rows (one per held item);
        // o2 has one clawback set (2 outputs) and one return set (3 inputs).
        assertThat(countType(ledger.listByRef(o1), "RELEASE")).isEqualTo(3);
        assertThat(countType(ledger.listByRef(o3), "RELEASE")).isEqualTo(3);
        assertThat(countType(ledger.listByRef(o2), "RECALL_CLAWBACK")).isEqualTo(2);
        assertThat(countType(ledger.listByRef(o2), "RECALL_RETURN")).isEqualTo(3);
        assertNoDuplicateCompensation(batchNo);
        assertCraftLifecycleNetsToConsumedReturnOnly(P);
    }

    @Test
    void duplicate_initiation_same_key_or_repeat_returns_original_batch() {
        preoccupy();
        String key = UUID.randomUUID().toString();
        var first = recall.initiate(VERSION_V1, key, "召回", 1L);
        var sameKey = recall.initiate(VERSION_V1, key, "再次召回", 1L);
        assertThat(sameKey.batchNo()).isEqualTo(first.batchNo());
        assertThat(sameKey.created()).isFalse();

        // A different key on the SAME already-recalled version also returns that batch.
        var otherKey = recall.initiate(VERSION_V1, UUID.randomUUID().toString(), "又一次", 1L);
        assertThat(otherKey.batchNo()).isEqualTo(first.batchNo());
        assertThat(otherKey.created()).isFalse();

        recall.runBatch(first.batchNo());
        // Running twice still yields one compensation chain per order.
        recall.runBatch(first.batchNo());
        assertNoDuplicateCompensation(first.batchNo());
    }

    // ---------------------------------------------------------------- (5)

    @Test
    void insufficient_outputs_parks_retryable_exception_then_replenish_retries_and_keeps_history() {
        String orderNo = preoccupy(); commit(orderNo);
        // Player spends the 100 GOLD output, so full clawback is impossible.
        grant(P, "GOLD", 0);

        var init = recall.initiate(VERSION_V1, UUID.randomUUID().toString(), "产出被耗用", 1L);
        String batchNo = init.batchNo();
        var summary = recall.runBatch(batchNo);
        assertThat(summary.status()).isEqualTo("PARTIAL_EXCEPTION");
        assertThat(summary.exception()).isEqualTo(1);

        // Order is parked (not partially liquidated): sword kept, no materials returned.
        assertThat(orders.findByNo(orderNo).orElseThrow().status()).isEqualTo("RECALL_EXCEPTION");
        assertThat(inventory.getQty(P, "EQP_FIRE_SWORD")).isEqualTo(1);
        long ironWhileException = inventory.getQty(P, "MAT_IRON");

        // Retry while still short fails again; batch remains partial; first PENDING record kept.
        var stillFails = recall.retryOrder(batchNo, orderNo, 1L);
        assertThat(stillFails.get("result")).isEqualTo("EXCEPTION");
        var pendingRows = ledger.listByRef(orderNo).stream()
                .filter(e -> "RECALL_PENDING".equals(e.entryType())).toList();
        assertThat(pendingRows).hasSizeGreaterThanOrEqualTo(2); // every attempt leaves a record

        // Operator replenishes the consumed output; retry now succeeds.
        grant(P, "GOLD", 100);
        var retried = recall.retryOrder(batchNo, orderNo, 1L);
        assertThat(retried.get("result")).isEqualTo("REVERSED");
        assertThat(orders.findByNo(orderNo).orElseThrow().status()).isEqualTo("RECALLED");

        // Outputs clawed back, original materials returned (one-shot, no partial leftovers).
        assertThat(inventory.getQty(P, "EQP_FIRE_SWORD")).isZero();
        assertThat(inventory.getQty(P, "GOLD")).isZero();
        assertThat(inventory.getQty(P, "MAT_IRON")).isEqualTo(ironWhileException + 3);

        // Batch now fully complete; prior PENDING history is preserved; compensation exactly once.
        var done = recall.batchReport(batchNo);
        assertThat(done.get("status")).isEqualTo("COMPLETED");
        assertThat(counts(batchNo).get("REVERSED")).isEqualTo(1);
        assertThat(countType(ledger.listByRef(orderNo), "RECALL_CLAWBACK")).isEqualTo(2);
        assertThat(countType(ledger.listByRef(orderNo), "RECALL_RETURN")).isEqualTo(3);
        assertNoDuplicateCompensation(batchNo);

        // The retryable exception queue no longer lists this order.
        assertThat(recall.listExceptions(batchNo)).isEmpty();
        // Attempt history is persisted (first ATTEMPT, failing RETRY, successful RETRY, SETTLED).
        var events = recall.orderEvents(batchNo, orderNo);
        assertThat(events).extracting(e -> e.get("eventType"))
                .contains("ATTEMPT", "RETRY", "SETTLED");
    }

    // ---------------------------------------------------------------- (6)

    @Test
    void original_flows_revoke_and_last_material_race_remain_available() {
        // Revoke on a NON-recalled version still follows the original semantics.
        long p3 = 3L;
        String bow = (String) craft.preoccupy(p3, 2L, UUID.randomUUID().toString()).get("orderNo");
        craft.commit(p3, bow, UUID.randomUUID().toString());
        assertThat(craftTx.revoke(bow, 1L).result()).isEqualTo("REVERSED");
        assertThat(orders.findByNo(bow).orElseThrow().status()).isEqualTo("REVOKED");

        // Last-material race on recipe 2: player2's reset inventory allows exactly one craft.
        grant(p3, "MAT_WOOD", 2);
        grant(p3, "MAT_MAGIC_CORE", 1);
        int threads = 5;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicInteger ok = new AtomicInteger();
        AtomicInteger rejected = new AtomicInteger();
        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                try {
                    start.await();
                    craft.preoccupy(p3, 2L, UUID.randomUUID().toString());
                    ok.incrementAndGet();
                } catch (ApiException e) {
                    if ("MATERIAL_INSUFFICIENT".equals(e.getCode())) {
                        rejected.incrementAndGet();
                    }
                } catch (Exception ignored) {
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        try {
            assertThat(done.await(60, TimeUnit.SECONDS)).isTrue();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        pool.shutdown();
        assertThat(ok.get()).isEqualTo(1);
        assertThat(rejected.get()).isEqualTo(threads - 1);
    }

    // ---- invariants --------------------------------------------------------

    /**
     * After a full recall liquidation of FIRE_SWORD orders, the posted ledger for the craft
     * item family must be internally consistent:
     *  - outputs (sword/gold): CONSUME none; produced == clawed back -> net 0
     *  - inputs (iron/core/shard): CONSUME(-) + RELEASE(+) + RECALL_RETURN(+) == 0
     *    (cancelled/timed-out orders release via RELEASE; reversed orders via RECALL_RETURN;
     *     released-preoccupied via RELEASE)
     */
    private void assertCraftLifecycleNetsToConsumedReturnOnly(long playerId) {
        Map<String, Long> net = new java.util.HashMap<>();
        for (LedgerEntry e : ledger.listByPlayer(playerId, 10_000)) {
            if (!"POSTED".equals(e.status())) {
                continue;
            }
            if (e.entryType().equals("GRANT")) {
                continue; // operator setup, outside the craft lifecycle
            }
            net.merge(e.itemCode(), e.qtyDelta(), Long::sum);
        }
        for (String item : new String[]{"MAT_IRON", "MAT_MAGIC_CORE", "MAT_FIRE_SHARD",
                "EQP_FIRE_SWORD", "GOLD"}) {
            assertThat(net.getOrDefault(item, 0L))
                    .as("posted ledger net for %s must be zero after full recall", item)
                    .isZero();
        }
    }

    /**
     * Compensation must never be duplicated: for any batch, each order has at most one
     * settle doc producing clawback/return rows, and no two rows share the anti-double key.
     */
    private void assertNoDuplicateCompensation(String batchNo) {
        // Group recall ledger rows by (player,item,type) across the WHOLE batch: distinct ref_no
        // per order; two settled rows for the same order+type would be a duplicated compensation.
        List<LedgerEntry> all = ledger.listByPlayer(P, 10_000).stream()
                .filter(e -> e.entryType().equals("RECALL_CLAWBACK")
                        || e.entryType().equals("RECALL_RETURN"))
                .toList();
        // Key by (relatedRef=orderNo, item, type): must be unique.
        var seen = new java.util.HashSet<String>();
        for (LedgerEntry e : all) {
            String key = e.relatedRef() + "|" + e.itemCode() + "|" + e.entryType();
            assertThat(seen.add(key)).as("no duplicated compensation: %s", key).isTrue();
        }
        // RELEASE rows via recall must also be unique per released order+item.
        List<LedgerEntry> releases = ledger.listByPlayer(P, 10_000).stream()
                .filter(e -> "RELEASE".equals(e.entryType())
                        && e.remark() != null && e.remark().contains("召回"))
                .toList();
        var seenRelease = new java.util.HashSet<String>();
        for (LedgerEntry e : releases) {
            String key = e.relatedRef() + "|" + e.itemCode();
            assertThat(seenRelease.add(key)).as("no duplicated recall release: %s", key).isTrue();
        }
    }
}
