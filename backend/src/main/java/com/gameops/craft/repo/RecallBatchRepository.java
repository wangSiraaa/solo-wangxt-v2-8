package com.gameops.craft.repo;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Repository;

/**
 * Persistence for emergency recall batches:
 *  - one batch per recalled version (uk_recall_batch_version)
 *  - one batch per idempotency key (uk_recall_batch_key): repeats return the original batch
 *  - one snapshot row per affected order (uk_recall_order): exactly one final result each
 */
@Repository
public class RecallBatchRepository {

    public record BatchRow(long id, String batchNo, String idempotencyKey, long recipeId,
                           long versionId, int versionNo, Instant cutoffAt, String status,
                           String reason, long operatorId, int totalOrders, int processedOrders,
                           int resultReleased, int resultReversed, int resultSkipped,
                           int resultException, Instant createdAt, Instant startedAt,
                           Instant finishedAt) {}

    public record BatchOrderRow(long id, long batchId, String batchNo, long orderId, String orderNo,
                                long playerId, String snapshotStatus, String status, String result,
                                String detailJson, int attempts, Instant settledAt,
                                Instant createdAt, Instant updatedAt) {}

    public record EventRow(long id, String batchNo, long orderId, String orderNo, String eventType,
                           String fromStatus, String toStatus, String result, String message,
                           Instant createdAt) {}

    private final JdbcTemplate jdbc;

    public RecallBatchRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    private static BatchRow mapBatch(ResultSet rs) throws SQLException {
        return new BatchRow(
                rs.getLong("id"), rs.getString("batch_no"), rs.getString("idempotency_key"),
                rs.getLong("recipe_id"), rs.getLong("recipe_version_id"), rs.getInt("version_no"),
                ts(rs, "cutoff_at"), rs.getString("status"), rs.getString("reason"),
                rs.getLong("operator_id"), rs.getInt("total_orders"), rs.getInt("processed_orders"),
                rs.getInt("result_released"), rs.getInt("result_reversed"),
                rs.getInt("result_skipped"), rs.getInt("result_exception"),
                ts(rs, "created_at"), ts(rs, "started_at"), ts(rs, "finished_at"));
    }

    private static Instant ts(ResultSet rs, String col) throws SQLException {
        Timestamp t = rs.getTimestamp(col);
        return t == null ? null : t.toInstant();
    }

    private static final String BATCH_COLS = """
            id, batch_no, idempotency_key, recipe_id, recipe_version_id, version_no, cutoff_at,
            status, reason, operator_id, total_orders, processed_orders, result_released,
            result_reversed, result_skipped, result_exception, created_at, started_at, finished_at
            """;

    /**
     * Insert a batch. The two unique keys make both "repeat with same idempotency key" and
     * "second recall on the same version" land as DuplicateKeyException; callers then fetch
     * and return the existing batch instead of liquidating twice.
     */
    public void insertBatch(String batchNo, String idempotencyKey, long recipeId, long versionId,
                            int versionNo, Instant cutoff, String reason, long operatorId,
                            int totalOrders, Instant now) {
        jdbc.update("""
                INSERT INTO recall_batch
                  (batch_no, idempotency_key, recipe_id, recipe_version_id, version_no, cutoff_at,
                   status, reason, operator_id, total_orders, processed_orders,
                   result_released, result_reversed, result_skipped, result_exception,
                   created_at, started_at)
                VALUES (?, ?, ?, ?, ?, ?, 'PENDING', ?, ?, ?, 0, 0, 0, 0, 0, ?, ?)
                """, batchNo, idempotencyKey, recipeId, versionId, versionNo, Timestamp.from(cutoff),
                reason, operatorId, totalOrders, Timestamp.from(now), Timestamp.from(now));
    }

    public Optional<BatchRow> findByNo(String batchNo) {
        return jdbc.query("SELECT " + BATCH_COLS + " FROM recall_batch WHERE batch_no = ?",
                (rs, n) -> mapBatch(rs), batchNo).stream().findFirst();
    }

    public Optional<BatchRow> findByIdempotencyKey(String key) {
        return jdbc.query("SELECT " + BATCH_COLS + " FROM recall_batch WHERE idempotency_key = ?",
                (rs, n) -> mapBatch(rs), key).stream().findFirst();
    }

    public Optional<BatchRow> findByVersionId(long versionId) {
        return jdbc.query("SELECT " + BATCH_COLS + " FROM recall_batch WHERE recipe_version_id = ?",
                (rs, n) -> mapBatch(rs), versionId).stream().findFirst();
    }

    public Optional<BatchRow> findByNoForUpdate(String batchNo) {
        return jdbc.query("SELECT " + BATCH_COLS + " FROM recall_batch WHERE batch_no = ? FOR UPDATE",
                (rs, n) -> mapBatch(rs), batchNo).stream().findFirst();
    }

    public List<BatchRow> listRecent(int limit) {
        return jdbc.query("SELECT " + BATCH_COLS + " FROM recall_batch ORDER BY id DESC LIMIT ?",
                (rs, n) -> mapBatch(rs), limit);
    }

    public boolean isDuplicate(Exception e) {
        return e instanceof DuplicateKeyException
                || (e.getCause() instanceof DuplicateKeyException);
    }

    // ---- snapshot rows -----------------------------------------------------

    public void insertSnapshotRow(long batchId, String batchNo, long orderId, String orderNo,
                                  long playerId, String snapshotStatus, Instant now) {
        jdbc.update("""
                INSERT INTO recall_batch_order
                  (batch_id, batch_no, order_id, order_no, player_id, snapshot_status,
                   status, result, attempts, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, 'PENDING', NULL, 0, ?, ?)
                """, batchId, batchNo, orderId, orderNo, playerId, snapshotStatus,
                Timestamp.from(now), Timestamp.from(now));
    }

    private static BatchOrderRow mapOrder(ResultSet rs) throws SQLException {
        return new BatchOrderRow(
                rs.getLong("id"), rs.getLong("batch_id"), rs.getString("batch_no"),
                rs.getLong("order_id"), rs.getString("order_no"), rs.getLong("player_id"),
                rs.getString("snapshot_status"), rs.getString("status"), rs.getString("result"),
                rs.getString("detail_json"), rs.getInt("attempts"),
                ts(rs, "settled_at"), ts(rs, "created_at"), ts(rs, "updated_at"));
    }

    private static final String ORDER_COLS = """
            id, batch_id, batch_no, order_id, order_no, player_id, snapshot_status, status,
            result, detail_json, attempts, settled_at, created_at, updated_at
            """;

    public Optional<BatchOrderRow> findSnapshotRowForUpdate(long batchId, long orderId) {
        return jdbc.query(
                "SELECT " + ORDER_COLS + " FROM recall_batch_order WHERE batch_id = ? AND order_id = ? FOR UPDATE",
                (rs, n) -> mapOrder(rs), batchId, orderId).stream().findFirst();
    }

    public List<BatchOrderRow> listOrdersByBatch(long batchId) {
        return jdbc.query(
                "SELECT " + ORDER_COLS + " FROM recall_batch_order WHERE batch_id = ? ORDER BY id",
                (rs, n) -> mapOrder(rs), batchId);
    }

    /**
     * Outstanding work after a crash/restart: only genuinely unfinished PENDING rows.
     * EXCEPTION rows are DONE (parked, awaiting stock + an explicit retry) and are NOT
     * auto-retried here, so a batch with parked orders can reach PARTIAL_EXCEPTION.
     */
    public List<BatchOrderRow> listOutstanding(long batchId, int limit) {
        return jdbc.query("""
                SELECT """ + " " + ORDER_COLS + """
                  FROM recall_batch_order
                 WHERE batch_id = ? AND status = 'PENDING'
                 ORDER BY id LIMIT ?
                """, (rs, n) -> mapOrder(rs), batchId, limit);
    }

    public List<BatchOrderRow> listExceptionRows(long batchId) {
        return jdbc.query(
                "SELECT " + ORDER_COLS + " FROM recall_batch_order WHERE batch_id = ? AND result = 'EXCEPTION'",
                (rs, n) -> mapOrder(rs), batchId);
    }

    public void markOrderDone(long rowId, String result, String detailJson, Instant now) {
        jdbc.update("""
                UPDATE recall_batch_order
                   SET status = 'DONE', result = ?, detail_json = ?, attempts = attempts + 1,
                       settled_at = ?, updated_at = ?
                 WHERE id = ?
                """, result, detailJson, Timestamp.from(now), Timestamp.from(now), rowId);
    }

    /** A parked EXCEPTION row starts another explicit attempt: back to PENDING for this run. */
    public void markExceptionRetrying(long rowId, Instant now) {
        jdbc.update("""
                UPDATE recall_batch_order
                   SET status = 'PENDING', attempts = attempts + 1, updated_at = ?
                 WHERE id = ? AND result = 'EXCEPTION'
                """, Timestamp.from(now), rowId);
    }

    /** Another attempt that did not change the outcome (e.g. concurrent commit won). */
    public void touchAttempt(long rowId, String detailJson, Instant now) {
        jdbc.update("""
                UPDATE recall_batch_order
                   SET attempts = attempts + 1, detail_json = COALESCE(?, detail_json), updated_at = ?
                 WHERE id = ?
                """, detailJson, Timestamp.from(now), rowId);
    }

    // ---- batch counters ----------------------------------------------------

    /** Recompute progress/counters straight from the per-order rows (single source of truth). */
    public void refreshBatchCounters(long batchId, Instant now) {
        jdbc.update("""
                UPDATE recall_batch b
                   SET processed_orders = (
                         SELECT COUNT(*) FROM recall_batch_order bo
                          WHERE bo.batch_id = b.id AND bo.status = 'DONE'),
                       result_released = (
                         SELECT COUNT(*) FROM recall_batch_order bo
                          WHERE bo.batch_id = b.id AND bo.result = 'RELEASED'),
                       result_reversed = (
                         SELECT COUNT(*) FROM recall_batch_order bo
                          WHERE bo.batch_id = b.id AND bo.result = 'REVERSED'),
                       result_skipped = (
                         SELECT COUNT(*) FROM recall_batch_order bo
                          WHERE bo.batch_id = b.id AND bo.result = 'SKIPPED'),
                       result_exception = (
                         SELECT COUNT(*) FROM recall_batch_order bo
                          WHERE bo.batch_id = b.id AND bo.result = 'EXCEPTION'),
                       status = CASE
                         WHEN (SELECT COUNT(*) FROM recall_batch_order bo
                                WHERE bo.batch_id = b.id
                                  AND (bo.status <> 'DONE' OR bo.result = 'EXCEPTION')) = 0
                           THEN 'COMPLETED'
                         WHEN (SELECT COUNT(*) FROM recall_batch_order bo
                                WHERE bo.batch_id = b.id AND bo.status = 'DONE') > 0
                           THEN 'PARTIAL_EXCEPTION'
                         ELSE 'PENDING'
                       END,
                       finished_at = CASE
                         WHEN (SELECT COUNT(*) FROM recall_batch_order bo
                                WHERE bo.batch_id = b.id
                                  AND (bo.status <> 'DONE' OR bo.result = 'EXCEPTION')) = 0
                           THEN ? ELSE finished_at END
                 WHERE b.id = ?
                """, Timestamp.from(now), batchId);
    }

    // ---- events ------------------------------------------------------------

    public void insertEvent(String batchNo, long orderId, String orderNo, String eventType,
                            String fromStatus, String toStatus, String result,
                            String message, Instant now) {
        jdbc.update("""
                INSERT INTO recall_batch_event
                  (batch_no, order_id, order_no, event_type, from_status, to_status,
                   result, message, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, batchNo, orderId, orderNo, eventType, fromStatus, toStatus, result,
                truncate(message, 500), Timestamp.from(now));
    }

    public List<EventRow> listEvents(String batchNo) {
        return jdbc.query("""
                SELECT id, batch_no, order_id, order_no, event_type, from_status, to_status,
                       result, message, created_at
                  FROM recall_batch_event WHERE batch_no = ? ORDER BY id
                """, (rs, n) -> new EventRow(rs.getLong("id"), rs.getString("batch_no"),
                        rs.getLong("order_id"), rs.getString("order_no"), rs.getString("event_type"),
                        rs.getString("from_status"), rs.getString("to_status"), rs.getString("result"),
                        rs.getString("message"), ts(rs, "created_at")), batchNo);
    }

    public List<EventRow> listEventsByOrder(String batchNo, long orderId) {
        return jdbc.query("""
                SELECT id, batch_no, order_id, order_no, event_type, from_status, to_status,
                       result, message, created_at
                  FROM recall_batch_event WHERE batch_no = ? AND order_id = ? ORDER BY id
                """, (rs, n) -> new EventRow(rs.getLong("id"), rs.getString("batch_no"),
                        rs.getLong("order_id"), rs.getString("order_no"), rs.getString("event_type"),
                        rs.getString("from_status"), rs.getString("to_status"), rs.getString("result"),
                        rs.getString("message"), ts(rs, "created_at")), batchNo, orderId);
    }

    private static String truncate(String s, int max) {
        if (s == null) {
            return null;
        }
        return s.length() <= max ? s : s.substring(0, max);
    }
}
