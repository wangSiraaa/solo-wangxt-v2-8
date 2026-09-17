package com.gameops.craft.service;

import com.gameops.craft.repo.RecallBatchRepository;
import com.gameops.craft.repo.RecallBatchRepository.BatchRow;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Keeps recall batches progressing across process restarts:
 * state lives entirely in the database, so a fresh instance simply resumes
 * PENDING rows and retries EXCEPTION rows (e.g. after stock was replenished).
 * One batch at a time, bounded per tick; per-order transactions prevent partial work.
 */
@Component
public class RecallBatchProcessor {

    private static final Logger log = LoggerFactory.getLogger(RecallBatchProcessor.class);

    private final RecallBatchRepository batches;
    private final RecallService recallService;

    public RecallBatchProcessor(RecallBatchRepository batches, RecallService recallService) {
        this.batches = batches;
        this.recallService = recallService;
    }

    @Scheduled(fixedDelayString = "${app.recall.processor-delay-ms:3000}")
    public void tick() {
        List<BatchRow> open = batches.listRecent(100).stream()
                .filter(b -> !"COMPLETED".equals(b.status()))
                .toList();
        for (BatchRow b : open) {
            try {
                RecallService.RunSummary s = recallService.runBatch(b.batchNo(), 100);
                if (s.processed() > 0) {
                    log.info("recall batch {} progressed: {}/{} (released={}, reversed={}, skipped={}, exception={})",
                            b.batchNo(), s.processed(), s.total(), s.released(), s.reversed(),
                            s.skipped(), s.exception());
                }
            } catch (Exception e) {
                // Next tick retries; per-order persistence guarantees no duplicate compensation.
                log.warn("recall batch {} tick failed: {}", b.batchNo(), e.getMessage());
            }
        }
    }
}
