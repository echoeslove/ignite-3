/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.table.distributed.gc;

import java.util.List;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionDataStorage;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.MvPartitionStorage.Locker;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.gc.GcEntry;
import org.apache.ignite.internal.table.distributed.index.IndexUpdateHandler;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;

/**
 * Garbage collection update handler.
 */
public class GcUpdateHandler {
    private final PartitionDataStorage storage;

    private final IndexUpdateHandler indexUpdateHandler;

    private final PendingComparableValuesTracker<HybridTimestamp, Void> safeTimeTracker;

    /**
     * Constructor.
     *
     * @param storage Partition data storage.
     * @param indexUpdateHandler Index update handler.
     * @param safeTimeTracker Partition safe time tracker.
     */
    public GcUpdateHandler(
            PartitionDataStorage storage,
            PendingComparableValuesTracker<HybridTimestamp, Void> safeTimeTracker,
            IndexUpdateHandler indexUpdateHandler
    ) {
        this.storage = storage;
        this.indexUpdateHandler = indexUpdateHandler;
        this.safeTimeTracker = safeTimeTracker;
    }

    /**
     * Returns the partition safe time tracker.
     */
    public PendingComparableValuesTracker<HybridTimestamp, Void> getSafeTimeTracker() {
        return safeTimeTracker;
    }

    /**
     * Tries removing {@code count} oldest stale entries and their indexes.
     * If there are fewer rows than the {@code count}, then exits prematurely.
     *
     * <p>Hotfix2 downstream note: the public contract remains the original boolean scheduling decision. The implementation delegates
     * to the package-private {@link #vacuumBatch(HybridTimestamp, int, GcBatchResult)} overload only to expose diagnostics to
     * {@link MvGc}; callers do not observe the statistics object.</p>
     *
     * @param lowWatermark Low watermark for the vacuum.
     * @param count Count of entries to GC.
     * @return {@code False} if there is no garbage left in the storage.
     */
    public boolean vacuumBatch(HybridTimestamp lowWatermark, int count) {
        return vacuumBatch(lowWatermark, count, new GcBatchResult(count));
    }

    /**
     * Hotfix2 downstream extension, not part of upstream IGNITE-26995, IGNITE-26996, or IGNITE-26998.
     *
     * <p>Runs the same vacuum control flow as {@link #vacuumBatch(HybridTimestamp, int)} and additionally fills the supplied aggregate
     * counters for the downstream MVGC summary log. The counters are observational: they do not select GC entries, change lock
     * acquisition, alter retry decisions, or affect the returned scheduling decision.</p>
     *
     * <p>The accumulator is confined to this synchronous invocation. Package visibility is intentional so that {@link MvGc} can
     * consume the result without adding diagnostic state to {@link GcStorageHandler}.</p>
     *
     * @param lowWatermark Low watermark for the vacuum.
     * @param count Maximum number of entries requested for this batch.
     * @param statistics Invocation-local aggregate diagnostic counters.
     * @return Original boolean scheduling decision.
     */
    boolean vacuumBatch(HybridTimestamp lowWatermark, int count, GcBatchResult statistics) {
        if (count <= 0) {
            statistics.garbageLeft = true;

            return true;
        }

        var countHolder = new IntHolder(count);

        while (countHolder.get() > 0) {
            VacuumResult vacuumResult = internalVacuumBatch(lowWatermark, countHolder, statistics);

            switch (vacuumResult) {
                case NO_GARBAGE_LEFT:
                    statistics.garbageLeft = false;

                    return false;
                case SUCCESS:
                    statistics.garbageLeft = true;

                    return true;
                case FAILED_ACQUIRE_LOCK:
                case REMOVED_BY_ANOTHER_THREAD:
                    continue;
                case SHOULD_RELEASE:
                    // Storage engine needs resources (e.g., checkpoint needs write lock).
                    // Exit the closure to allow the engine to proceed.
                    statistics.garbageLeft = true;

                    return true;
                default:
                    throw new IllegalStateException(vacuumResult.toString());
            }
        }

        statistics.garbageLeft = true;

        return true;
    }

    private VacuumResult internalVacuumBatch(
            HybridTimestamp lowWatermark,
            IntHolder countHolder,
            GcBatchResult statistics
    ) {
        int count = countHolder.get();

        List<GcEntry> peekEntries = storage.peek(lowWatermark, count);

        // Count entries returned by every peek attempt. Retries can intentionally count the same queue entry more than once.
        statistics.peekedEntries += peekEntries.size();

        if (peekEntries.isEmpty()) {
            return VacuumResult.NO_GARBAGE_LEFT;
        }

        return storage.runConsistently(locker -> {
            boolean someRowRemovedByAnotherThread = false;

            for (int i = 0; i < peekEntries.size(); i++) {
                // Check if the storage engine needs resources before continuing.
                if (locker.shouldRelease()) {
                    // This flag explains why garbage can remain even when the requested batch was not filled.
                    statistics.yieldedToCheckpoint = true;

                    return VacuumResult.SHOULD_RELEASE;
                }

                // It is safe for the first iteration to use a lock instead of tryLock, since there will be no deadlock for the first RowId
                // and a deadlock may happen with subsequent ones.
                VacuumResult vacuumResult = internalVacuum(peekEntries.get(i), locker, i > 0, statistics);

                if (vacuumResult == VacuumResult.REMOVED_BY_ANOTHER_THREAD) {
                    someRowRemovedByAnotherThread = true;

                    // The queue entry was observed by this batch, but another thread completed its removal first.
                    statistics.removedByAnotherThread++;

                    continue;
                } else if (vacuumResult != VacuumResult.SUCCESS) {
                    return vacuumResult;
                }

                countHolder.getAndDecrement();
            }

            if (someRowRemovedByAnotherThread) {
                return VacuumResult.REMOVED_BY_ANOTHER_THREAD;
            }

            return countHolder.get() == 0 ? VacuumResult.SUCCESS : VacuumResult.NO_GARBAGE_LEFT;
        });
    }

    private VacuumResult internalVacuum(
            GcEntry gcEntry,
            Locker locker,
            boolean useTryLock,
            GcBatchResult statistics
    ) {
        RowId rowId = gcEntry.getRowId();

        if (useTryLock) {
            if (!locker.tryLock(rowId)) {
                return VacuumResult.FAILED_ACQUIRE_LOCK;
            }
        } else {
            locker.lock(rowId);
        }

        BinaryRow binaryRow = storage.vacuum(gcEntry);

        if (binaryRow == null) {
            return VacuumResult.REMOVED_BY_ANOTHER_THREAD;
        }

        try (Cursor<ReadResult> cursor = storage.scanVersions(rowId)) {
            // TODO: IGNITE-21005 We need to choose only those indexes that are not available for transactions
            indexUpdateHandler.tryRemoveFromIndexes(binaryRow, rowId, countingCursor(cursor, statistics), null);
        }

        statistics.vacuumedEntries++;

        return VacuumResult.SUCCESS;
    }

    /**
     * Wraps the original version cursor to measure versions actually consumed by index cleanup.
     *
     * <p>Only successful {@link Cursor#next()} calls increment the counter; {@link Cursor#hasNext()} probes do not. The wrapper neither
     * buffers nor reorders rows, so {@code IndexUpdateHandler} sees the same stream. Closing is deliberately left to the enclosing
     * try-with-resources block that owns {@code delegate}.</p>
     *
     * @param delegate Storage cursor owned by the caller.
     * @param statistics Invocation-local diagnostic accumulator.
     * @return Transparent cursor that counts consumed versions.
     */
    private static Cursor<ReadResult> countingCursor(Cursor<ReadResult> delegate, GcBatchResult statistics) {
        return new Cursor<>() {
            @Override
            public boolean hasNext() {
                return delegate.hasNext();
            }

            @Override
            public ReadResult next() {
                ReadResult result = delegate.next();

                statistics.versionsScanned++;

                return result;
            }

            @Override
            public void close() {
                // The owning try-with-resources block closes the delegate.
            }
        };
    }

    /**
     * Invocation-local Hotfix2 downstream statistics populated by
     * {@link #vacuumBatch(HybridTimestamp, int, GcBatchResult)}.
     *
     * <p>These values describe work observed by one batch and are used only for diagnostics. They are not persisted and are not used
     * to make storage or index correctness decisions.</p>
     */
    static class GcBatchResult {
        /** Original scheduling result: {@code true} means another MVGC batch may be required. */
        boolean garbageLeft;

        /** Batch size requested by the caller before retries or early exit. */
        final int requestedEntries;

        /** Sum of entries returned by all {@code storage.peek} calls; retries may include the same entry. */
        int peekedEntries;

        /** Entries removed by this invocation after index cleanup completed successfully. */
        int vacuumedEntries;

        /** Previous row versions actually consumed through the index-cleanup cursor. */
        long versionsScanned;

        /** Whether the batch stopped because the storage locker requested resource release. */
        boolean yieldedToCheckpoint;

        /** Entries observed by this invocation but already removed by a concurrent vacuum. */
        int removedByAnotherThread;

        GcBatchResult(int requestedEntries) {
            this.requestedEntries = requestedEntries;
        }

        boolean garbageLeft() {
            return garbageLeft;
        }
    }

    private enum VacuumResult {
        SUCCESS, NO_GARBAGE_LEFT, FAILED_ACQUIRE_LOCK, SHOULD_RELEASE, REMOVED_BY_ANOTHER_THREAD
    }
}
