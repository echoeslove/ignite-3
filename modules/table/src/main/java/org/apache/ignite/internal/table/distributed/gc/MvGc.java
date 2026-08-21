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

import static org.apache.ignite.internal.event.EventListener.fromConsumer;
import static org.apache.ignite.internal.lowwatermark.event.LowWatermarkEvent.LOW_WATERMARK_CHANGED;
import static org.apache.ignite.internal.thread.ThreadOperation.STORAGE_READ;
import static org.apache.ignite.internal.thread.ThreadOperation.STORAGE_WRITE;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.hasCause;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLockSafe;
import static org.apache.ignite.internal.util.IgniteUtils.shutdownAndAwaitTermination;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.failure.FailureContext;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.IgniteThrottledLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.lowwatermark.LowWatermark;
import org.apache.ignite.internal.lowwatermark.event.ChangeLowWatermarkEventParameters;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.schema.configuration.GcConfiguration;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.TrackerClosedException;
import org.apache.ignite.lang.ErrorGroups.GarbageCollector;
import org.jetbrains.annotations.TestOnly;

/**
 * Garbage collector for multi-versioned storages and their indexes in the background.
 *
 * @see GcUpdateHandler#vacuumBatch
 */
public class MvGc implements ManuallyCloseable {
    private static final IgniteLogger LOG = Loggers.forClass(MvGc.class);

    /**
     * Hotfix2 downstream batch-summary logger, not part of upstream IGNITE-26995, IGNITE-26996, or IGNITE-26998.
     *
     * <p>It is separated from {@link #LOG} so throttling cannot change existing MVGC or thread-factory logging.</p>
     */
    private static final IgniteThrottledLogger BATCH_SUMMARY_LOG = Loggers.toThrottledLogger(LOG, Runnable::run);

    /** Node name. */
    private final String nodeName;

    /** Garbage collector configuration. */
    private final GcConfiguration gcConfig;

    /** Garbage collection thread pool. */
    private volatile ThreadPoolExecutor executor;

    /** Prevents double closing. */
    private final AtomicBoolean closeGuard = new AtomicBoolean();

    /** Busy lock to close synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Low watermark. */
    private final LowWatermark lowWatermark;

    private final FailureProcessor failureProcessor;

    /** Storage handler by table partition ID for which garbage will be collected. */
    private final ConcurrentMap<TablePartitionId, GcStorageHandler> storageHandlerByPartitionId = new ConcurrentHashMap<>();

    /**
     * Constructor.
     *
     * @param nodeName Node name.
     * @param gcConfig Garbage collector configuration.
     * @param lowWatermark Low watermark.
     * @param failureProcessor Failure processor.
     */
    public MvGc(String nodeName, GcConfiguration gcConfig, LowWatermark lowWatermark, FailureProcessor failureProcessor) {
        this.nodeName = nodeName;
        this.gcConfig = gcConfig;
        this.lowWatermark = lowWatermark;
        this.failureProcessor = failureProcessor;
    }

    /** Starts the garbage collector. */
    public void start() {
        inBusyLock(() -> {
            int threadCount = gcConfig.threads().value();

            executor = new ThreadPoolExecutor(
                    threadCount,
                    threadCount,
                    30,
                    TimeUnit.SECONDS,
                    new LinkedBlockingQueue<>(),
                    IgniteThreadFactory.create(nodeName, "mv-gc", LOG, STORAGE_READ, STORAGE_WRITE)
            );
            executor.allowCoreThreadTimeOut(true);

            lowWatermark.listen(LOW_WATERMARK_CHANGED, fromConsumer(this::onLwmChanged));
        });
    }

    /**
     * Adds storage for background garbage collection when updating a low watermark.
     *
     * @param tablePartitionId Table partition ID.
     * @param gcUpdateHandler Gc update handler.
     * @throws IgniteInternalException with {@link GarbageCollector#CLOSED_ERR} If the garbage collector is closed.
     */
    public void addStorage(TablePartitionId tablePartitionId, GcUpdateHandler gcUpdateHandler) {
        inBusyLock(() -> {
            GcStorageHandler previous = storageHandlerByPartitionId.putIfAbsent(
                    tablePartitionId,
                    new GcStorageHandler(gcUpdateHandler)
            );

            // TODO: IGNITE-18939 Should be called once, you need to check that previous == null
            if (previous == null && lowWatermark.getLowWatermark() != null) {
                scheduleGcForStorage(tablePartitionId);
            }
        });
    }

    /**
     * Removes storage for background garbage collection and completes the garbage collection for it.
     *
     * <p>Should be called before rebalancing/closing/destroying the storage.
     *
     * @param tablePartitionId Table partition ID.
     * @return Storage garbage collection completion future.
     * @throws IgniteInternalException with {@link GarbageCollector#CLOSED_ERR} If the garbage collector is closed.
     */
    public CompletableFuture<Void> removeStorage(TablePartitionId tablePartitionId) {
        return inBusyLock(() -> {
            GcStorageHandler removed = storageHandlerByPartitionId.remove(tablePartitionId);

            if (removed == null) {
                return nullCompletedFuture();
            }

            CompletableFuture<Void> awaitSafeTimeFuture = removed.awaitSafeTimeFuture.get();

            if (awaitSafeTimeFuture != null && !awaitSafeTimeFuture.isDone()) {
                awaitSafeTimeFuture.completeExceptionally(new StorageRemovedException());
            }

            CompletableFuture<Void> gcInProgressFuture = removed.gcInProgressFuture.get();

            return gcInProgressFuture == null ? nullCompletedFuture() : gcInProgressFuture;
        });
    }

    private void onLwmChanged(ChangeLowWatermarkEventParameters parameters) {
        inBusyLockSafe(busyLock, () -> executor.submit(() -> inBusyLock(this::initNewGcBusy)));
    }

    @Override
    public void close() throws Exception {
        if (!closeGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        if (executor != null) {
            shutdownAndAwaitTermination(executor, 10, TimeUnit.SECONDS);
        }
    }

    private void initNewGcBusy() {
        storageHandlerByPartitionId.keySet().forEach(this::scheduleGcForStorage);
    }

    private void scheduleGcForStorage(TablePartitionId tablePartitionId) {
        inBusyLock(() -> {
            var currentGcFuture = new CompletableFuture<Void>();
            var currentAwaitSafeTimeFuture = new CompletableFuture<Void>();

            GcStorageHandler storageHandler = storageHandlerByPartitionId.compute(tablePartitionId, (tablePartId, gcStorageHandler) -> {
                if (gcStorageHandler == null) {
                    // Storage has been removed from garbage collection.
                    return null;
                }

                CompletableFuture<Void> inProgressFuture = gcStorageHandler.gcInProgressFuture.get();
                CompletableFuture<Void> awaitProgressFuture = gcStorageHandler.awaitSafeTimeFuture.get();

                if (inProgressFuture == null || inProgressFuture.isDone()) {
                    boolean casResult = gcStorageHandler.gcInProgressFuture.compareAndSet(inProgressFuture, currentGcFuture);

                    assert casResult : tablePartId;

                    casResult = gcStorageHandler.awaitSafeTimeFuture.compareAndSet(awaitProgressFuture, currentAwaitSafeTimeFuture);

                    assert casResult : tablePartId;
                } else {
                    inProgressFuture.whenComplete((unused, throwable) -> scheduleGcForStorage(tablePartitionId));
                }

                return gcStorageHandler;
            });

            if (storageHandler == null) {
                // Storage has been removed from garbage collection.
                return;
            }

            if (storageHandler.gcInProgressFuture.get() != currentGcFuture) {
                // Someone in parallel is already collecting garbage, we will try once again after completion of gcInProgressFuture.
                return;
            }

            try {
                HybridTimestamp lowWatermark = this.lowWatermark.getLowWatermark();

                assert lowWatermark != null : tablePartitionId;

                // If the storage has been deleted, then garbage collection is no longer necessary.
                if (!storageHandlerByPartitionId.containsKey(tablePartitionId)) {
                    currentGcFuture.complete(null);

                    return;
                }

                GcUpdateHandler gcUpdateHandler = storageHandler.gcUpdateHandler;

                // We can only start garbage collection when the partition safe time is reached.
                gcUpdateHandler.getSafeTimeTracker()
                        .waitFor(lowWatermark)
                        .whenComplete((unused, throwable) -> {
                            if (throwable == null) {
                                currentAwaitSafeTimeFuture.complete(null);
                            } else {
                                currentAwaitSafeTimeFuture.completeExceptionally(throwable);
                            }
                        });

                currentAwaitSafeTimeFuture
                        .thenApplyAsync(
                                unused -> vacuumBatch(tablePartitionId, storageHandler, lowWatermark, gcConfig.value().batchSize()),
                                executor
                        )
                        .whenComplete((isGarbageLeft, throwable) -> {
                            if (throwable != null) {
                                if (hasCause(throwable, TrackerClosedException.class, StorageRemovedException.class)) {
                                    LOG.debug("Caught an expected exception", throwable);

                                    currentGcFuture.complete(null);
                                } else {
                                    failureProcessor.process(new FailureContext(throwable, "Error when running GC"));

                                    currentGcFuture.completeExceptionally(throwable);
                                }

                                return;
                            }

                            currentGcFuture.complete(null);

                            // If there is garbage left and the storage has not been deleted, then we will schedule the next garbage
                            // collection.
                            if (isGarbageLeft && storageHandlerByPartitionId.containsKey(tablePartitionId)) {
                                scheduleGcForStorage(tablePartitionId);
                            }
                        });
            } catch (Throwable t) {
                failureProcessor.process(new FailureContext(t, "Error when running GC"));

                currentGcFuture.completeExceptionally(t);
            }
        });
    }

    /**
     * Runs one official MVGC batch and emits Hotfix2 downstream aggregate diagnostics around it.
     *
     * <p>The method preserves the scheduler contract by returning {@link GcUpdateHandler.GcBatchResult#garbageLeft()} unchanged.
     * Timing and logging happen after the synchronous batch and do not alter storage locks, low-watermark handling, or rescheduling.</p>
     *
     * <p>Scan amplification uses successfully vacuumed entries as the denominator, falling back to one when nothing was removed.
     * Backlog convergence spans an immediately rescheduled sequence: this is required because an exact full batch conservatively
     * returns {@code garbageLeft=true}, and only the following empty batch proves that the queue drained. Slow-batch and
     * scan-amplification classification remains in offline analysis.</p>
     *
     * @param tablePartitionId Partition used only to identify the summary and its throttle key.
     * @param storageHandler Existing official storage handler and owner of per-partition convergence state.
     * @param lowWatermark Low watermark passed unchanged to the batch.
     * @param requestedEntries Configured maximum batch size.
     * @return Original MVGC decision indicating whether another batch may be required.
     */
    private boolean vacuumBatch(
            TablePartitionId tablePartitionId,
            GcStorageHandler storageHandler,
            HybridTimestamp lowWatermark,
            int requestedEntries
    ) {
        long startNanos = System.nanoTime();

        var result = new GcUpdateHandler.GcBatchResult(requestedEntries);

        storageHandler.gcUpdateHandler.vacuumBatch(
                lowWatermark,
                requestedEntries,
                result
        );

        long durationMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
        double scanAmplification = (double) result.versionsScanned / Math.max(result.vacuumedEntries, 1);
        boolean backlogConverged = storageHandler.recordBatchAndCheckConvergence(result);
        boolean logAtInfo = result.garbageLeft() || backlogConverged;

        if (logAtInfo || LOG.isDebugEnabled()) {
            // Example: MVGC batch summary [tableId=1, partitionId=0,
            // lowWatermark=HybridTimestamp [physical=1970-01-01 00:00:00:002 +0000, logical=2, composite=131074],
            // requestedEntries=5, peekedEntries=5, vacuumedEntries=5, versionsScanned=5, scanAmplification=1.0,
            // durationMs=12, garbageLeft=true, yieldedToCheckpoint=false, removedByAnotherThread=0].
            String message = "MVGC batch summary [tableId=" + tablePartitionId.tableId()
                    + ", partitionId=" + tablePartitionId.partitionId()
                    + ", lowWatermark=" + lowWatermark
                    + ", requestedEntries=" + result.requestedEntries
                    + ", peekedEntries=" + result.peekedEntries
                    + ", vacuumedEntries=" + result.vacuumedEntries
                    + ", versionsScanned=" + result.versionsScanned
                    + ", scanAmplification=" + scanAmplification
                    + ", durationMs=" + durationMillis
                    + ", garbageLeft=" + result.garbageLeft()
                    + ", yieldedToCheckpoint=" + result.yieldedToCheckpoint
                    + ", removedByAnotherThread=" + result.removedByAnotherThread
                    + ']';

            // Repeated backlog summaries are limited per node and partition.
            String throttleKey = nodeName + ':' + tablePartitionId;

            if (result.garbageLeft()) {
                BATCH_SUMMARY_LOG.info(throttleKey, message);
            } else if (backlogConverged) {
                // Use a separate key so an explicit garbageLeft=false is not suppressed by the preceding backlog=true summary.
                BATCH_SUMMARY_LOG.info(throttleKey + ":converged", message);
            } else {
                LOG.debug(message);
            }
        }

        return result.garbageLeft();
    }

    private <T> T inBusyLock(Supplier<T> supplier) {
        if (!busyLock.enterBusy()) {
            throw new IgniteInternalException(GarbageCollector.CLOSED_ERR);
        }

        try {
            return supplier.get();
        } finally {
            busyLock.leaveBusy();
        }
    }

    private void inBusyLock(Runnable runnable) {
        inBusyLock(() -> {
            runnable.run();

            return null;
        });
    }

    /**
     * Schedule a new garbage collection for all storages.
     */
    @TestOnly
    void scheduleGcForAllStorages() {
        inBusyLock(this::initNewGcBusy);
    }
}
