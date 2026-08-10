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

package org.apache.ignite.internal.compute.loader;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.deployment.DeploymentUnit;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JobClassLoaderCacheTest extends BaseIgniteAbstractTest {
    private static final long TTL_MILLIS = 1_000;

    private final AtomicLong nanoTime = new AtomicLong();

    private final List<Runnable> expirationTasks = new CopyOnWriteArrayList<>();

    private ScheduledExecutorService scheduler;

    private JobClassLoaderCache cache;

    @BeforeEach
    void setUp() {
        scheduler = mock(ScheduledExecutorService.class);

        when(scheduler.schedule(any(Runnable.class), anyLong(), eq(NANOSECONDS))).thenAnswer(invocation -> {
            expirationTasks.add(invocation.getArgument(0));
            return mock(ScheduledFuture.class);
        });

        cache = new JobClassLoaderCache(scheduler, 2, TTL_MILLIS, nanoTime::get);
    }

    @Test
    void reusesIdleLoaderBeforeTtl() {
        DeploymentUnit unit = unit("unit", "1.0.0");
        JobClassLoader loader = mock(JobClassLoader.class);
        AtomicInteger creations = new AtomicInteger();

        JobClassLoaderCache.Lease first = cache.acquire(List.of(unit), key -> {
            creations.incrementAndGet();
            return loader;
        });
        cache.release(first);

        JobClassLoaderCache.Lease second = cache.acquire(List.of(unit), key -> {
            creations.incrementAndGet();
            return mock(JobClassLoader.class);
        });

        assertSame(loader, second.classLoader());
        assertEquals(1, creations.get());

        cache.release(second);
        cache.close();

        verify(loader).close();
    }

    @Test
    void sharesLoaderBetweenConcurrentAcquires() {
        DeploymentUnit unit = unit("unit", "1.0.0");
        JobClassLoader loader = mock(JobClassLoader.class);
        AtomicInteger creations = new AtomicInteger();

        JobClassLoaderCache.Lease first = cache.acquire(List.of(unit), key -> {
            creations.incrementAndGet();
            return loader;
        });
        JobClassLoaderCache.Lease second = cache.acquire(List.of(unit), key -> {
            creations.incrementAndGet();
            return mock(JobClassLoader.class);
        });

        assertSame(loader, second.classLoader());
        assertEquals(1, creations.get());

        cache.release(first);
        verify(loader, never()).close();
        assertEquals(0, cache.idleEntryCount());

        cache.release(second);
        assertEquals(1, cache.idleEntryCount());
        cache.close();
    }

    @Test
    void ttlStartsAfterLastRelease() {
        DeploymentUnit unit = unit("unit", "1.0.0");
        JobClassLoader loader = mock(JobClassLoader.class);

        JobClassLoaderCache.Lease first = cache.acquire(List.of(unit), key -> loader);
        JobClassLoaderCache.Lease second = cache.acquire(List.of(unit), key -> mock(JobClassLoader.class));

        cache.release(first);
        advanceMillis(TTL_MILLIS * 2);

        verify(loader, never()).close();
        assertEquals(0, expirationTasks.size());

        cache.release(second);
        advanceMillis(TTL_MILLIS);
        expirationTasks.get(0).run();

        verify(loader).close();
    }

    @Test
    void createsNewLoaderAfterTtlExpires() {
        DeploymentUnit unit = unit("unit", "1.0.0");
        JobClassLoader firstLoader = mock(JobClassLoader.class);
        JobClassLoader secondLoader = mock(JobClassLoader.class);

        JobClassLoaderCache.Lease first = cache.acquire(List.of(unit), key -> firstLoader);
        cache.release(first);

        advanceMillis(TTL_MILLIS);
        expirationTasks.get(0).run();

        JobClassLoaderCache.Lease second = cache.acquire(List.of(unit), key -> secondLoader);

        assertSame(secondLoader, second.classLoader());
        verify(firstLoader).close();

        cache.release(second);
        cache.close();
    }

    @Test
    void staleExpirationCannotCloseReacquiredLoader() {
        DeploymentUnit unit = unit("unit", "1.0.0");
        JobClassLoader loader = mock(JobClassLoader.class);

        JobClassLoaderCache.Lease first = cache.acquire(List.of(unit), key -> loader);
        cache.release(first);

        Runnable staleExpiration = expirationTasks.get(0);
        JobClassLoaderCache.Lease second = cache.acquire(List.of(unit), key -> mock(JobClassLoader.class));

        advanceMillis(TTL_MILLIS);
        staleExpiration.run();

        verify(loader, never()).close();

        cache.release(second);
        cache.close();

        verify(loader).close();
    }

    @Test
    void keepsExpirationQueueBoundedAcrossRepeatedReuse() {
        ScheduledThreadPoolExecutor realScheduler = new ScheduledThreadPoolExecutor(1);
        JobClassLoaderCache realCache = new JobClassLoaderCache(realScheduler, 2, 600_000);
        JobClassLoader loader = mock(JobClassLoader.class);
        DeploymentUnit unit = unit("unit", "1.0.0");

        try {
            for (int i = 0; i < 10_000; i++) {
                JobClassLoaderCache.Lease lease = realCache.acquire(List.of(unit), key -> loader);
                realCache.release(lease);
            }

            assertEquals(1, realScheduler.getQueue().size());
            assertEquals(1, realCache.idleEntryCount());
            verify(loader, never()).close();
        } finally {
            realCache.close();
            realScheduler.shutdownNow();
        }

        verify(loader).close();
    }

    @Test
    void ttlUpdateWhileActiveSchedulesNewTimeoutAfterRelease() {
        DeploymentUnit unit = unit("unit", "1.0.0");
        JobClassLoader loader = mock(JobClassLoader.class);

        JobClassLoaderCache.Lease first = cache.acquire(List.of(unit), key -> loader);
        cache.release(first);

        Runnable oldExpiration = expirationTasks.get(0);
        JobClassLoaderCache.Lease second = cache.acquire(List.of(unit), key -> mock(JobClassLoader.class));

        cache.updateIdleTimeoutMillis(200);
        cache.release(second);

        assertEquals(2, expirationTasks.size());

        advanceMillis(200);
        expirationTasks.get(1).run();
        oldExpiration.run();

        verify(loader, times(1)).close();
    }

    @Test
    void evictsLeastRecentlyUsedIdleLoaderAtCapacity() {
        cache.updateMaxIdleEntries(1);

        JobClassLoader oldestLoader = mock(JobClassLoader.class);
        JobClassLoader newestLoader = mock(JobClassLoader.class);

        JobClassLoaderCache.Lease oldest = cache.acquire(List.of(unit("oldest", "1.0.0")), key -> oldestLoader);
        cache.release(oldest);

        advanceMillis(1);

        JobClassLoaderCache.Lease newest = cache.acquire(List.of(unit("newest", "1.0.0")), key -> newestLoader);
        cache.release(newest);

        assertEquals(1, cache.idleEntryCount());
        verify(oldestLoader).close();
        verify(newestLoader, never()).close();

        cache.close();
    }

    @Test
    void neverEvictsActiveLoader() {
        cache.updateMaxIdleEntries(1);

        JobClassLoader activeLoader = mock(JobClassLoader.class);
        JobClassLoader firstIdleLoader = mock(JobClassLoader.class);
        JobClassLoader secondIdleLoader = mock(JobClassLoader.class);

        JobClassLoaderCache.Lease active = cache.acquire(List.of(unit("active", "1.0.0")), key -> activeLoader);
        JobClassLoaderCache.Lease firstIdle = cache.acquire(List.of(unit("idle-1", "1.0.0")), key -> firstIdleLoader);
        cache.release(firstIdle);

        advanceMillis(1);

        JobClassLoaderCache.Lease secondIdle = cache.acquire(List.of(unit("idle-2", "1.0.0")), key -> secondIdleLoader);
        cache.release(secondIdle);

        verify(activeLoader, never()).close();
        verify(firstIdleLoader).close();
        verify(secondIdleLoader, never()).close();

        cache.release(active);
        cache.close();
    }

    @Test
    void zeroCapacityClosesOnLastRelease() {
        cache.updateMaxIdleEntries(0);

        JobClassLoader loader = mock(JobClassLoader.class);
        JobClassLoaderCache.Lease lease = cache.acquire(List.of(unit("unit", "1.0.0")), key -> loader);

        cache.release(lease);

        assertEquals(0, cache.entryCount());
        verify(loader).close();
    }

    @Test
    void invalidatesIdleLoaderImmediately() {
        DeploymentUnit unit = unit("unit", "1.0.0");
        JobClassLoader loader = mock(JobClassLoader.class);

        JobClassLoaderCache.Lease lease = cache.acquire(List.of(unit), key -> loader);
        cache.release(lease);
        cache.invalidate(unit);

        assertEquals(0, cache.entryCount());
        verify(loader).close();
    }

    @Test
    void invalidatingOneUnitInvalidatesCompositeEntry() {
        DeploymentUnit firstUnit = unit("first", "1.0.0");
        DeploymentUnit secondUnit = unit("second", "1.0.0");
        JobClassLoader loader = mock(JobClassLoader.class);

        JobClassLoaderCache.Lease lease = cache.acquire(List.of(firstUnit, secondUnit), key -> loader);
        cache.release(lease);

        cache.invalidate(secondUnit);

        assertEquals(0, cache.entryCount());
        verify(loader).close();
    }

    @Test
    void schedulerRejectionClosesLoaderInsteadOfLeakingIt() {
        when(scheduler.schedule(any(Runnable.class), anyLong(), eq(NANOSECONDS)))
                .thenThrow(new RejectedExecutionException("scheduler is stopping"));
        JobClassLoader loader = mock(JobClassLoader.class);

        JobClassLoaderCache.Lease lease = cache.acquire(List.of(unit("unit", "1.0.0")), key -> loader);
        cache.release(lease);

        assertEquals(0, cache.entryCount());
        verify(loader).close();
    }

    @Test
    void rejectsInvalidRuntimeConfiguration() {
        assertThrows(IllegalArgumentException.class, () -> cache.updateMaxIdleEntries(-1));
        assertThrows(IllegalArgumentException.class, () -> cache.updateIdleTimeoutMillis(0));
        assertThrows(IllegalArgumentException.class, () -> cache.updateIdleTimeoutMillis(-1));
    }

    @Test
    void doesNotCreateLoaderWhenInvalidatedAfterGenerationCapture() {
        DeploymentUnit unit = unit("unit", "1.0.0");
        JobClassLoaderCache.InvalidationSnapshot snapshot = cache.invalidationSnapshot(List.of(unit));
        AtomicInteger creations = new AtomicInteger();

        cache.invalidate(unit);

        JobClassLoaderCache.Lease lease = cache.acquire(List.of(unit), snapshot, key -> {
            creations.incrementAndGet();
            return mock(JobClassLoader.class);
        });

        assertNull(lease);
        assertEquals(0, creations.get());
        assertEquals(0, cache.entryCount());
    }

    @Test
    void unrelatedInvalidationDoesNotRejectAcquire() {
        DeploymentUnit requestedUnit = unit("requested", "1.0.0");
        JobClassLoaderCache.InvalidationSnapshot snapshot = cache.invalidationSnapshot(List.of(requestedUnit));
        JobClassLoader loader = mock(JobClassLoader.class);

        cache.invalidate(unit("unrelated", "1.0.0"));

        JobClassLoaderCache.Lease lease = cache.acquire(List.of(requestedUnit), snapshot, key -> loader);

        assertSame(loader, lease.classLoader());

        cache.release(lease);
        cache.close();
    }

    @Test
    void invalidatedActiveLoaderClosesAfterLastReleaseAndIsNotReused() {
        DeploymentUnit unit = unit("unit", "1.0.0");
        JobClassLoader oldLoader = mock(JobClassLoader.class);
        JobClassLoader newLoader = mock(JobClassLoader.class);

        JobClassLoaderCache.Lease oldLease = cache.acquire(List.of(unit), key -> oldLoader);
        cache.invalidate(unit);

        JobClassLoaderCache.Lease newLease = cache.acquire(List.of(unit), key -> newLoader);

        assertSame(newLoader, newLease.classLoader());
        verify(oldLoader, never()).close();

        cache.release(oldLease);
        verify(oldLoader).close();

        cache.release(newLease);
        cache.close();
    }

    @Test
    void changingTtlExpiresAlreadyOldIdleLoader() {
        JobClassLoader loader = mock(JobClassLoader.class);
        JobClassLoaderCache.Lease lease = cache.acquire(List.of(unit("unit", "1.0.0")), key -> loader);
        cache.release(lease);

        advanceMillis(500);
        cache.updateIdleTimeoutMillis(400);

        verify(loader).close();
        assertEquals(0, cache.entryCount());
    }

    @Test
    void copiesMutableKey() {
        DeploymentUnit unit = unit("unit", "1.0.0");
        List<DeploymentUnit> mutableUnits = new ArrayList<>();
        mutableUnits.add(unit);
        JobClassLoader loader = mock(JobClassLoader.class);

        JobClassLoaderCache.Lease first = cache.acquire(mutableUnits, key -> loader);
        cache.release(first);
        mutableUnits.clear();

        JobClassLoaderCache.Lease second = cache.acquire(List.of(unit), key -> mock(JobClassLoader.class));

        assertSame(loader, second.classLoader());

        cache.release(second);
        cache.close();
    }

    @Test
    void closesEveryLoaderOnceOnShutdown() {
        JobClassLoader activeLoader = mock(JobClassLoader.class);
        JobClassLoader idleLoader = mock(JobClassLoader.class);

        JobClassLoaderCache.Lease active = cache.acquire(List.of(unit("active", "1.0.0")), key -> activeLoader);
        JobClassLoaderCache.Lease idle = cache.acquire(List.of(unit("idle", "1.0.0")), key -> idleLoader);
        cache.release(idle);

        cache.close();
        cache.close();
        cache.release(active);

        verify(activeLoader, times(1)).close();
        verify(idleLoader, times(1)).close();
        assertThrows(
                IllegalStateException.class,
                () -> cache.acquire(List.of(unit("new", "1.0.0")), key -> mock(JobClassLoader.class))
        );
    }

    @Test
    void closeFailureDoesNotSkipOtherEntries() {
        JobClassLoader failingLoader = mock(JobClassLoader.class);
        JobClassLoader otherLoader = mock(JobClassLoader.class);
        doThrow(new RuntimeException("boom")).when(failingLoader).close();

        cache.acquire(List.of(unit("failing", "1.0.0")), key -> failingLoader);
        cache.acquire(List.of(unit("other", "1.0.0")), key -> otherLoader);

        cache.close();

        verify(failingLoader).close();
        verify(otherLoader).close();
    }

    @Test
    void concurrentAcquireReleaseExpireAndInvalidatePreserveCloseInvariant() throws Exception {
        DeploymentUnit unit = unit("unit", "1.0.0");
        List<JobClassLoader> createdLoaders = new CopyOnWriteArrayList<>();
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(3);

        try {
            Future<?> firstWorker = executor.submit(() -> runAcquireReleaseLoop(unit, createdLoaders, start));
            Future<?> secondWorker = executor.submit(() -> runAcquireReleaseLoop(unit, createdLoaders, start));
            Future<?> invalidator = executor.submit(() -> {
                start.await();

                for (int i = 0; i < 100; i++) {
                    cache.invalidate(unit);
                    advanceMillis(TTL_MILLIS);

                    if (!expirationTasks.isEmpty()) {
                        expirationTasks.remove(0).run();
                    }
                }

                return null;
            });

            start.countDown();
            firstWorker.get(10, SECONDS);
            secondWorker.get(10, SECONDS);
            invalidator.get(10, SECONDS);
        } finally {
            executor.shutdownNow();
            cache.close();
        }

        assertEquals(0, cache.entryCount());

        for (JobClassLoader loader : createdLoaders) {
            verify(loader, times(1)).close();
        }
    }

    @Test
    void rejectsDoubleRelease() {
        JobClassLoaderCache.Lease lease = cache.acquire(
                List.of(unit("unit", "1.0.0")),
                key -> mock(JobClassLoader.class)
        );

        cache.release(lease);

        assertThrows(IllegalStateException.class, () -> cache.release(lease));
        cache.close();
    }

    private void advanceMillis(long millis) {
        nanoTime.addAndGet(NANOSECONDS.convert(millis, java.util.concurrent.TimeUnit.MILLISECONDS));
    }

    private void runAcquireReleaseLoop(
            DeploymentUnit unit,
            List<JobClassLoader> createdLoaders,
            CountDownLatch start
    ) {
        try {
            start.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }

        for (int i = 0; i < 100; i++) {
            JobClassLoaderCache.Lease lease = cache.acquire(List.of(unit), key -> {
                JobClassLoader loader = mock(JobClassLoader.class);
                createdLoaders.add(loader);
                return loader;
            });

            cache.release(lease);
        }
    }

    private static DeploymentUnit unit(String name, String version) {
        return new DeploymentUnit(name, version);
    }
}
