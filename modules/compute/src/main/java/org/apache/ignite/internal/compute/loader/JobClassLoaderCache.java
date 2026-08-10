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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.function.Function;
import java.util.function.LongSupplier;
import org.apache.ignite.deployment.DeploymentUnit;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.jetbrains.annotations.Nullable;

/**
 * A node-local, reference-counted cache of compute job class loaders.
 *
 * <p>Loaders with no active references are retained for a bounded idle period. Active loaders are never evicted. Invalidation removes
 * a loader from reuse immediately and closes it after its final active reference is released.
 */
final class JobClassLoaderCache implements AutoCloseable {
    private static final IgniteLogger LOG = Loggers.forClass(JobClassLoaderCache.class);

    private final ScheduledExecutorService scheduler;

    private final LongSupplier nanoTime;

    /** Reusable entries. Invalidated active entries are removed from this map. */
    private final Map<List<DeploymentUnit>, Entry> entries = new HashMap<>();

    /** All entries that have not yet been claimed for closing, including invalidated active entries. */
    private final Set<Entry> allEntries = Collections.newSetFromMap(new IdentityHashMap<>());

    private int maxIdleEntries;

    private long idleTimeoutNanos;

    /** Invalidation generations scoped to individual deployment units. */
    private final Map<DeploymentUnit, Long> invalidationGenerations = new HashMap<>();

    private boolean closed;

    JobClassLoaderCache(
            ScheduledExecutorService scheduler,
            int maxIdleEntries,
            long idleTimeoutMillis
    ) {
        this(scheduler, maxIdleEntries, idleTimeoutMillis, System::nanoTime);
    }

    JobClassLoaderCache(
            ScheduledExecutorService scheduler,
            int maxIdleEntries,
            long idleTimeoutMillis,
            LongSupplier nanoTime
    ) {
        if (maxIdleEntries < 0) {
            throw new IllegalArgumentException("maxIdleEntries must not be negative");
        }
        if (idleTimeoutMillis <= 0) {
            throw new IllegalArgumentException("idleTimeoutMillis must be positive");
        }

        this.scheduler = scheduler;
        this.maxIdleEntries = maxIdleEntries;
        this.idleTimeoutNanos = MILLISECONDS.toNanos(idleTimeoutMillis);
        this.nanoTime = nanoTime;
    }

    /** Acquires a loader for the given ordered list of deployment units. */
    Lease acquire(List<DeploymentUnit> units, Function<List<DeploymentUnit>, JobClassLoader> loaderFactory) {
        List<DeploymentUnit> key = List.copyOf(units);

        synchronized (this) {
            return acquireInternal(key, loaderFactory);
        }
    }

    /**
     * Acquires a loader only if none of the requested deployment units was invalidated after the caller captured {@code snapshot}.
     *
     * @return A lease, or {@code null} when the caller must repeat deployment status validation.
     */
    @Nullable Lease acquire(
            List<DeploymentUnit> units,
            InvalidationSnapshot snapshot,
            Function<List<DeploymentUnit>, JobClassLoader> loaderFactory
    ) {
        List<DeploymentUnit> key = List.copyOf(units);

        synchronized (this) {
            if (!isCurrent(key, snapshot)) {
                return null;
            }

            return acquireInternal(key, loaderFactory);
        }
    }

    synchronized InvalidationSnapshot invalidationSnapshot(List<DeploymentUnit> units) {
        Map<DeploymentUnit, Long> snapshot = new HashMap<>();

        for (DeploymentUnit unit : units) {
            snapshot.put(unit, invalidationGenerations.getOrDefault(unit, 0L));
        }

        return new InvalidationSnapshot(snapshot);
    }

    private Lease acquireInternal(List<DeploymentUnit> key, Function<List<DeploymentUnit>, JobClassLoader> loaderFactory) {
        if (closed) {
            throw new IllegalStateException("Job class loader cache is closed");
        }

        Entry entry = entries.get(key);

        if (entry == null) {
            entry = new Entry(key, loaderFactory.apply(key));
            entries.put(key, entry);
            allEntries.add(entry);
            LOG.debug("Created job class loader cache entry [units={}]", key);
        } else if (entry.references == 0) {
            LOG.debug("Reusing idle job class loader cache entry [units={}]", key);
        }

        entry.references++;

        return new Lease(entry);
    }

    /** Releases a previously acquired loader. */
    void release(Lease lease) {
        List<JobClassLoader> loadersToClose = new ArrayList<>(1);

        synchronized (this) {
            if (lease.released) {
                throw new IllegalStateException("Job class loader lease is already released");
            }

            lease.released = true;

            Entry entry = lease.entry;

            if (entry.closeClaimed) {
                return;
            }

            if (entry.references <= 0) {
                throw new IllegalStateException("Job class loader is not acquired: " + entry.key);
            }

            entry.references--;

            if (entry.references == 0) {
                if (closed || entry.invalidated || maxIdleEntries == 0) {
                    claimForClose(entry, loadersToClose);
                } else {
                    entry.idleSinceNanos = nanoTime.getAsLong();
                    LOG.debug("Retaining idle job class loader cache entry [units={}]", entry.key);

                    if (entry.expirationTask == null
                            && !scheduleExpiration(entry, idleTimeoutNanos, entry.generation)) {
                        claimForClose(entry, loadersToClose);
                    } else {
                        evictExcessIdleEntries(loadersToClose);
                    }
                }
            }
        }

        closeLoaders(loadersToClose);
    }

    /** Invalidates every loader whose class path contains the deployment unit. */
    void invalidate(DeploymentUnit unit) {
        List<JobClassLoader> loadersToClose = new ArrayList<>();

        synchronized (this) {
            invalidationGenerations.merge(unit, 1L, Long::sum);

            for (Entry entry : new ArrayList<>(allEntries)) {
                if (!entry.key.contains(unit)) {
                    continue;
                }

                entry.invalidated = true;
                entries.remove(entry.key, entry);
                cancelExpiration(entry);
                entry.generation++;
                LOG.debug("Invalidated job class loader cache entry [units={}]", entry.key);

                if (entry.references == 0) {
                    claimForClose(entry, loadersToClose);
                }
            }
        }

        closeLoaders(loadersToClose);
    }

    /** Applies a new maximum idle-entry count and evicts excess idle entries immediately. */
    void updateMaxIdleEntries(int newMaxIdleEntries) {
        if (newMaxIdleEntries < 0) {
            throw new IllegalArgumentException("maxIdleEntries must not be negative");
        }

        List<JobClassLoader> loadersToClose = new ArrayList<>();

        synchronized (this) {
            maxIdleEntries = newMaxIdleEntries;
            evictExcessIdleEntries(loadersToClose);
        }

        closeLoaders(loadersToClose);
    }

    /** Applies a new idle TTL and recalculates existing idle deadlines. */
    void updateIdleTimeoutMillis(long newIdleTimeoutMillis) {
        if (newIdleTimeoutMillis <= 0) {
            throw new IllegalArgumentException("idleTimeoutMillis must be positive");
        }

        List<JobClassLoader> loadersToClose = new ArrayList<>();

        synchronized (this) {
            idleTimeoutNanos = MILLISECONDS.toNanos(newIdleTimeoutMillis);
            long now = nanoTime.getAsLong();

            for (Entry entry : new ArrayList<>(entries.values())) {
                if (entry.invalidated) {
                    continue;
                }

                cancelExpiration(entry);
                entry.generation++;

                if (entry.references != 0) {
                    continue;
                }

                long remainingNanos = idleTimeoutNanos - (now - entry.idleSinceNanos);

                if (remainingNanos <= 0 || !scheduleExpiration(entry, remainingNanos, entry.generation)) {
                    claimForClose(entry, loadersToClose);
                }
            }
        }

        closeLoaders(loadersToClose);
    }

    @Override
    public void close() {
        List<JobClassLoader> loadersToClose = new ArrayList<>();

        synchronized (this) {
            if (closed) {
                return;
            }

            closed = true;

            for (Entry entry : new ArrayList<>(allEntries)) {
                claimForClose(entry, loadersToClose);
            }
        }

        closeLoaders(loadersToClose);
    }

    synchronized int idleEntryCount() {
        return idleEntryCountInternal();
    }

    synchronized int entryCount() {
        return allEntries.size();
    }

    private void expire(Entry entry, long generation) {
        List<JobClassLoader> loadersToClose = new ArrayList<>(1);

        synchronized (this) {
            if (entry.closeClaimed || entry.invalidated || entry.generation != generation) {
                return;
            }

            entry.expirationTask = null;

            if (entry.references != 0) {
                return;
            }

            long remainingNanos = idleTimeoutNanos - (nanoTime.getAsLong() - entry.idleSinceNanos);

            if (remainingNanos <= 0) {
                LOG.debug("Expired idle job class loader cache entry [units={}]", entry.key);
                claimForClose(entry, loadersToClose);
            } else if (!scheduleExpiration(entry, remainingNanos, generation)) {
                claimForClose(entry, loadersToClose);
            }
        }

        closeLoaders(loadersToClose);
    }

    private boolean scheduleExpiration(Entry entry, long delayNanos, long generation) {
        assert entry.expirationTask == null;

        try {
            entry.expirationTask = scheduler.schedule(() -> expire(entry, generation), delayNanos, NANOSECONDS);
            return true;
        } catch (RuntimeException e) {
            LOG.warn("Failed to schedule job class loader expiration, closing it immediately: {}", e, entry.key);
            return false;
        }
    }

    private void evictExcessIdleEntries(List<JobClassLoader> loadersToClose) {
        while (idleEntryCountInternal() > maxIdleEntries) {
            Entry oldest = null;

            for (Entry entry : entries.values()) {
                if (entry.references == 0 && !entry.invalidated
                        && (oldest == null || entry.idleSinceNanos < oldest.idleSinceNanos)) {
                    oldest = entry;
                }
            }

            assert oldest != null;

            LOG.debug("Evicting idle job class loader cache entry [units={}]", oldest.key);
            claimForClose(oldest, loadersToClose);
        }
    }

    private int idleEntryCountInternal() {
        int count = 0;

        for (Entry entry : entries.values()) {
            if (entry.references == 0 && !entry.invalidated) {
                count++;
            }
        }

        return count;
    }

    private boolean isCurrent(List<DeploymentUnit> units, InvalidationSnapshot snapshot) {
        for (DeploymentUnit unit : units) {
            Long expectedGeneration = snapshot.generations.get(unit);

            if (expectedGeneration == null
                    || expectedGeneration.longValue() != invalidationGenerations.getOrDefault(unit, 0L)) {
                return false;
            }
        }

        return true;
    }

    private void claimForClose(Entry entry, List<JobClassLoader> loadersToClose) {
        if (entry.closeClaimed) {
            return;
        }

        entry.closeClaimed = true;
        entries.remove(entry.key, entry);
        allEntries.remove(entry);
        cancelExpiration(entry);
        loadersToClose.add(entry.loader);
    }

    private static void cancelExpiration(Entry entry) {
        ScheduledFuture<?> expirationTask = entry.expirationTask;

        if (expirationTask != null) {
            expirationTask.cancel(false);
            entry.expirationTask = null;
        }
    }

    private static void closeLoaders(List<JobClassLoader> loaders) {
        for (JobClassLoader loader : loaders) {
            try {
                loader.close();
            } catch (RuntimeException e) {
                LOG.error("Failed to close cached job class loader", e);
            }
        }
    }

    /** A single acquisition of a cached loader. */
    static final class Lease {
        private final Entry entry;

        private boolean released;

        private Lease(Entry entry) {
            this.entry = entry;
        }

        JobClassLoader classLoader() {
            return entry.loader;
        }
    }

    /** Invalidation generations captured for a set of deployment units. */
    static final class InvalidationSnapshot {
        private final Map<DeploymentUnit, Long> generations;

        private InvalidationSnapshot(Map<DeploymentUnit, Long> generations) {
            this.generations = Map.copyOf(generations);
        }
    }

    private static final class Entry {
        private final List<DeploymentUnit> key;

        private final JobClassLoader loader;

        private int references;

        private boolean invalidated;

        private boolean closeClaimed;

        private long idleSinceNanos;

        private long generation;

        private @Nullable ScheduledFuture<?> expirationTask;

        private Entry(List<DeploymentUnit> key, JobClassLoader loader) {
            this.key = key;
            this.loader = loader;
        }
    }
}
