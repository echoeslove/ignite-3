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

package org.apache.ignite.internal.failure;

import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntConsumer;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;

/**
 * Opt-in, one-node-per-JVM recovery. The launcher must restart ONLY exit code 75 with the SAME work mount.
 * No in-process restart is attempted after shutdown. Other failure types retain their configured policy.
 */
public final class StaleNodeRecovery implements AutoCloseable {
    /** Deliberately distinct from StopNodeOrHaltFailureHandler's non-restart exit code 130. */
    public static final int RESTART_EXIT_CODE = 75;

    private static final IgniteLogger LOG = Loggers.forClass(StaleNodeRecovery.class);

    private static final ConcurrentHashMap<Path, UUID> GENERATIONS = new ConcurrentHashMap<>();

    private static final Object GENERATION_LOCK = new Object();

    private final AtomicBoolean claimed = new AtomicBoolean();

    private final AtomicBoolean closed = new AtomicBoolean();

    private final UUID generation = UUID.randomUUID();

    private final Path directory;

    private final NodeStopper stopper;

    private final StaleRecoveryBudget budget;

    private final long backoffMillis;

    private final long stopTimeoutMillis;

    private final IntConsumer exit;

    /** Guarded by GENERATION_LOCK; committed before stopping can fail initialization. */
    private boolean exitOwned;

    /** Creates a recovery coordinator bound to one launch and one persistent work directory. */
    public StaleNodeRecovery(NodeStopper stopper, Path directory, int attempts, long backoffMillis, long stopTimeoutMillis,
            IntConsumer exit) {
        if (backoffMillis < 0 || backoffMillis > 300_000 || stopTimeoutMillis < 1 || stopTimeoutMillis > 300_000) {
            throw new IllegalArgumentException("Invalid stale recovery timing");
        }
        this.stopper = stopper;
        this.directory = directory.toAbsolutePath().normalize();
        this.budget = new StaleRecoveryBudget(this.directory, attempts);
        this.backoffMillis = backoffMillis;
        this.stopTimeoutMillis = stopTimeoutMillis;
        this.exit = exit;
        synchronized (GENERATION_LOCK) {
            GENERATIONS.put(this.directory, generation);
        }
    }

    /** Schedules at most once per launch; acceptance is not node invalidation or a successful budget reservation. */
    public boolean request() {
        if (closed.get() || !generation.equals(GENERATIONS.get(directory)) || !claimed.compareAndSet(false, true)) {
            return false;
        }
        new Thread(this::recover, "stale-node-recovery").start();
        return true;
    }

    private void recover() {
        try {
            if (!current()) {
                return;
            }
            int attempt = budget.reserve();
            if (attempt < 0) {
                LOG.error("STALE_RECOVERY_BLOCKED: persistent budget exhausted; no exit or automatic budget reset [{}]", directory);
                return;
            }
            LOG.warn("STALE_RECOVERY_RESERVED: attempt={}, budget={}", attempt, directory);
            TimeUnit.MILLISECONDS.sleep(Math.min(300_000, backoffMillis * (1L << Math.min(10, attempt - 1))));
            synchronized (GENERATION_LOCK) {
                if (!current()) {
                    return;
                }
                exitOwned = true;
            }

            CountDownLatch stopped = new CountDownLatch(1);
            AtomicBoolean stopSucceeded = new AtomicBoolean();
            new Thread(() -> {
                try {
                    stopper.stopNode();
                    stopSucceeded.set(true);
                } catch (Throwable error) {
                    LOG.error("Stale recovery graceful stop failed", error);
                } finally {
                    stopped.countDown();
                }
            }, "stale-node-stopper").start();

            boolean graceful = stopped.await(stopTimeoutMillis, TimeUnit.MILLISECONDS) && stopSucceeded.get();
            // A replacement instance for this work supersedes all callbacks, including a late stop watchdog.
            synchronized (GENERATION_LOCK) {
                if (generation.equals(GENERATIONS.get(directory))) {
                    LOG.warn("STALE_RECOVERY_EXIT: code={}, graceful={}, attempt={}", RESTART_EXIT_CODE, graceful, attempt);
                    exit.accept(RESTART_EXIT_CODE);
                }
            }
        } catch (Exception error) {
            LOG.error("STALE_RECOVERY_BLOCKED: cannot safely persist/execute recovery; manual investigation required", error);
        }
    }

    private boolean current() {
        return !closed.get() && generation.equals(GENERATIONS.get(directory));
    }

    /** Whether this launch has committed to the bounded stop and process exit (including during its own stop). */
    public boolean ownsProcessExit() {
        synchronized (GENERATION_LOCK) {
            return exitOwned && generation.equals(GENERATIONS.get(directory));
        }
    }

    /**
     * Revokes a pending watchdog before an in-process replacement starts. A committed process exit wins over an internal restart.
     *
     * @return Whether the internal restart may proceed.
     */
    public boolean prepareForRestart() {
        synchronized (GENERATION_LOCK) {
            if (exitOwned && generation.equals(GENERATIONS.get(directory))) {
                return false;
            }
            closed.set(true);
            GENERATIONS.remove(directory, generation);
            return true;
        }
    }

    @Override
    public void close() {
        synchronized (GENERATION_LOCK) {
            closed.set(true);
            // The coordinator is closed by its own graceful stop, but must still deliver the committed exit code.
            if (!exitOwned) {
                GENERATIONS.remove(directory, generation);
            }
        }
    }
}
