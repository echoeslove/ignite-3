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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class StaleRecoveryBudgetTest {
    @TempDir
    Path directory;

    @Test
    void persistsAcrossInstancesAndDoesNotResetOnElapsedTimeOrStartup() throws Exception {
        for (int i = 1; i <= 3; i++) {
            assertEquals(i, new StaleRecoveryBudget(directory, 3).reserve());
        }
        assertEquals(-1, new StaleRecoveryBudget(directory, 3).reserve());
        assertEquals("stale-recovery-v1\n3\n", Files.readString(directory.resolve("budget.v1")));
    }

    @Test
    void corruptOrSymlinkStateFailsClosed() throws Exception {
        Path state = directory.resolve("budget.v1");
        Files.writeString(state, "");
        assertThrows(IOException.class, () -> new StaleRecoveryBudget(directory, 3).reserve());
        Files.delete(state);
        Files.createSymbolicLink(state, directory.resolve("elsewhere"));
        assertThrows(IOException.class, () -> new StaleRecoveryBudget(directory, 3).reserve());
    }

    @Test
    void oneExitPerLaunchAndGracefulStopBeforeExit() throws Exception {
        AtomicInteger stops = new AtomicInteger();
        AtomicInteger code = new AtomicInteger();
        CountDownLatch exited = new CountDownLatch(1);
        try (StaleNodeRecovery recovery = new StaleNodeRecovery(stops::incrementAndGet, directory, 3, 0, 1000, status -> {
            assertEquals(1, stops.get());
            code.set(status);
            exited.countDown();
        })) {
            assertTrue(recovery.request());
            assertFalse(recovery.request());
            assertTrue(exited.await(5, SECONDS));
            assertEquals(75, code.get());
            assertEquals("stale-recovery-v1\n1\n", Files.readString(directory.resolve("budget.v1")));
        }
    }

    @Test
    void replacementGenerationSuppressesOldStopWatchdog() throws Exception {
        CountDownLatch stopping = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger exits = new AtomicInteger();
        StaleNodeRecovery old = new StaleNodeRecovery(() -> {
            stopping.countDown();
            try {
                release.await(5, SECONDS);
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
            }
        }, directory, 3, 0, 1000, ignored -> exits.incrementAndGet());
        assertTrue(old.request());
        assertTrue(stopping.await(5, SECONDS));
        try (StaleNodeRecovery replacement = new StaleNodeRecovery(() -> {}, directory, 3, 0, 1000,
                ignored -> exits.incrementAndGet())) {
            old.close();
            release.countDown();
            // Synchronize with the old watchdog's timeout; it must not halt a replacement instance.
            Thread.sleep(1200);
            assertEquals(0, exits.get());
            assertFalse(old.request());
        }
    }

    @Test
    void internalRestartRevokesPendingRecoveryBeforeReplacementConstruction() throws Exception {
        CountDownLatch exited = new CountDownLatch(1);
        AtomicInteger stops = new AtomicInteger();
        try (StaleNodeRecovery recovery = new StaleNodeRecovery(stops::incrementAndGet, directory, 3, 300, 1000,
                ignored -> exited.countDown())) {
            assertTrue(recovery.request());
            assertTrue(org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition(
                    () -> Files.exists(directory.resolve("budget.v1")), 5000));
            assertTrue(recovery.prepareForRestart());
            assertFalse(recovery.ownsProcessExit());
            assertFalse(recovery.request());
            // No replacement exists yet: cancellation must already protect the construction window.
            assertFalse(exited.await(600, java.util.concurrent.TimeUnit.MILLISECONDS));
            assertEquals(0, stops.get());
        }
    }

    @Test
    void committedExitRejectsInternalRestartAndSurvivesItsOwnClose() throws Exception {
        CountDownLatch stopping = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch exited = new CountDownLatch(1);
        AtomicReference<StaleNodeRecovery> reference = new AtomicReference<>();
        try (StaleNodeRecovery recovery = new StaleNodeRecovery(() -> {
            reference.get().close();
            stopping.countDown();
            try {
                release.await(5, SECONDS);
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
            }
        }, directory, 3, 0, 3000, code -> {
            assertEquals(75, code);
            exited.countDown();
        })) {
            reference.set(recovery);
            assertTrue(recovery.request());
            try {
                assertTrue(stopping.await(5, SECONDS));
                assertTrue(recovery.ownsProcessExit());
                assertFalse(recovery.prepareForRestart());
            } finally {
                release.countDown();
            }
            assertTrue(exited.await(5, SECONDS));
        }
    }

    @Test
    void committedStopTimeoutStillExitsWithRestartCode() throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch exited = new CountDownLatch(1);
        try (StaleNodeRecovery recovery = new StaleNodeRecovery(() -> {
            try {
                release.await(5, SECONDS);
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
            }
        }, directory, 3, 0, 50, code -> {
            assertEquals(75, code);
            exited.countDown();
        })) {
            assertTrue(recovery.request());
            try {
                assertTrue(exited.await(5, SECONDS));
            } finally {
                release.countDown();
            }
        }
    }

    @Test
    void realJvmExitsAndRelaunchesKeepTheSameBudget() throws Exception {
        String classPath = System.getProperty("java.class.path");
        assertFalse(classPath.isEmpty());
        for (int expected : List.of(75, 75, 75, 42)) {
            Process process = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                    "-cp", classPath, RecoveryProcess.class.getName(), directory.toString()).redirectErrorStream(true).start();
            assertTrue(process.waitFor(10, SECONDS), "Recovery child did not exit");
            String log = new String(process.getInputStream().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
            assertEquals(expected, process.exitValue(), log);
        }
        assertEquals("stale-recovery-v1\n3\n", Files.readString(directory.resolve("budget.v1")));
        assertEquals(3, Files.readAllLines(directory.resolve("stops")).size());
    }

    @Test
    void actualContainerEntrypointRelaunchesRealJvmOnlyWithinBudget() throws Exception {
        Path repository = Path.of(getClass().getProtectionDomain().getCodeSource().getLocation().toURI());
        while (repository != null && !Files.exists(repository.resolve("packaging/docker/docker-entrypoint.sh"))) {
            repository = repository.getParent();
        }
        assertTrue(repository != null);
        String javaExecutable = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        String classPath = System.getProperty("java.class.path");
        Files.writeString(directory.resolve("bootstrap"), "JAVACMD=" + javaExecutable + "\nCOMMON_JAVA_OPTS=''\nCLASSPATH='-cp "
                + classPath + " " + RecoveryProcess.class.getName() + " " + directory + "'\n");
        String template = Files.readString(repository.resolve("packaging/docker/docker-entrypoint.sh"));
        Path entrypoint = directory.resolve("entrypoint.sh");
        Files.writeString(entrypoint, template.replace("@LIB_DIR@", directory.toString())
                .replace("@BOOTSTRAP_FILE_NAME@", "bootstrap").replace("@CONF_DIR@", directory.toString()));
        ProcessBuilder builder = new ProcessBuilder("sh", entrypoint.toString()).redirectErrorStream(true);
        builder.environment().putAll(java.util.Map.of("JVM_MAX_MEM", "64m", "JVM_MIN_MEM", "64m", "JVM_GC", "G1GC",
                "JVM_G1HeapRegionSize", "1m", "IGNITE_STALE_RECOVERY_ENABLED", "true"));
        Process process = builder.start();
        assertTrue(process.waitFor(15, SECONDS));
        String log = new String(process.getInputStream().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
        assertEquals(42, process.exitValue(), log);
        assertEquals(3, log.split("STALE_RECOVERY_RELAUNCH", -1).length - 1, log);
        assertEquals("stale-recovery-v1\n3\n", Files.readString(directory.resolve("budget.v1")));
        assertEquals(3, Files.readAllLines(directory.resolve("stops")).size());
    }

    /** Separate JVM, intentionally halted by the real recovery exit adapter. Never starts an Ignite node. */
    public static class RecoveryProcess {
        public static void main(String[] args) throws Exception {
            Path work = Path.of(args[0]);
            StaleNodeRecovery recovery = new StaleNodeRecovery(() -> {
                try {
                    Files.writeString(work.resolve("stops"), "stopped\n", java.nio.file.StandardOpenOption.CREATE,
                            java.nio.file.StandardOpenOption.APPEND);
                } catch (IOException error) {
                    throw new java.io.UncheckedIOException(error);
                }
            }, work, 3, 0, 1000, code -> Runtime.getRuntime().halt(code));
            recovery.request();
            Thread.sleep(1500);
            // Budget exhausted: no further automatic exit; this does not assert business-operation fencing.
            System.exit(42);
        }
    }
}
