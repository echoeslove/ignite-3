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

package org.apache.ignite.internal.app;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.setFieldValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.failure.StaleNodeRecovery;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.InOrder;

/** Exercises the actual runner/server arbitration without starting storage or a cluster. */
class StaleRecoveryLifecycleTest {
    @TempDir
    Path directory;

    private IgniteServerImpl serverWith(IgniteImpl instance) throws Exception {
        Path config = Files.createFile(directory.resolve("ignite.conf"));
        IgniteServerImpl server = new IgniteServerImpl("lifecycle-test", config, directory, null, Runnable::run);
        setFieldValue(server, "ignite", instance);
        return server;
    }

    @Test
    void committedProcessExitRejectsRestartBeforeDetachOrStop() throws Exception {
        IgniteImpl instance = mock(IgniteImpl.class);
        when(instance.prepareForRestart()).thenReturn(false);
        when(instance.staleRecoveryOwnsExit()).thenReturn(true);
        IgniteServerImpl server = serverWith(instance);

        assertThrows(CompletionException.class, () -> server.restartAsync().join());
        verify(instance, never()).stopAsync();
        assertTrue(server.staleRecoveryOwnsExit(), "The original instance must remain attached for runner exit arbitration");
    }

    @Test
    void internalRestartRevokesOldRecoveryBeforeStoppingOrConstructingReplacement() throws Exception {
        IgniteImpl instance = mock(IgniteImpl.class);
        CompletableFuture<Void> stop = new CompletableFuture<>();
        when(instance.prepareForRestart()).thenReturn(true);
        when(instance.stopAsync()).thenReturn(stop);
        IgniteServerImpl server = serverWith(instance);

        CompletableFuture<Void> restart = server.restartAsync();
        InOrder order = inOrder(instance);
        order.verify(instance).prepareForRestart();
        order.verify(instance).stopAsync();
        assertFalse(restart.isDone());
        // Fail the stop deliberately: never construct a real replacement in this unit test.
        stop.completeExceptionally(new NodeStoppingException("test stop failed"));
        assertThrows(CompletionException.class, restart::join);
    }

    @Test
    void ordinaryInitializationFailureStillExitsOne() {
        IgniteServerImpl server = mock(IgniteServerImpl.class);
        when(server.waitForInitAsync()).thenReturn(CompletableFuture.failedFuture(new IllegalStateException("ordinary startup failure")));
        AtomicInteger code = new AtomicInteger();
        IgniteRunner.awaitInitialization(server, new AtomicBoolean(), code::set);
        assertEquals(1, code.get());
    }

    @Test
    void signalShutdownStillSuppressesStartupFailureExit() {
        IgniteServerImpl server = mock(IgniteServerImpl.class);
        when(server.waitForInitAsync()).thenReturn(CompletableFuture.failedFuture(new NodeStoppingException()));
        AtomicInteger code = new AtomicInteger();
        IgniteRunner.awaitInitialization(server, new AtomicBoolean(true), code::set);
        assertEquals(0, code.get());
    }

    @Test
    void actualRunnerInitializationCatchDoesNotBeatRecoveryExitInSeparateJvm() throws Exception {
        Process process = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", System.getProperty("java.class.path"), InitializationRecoveryProcess.class.getName(), directory.toString())
                .redirectErrorStream(true).redirectOutput(directory.resolve("child.log").toFile()).start();
        try {
            assertTrue(process.waitFor(20, SECONDS), "Recovery child did not exit");
            assertEquals(75, process.exitValue(), Files.readString(directory.resolve("child.log"), StandardCharsets.UTF_8));
            assertEquals("stale-recovery-v1\n1\n", Files.readString(directory.resolve("budget.v1")));
        } finally {
            if (process.isAlive()) {
                process.destroyForcibly();
            }
        }
    }

    /** Real coordinator and runner catch path; mock only the cluster's failed initialization future. */
    public static class InitializationRecoveryProcess {
        public static void main(String[] args) throws Exception {
            CompletableFuture<Void> initialization = new CompletableFuture<>();
            CountDownLatch release = new CountDownLatch(1);
            AtomicReference<StaleNodeRecovery> reference = new AtomicReference<>();
            StaleNodeRecovery recovery = new StaleNodeRecovery(() -> {
                reference.get().close();
                initialization.completeExceptionally(new NodeStoppingException("stale stop during initialization"));
                try {
                    release.await(10, SECONDS);
                } catch (InterruptedException error) {
                    Thread.currentThread().interrupt();
                }
            }, Path.of(args[0]), 3, 0, 15000, code -> Runtime.getRuntime().halt(code));
            reference.set(recovery);
            IgniteServerImpl server = mock(IgniteServerImpl.class);
            when(server.waitForInitAsync()).thenReturn(initialization);
            when(server.staleRecoveryOwnsExit()).thenAnswer(ignored -> recovery.ownsProcessExit());
            recovery.request();
            IgniteRunner.awaitInitialization(server, new AtomicBoolean(), System::exit);
            release.countDown();
        }
    }
}
