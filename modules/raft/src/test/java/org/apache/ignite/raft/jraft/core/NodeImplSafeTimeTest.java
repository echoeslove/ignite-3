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

package org.apache.ignite.raft.jraft.core;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.setFieldValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.lmax.disruptor.EventHandler;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.internal.raft.service.SafeTimeAwareCommandClosure;
import org.apache.ignite.internal.util.SafeTimeValuesTracker;
import org.apache.ignite.raft.jraft.Closure;
import org.apache.ignite.raft.jraft.conf.Configuration;
import org.apache.ignite.raft.jraft.conf.ConfigurationEntry;
import org.apache.ignite.raft.jraft.entity.LogEntry;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.option.NodeOptions;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.storage.LogManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests safe timestamp assignment at the Raft command admission boundary. */
class NodeImplSafeTimeTest {
    private final NodeImpl node = new NodeImpl("safe-time-test", new PeerId("node"));
    private final LogManager logManager = mock(LogManager.class);
    private final BallotBox ballotBox = mock(BallotBox.class);
    private final HybridClock clock = new HybridClockImpl();
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private EventHandler<NodeImpl.LogEntryAndClosure> handler;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() throws Exception {
        NodeOptions options = new NodeOptions();
        options.setCommonExecutor(executor);
        ConfigurationEntry configuration = new ConfigurationEntry();
        setFieldValue(node, "options", options);
        setFieldValue(node, "raftOptions", new RaftOptions());
        setFieldValue(node, "logManager", logManager);
        setFieldValue(node, "ballotBox", ballotBox);
        setFieldValue(node, "clock", clock);
        setFieldValue(node, "conf", configuration);
        setFieldValue(node, "state", State.STATE_LEADER);
        setFieldValue(node, "currTerm", 3L);
        when(logManager.hasAvailableCapacityToAppendEntries(1)).thenReturn(true);
        when(logManager.checkAndSetConfiguration(configuration)).thenReturn(configuration);
        when(ballotBox.appendPendingTask(any(Configuration.class), any(), any())).thenReturn(true);

        Class<?> handlerClass = Class.forName(NodeImpl.class.getName() + "$LogEntryAndClosureHandler");
        Constructor<?> constructor = handlerClass.getDeclaredConstructor(NodeImpl.class);
        constructor.setAccessible(true);
        handler = (EventHandler<NodeImpl.LogEntryAndClosure>) constructor.newInstance(node);
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        executor.shutdownNow();
        assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }

    @Test
    void queuedCommandGetsFreshTimestampAfterLeadershipChanges() throws Exception {
        HybridTimestamp initiatorTimestamp = clock.now();
        AtomicReference<HybridTimestamp> assigned = new AtomicReference<>();
        TestClosure closure = closure(initiatorTimestamp, assigned);
        NodeImpl.LogEntryAndClosure queued = task(closure, -1);

        handler.onEvent(queued, 0, false);

        // Other leaders have advanced the clock while this command is still waiting in the batch.
        setFieldValue(node, "state", State.STATE_FOLLOWER);
        HybridTimestamp newerLeaderTimestamp = clock.update(initiatorTimestamp.addPhysicalTime(1000));
        setFieldValue(node, "currTerm", 5L);
        setFieldValue(node, "state", State.STATE_LEADER);

        handler.onEvent(task(closure(initiatorTimestamp, new AtomicReference<>()), -1), 1, true);

        SafeTimeValuesTracker tracker = new SafeTimeValuesTracker(newerLeaderTimestamp);
        tracker.update(assigned.get(), null);
        assertTrue(assigned.get().compareTo(newerLeaderTimestamp) > 0);
        assertEquals(assigned.get(), tracker.current());
        verify(logManager).appendEntries(anyList(), any());
    }

    @Test
    void followerRejectsCommandWithoutAssigningSafeTime() throws Exception {
        setFieldValue(node, "state", State.STATE_FOLLOWER);
        AtomicReference<HybridTimestamp> assigned = new AtomicReference<>();
        TestClosure closure = closure(clock.now(), assigned);
        CompletableFuture<Void> rejected = new CompletableFuture<>();
        doAnswer(invocation -> {
            assertFalse(invocation.getArgument(0, org.apache.ignite.raft.jraft.Status.class).isOk());
            rejected.complete(null);
            return null;
        }).when(closure).run(any());

        handler.onEvent(task(closure, -1), 0, true);

        rejected.get(10, TimeUnit.SECONDS);
        assertNull(assigned.get());
        verify(closure, never()).safeTimestamp(any());
        verify(logManager, never()).appendEntries(anyList(), any());
    }

    @Test
    void mismatchedTermDoesNotAssignSafeTime() throws Exception {
        AtomicReference<HybridTimestamp> assigned = new AtomicReference<>();
        TestClosure closure = closure(clock.now(), assigned);

        handler.onEvent(task(closure, 2), 0, true);

        assertNull(assigned.get());
        verify(closure, never()).safeTimestamp(any());
    }

    @Test
    void commandsInSameBatchMayShareTimestamp() throws Exception {
        HybridTimestamp timestamp = clock.now();
        AtomicReference<HybridTimestamp> first = new AtomicReference<>();
        AtomicReference<HybridTimestamp> second = new AtomicReference<>();

        handler.onEvent(task(closure(timestamp, first), -1), 0, false);
        handler.onEvent(task(closure(timestamp, second), -1), 1, true);

        assertTrue(first.get().compareTo(timestamp) > 0);
        assertEquals(first.get(), second.get());
    }

    private static TestClosure closure(
            HybridTimestamp initiatorTimestamp,
            AtomicReference<HybridTimestamp> assigned
    ) {
        WriteCommand command = mock(WriteCommand.class);
        when(command.initiatorTime()).thenReturn(initiatorTimestamp);
        TestClosure closure = mock(TestClosure.class);
        when(closure.command()).thenReturn(command);
        doAnswer(invocation -> {
            assigned.set(invocation.getArgument(0));
            return null;
        }).when(closure).safeTimestamp(any());
        return closure;
    }

    private static NodeImpl.LogEntryAndClosure task(TestClosure closure, long expectedTerm) {
        NodeImpl.LogEntryAndClosure task = new NodeImpl.LogEntryAndClosure();
        task.entry = new LogEntry();
        task.entry.setData(ByteBuffer.allocate(1));
        task.done = closure;
        task.expectedTerm = expectedTerm;
        return task;
    }

    /** Combines the command timestamp contract with the Raft completion callback. */
    private interface TestClosure extends SafeTimeAwareCommandClosure, Closure {
    }
}
