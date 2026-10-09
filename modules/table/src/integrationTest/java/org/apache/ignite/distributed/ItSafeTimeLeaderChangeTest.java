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

package org.apache.ignite.distributed;

import static org.apache.ignite.internal.raft.PeersAndLearners.fromConsistentIds;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.setFieldValue;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.configuration.ComponentWorkingDir;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.StaticNodeFinder;
import org.apache.ignite.internal.network.utils.ClusterServiceTestUtils;
import org.apache.ignite.internal.partition.replicator.raft.ZonePartitionRaftListener;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.outgoing.PartitionsSnapshots;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.RaftGroupEventsListener;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.apache.ignite.internal.raft.TestLozaFactory;
import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.raft.server.RaftGroupOptions;
import org.apache.ignite.internal.raft.server.impl.JraftServerImpl;
import org.apache.ignite.internal.raft.service.CommandClosure;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.raft.service.SafeTimeAwareCommandClosure;
import org.apache.ignite.internal.raft.storage.LogStorageFactory;
import org.apache.ignite.internal.raft.util.SharedLogStorageFactoryUtils;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.table.distributed.schema.ThreadLocalPartitionCommandsMarshaller;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.storage.state.test.TestTxStatePartitionStorage;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.internal.util.SafeTimeValuesTracker;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.raft.jraft.Closure;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.core.NodeImpl;
import org.apache.ignite.raft.jraft.core.State;
import org.apache.ignite.raft.jraft.entity.LogEntry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.extension.ExtendWith;

/** Verifies delayed command admission across real leadership changes and zone state-machine replication. */
@ExtendWith(ConfigurationExtension.class)
class ItSafeTimeLeaderChangeTest extends IgniteAbstractTest {
    private static final ZonePartitionId GROUP_ID = new ZonePartitionId(20, 5);

    private static final int BASE_PORT = 18350;

    private static final ReplicaMessagesFactory MESSAGES = new ReplicaMessagesFactory();

    @InjectConfiguration("mock: { fsync: false }")
    private RaftConfiguration raftConfiguration;

    private final List<PartialNode> nodes = new ArrayList<>();

    @AfterEach
    void stopNodes() throws Exception {
        closeAll(nodes.stream().map(node -> (AutoCloseable) node::stop).collect(Collectors.toList()));
    }

    @RepeatedTest(5)
    void delayedBatchRemainsSafeAfterLeaderChanges() throws Exception {
        for (int i = 0; i < 3; i++) {
            PartialNode node = new PartialNode("node" + i, BASE_PORT + i);
            nodes.add(node);
            node.start();
        }

        PartialNode originalLeader = leader(nodes.get(0).client);
        sendAndWait(originalLeader.client);
        waitForReplication(originalLeader.safeTime.current());

        QueuedCommand first = new QueuedCommand(originalLeader, originalLeader.clock.now());
        PausedBatch pausedBatch = batchHandler(originalLeader.raftNode());
        pausedBatch.onEvent(first.event(), 0, false);

        // Hold this apply batch at a deterministic boundary; do not mutate Raft state, term, or clocks.
        Thread.sleep(500);
        PartialNode secondLeader = nodes.stream().filter(node -> node != originalLeader).findFirst().orElseThrow();
        transfer(originalLeader, secondLeader);
        sendAndWait(secondLeader.client);
        HybridTimestamp advancedTime = secondLeader.safeTime.current();
        waitForReplication(advancedTime);
        log.info("Advanced safe time on intermediate leader [leader={}, timestamp={}]", secondLeader.name, advancedTime);

        Thread.sleep(500);
        transfer(secondLeader, originalLeader);
        QueuedCommand second = new QueuedCommand(originalLeader, first.command.initiatorTime());
        assertTrue(waitForCondition(() -> nodes.stream().allMatch(node -> node.processedBatches.get() >= 2), 20_000));
        Map<PartialNode, Integer> before = nodes.stream().collect(Collectors.toMap(node -> node, node -> node.processedBatches.get()));
        originalLeader.pauseApply = true;
        try {
            pausedBatch.onEvent(second.event(), 1, true);
            // Keep the leader able to publish commit while both followers apply the delayed command.
            assertTrue(waitForCondition(() -> nodes.stream().filter(node -> node != originalLeader)
                    .allMatch(node -> node.processedBatches.get() > before.get(node)), 20_000));
        } finally {
            originalLeader.pauseApply = false;
            originalLeader.applyGate.complete(null);
        }

        try {
            first.result.get(20, TimeUnit.SECONDS);
            second.result.get(20, TimeUnit.SECONDS);
        } catch (ExecutionException error) {
            assertTrue(waitForCondition(
                    () -> nodes.stream().allMatch(node -> node.raftNode().getNodeState() == State.STATE_ERROR), 20_000
            ), "Expected a replicated stale timestamp to put all three state machines into ERROR");
            for (PartialNode node : nodes) {
                log.error("Replicated command failed [node={}, state={}, safeTime={}, assigned={}, previous={}]",
                        error, node.name, node.raftNode().getNodeState(), node.safeTime.current(), first.safeTs, advancedTime);
            }
            throw new AssertionError("Delayed batch caused replicated ERROR_TYPE_STATE_MACHINE", error);
        }

        assertTrue(first.safeTs.compareTo(advancedTime) > 0, "The accepted batch must not reuse its pre-election timestamp");
        waitForReplication(first.safeTs);
        log.info("Delayed batch applied on every replica [timestamp={}, previous={}]", first.safeTs, advancedTime);

        Thread.sleep(500);
        transfer(originalLeader, nodes.get(2) == originalLeader ? nodes.get(1) : nodes.get(2));
        RaftGroupService client = nodes.get(0).client;
        for (int i = 0; i < 20; i++) {
            sendAndWait(client);
            waitForReplication(leader(client).safeTime.current());
            assertTrue(nodes.stream().allMatch(node -> node.raftNode().getNodeState().isActive()));
            Thread.sleep(50);
        }
        log.info("Post-fault progress verified [commands=20, states={}]",
                nodes.stream().map(node -> node.name + ":" + node.raftNode().getNodeState()).collect(Collectors.toList()));
    }

    private PartialNode leader(RaftGroupService client) throws Exception {
        String leaderName = client.refreshAndGetLeaderWithTerm().get(20, TimeUnit.SECONDS).leader().consistentId();
        return nodes.stream().filter(node -> node.name.equals(leaderName)).findFirst().orElseThrow();
    }

    private void transfer(PartialNode from, PartialNode to) throws Exception {
        assertEquals(from, leader(from.client));
        long term = from.raftNode().getCurrentTerm();
        from.client.transferLeadership(new Peer(to.name)).get(20, TimeUnit.SECONDS);
        assertTrue(waitForCondition(
                () -> to.raftNode().isLeader() && to.raftNode().getCurrentTerm() > term
                        && !from.raftNode().isLeader(), 20_000
        ));
        assertEquals(to, leader(from.client));
        log.info("Real leader change completed [from={}, to={}, term={}]", from.name, to.name, to.raftNode().getCurrentTerm());
    }

    private void sendAndWait(RaftGroupService client) throws Exception {
        client.run(MESSAGES.safeTimeSyncCommand().initiatorTime(new HybridClockImpl().now()).build())
                .get(20, TimeUnit.SECONDS);
    }

    private void waitForReplication(HybridTimestamp timestamp) throws InterruptedException {
        assertTrue(waitForCondition(
                () -> nodes.stream().allMatch(node -> node.safeTime.current().compareTo(timestamp) >= 0), 20_000
        ));
    }

    private static PausedBatch batchHandler(NodeImpl node) throws Exception {
        Class<?> type = Class.forName(NodeImpl.class.getName() + "$LogEntryAndClosureHandler");
        Constructor<?> constructor = type.getDeclaredConstructor(NodeImpl.class);
        constructor.setAccessible(true);
        Object handler = constructor.newInstance(node);
        Method onEvent = type.getDeclaredMethod("onEvent", NodeImpl.LogEntryAndClosure.class, long.class, boolean.class);
        onEvent.setAccessible(true);
        return (event, sequence, endOfBatch) -> onEvent.invoke(handler, event, sequence, endOfBatch);
    }

    /** Controls the production handler batch boundary without adding a test-only dependency to the table module. */
    private interface PausedBatch {
        void onEvent(NodeImpl.LogEntryAndClosure event, long sequence, boolean endOfBatch) throws Exception;
    }

    /** Runs actual networking, log storage, Raft, and the production zone listener with test transaction storage. */
    private class PartialNode {
        private final String name;

        private final int port;

        private final HybridClock clock = new HybridClockImpl();

        private final SafeTimeValuesTracker safeTime = new SafeTimeValuesTracker(HybridTimestamp.MIN_VALUE);

        private final AtomicInteger processedBatches = new AtomicInteger();

        private final CompletableFuture<Void> applyGate = new CompletableFuture<>();

        private volatile boolean pauseApply;

        private ClusterService network;

        private LogStorageFactory logStorage;

        private Loza raft;

        private RaftGroupService client;

        PartialNode(String name, int port) {
            this.name = name;
            this.port = port;
        }

        void start() throws Exception {
            StaticNodeFinder finder = new StaticNodeFinder(IntStream.range(BASE_PORT, BASE_PORT + 3)
                    .mapToObj(value -> new NetworkAddress("localhost", value)).collect(Collectors.toList()));
            network = ClusterServiceTestUtils.clusterService(name, port, finder);
            assertThat(network.startAsync(new ComponentContext()), willCompleteSuccessfully());
            ComponentWorkingDir directory = new ComponentWorkingDir(workDir.resolve(name));
            logStorage = SharedLogStorageFactoryUtils.create("test", name, directory.raftLogPath(), false);
            assertThat(logStorage.startAsync(new ComponentContext()), willCompleteSuccessfully());
            raft = TestLozaFactory.create(network, raftConfiguration, clock);
            assertThat(raft.startAsync(new ComponentContext()), willCompleteSuccessfully());

            ZonePartitionRaftListener listener = new ZonePartitionRaftListener(
                    GROUP_ID, new TestTxStatePartitionStorage(), mock(TxManager.class), safeTime,
                    new PendingComparableValuesTracker<>(0L), mock(PartitionsSnapshots.class, RETURNS_DEEP_STUBS), Runnable::run
            ) {
                @Override
                public void onWrite(Iterator<CommandClosure<WriteCommand>> iterator) {
                    if (pauseApply) {
                        try {
                            applyGate.get(30, TimeUnit.SECONDS);
                        } catch (Exception error) {
                            throw new IllegalStateException("Test leader apply gate did not open", error);
                        }
                    }
                    try {
                        super.onWrite(iterator);
                    } finally {
                        processedBatches.incrementAndGet();
                    }
                }
            };
            client = raft.startRaftGroupNode(
                    new RaftNodeId(GROUP_ID, new Peer(name)), fromConsistentIds(Set.of("node0", "node1", "node2")),
                    listener, RaftGroupEventsListener.noopLsnr, RaftGroupOptions.defaults()
                            .maxClockSkew(500)
                            .commandsMarshaller(new ThreadLocalPartitionCommandsMarshaller(network.serializationRegistry()))
                            .serverDataPath(directory.metaPath()).setLogStorageFactory(logStorage)
            );
        }

        NodeImpl raftNode() {
            return (NodeImpl) ((JraftServerImpl) raft.server())
                    .raftGroupService(new RaftNodeId(GROUP_ID, new Peer(name))).getRaftNode();
        }

        void stop() throws Exception {
            closeAll(
                    raft == null ? null : () -> raft.stopRaftNodes(GROUP_ID),
                    raft == null ? null : raft::beforeNodeStop,
                    network == null ? null : network::beforeNodeStop,
                    raft == null ? null : () -> assertThat(raft.stopAsync(new ComponentContext()), willCompleteSuccessfully()),
                    logStorage == null ? null : () -> assertThat(logStorage.stopAsync(new ComponentContext()), willCompleteSuccessfully()),
                    network == null ? null : () -> assertThat(network.stopAsync(new ComponentContext()), willCompleteSuccessfully())
            );
        }
    }

    /** Patches the serialized command exactly as the production ActionRequestProcessor closure does. */
    private static class QueuedCommand implements Closure, SafeTimeAwareCommandClosure {
        private final PartialNode node;

        private final WriteCommand command;

        private final ByteBuffer data;

        private final CompletableFuture<Void> result = new CompletableFuture<>();

        private HybridTimestamp safeTs;

        QueuedCommand(PartialNode node, HybridTimestamp initiatorTime) {
            this.node = node;
            command = MESSAGES.safeTimeSyncCommand().initiatorTime(initiatorTime).build();
            data = ByteBuffer.wrap(node.raftNode().getOptions().getCommandsMarshaller().marshall(command));
        }

        NodeImpl.LogEntryAndClosure event() throws Exception {
            NodeImpl.LogEntryAndClosure event = new NodeImpl.LogEntryAndClosure();
            LogEntry entry = new LogEntry();
            entry.setData(data);
            setFieldValue(event, "entry", entry);
            setFieldValue(event, "done", this);
            setFieldValue(event, "expectedTerm", -1L);
            return event;
        }

        @Override
        public WriteCommand command() {
            return command;
        }

        @Override
        public HybridTimestamp safeTimestamp() {
            return safeTs;
        }

        @Override
        public void safeTimestamp(HybridTimestamp timestamp) {
            node.raftNode().getOptions().getCommandsMarshaller().patch(data, timestamp);
            safeTs = timestamp;
        }

        @Override
        public void result(Serializable response) {
            if (response instanceof Throwable) {
                result.completeExceptionally((Throwable) response);
            } else {
                result.complete(null);
            }
        }

        @Override
        public void run(Status status) {
            result.completeExceptionally(new IllegalStateException(status.toString()));
        }
    }
}
