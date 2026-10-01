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

package org.apache.ignite.internal.raftsnapshot;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_AIMEM_PROFILE_NAME;
import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_AIPERSIST_PROFILE_NAME;
import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_ROCKSDB_PROFILE_NAME;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.TestWrappers.unwrapTableImpl;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.raft.util.OptimizedMarshaller.NO_POOL;
import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.executeUpdate;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.getFieldValue;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedIn;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_READ_ONLY_TOO_OLD_ERR;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.Cluster;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.cluster.management.CmgGroupId;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.lowwatermark.message.GetLowWatermarkResponse;
import org.apache.ignite.internal.metastorage.server.WatchListenerInhibitor;
import org.apache.ignite.internal.metastorage.server.raft.MetastorageGroupId;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.serialization.MessageSerializationRegistry;
import org.apache.ignite.internal.partition.replicator.network.raft.SnapshotMetaResponse;
import org.apache.ignite.internal.partition.replicator.network.raft.SnapshotMvDataResponse;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadOnlyMultiRowPkReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadOnlySingleRowPkReplicaRequest;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.incoming.IncomingSnapshotCopier;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.raft.server.RaftServer;
import org.apache.ignite.internal.raft.server.impl.JraftServerImpl;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.command.SafeTimeSyncCommand;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.pagememory.PersistentPageMemoryStorageEngine;
import org.apache.ignite.internal.storage.pagememory.VolatilePageMemoryStorageEngine;
import org.apache.ignite.internal.storage.pagememory.mv.AbstractPageMemoryMvPartitionStorage;
import org.apache.ignite.internal.storage.rocksdb.RocksDbStorageEngine;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.distributed.schema.PartitionCommandsMarshallerImpl;
import org.apache.ignite.internal.table.distributed.storage.InternalTableImpl;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.testframework.log4j2.LogInspector;
import org.apache.ignite.internal.testframework.log4j2.LogInspector.Handler;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.InternalTxOptions;
import org.apache.ignite.internal.wrapper.Wrapper;
import org.apache.ignite.raft.jraft.RaftGroupService;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.core.Replicator;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.rpc.Message;
import org.apache.ignite.raft.jraft.rpc.RaftRpcFactory;
import org.apache.ignite.raft.jraft.rpc.RaftServerService;
import org.apache.ignite.raft.jraft.rpc.RpcProcessor;
import org.apache.ignite.raft.jraft.rpc.RpcRequestClosure;
import org.apache.ignite.raft.jraft.rpc.RpcRequestProcessor;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.AppendEntriesRequest;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.InstallSnapshotRequest;
import org.apache.ignite.raft.jraft.rpc.RpcServer;
import org.apache.ignite.raft.jraft.rpc.WriteActionRequest;
import org.apache.ignite.raft.jraft.rpc.impl.IgniteRpcServer;
import org.apache.ignite.raft.jraft.rpc.impl.core.AppendEntriesRequestProcessor;
import org.apache.ignite.raft.jraft.storage.snapshot.SnapshotExecutorImpl;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionOptions;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests how RAFT snapshots installation works for table partitions.
 */
@SuppressWarnings("resource")
@Timeout(90)
class ItTableRaftSnapshotsTest extends ClusterPerTestIntegrationTest {
    private static final IgniteLogger LOG = Loggers.forClass(ItTableRaftSnapshotsTest.class);

    private static final int AWAIT_PRIMARY_REPLICA_SECONDS = 10;

    private static final int PARTITION_ID = 0;

    private static final String TEST_ZONE_NAME = "test_zone";

    private static final String TEST_TABLE_NAME = "test";

    private static final boolean LONG_SNAPSHOT_TIMEOUT_TEST = "true".equals(System.getenv("IGNITE_HF4_LONG_SNAPSHOT_TIMEOUT_TEST"));

    /**
     * Nodes bootstrap configuration pattern.
     *
     * <p>installSnapshotTimeoutMillis is changed to 10 seconds so that sporadic snapshot installation failures still
     * allow tests pass thanks to retries.
     */
    private static final String NODE_BOOTSTRAP_CFG = "ignite {\n"
            + "  network: {\n"
            + "    port: {},\n"
            + "    nodeFinder.netClusterNodes: [ {} ]\n"
            + "  },\n"
            + "  raft.installSnapshotTimeoutMillis: " + (LONG_SNAPSHOT_TIMEOUT_TEST ? 660000 : 10000) + ",\n"
            + "  storage.profiles: {"
            + "        " + DEFAULT_AIPERSIST_PROFILE_NAME + ".engine: aipersist, "
            + "        " + DEFAULT_AIMEM_PROFILE_NAME + ".engine: aimem, "
            + "        " + DEFAULT_ROCKSDB_PROFILE_NAME + ".engine: rocksdb"
            + "  },\n"
            + "  clientConnector.port: {},\n"
            + "  rest.port: {},\n"
            + "  failureHandler.dumpThreadsOnFailure: false\n"
            + "}";

    /**
     * Marker that instructs to create a table with the default storage engine. Used in tests that are indifferent
     * to a storage engine used.
     */
    private static final String DEFAULT_STORAGE_ENGINE = "<default>";

    private LogInspector replicatorLogInspector;

    private LogInspector copierLogInspector;

    @Override
    protected String getNodeBootstrapConfigTemplate() {
        return NODE_BOOTSTRAP_CFG;
    }

    @Override
    protected int[] cmgMetastoreNodes() {
        return new int[]{0, 1, 2};
    }

    @BeforeEach
    void createLogInspectors(TestInfo testInfo) {
        replicatorLogInspector = LogInspector.create(Replicator.class, true);
        copierLogInspector = LogInspector.create(IncomingSnapshotCopier.class, true);
    }

    @AfterEach
    void stopLogInspectors() {
        replicatorLogInspector.stop();
        copierLogInspector.stop();
    }

    /**
     * Tests that a leader successfully feeds a follower with a RAFT snapshot on any of the supported storage engines.
     */
    @ParameterizedTest(name = "engine = {0}")
    @MethodSource("storageEngines")
    void leaderFeedsFollowerWithSnapshot(String storageEngine) throws Exception {
        testLeaderFeedsFollowerWithSnapshot(storageEngine);
    }

    /**
     * Tests that data can be recovered on node start after snapshot installation.
     */
    @ParameterizedTest(name = "engine = {0}")
    @MethodSource("storageEngines")
    void testDataRecoveryAfterSnapshot(String storageEngine) throws Exception {
        assumeFalse(
                storageEngine.equals(VolatilePageMemoryStorageEngine.ENGINE_NAME),
                "https://issues.apache.org/jira/browse/IGNITE-24857"
        );

        feedNode2WithSnapshotOfOneRow(storageEngine);

        cluster.restartNode(2);

        transferLeadershipOnSolePartitionTo(2);

        assertThat(getFromNode(2, 1), is("one"));
    }

    // TODO: IGNITE-18481 - make sure we don't forget to add new storage engines here
    private static Stream<String> storageEngines() {
        return Stream.of(
                RocksDbStorageEngine.ENGINE_NAME,
                PersistentPageMemoryStorageEngine.ENGINE_NAME,
                VolatilePageMemoryStorageEngine.ENGINE_NAME
        );
    }

    /** Tests that a persistent PageMemory tombstone can be transferred in a RAFT snapshot. */
    @Test
    @Timeout(180)
    void installsSnapshotContainingCommittedTombstone() throws Exception {
        createTestTableWith3Replicas(PersistentPageMemoryStorageEngine.ENGINE_NAME);
        transferLeadershipOnSolePartitionTo(0);
        knockoutNode(2);

        putToNode(0, 1, "one");
        putToNode(0, 2, "two");
        assertTrue(tableViewAt(0).remove(null, 1));

        causeLogTruncationOnSolePartitionLeader(0);
        reanimateNode2AndWaitForSnapshotInstalled();

        transferLeadershipOnSolePartitionTo(2);
        assertNull(getFromNode(2, 1));
        assertEquals("two", getFromNode(2, 2));
    }

    /** Verifies that a permanently unanswered metadata RPC times out and a later snapshot with a tombstone succeeds. */
    @Test
    @EnabledIfEnvironmentVariable(named = "IGNITE_HF4_LONG_SNAPSHOT_TIMEOUT_TEST", matches = "true")
    @Timeout(value = 6, unit = TimeUnit.MINUTES)
    void committedTombstoneSnapshotRecoversAfterUnansweredMetadataRequest() throws Exception {
        createTestTableWith3Replicas(PersistentPageMemoryStorageEngine.ENGINE_NAME);
        transferLeadershipOnSolePartitionTo(0);
        transferPrimaryOnSolePartitionTo(0);
        knockoutNode(2);

        putToNode(0, 1, "one");
        putToNode(0, 2, "two");
        assertTrue(tableViewAt(0).remove(null, 1));
        causeLogTruncationOnSolePartitionLeader(0);

        CompletableFuture<Void> firstMetadataResponse = new CompletableFuture<>();
        CompletableFuture<Void> copierTimeout = new CompletableFuture<>();
        CompletableFuture<Void> failedInstall = new CompletableFuture<>();
        CompletableFuture<Void> successfulInstall = new CompletableFuture<>();

        copierLogInspector.addHandler(
                event -> event.getMessage().getFormattedMessage().startsWith("Snapshot request timed out while completing copier"),
                () -> copierTimeout.complete(null)
        );
        replicatorLogInspector.addHandler(
                event -> copierTimeout.isDone()
                        && event.getMessage().getFormattedMessage().startsWith("Node 20_part_0:")
                        && event.getMessage().getFormattedMessage().contains(
                                " received InstallSnapshotResponse from " + cluster.nodeName(2) + " ")
                        && event.getMessage().getFormattedMessage().contains("Unknown error on completion the copier"),
                () -> failedInstall.complete(null)
        );
        listenForSnapshotInstalledSuccessFromLogger(0, 2, successfulInstall);

        BiPredicate<String, NetworkMessage> dropMetadataResponse = dropSnapshotMetaResponse(firstMetadataResponse);
        unwrapIgniteImpl(cluster.node(0)).dropMessages(
                (targetNode, message) -> !failedInstall.isDone() && dropMetadataResponse.test(targetNode, message)
        );
        reanimateNode(2);

        assertThat(firstMetadataResponse, willSucceedIn(1, TimeUnit.MINUTES));
        ReplicationGroupId groupId = cluster.solePartitionId(TEST_ZONE_NAME, TEST_TABLE_NAME);
        IgniteImpl node2 = unwrapIgniteImpl(cluster.node(2));
        AtomicInteger localReads = new AtomicInteger();
        AtomicInteger remoteReads = new AtomicInteger();

        node2.dropMessages((recipient, message) -> {
            if (message instanceof ReadOnlySingleRowPkReplicaRequest || message instanceof ReadOnlyMultiRowPkReplicaRequest) {
                (node2.name().equals(recipient) ? localReads : remoteReads).incrementAndGet();
            }

            return false;
        });

        try {
            for (int sample = 0; sample < 4; sample++) {
                if (sample != 0) {
                    TimeUnit.SECONDS.sleep(15);
                }

                assertFalse(copierTimeout.isDone(), "Samples must be taken before the first copier times out");
                assertTrue(waitForCondition(() -> snapshotInstallingOnNode2(groupId), 30_000));
                assertTrue(node2.replicaManager().isReplicaReady(groupId));
                assertFalse(node2.replicaManager().isLocalReplicaHealthyForRead(groupId));
                assertReadOnlyDataFromNode2();
                assertEquals(0, localReads.get(), "A replica installing a snapshot must not receive local KV reads");
                assertEquals(2 * (sample + 1), remoteReads.get());
                LOG.info("HF4_SNAPSHOT_RO_SAMPLE sample={} installing=true replicaReady=true healthyForRead=false "
                                + "localReads={} remoteReads={}",
                        sample, localReads.get(), remoteReads.get());
            }

            assertThat(copierTimeout, willSucceedIn(3, TimeUnit.MINUTES));
            assertThat(failedInstall, willSucceedIn(1, TimeUnit.MINUTES));

            unwrapIgniteImpl(cluster.node(0)).stopDroppingMessages();
            assertThat(successfulInstall, willSucceedIn(1, TimeUnit.MINUTES));
            assertTrue(waitForCondition(() -> node2.replicaManager().isLocalReplicaHealthyForRead(groupId), 30_000));
            assertReadOnlyDataFromNode2();
            assertEquals(2, localReads.get(), "Recovered replica should serve local KV reads again");
            assertEquals(8, remoteReads.get());
            LOG.info("HF4_SNAPSHOT_RO_RECOVERED installing=false healthyForRead=true localReads={} remoteReads={}",
                    localReads.get(), remoteReads.get());
        } finally {
            node2.stopDroppingMessages();
            unwrapIgniteImpl(cluster.node(0)).stopDroppingMessages();
        }

        transferLeadershipOnSolePartitionTo(2);
        assertNull(getFromNode(2, 1));
        assertEquals("two", getFromNode(2, 2));
    }

    /** Observes local read routing after a failed snapshot has aborted the MV rebalance. */
    @Test
    @EnabledIfEnvironmentVariable(named = "IGNITE_HF4_FAILED_SNAPSHOT_WINDOW_TEST", matches = "true")
    @Timeout(value = 17, unit = TimeUnit.MINUTES)
    void failedSnapshotAbortWindowLocalReadProbe() throws Exception {
        createTestTableWith3Replicas(PersistentPageMemoryStorageEngine.ENGINE_NAME);
        pauseForSnapshotProbe("table-created");
        transferLeadershipOnSolePartitionTo(0);
        pauseForSnapshotProbe("leader-transferred");
        transferPrimaryOnSolePartitionTo(0);
        pauseForSnapshotProbe("primary-transferred");

        putToNode(0, 1, "one");
        pauseForSnapshotProbe("first-write");
        putToNode(0, 2, "two");
        pauseForSnapshotProbe("second-write");

        knockoutNode(2);
        pauseForSnapshotProbe("node2-stopped");
        putToNode(0, 3, "three");
        pauseForSnapshotProbe("third-write");
        causeLogTruncationOnSolePartitionLeader(0);
        pauseForSnapshotProbe("leader-log-truncated");

        CompletableFuture<Void> firstMvResponse = new CompletableFuture<>();
        CompletableFuture<Void> copierTimeout = new CompletableFuture<>();
        CompletableFuture<Void> copierCanceled = new CompletableFuture<>();
        CompletableFuture<Void> failedInstall = new CompletableFuture<>();
        CompletableFuture<Void> successfulInstall = new CompletableFuture<>();
        AtomicInteger installRequests = new AtomicInteger();
        AtomicInteger droppedMvResponses = new AtomicInteger();
        AtomicInteger localReads = new AtomicInteger();
        AtomicInteger remoteReads = new AtomicInteger();
        AtomicBoolean releaseFault = new AtomicBoolean();

        copierLogInspector.addHandler(
                event -> event.getMessage().getFormattedMessage().startsWith("Snapshot request timed out while completing copier"),
                () -> copierTimeout.complete(null)
        );
        copierLogInspector.addHandler(
                event -> event.getMessage().getFormattedMessage().startsWith("Copier is canceled for partition"),
                () -> copierCanceled.complete(null)
        );
        replicatorLogInspector.addHandler(
                event -> (copierTimeout.isDone() || copierCanceled.isDone())
                        && event.getMessage().getFormattedMessage().contains(
                                " received InstallSnapshotResponse from " + cluster.nodeName(2) + " ")
                        && (event.getMessage().getFormattedMessage().contains("error:Status[UNKNOWN")
                                || event.getMessage().getFormattedMessage().contains("error:Status[ECANCELED")),
                () -> failedInstall.complete(null)
        );
        listenForSnapshotInstalledSuccessFromLogger(0, 2, successfulInstall);

        String node2Name = cluster.nodeName(2);
        IgniteImpl node0 = unwrapIgniteImpl(cluster.node(0));

        node0.dropMessages((recipient, message) -> {
            if (!node2Name.equals(recipient)) {
                return false;
            }

            if (message instanceof InstallSnapshotRequest) {
                installRequests.incrementAndGet();
                return firstMvResponse.isDone() && !releaseFault.get();
            }

            if (message instanceof SnapshotMvDataResponse && !releaseFault.get()) {
                droppedMvResponses.incrementAndGet();
                firstMvResponse.complete(null);
                return true;
            }

            return false;
        });

        try {
            reanimateNode(2);
            pauseForSnapshotProbe("node2-restarted");
            assertThat(firstMvResponse, willSucceedIn(1, TimeUnit.MINUTES));
            assertTrue(installRequests.get() > 0, "InstallSnapshotRequest was not observed by the fault injector");

            IgniteImpl node2 = unwrapIgniteImpl(cluster.node(2));
            ReplicationGroupId groupId = cluster.solePartitionId(TEST_ZONE_NAME, TEST_TABLE_NAME);

            node2.dropMessages((recipient, message) -> {
                if (message instanceof ReadOnlySingleRowPkReplicaRequest || message instanceof ReadOnlyMultiRowPkReplicaRequest) {
                    (node2.name().equals(recipient) ? localReads : remoteReads).incrementAndGet();
                }

                return false;
            });

            try {
                assertTrue(waitForCondition(() -> snapshotInstallingOnNode2(groupId), 30_000));
                LOG.info("HF4_ABORT_PROBE phase=installing healthy={} lastApplied={} safeTime={}",
                        node2.replicaManager().isLocalReplicaHealthyForRead(groupId),
                        mvPartitionAtNode(2).lastAppliedIndex(),
                        unwrapTableImpl(tableAt(2)).internalTable().getPartitionSafeTimeTracker(PARTITION_ID).current());

                assertThat(CompletableFuture.anyOf(copierTimeout, copierCanceled), willSucceedIn(11, TimeUnit.MINUTES));
                assertThat(failedInstall, willSucceedIn(1, TimeUnit.MINUTES));
                pauseForSnapshotProbe("failed-install-cleared");

                LOG.info("HF4_ABORT_PROBE phase=failure-trigger cause={}",
                        copierTimeout.isDone() ? "MV_RPC_TIMEOUT" : "RAFT_CANCEL");

                for (int sample = 0; sample < 3; sample++) {
                    boolean installing = snapshotInstallingOnNode2(groupId);
                    boolean healthy = node2.replicaManager().isLocalReplicaHealthyForRead(groupId);
                    long lastApplied = mvPartitionAtNode(2).lastAppliedIndex();
                    String getOutcome = probeReadOnlyGetAtNode2(2);

                    assertFalse(installing);
                    assertEquals(0, lastApplied);
                    assertEquals("two", getOutcome);
                    assertEquals(0, localReads.get());
                    assertEquals(sample + 1, remoteReads.get());

                    LOG.info("HF4_ABORT_PROBE phase=after-failure sample={} installing={} healthy={} lastApplied={} "
                                    + "safeTime={} getOutcome={} localReads={} remoteReads={} installRequests={} droppedMvResponses={}",
                            sample, installing, healthy, lastApplied,
                            unwrapTableImpl(tableAt(2)).internalTable().getPartitionSafeTimeTracker(PARTITION_ID).current(),
                            getOutcome, localReads.get(), remoteReads.get(), installRequests.get(), droppedMvResponses.get());
                    pauseForSnapshotProbe("after-failure-sample-" + sample);
                }
            } finally {
                node2.stopDroppingMessages();
            }
        } finally {
            releaseFault.set(true);
            node0.stopDroppingMessages();
        }

        pauseForSnapshotProbe("fault-released");
        assertThat(successfulInstall, willSucceedIn(2, TimeUnit.MINUTES));
        pauseForSnapshotProbe("recovered");
        assertEquals("two", getFromNode(2, 2));
        assertEquals("three", getFromNode(2, 3));
        LOG.info("HF4_ABORT_PROBE phase=recovered result=PASS");
    }

    private static void pauseForSnapshotProbe(String stage) throws InterruptedException {
        LOG.info("HF4_ABORT_PROBE phase={} pauseSeconds=5", stage);
        TimeUnit.SECONDS.sleep(5);
    }

    private String probeReadOnlyGetAtNode2(int key) {
        return probeReadOnlyGetAtNode2(key, 5_000);
    }

    private String probeReadOnlyGetAtNode2(int key, long timeoutMillis) {
        Transaction tx = cluster.node(2).transactions().begin(new TransactionOptions().readOnly(true).timeoutMillis(timeoutMillis));

        try {
            return String.valueOf(tableViewAt(2).get(tx, key));
        } catch (RuntimeException e) {
            return "ERROR:" + e.getClass().getSimpleName() + ":" + e.getMessage();
        } finally {
            tx.rollback();
        }
    }

    /** Verifies that MV rebalance remains incomplete until the sender's low-watermark response arrives. */
    @Test
    @EnabledIfEnvironmentVariable(named = "IGNITE_HF4_LWM_RESPONSE_PROBE", matches = "true")
    @Timeout(value = 6, unit = TimeUnit.MINUTES)
    void lowWatermarkResponseTimeoutProbe() throws Exception {
        createTestTableWith3Replicas(PersistentPageMemoryStorageEngine.ENGINE_NAME);
        pauseForSnapshotProbe("lwm-table-created");
        transferLeadershipOnSolePartitionTo(0);
        pauseForSnapshotProbe("lwm-leader-transferred");
        transferPrimaryOnSolePartitionTo(0);
        pauseForSnapshotProbe("lwm-primary-transferred");

        putToNode(0, 1, "one");
        pauseForSnapshotProbe("lwm-first-write");
        putToNode(0, 2, "two");
        pauseForSnapshotProbe("lwm-second-write");
        assertTrue(tableViewAt(0).remove(null, 1));
        pauseForSnapshotProbe("lwm-first-row-deleted");
        knockoutNode(2);
        pauseForSnapshotProbe("lwm-node2-stopped");
        putToNode(0, 3, "three");
        pauseForSnapshotProbe("lwm-third-write");
        causeLogTruncationOnSolePartitionLeader(0);
        pauseForSnapshotProbe("lwm-leader-log-truncated");

        CompletableFuture<Void> firstLowWatermarkResponse = new CompletableFuture<>();
        CompletableFuture<Void> successfulInstall = new CompletableFuture<>();
        AtomicInteger localReads = new AtomicInteger();
        AtomicInteger remoteReads = new AtomicInteger();
        AtomicInteger droppedResponses = new AtomicInteger();
        listenForSnapshotInstalledSuccessFromLogger(0, 2, successfulInstall);

        IgniteImpl node0 = unwrapIgniteImpl(cluster.node(0));
        String node2Name = cluster.nodeName(2);

        node0.dropMessages((recipient, message) -> {
            if (node2Name.equals(recipient) && message instanceof GetLowWatermarkResponse) {
                droppedResponses.incrementAndGet();
                firstLowWatermarkResponse.complete(null);
                return true;
            }

            return false;
        });

        try {
            reanimateNode(2);
            pauseForSnapshotProbe("lwm-node2-restarted");
            assertThat(firstLowWatermarkResponse, willSucceedIn(1, TimeUnit.MINUTES));

            IgniteImpl node2 = unwrapIgniteImpl(cluster.node(2));
            ReplicationGroupId groupId = cluster.solePartitionId(TEST_ZONE_NAME, TEST_TABLE_NAME);

            node2.dropMessages((recipient, message) -> {
                if (message instanceof ReadOnlySingleRowPkReplicaRequest || message instanceof ReadOnlyMultiRowPkReplicaRequest) {
                    (node2.name().equals(recipient) ? localReads : remoteReads).incrementAndGet();
                }

                return false;
            });

            try {
                assertTrue(snapshotInstallingOnNode2(groupId));
                assertFalse(node2.replicaManager().isLocalReplicaHealthyForRead(groupId));
                assertTrue(mvPartitionAtNode(2).lastAppliedIndex() < 0, "MV rebalance must remain incomplete");
                assertReadOnlyDataFromNode2();
                LOG.info("HF4_LWM_PROBE phase=waiting installing={} healthy={} lastApplied={} localReads={} remoteReads={} "
                                + "droppedResponses={}",
                        snapshotInstallingOnNode2(groupId), node2.replicaManager().isLocalReplicaHealthyForRead(groupId),
                        mvPartitionAtNode(2).lastAppliedIndex(), localReads.get(), remoteReads.get(), droppedResponses.get());
                pauseForSnapshotProbe("lwm-waiting");

                TimeUnit.SECONDS.sleep(5);
                assertFalse(successfulInstall.isDone(), "A missing low watermark must not produce snapshot success");
                node0.stopDroppingMessages();
                assertThat(successfulInstall, willSucceedIn(3, TimeUnit.MINUTES));
                pauseForSnapshotProbe("lwm-installed-after-retry");
                assertTrue(waitForCondition(() -> node2.replicaManager().isLocalReplicaHealthyForRead(groupId), 30_000));
                assertReadOnlyDataFromNode2();
                assertEquals(2, localReads.get(), "Recovered replica should serve local KV reads");
                assertEquals(2, remoteReads.get(), "Pending snapshot should route both KV reads remotely");
                LOG.info("HF4_LWM_PROBE phase=recovered result=PASS installing={} healthy={} lastApplied={} "
                                + "localReads={} remoteReads={} droppedResponses={}",
                        snapshotInstallingOnNode2(groupId), node2.replicaManager().isLocalReplicaHealthyForRead(groupId),
                        mvPartitionAtNode(2).lastAppliedIndex(), localReads.get(), remoteReads.get(), droppedResponses.get());
            } finally {
                node2.stopDroppingMessages();
            }
        } finally {
            node0.stopDroppingMessages();
        }
    }

    /** Verifies that missing low watermark aborts a snapshot and retry preserves the historical read boundary. */
    @Test
    @EnabledIfEnvironmentVariable(named = "IGNITE_HF4_STALE_LWM_PROBE", matches = "true")
    @Timeout(value = 6, unit = TimeUnit.MINUTES)
    void snapshotWithMissingLowWatermarkMustNotServeOldRead() throws Exception {
        createTestTableWith3Replicas(PersistentPageMemoryStorageEngine.ENGINE_NAME);
        transferLeadershipOnSolePartitionTo(0);
        transferPrimaryOnSolePartitionTo(0);

        putToNode(0, 1, "one");
        putToNode(0, 2, "two");
        IgniteImpl node0 = unwrapIgniteImpl(cluster.node(0));
        HybridTimestamp oldReadTimestamp = node0.clockService().now();
        assertEquals("one", readAtTimestamp(2, 1, oldReadTimestamp));

        knockoutNode(2);
        assertTrue(tableViewAt(0).remove(null, 1));
        assertFalse(IgniteTestUtils.bypassingThreadAssertions(
                () -> mvPartitionAtNode(0).peek(HybridTimestamp.MAX_VALUE, 1).isEmpty()));

        node0.lowWatermark().updateLowWatermark(node0.clockService().now());
        assertTrue(waitForCondition(() -> {
            HybridTimestamp watermark = node0.lowWatermark().getLowWatermark();
            return watermark != null && watermark.compareTo(oldReadTimestamp) > 0;
        }, 30_000));
        assertTrue(waitForCondition(() -> IgniteTestUtils.bypassingThreadAssertions(
                () -> mvPartitionAtNode(0).peek(HybridTimestamp.MAX_VALUE, 1).isEmpty()), 60_000),
                "The sender must actually GC the old row version before taking the snapshot");
        IgniteInternalException senderRejection = assertThrows(IgniteInternalException.class,
                () -> readAtTimestamp(0, 1, oldReadTimestamp));
        assertEquals(TX_READ_ONLY_TOO_OLD_ERR, senderRejection.code());
        causeLogTruncationOnSolePartitionLeader(0);

        CompletableFuture<Void> lowWatermarkResponseDropped = new CompletableFuture<>();
        CompletableFuture<Void> snapshotInstalled = new CompletableFuture<>();
        CompletableFuture<Void> lowWatermarkTimedOut = new CompletableFuture<>();
        CompletableFuture<Void> failedInstall = new CompletableFuture<>();
        AtomicInteger localReads = new AtomicInteger();
        AtomicInteger remoteReads = new AtomicInteger();
        copierLogInspector.addHandler(
                event -> event.getMessage().getFormattedMessage().startsWith("Snapshot request timed out; aborting rebalance"),
                () -> lowWatermarkTimedOut.complete(null)
        );
        replicatorLogInspector.addHandler(
                event -> lowWatermarkTimedOut.isDone()
                        && event.getMessage().getFormattedMessage().startsWith("Node 20_part_0:")
                        && event.getMessage().getFormattedMessage().contains(
                                " received InstallSnapshotResponse from " + cluster.nodeName(2) + " ")
                        && event.getMessage().getFormattedMessage().contains("error:"),
                () -> failedInstall.complete(null)
        );
        listenForSnapshotInstalledSuccessFromLogger(0, 2, snapshotInstalled);
        node0.dropMessages((recipient, message) -> {
            if (cluster.nodeName(2).equals(recipient) && message instanceof GetLowWatermarkResponse) {
                lowWatermarkResponseDropped.complete(null);
                return true;
            }

            return false;
        });

        try {
            reanimateNode(2);
            assertThat(lowWatermarkResponseDropped, willSucceedIn(1, TimeUnit.MINUTES));

            IgniteImpl node2 = unwrapIgniteImpl(cluster.node(2));
            ReplicationGroupId groupId = cluster.solePartitionId(TEST_ZONE_NAME, TEST_TABLE_NAME);
            InternalTransaction historicalTransaction = node2.txManager().beginExplicitRo(
                    node2.observableTimeTracker(), InternalTxOptions.builder().readTimestamp(oldReadTimestamp).build());
            node2.dropMessages((recipient, message) -> {
                if (message instanceof ReadOnlySingleRowPkReplicaRequest || message instanceof ReadOnlyMultiRowPkReplicaRequest) {
                    (node2.name().equals(recipient) ? localReads : remoteReads).incrementAndGet();
                }

                return false;
            });

            try {
                assertTrue(snapshotInstallingOnNode2(groupId));
                assertFalse(node2.replicaManager().isLocalReplicaHealthyForRead(groupId));
                assertReadOnlyDataFromNode2();
                assertEquals(0, localReads.get());
                assertEquals(2, remoteReads.get());
                assertThat(failedInstall, willSucceedIn(3, TimeUnit.MINUTES));
                assertFalse(snapshotInstalled.isDone(), "Missing low watermark must not produce snapshot success");
                assertTrue(waitForCondition(() -> mvPartitionAtNode(2).lastAppliedIndex() <= 0, 30_000),
                        "Failed snapshot must be aborted or already retrying, never expose completed storage");
                InternalTableImpl internalTable = (InternalTableImpl) unwrapTableImpl(tableAt(2)).internalTable();
                Predicate<ReplicationGroupId> localReadiness = getFieldValue(internalTable, "localReplicaReady");
                assertFalse(localReadiness.test(groupId), "Both Raft health and the MV storage guard must permit a local read");
                LOG.info("HF4_LWM_FIX phase=failed installing={} healthy=false lastApplied={} localReads={} remoteReads={}",
                        snapshotInstallingOnNode2(groupId), mvPartitionAtNode(2).lastAppliedIndex(), localReads.get(), remoteReads.get());

                CompletableFuture<Void> retryLowWatermarkResponse = new CompletableFuture<>();
                node0.dropMessages((recipient, message) -> {
                    if (cluster.nodeName(2).equals(recipient) && message instanceof GetLowWatermarkResponse) {
                        retryLowWatermarkResponse.complete(null);
                    }

                    return false;
                });
                assertThat(retryLowWatermarkResponse, willSucceedIn(1, TimeUnit.MINUTES));
                AtomicReference<?> updateCandidate = getFieldValue(node2.lowWatermark(), "lowWatermarkCandidate");
                assertTrue(waitForCondition(() -> {
                    HybridTimestamp candidateTimestamp = getFieldValue(updateCandidate.get(), "lowWatermark");
                    return candidateTimestamp.compareTo(oldReadTimestamp) > 0;
                }, 30_000));
                TimeUnit.SECONDS.sleep(2);
                assertFalse(snapshotInstalled.isDone(), "An active older RO transaction must delay low watermark completion");
                assertFalse(node2.replicaManager().isLocalReplicaHealthyForRead(groupId));
                assertReadOnlyDataFromNode2();
                assertEquals(0, localReads.get());
                assertEquals(4, remoteReads.get());
                LOG.info("HF4_LWM_FIX phase=update-waiting healthy=false localReads={} remoteReads={}",
                        localReads.get(), remoteReads.get());
                historicalTransaction.rollback();
                assertThat(snapshotInstalled, willSucceedIn(1, TimeUnit.MINUTES));
                assertTrue(waitForCondition(() -> node2.replicaManager().isLocalReplicaHealthyForRead(groupId), 30_000));
                HybridTimestamp node2Watermark = node2.lowWatermark().getLowWatermark();
                assertNotNull(node2Watermark);
                assertTrue(node2Watermark.compareTo(oldReadTimestamp) > 0);
                IgniteInternalException rejection = assertThrows(IgniteInternalException.class,
                        () -> readAtTimestamp(2, 1, oldReadTimestamp));
                assertEquals(TX_READ_ONLY_TOO_OLD_ERR, rejection.code());
                assertReadOnlyDataFromNode2();
                assertEquals(2, localReads.get());
                assertEquals(4, remoteReads.get());
                LOG.info("HF4_LWM_FIX phase=recovered healthy=true receiverWatermark={} readTimestamp={} "
                                + "oldReadRejected=true localReads={} remoteReads={}",
                        node2Watermark, oldReadTimestamp, localReads.get(), remoteReads.get());
            } finally {
                historicalTransaction.rollback();
                node2.stopDroppingMessages();
            }
        } finally {
            node0.stopDroppingMessages();
        }
    }

    private String readAtTimestamp(int nodeIndex, int key, HybridTimestamp timestamp) {
        IgniteImpl node = unwrapIgniteImpl(cluster.node(nodeIndex));
        InternalTransaction tx = node.txManager().beginExplicitRo(
                node.observableTimeTracker(), InternalTxOptions.builder().readTimestamp(timestamp).build());

        try {
            return tableViewAt(nodeIndex).get(tx, key);
        } finally {
            tx.rollback();
        }
    }

    /** Stops a real node while its snapshot copier is waiting for an unanswered low-watermark RPC. */
    @Test
    @EnabledIfEnvironmentVariable(named = "IGNITE_HF4_LWM_CANCEL_PROBE", matches = "true")
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    void lowWatermarkPendingCancellationProbe() throws Exception {
        createTestTableWith3Replicas(PersistentPageMemoryStorageEngine.ENGINE_NAME);
        pauseForSnapshotProbe("cancel-table-created");
        transferLeadershipOnSolePartitionTo(0);
        pauseForSnapshotProbe("cancel-leader-transferred");
        transferPrimaryOnSolePartitionTo(0);
        pauseForSnapshotProbe("cancel-primary-transferred");
        putToNode(0, 1, "one");
        pauseForSnapshotProbe("cancel-first-write");
        knockoutNode(2);
        pauseForSnapshotProbe("cancel-node2-stopped");
        putToNode(0, 2, "two");
        pauseForSnapshotProbe("cancel-second-write");
        causeLogTruncationOnSolePartitionLeader(0);
        pauseForSnapshotProbe("cancel-leader-log-truncated");

        CompletableFuture<Void> firstLowWatermarkResponse = new CompletableFuture<>();
        CompletableFuture<Void> copierCanceled = new CompletableFuture<>();
        CompletableFuture<Void> successfulInstall = new CompletableFuture<>();
        copierLogInspector.addHandler(
                event -> event.getMessage().getFormattedMessage().startsWith("Copier is canceled for partition"),
                () -> copierCanceled.complete(null)
        );
        listenForSnapshotInstalledSuccessFromLogger(0, 2, successfulInstall);

        IgniteImpl node0 = unwrapIgniteImpl(cluster.node(0));
        String node2Name = cluster.nodeName(2);
        node0.dropMessages((recipient, message) -> {
            if (node2Name.equals(recipient) && message instanceof GetLowWatermarkResponse) {
                firstLowWatermarkResponse.complete(null);
                return true;
            }

            return false;
        });

        try {
            reanimateNode(2);
            pauseForSnapshotProbe("cancel-node2-restarted");
            assertThat(firstLowWatermarkResponse, willSucceedIn(1, TimeUnit.MINUTES));
            assertFalse(successfulInstall.isDone(), "Snapshot must not report success while low watermark is pending");
            pauseForSnapshotProbe("cancel-lwm-pending");

            long startedAt = System.nanoTime();
            knockoutNode(2);
            long stopMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt);
            LOG.info("HF4_LWM_CANCEL_PROBE phase=stopped stopMillis={} copierCanceled={} successfulInstall={}",
                    stopMillis, copierCanceled.isDone(), successfulInstall.isDone());
            assertTrue(stopMillis < TimeUnit.MINUTES.toMillis(1), "Stopping the node must not wait for the 10-minute LWM timeout");
            assertThat(copierCanceled, willSucceedIn(10, TimeUnit.SECONDS));
            pauseForSnapshotProbe("cancel-status-settled");
            assertFalse(successfulInstall.isDone(), "A canceled snapshot must not be reported as successful");
            pauseForSnapshotProbe("cancel-fault-released");
        } finally {
            node0.stopDroppingMessages();
        }

        reanimateNode(2);
        pauseForSnapshotProbe("cancel-node2-restarted-again");
        ReplicationGroupId groupId = cluster.solePartitionId(TEST_ZONE_NAME, TEST_TABLE_NAME);
        IgniteImpl node2 = unwrapIgniteImpl(cluster.node(2));
        assertTrue(waitForCondition(() -> node2.replicaManager().isLocalReplicaHealthyForRead(groupId)
                && mvPartitionAtNode(2).lastAppliedIndex() > 0, 60_000));
        assertEquals("one", probeReadOnlyGetAtNode2(1));
        pauseForSnapshotProbe("cancel-recovered-first-read");
        assertEquals("two", probeReadOnlyGetAtNode2(2));
        LOG.info("HF4_LWM_CANCEL_PROBE phase=recovered result=PASS lastApplied={} healthy={}",
                mvPartitionAtNode(2).lastAppliedIndex(), node2.replicaManager().isLocalReplicaHealthyForRead(groupId));
    }

    /** Forces MV storage into REBALANCE after the local RO routing check has passed on a real cluster. */
    @Test
    @EnabledIfEnvironmentVariable(named = "IGNITE_HF4_READ_REBALANCE_RACE_PROBE", matches = "true")
    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    void readOnlyLocalRoutingRebalanceRaceProbe() throws Exception {
        createTestTableWith3Replicas(PersistentPageMemoryStorageEngine.ENGINE_NAME);
        pauseForSnapshotProbe("race-table-created");
        transferPrimaryOnSolePartitionTo(0);
        pauseForSnapshotProbe("race-primary-transferred");
        putToNode(0, 1, "one");
        pauseForSnapshotProbe("race-first-write");

        IgniteImpl node2 = unwrapIgniteImpl(cluster.node(2));
        ReplicationGroupId groupId = cluster.solePartitionId(TEST_ZONE_NAME, TEST_TABLE_NAME);
        assertTrue(waitForCondition(() -> node2.replicaManager().isLocalReplicaHealthyForRead(groupId), 30_000));
        assertEquals("one", probeReadOnlyGetAtNode2(1));
        pauseForSnapshotProbe("race-baseline-read");

        InternalTableImpl internalTable = (InternalTableImpl) unwrapTableImpl(tableAt(2)).internalTable();
        Predicate<ReplicationGroupId> originalReadiness = getFieldValue(internalTable, "localReplicaReady");
        CountDownLatch thirdReadinessCheck = new CountDownLatch(1);
        CountDownLatch releaseReadinessCheck = new CountDownLatch(1);
        AtomicInteger readinessChecks = new AtomicInteger();
        AtomicInteger localReads = new AtomicInteger();
        AtomicInteger remoteReads = new AtomicInteger();
        AtomicBoolean storageInRebalance = new AtomicBoolean();
        AbstractPageMemoryMvPartitionStorage mvStorage = ((Wrapper) mvPartitionAtNode(2))
                .unwrap(AbstractPageMemoryMvPartitionStorage.class);

        node2.dropMessages((recipient, message) -> {
            if (message instanceof ReadOnlySingleRowPkReplicaRequest) {
                (node2.name().equals(recipient) ? localReads : remoteReads).incrementAndGet();
            }

            return false;
        });

        IgniteTestUtils.setFieldValue(internalTable, "localReplicaReady", (Predicate<ReplicationGroupId>) requestedGroupId -> {
            if (readinessChecks.incrementAndGet() == 3) {
                thirdReadinessCheck.countDown();

                try {
                    if (!releaseReadinessCheck.await(30, TimeUnit.SECONDS)) {
                        throw new AssertionError("Timed out waiting to release local read routing check");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError("Interrupted while holding local read routing check", e);
                }
            }

            return originalReadiness.test(requestedGroupId);
        });

        try {
            CompletableFuture<String> read = CompletableFuture.supplyAsync(() -> probeReadOnlyGetAtNode2(1, 60_000));
            assertTrue(thirdReadinessCheck.await(30, TimeUnit.SECONDS));
            pauseForSnapshotProbe("race-local-check-blocked");
            mvStorage.startRebalance();
            storageInRebalance.set(true);
            pauseForSnapshotProbe("race-storage-in-rebalance");
            releaseReadinessCheck.countDown();

            String outcome = read.get(75, TimeUnit.SECONDS);
            LOG.info("HF4_REBALANCE_RACE_PROBE phase=read outcome={} localReads={} remoteReads={} readinessChecks={}",
                    outcome, localReads.get(), remoteReads.get(), readinessChecks.get());
            assertEquals("one", outcome);
            assertEquals(1, localReads.get());
            assertEquals(1, remoteReads.get(), "The storage transition must trigger one Primary fallback");
        } finally {
            releaseReadinessCheck.countDown();
            IgniteTestUtils.setFieldValue(internalTable, "localReplicaReady", originalReadiness);

            if (storageInRebalance.get()) {
                mvStorage.completeRebalance();
            }

            node2.stopDroppingMessages();
        }

        pauseForSnapshotProbe("race-storage-restored");
        assertEquals("one", probeReadOnlyGetAtNode2(1));
        LOG.info("HF4_REBALANCE_RACE_PROBE phase=recovered result=PASS");
    }

    private boolean snapshotInstallingOnNode2(ReplicationGroupId groupId) {
        JraftServerImpl server = (JraftServerImpl) unwrapIgniteImpl(cluster.node(2)).raftManager().server();

        return server.localNodes().stream()
                .filter(nodeId -> nodeId.groupId().equals(groupId))
                .map(server::raftGroupService)
                .filter(Objects::nonNull)
                .anyMatch(service -> service.getRaftNode().isInstallingSnapshot());
    }

    private void assertReadOnlyDataFromNode2() {
        Transaction tx = cluster.node(2).transactions().begin(new TransactionOptions().readOnly(true).timeoutMillis(20_000));

        try {
            assertEquals("two", tableViewAt(2).get(tx, 2));
            assertEquals(Map.of(2, "two"), tableViewAt(2).getAll(tx, Set.of(1, 2)));
        } finally {
            tx.rollback();
        }
    }

    /**
     * Tests that a leader successfully feeds a follower with a RAFT snapshot (using the given storage engine).
     */
    private void testLeaderFeedsFollowerWithSnapshot(String storageEngine) throws Exception {
        feedNode2WithSnapshotOfOneRow(storageEngine);

        transferLeadershipOnSolePartitionTo(2);

        assertThat(getFromNode(2, 1), is("one"));
        assertThat(estimatedSizeFromNode(2), is(1L));

        MvPartitionStorage partitionAtNode0 = mvPartitionAtNode(0);
        MvPartitionStorage partitionAtNode2 = mvPartitionAtNode(2);

        assertThat(partitionAtNode2.leaseInfo(), is(partitionAtNode0.leaseInfo()));
    }

    private @Nullable String getFromNode(int clusterNode, int key) {
        return tableViewAt(clusterNode).get(null, key);
    }

    private long estimatedSizeFromNode(int clusterNode) {
        return mvPartitionAtNode(clusterNode).estimatedSize();
    }

    private MvPartitionStorage mvPartitionAtNode(int clusterNode) {
        InternalTable internalTable = unwrapTableImpl(tableAt(clusterNode)).internalTable();
        MvPartitionStorage mvPartition = internalTable.storage().getMvPartition(PARTITION_ID);

        assertNotNull(mvPartition);
        return mvPartition;
    }

    private void feedNode2WithSnapshotOfOneRow() throws Exception {
        feedNode2WithSnapshotOfOneRow(DEFAULT_STORAGE_ENGINE);
    }

    private void feedNode2WithSnapshotOfOneRow(String storageEngine) throws Exception {
        prepareClusterForInstallingSnapshotToNode2(storageEngine);

        reanimateNode2AndWaitForSnapshotInstalled();
    }

    /**
     * Transfer the cluster to a state in which, when node 2 is reanimated from being knocked-out, the only partition
     * of the only table (called TEST) is transferred to it using RAFT snapshot installation mechanism.
     */
    private void prepareClusterForInstallingSnapshotToNode2() throws Exception {
        prepareClusterForInstallingSnapshotToNode2(DEFAULT_STORAGE_ENGINE);
    }

    /**
     * Transfer the cluster to a state in which, when node 2 is reanimated from being knocked-out, the only partition
     * of the only table (called TEST) is transferred to it using RAFT snapshot installation mechanism.
     *
     * @param storageEngine Storage engine for the TEST table.
     */
    private void prepareClusterForInstallingSnapshotToNode2(String storageEngine) throws Exception {
        prepareClusterForInstallingSnapshotToNode2(storageEngine, theCluster -> {});
    }

    /**
     * Transfer the cluster to a state in which, when node 2 is reanimated from being knocked-out, the only partition
     * of the only table (called TEST) is transferred to it using RAFT snapshot installation mechanism.
     *
     * @param storageEngine Storage engine for the TEST table.
     * @param doOnClusterAfterInit Action executed just after the cluster is started and initialized.
     */
    private void prepareClusterForInstallingSnapshotToNode2(
            String storageEngine,
            Consumer<Cluster> doOnClusterAfterInit
    ) throws Exception {
        doOnClusterAfterInit.accept(cluster);

        createTestTableWith3Replicas(storageEngine);

        // Prepare the scene: force node 0 to be a leader, and node 2 to be a follower.

        transferLeadershipOnSolePartitionTo(0);

        knockoutNode(2);

        putToNode(0, 1, "one");

        // Make sure AppendEntries from leader to follower is impossible, making the leader to use InstallSnapshot.
        causeLogTruncationOnSolePartitionLeader(0);
    }

    private void waitForPrimaryReplica() {
        IgniteImpl node = unwrapIgniteImpl(cluster.node(0));

        CompletableFuture<ReplicaMeta> primary = node.placementDriver().awaitPrimaryReplica(
                cluster.solePartitionId(TEST_ZONE_NAME, TEST_TABLE_NAME),
                node.clockService().now(),
                AWAIT_PRIMARY_REPLICA_SECONDS,
                TimeUnit.SECONDS);

        assertThat(primary, willCompleteSuccessfully());

        LOG.info("Lease is accepted by [nodeConsistentId={}].", primary.join().getLeaseholder());
    }

    private void putToNode(int nodeIndex, int key, String value) {
        putToNode(nodeIndex, key, value, null);
    }

    private void putToNode(int nodeIndex, int key, String value, @Nullable Transaction tx) {
        tableViewAt(nodeIndex).put(tx, key, value);
    }

    private KeyValueView<Integer, String> tableViewAt(int nodeIndex) {
        Table table = tableAt(nodeIndex);
        return table.keyValueView(Integer.class, String.class);
    }

    private Table tableAt(int nodeIndex) {
        return cluster.node(nodeIndex).tables().table("test");
    }

    private void knockoutNode(int nodeIndex) {
        cluster.stopNode(nodeIndex);

        LOG.info("Node {} knocked out", nodeIndex);
    }

    private void createTestTableWith3Replicas(String storageEngine) {
        String storageProfile =
                DEFAULT_STORAGE_ENGINE.equals(storageEngine) ? DEFAULT_STORAGE_PROFILE : "default_" + storageEngine.toLowerCase();

        String zoneSql = "create zone " + TEST_ZONE_NAME + " (partitions 1, replicas 3) storage profiles ['" + storageProfile + "'];";

        String sql = "create table " + TEST_TABLE_NAME + " (key int primary key, val varchar(20))"
                + " zone " + TEST_ZONE_NAME + " storage profile '" + storageProfile + "';";

        cluster.doInSession(0, session -> {
            executeUpdate(zoneSql, session);
            executeUpdate(sql, session);
        });

        waitForPrimaryReplica();
    }

    /**
     * Causes log truncation on the RAFT leader of the sole table partition that exists in the cluster.
     * After such truncation, when a knocked-out follower gets reanimated, the leader will not be able to feed it
     * with AppendEntries (because the leader does not already have the index that is required to send AppendEntries
     * to the lagging follower), so the leader will have to send InstallSnapshot instead.
     */
    private void causeLogTruncationOnSolePartitionLeader(int expectedLeaderNodeIndex) throws Exception {
        doSnapshotOnSolePartitionLeader(expectedLeaderNodeIndex, true);
    }

    /**
     * Causes a RAFT snapshot to be taken on the RAFT leader of the sole table partition that exists in the cluster.
     */
    private void doSnapshotOnSolePartitionLeader(int expectedLeaderNodeIndex, boolean forced) throws Exception {
        ReplicationGroupId replicationGroupId = cluster.solePartitionId(TEST_ZONE_NAME, TEST_TABLE_NAME);

        doSnapshotOn(replicationGroupId, expectedLeaderNodeIndex, forced);
    }

    /**
     * Takes a RAFT snapshot on the leader of the RAFT group corresponding to the given table partition.
     */
    private void doSnapshotOn(ReplicationGroupId replicationGroupId, int expectedLeaderNodeIndex, boolean forced) throws Exception {
        RaftGroupService raftGroupService = cluster.leaderServiceFor(replicationGroupId);

        assertThat(
                "Unexpected leadership change on group: " + replicationGroupId,
                raftGroupService.getServerId().getConsistentId(), is(cluster.node(expectedLeaderNodeIndex).name())
        );

        CompletableFuture<Status> fut = new CompletableFuture<>();
        raftGroupService.getRaftNode().snapshot(fut::complete, forced);

        assertThat(fut, willCompleteSuccessfully());
        assertEquals(RaftError.SUCCESS, fut.get().getRaftError());
    }

    private void truncateSolePartitionLogOn(int nodeIndex) throws Exception {
        ReplicationGroupId groupId = cluster.solePartitionId(TEST_ZONE_NAME, TEST_TABLE_NAME);
        JraftServerImpl server = (JraftServerImpl) unwrapIgniteImpl(cluster.node(nodeIndex)).raftManager().server();
        var nodeId = server.localNodes().stream().filter(id -> id.groupId().equals(groupId)).findAny().orElseThrow();
        RaftGroupService service = server.raftGroupService(nodeId);

        var snapshotResult = new CompletableFuture<Status>();
        service.getRaftNode().snapshot(snapshotResult::complete, true);

        assertThat(snapshotResult, willCompleteSuccessfully());
        assertEquals(RaftError.SUCCESS, snapshotResult.get().getRaftError());
    }

    /**
     * Reanimates (that is, reverts the effects of a knock out) node 2 and waits until a RAFT snapshot is installed
     * on it for the sole table partition in the cluster.
     */
    private void reanimateNode2AndWaitForSnapshotInstalled() throws InterruptedException {
        reanimateNodeAndWaitForSnapshotInstalled(2);
    }

    /**
     * Reanimates (that is, reverts the effects of a knock out) a node with the given index and waits until a RAFT snapshot is installed
     * on it for the sole table partition in the cluster.
     */
    private void reanimateNodeAndWaitForSnapshotInstalled(int nodeIndex) throws InterruptedException {
        CountDownLatch snapshotInstalledLatch = snapshotInstalledLatch(nodeIndex);

        reanimateNode(nodeIndex);

        assertTrue(snapshotInstalledLatch.await(60, TimeUnit.SECONDS), "Did not install a snapshot in time");
    }

    private CountDownLatch snapshotInstalledLatch(int nodeIndex) {
        CountDownLatch snapshotInstalledLatch = new CountDownLatch(1);

        replicatorLogInspector.addHandler(
                evt -> evt.getMessage().getFormattedMessage().matches(
                        "Node \\S+ received InstallSnapshotResponse from " + cluster.nodeName(nodeIndex) + " .+ success=true"),
                snapshotInstalledLatch::countDown
        );

        return snapshotInstalledLatch;
    }

    private void reanimateNode(int nodeIndex) {
        cluster.startNode(nodeIndex);
    }

    private void transferLeadershipOnSolePartitionTo(int nodeIndex) throws InterruptedException {
        cluster.transferLeadershipTo(nodeIndex, cluster.solePartitionId(TEST_ZONE_NAME, TEST_TABLE_NAME));
    }

    private void transferPrimaryOnSolePartitionTo(int nodeIndex) throws InterruptedException {
        cluster.transferPrimaryTo(nodeIndex, cluster.solePartitionId(TEST_ZONE_NAME, TEST_TABLE_NAME));
    }

    /**
     * Tests that, if first part of a transaction (everything before COMMIT) arrives using AppendEntries, and later the whole
     * partition state arrives in a RAFT snapshot, then the transaction is seen as committed (i.e. its effects are seen).
     */
    @Test
    void txSemanticsIsMaintained() throws Exception {
        txSemanticsIsMaintainedAfterInstallingSnapshot();
    }

    /**
     * Tests that, if first part of a transaction (everything before COMMIT) arrives using AppendEntries, and later the whole
     * partition state arrives in a RAFT snapshot, then the transaction is seen as committed (i.e. its effects are seen).
     */
    private void txSemanticsIsMaintainedAfterInstallingSnapshot() throws Exception {
        createTestTableWith3Replicas(DEFAULT_STORAGE_ENGINE);

        // Prepare the scene: first act - force node 0 to be a leader, and node 2 to be a follower.
        transferLeadershipOnSolePartitionTo(0);

        // Prepare the scene: second act - force node 0 to be a primary.
        transferPrimaryOnSolePartitionTo(0);

        Transaction tx = cluster.node(0).transactions().begin();

        putToNode(0, 1, "one", tx);

        knockoutNode(2);

        tx.commit();

        // Make sure AppendEntries from leader to follower is impossible, making the leader to use InstallSnapshot.
        causeLogTruncationOnSolePartitionLeader(0);

        reanimateNode2AndWaitForSnapshotInstalled();

        transferLeadershipOnSolePartitionTo(2);

        assertThat(getFromNode(2, 1), is("one"));
    }

    /**
     * Tests that entries can still be added to a follower using AppendEntries after it gets fed with a RAFT snapshot.
     */
    @Test
    void entriesKeepAppendedAfterSnapshotInstallation() throws Exception {
        feedNode2WithSnapshotOfOneRow();

        putToNode(0, 2, "two");

        transferLeadershipOnSolePartitionTo(2);

        assertThat(getFromNode(0, 1), is("one"));
        assertThat(getFromNode(0, 2), is("two"));
    }

    /**
     * Tests that, if commands are added to a leader while it installs a RAFT snapshot on a follower, these commands
     * reach the follower and get applied after the snapshot is installed.
     */
    @Test
    void entriesKeepAppendedDuringSnapshotInstallation() throws Exception {
        prepareClusterForInstallingSnapshotToNode2();

        AtomicBoolean installedSnapshot = new AtomicBoolean(false);
        AtomicInteger lastLoadedKey = new AtomicInteger();

        CompletableFuture<?> loadingFuture = IgniteTestUtils.runAsync(() -> {
            for (int key = 2; !installedSnapshot.get(); key++) {
                putToNode(0, key, "extra");
                lastLoadedKey.set(key);
            }
        });

        reanimateNode2AndWaitForSnapshotInstalled();

        installedSnapshot.set(true);

        assertThat(loadingFuture, willSucceedIn(30, TimeUnit.SECONDS));

        transferLeadershipOnSolePartitionTo(2);

        assertThat(getFromNode(2, 1), is("one"));

        List<Integer> expectedKeysAndNextKey = IntStream.rangeClosed(2, lastLoadedKey.get() + 1).boxed().collect(toList());
        Map<Integer, String> keysToValues = tableViewAt(2).getAll(null, expectedKeysAndNextKey);

        Set<Integer> expectedKeys = IntStream.rangeClosed(2, lastLoadedKey.get()).boxed().collect(toSet());
        assertThat(keysToValues.keySet(), equalTo(expectedKeys));
        assertThat(keysToValues.values(), everyItem(is("extra")));
    }

    /**
     * Tests that, after a node gets a RAFT snapshot installed to it, and it switches to a leader, it can act as a leader
     * (and can install a RAFT snapshot on the ex-leader).
     */
    @Test
    void nodeCanInstallSnapshotsAfterSnapshotInstalledToIt() throws Exception {
        feedNode2WithSnapshotOfOneRow();

        // The leader (0) has fed the follower (2). Now, change roles: the new leader will be node 2, it will feed node 0.

        transferLeadershipOnSolePartitionTo(2);

        knockoutNode(0);

        putToNode(2, 2, "two");

        // Make sure AppendEntries from leader to follower is impossible, making the leader to use InstallSnapshot.
        causeLogTruncationOnSolePartitionLeader(2);

        reanimateNodeAndWaitForSnapshotInstalled(0);

        transferLeadershipOnSolePartitionTo(0);

        assertThat(getFromNode(0, 1), is("one"));
        assertThat(getFromNode(0, 2), is("two"));
    }

    /**
     * Tests that, if a snapshot installation fails for some reason, a subsequent retry due to a timeout happens successfully.
     */
    @Test
    void snapshotInstallationRepeatsOnTimeout() throws Exception {
        prepareClusterForInstallingSnapshotToNode2(DEFAULT_STORAGE_ENGINE, theCluster -> {
            unwrapIgniteImpl(theCluster.node(0)).dropMessages(dropFirstSnapshotMetaResponse());
        });

        reanimateNode2AndWaitForSnapshotInstalled();
    }

    private BiPredicate<String, NetworkMessage> dropFirstSnapshotMetaResponse() {
        AtomicBoolean sentSnapshotMetaResponse = new AtomicBoolean(false);

        return dropFirstSnapshotMetaResponse(sentSnapshotMetaResponse);
    }

    private BiPredicate<String, NetworkMessage> dropFirstSnapshotMetaResponse(AtomicBoolean sentSnapshotMetaResponse) {
        String node2Name = cluster.node(2).name();

        return (targetConsistentId, message) -> {
            if (Objects.equals(targetConsistentId, node2Name) && message instanceof SnapshotMetaResponse) {
                return sentSnapshotMetaResponse.compareAndSet(false, true);
            } else {
                return false;
            }
        };
    }

    private BiPredicate<String, NetworkMessage> dropSnapshotMetaResponse(CompletableFuture<Void> sentFirstSnapshotMetaResponse) {
        String node2Name = cluster.nodeName(2);

        return (targetConsistentId, message) -> {
            if (Objects.equals(targetConsistentId, node2Name) && message instanceof SnapshotMetaResponse) {
                sentFirstSnapshotMetaResponse.complete(null);

                // Always drop.
                return true;
            } else {
                return false;
            }
        };
    }

    private BiPredicate<String, NetworkMessage> dropSnapshotMvDataResponse(
            int targetNodeIndex,
            CompletableFuture<Void> sentFirstResponse
    ) {
        String targetNodeName = cluster.node(targetNodeIndex).name();

        return (targetConsistentId, message) -> {
            if (Objects.equals(targetConsistentId, targetNodeName) && message instanceof SnapshotMvDataResponse) {
                sentFirstResponse.complete(null);
                return true;
            }

            return false;
        };
    }

    /**
     * This is a test for a tricky scenario:
     *
     * <ol>
     *     <li>First InstallSnapshot request is sent, its processing starts hanging forever (it will be cancelled on step 3</li>
     *     <li>After a timeout, second InstallSnapshot request is sent with same index+term as the first had; in JRaft, it causes
     *     a special handling (previous request processing is NOT cancelled)</li>
     *     <li>After a timeout, third InstallSnapshot request is sent with DIFFERENT index, so it cancels the first snapshot processing
     *     effectively unblocking the first thread</li>
     * </ol>
     *
     * <p>In the original JRaft implementation, after being unblocked, the first thread fails to clean up, so subsequent retries will
     * always see a phantom of an unfinished snapshot, so the snapshotting process will be jammed. Also, node stop might
     * stuck because one 'download' task will remain unfinished forever.
     */
    @Test
    void snapshotInstallTimeoutDoesNotBreakSubsequentInstallsWhenSecondAttemptIsIdenticalToFirst() throws Exception {
        AtomicBoolean snapshotInstallFailedDueToIdenticalRetry = new AtomicBoolean(false);

        LogInspector snapshotExecutorLogInspector = LogInspector.create(SnapshotExecutorImpl.class);

        Handler snapshotInstallFailedDueToIdenticalRetryHandler =
                snapshotExecutorLogInspector.addHandler(
                        evt -> evt.getMessage().getFormattedMessage().contains(
                                "Register DownloadingSnapshot failed: interrupted by retry installing request"),
                        () -> snapshotInstallFailedDueToIdenticalRetry.set(true));

        snapshotExecutorLogInspector.start();

        try {
            prepareClusterForInstallingSnapshotToNode2(DEFAULT_STORAGE_ENGINE, theCluster -> {
                IgniteImpl node = unwrapIgniteImpl(theCluster.node(0));
                MessageSerializationRegistry serializationRegistry = node.raftManager().service().serializationRegistry();

                BiPredicate<String, NetworkMessage> dropSafeTimeUntilSecondInstallSnapshotRequestIsProcessed = (recipientId, message) ->
                        message instanceof WriteActionRequest
                                && isSafeTimeSyncCommand((WriteActionRequest) message, serializationRegistry)
                                && !snapshotInstallFailedDueToIdenticalRetry.get();

                unwrapIgniteImpl(theCluster.node(0)).dropMessages(
                        dropFirstSnapshotMetaResponse().or(dropSafeTimeUntilSecondInstallSnapshotRequestIsProcessed)
                );

                unwrapIgniteImpl(theCluster.node(1)).dropMessages(dropSafeTimeUntilSecondInstallSnapshotRequestIsProcessed);
                unwrapIgniteImpl(theCluster.node(2)).dropMessages(dropSafeTimeUntilSecondInstallSnapshotRequestIsProcessed);
            });

            reanimateNode2AndWaitForSnapshotInstalled();
        } finally {
            snapshotExecutorLogInspector.removeHandler(snapshotInstallFailedDueToIdenticalRetryHandler);
            snapshotExecutorLogInspector.stop();
        }
    }

    private static boolean isSafeTimeSyncCommand(WriteActionRequest request, MessageSerializationRegistry serializationRegistry) {
        String groupId = request.groupId();

        if (groupId.equals(MetastorageGroupId.INSTANCE.toString()) || groupId.equals(CmgGroupId.INSTANCE.toString())) {
            return false;
        }

        var commandsMarshaller = new PartitionCommandsMarshallerImpl(serializationRegistry, NO_POOL);
        return commandsMarshaller.unmarshall(request.command()) instanceof SafeTimeSyncCommand;
    }

    @Test
    void testChangeLeaderOnInstallSnapshotInMiddle() throws Exception {
        CompletableFuture<Void> sentSnapshotMetaResponseFormNode1Future = new CompletableFuture<>();

        prepareClusterForInstallingSnapshotToNode2(DEFAULT_STORAGE_ENGINE, cluster -> {
            // Let's hang the InstallSnapshot in the "middle" from the leader with index 1.
            unwrapIgniteImpl(cluster.node(1)).dropMessages(dropSnapshotMetaResponse(sentSnapshotMetaResponseFormNode1Future));
        });

        // Change the leader and truncate its log so that InstallSnapshot occurs instead of AppendEntries.
        transferLeadershipOnSolePartitionTo(1);

        causeLogTruncationOnSolePartitionLeader(1);

        CompletableFuture<Void> installSnapshotSuccessfulFuture = new CompletableFuture<>();

        listenForSnapshotInstalledSuccessFromLogger(0, 2, installSnapshotSuccessfulFuture);

        // Return node 2.
        reanimateNode(2);

        // Waiting for the InstallSnapshot from node 2 to hang in the "middle".
        assertThat(sentSnapshotMetaResponseFormNode1Future, willSucceedIn(1, TimeUnit.MINUTES));

        // Change the leader to node 0.
        transferLeadershipOnSolePartitionTo(0);

        // Waiting for the InstallSnapshot successfully from node 0 to node 2.
        assertThat(installSnapshotSuccessfulFuture, willSucceedIn(1, TimeUnit.MINUTES));

        // Make sure the rebalancing is complete.
        assertThat(getFromNode(2, 1), is("one"));
    }

    @Test
    @Timeout(180)
    void testRestartAfterTruncatedLogAndInterruptedSnapshotInstall() throws Exception {
        createTestTableWith3Replicas(DEFAULT_STORAGE_ENGINE);
        transferLeadershipOnSolePartitionTo(0);

        putToNode(0, 1, "one");
        putToNode(0, 2, "two");

        truncateSolePartitionLogOn(0);
        truncateSolePartitionLogOn(1);
        truncateSolePartitionLogOn(2);

        var firstMvResponse = new CompletableFuture<Void>();
        unwrapIgniteImpl(cluster.node(0)).dropMessages(dropSnapshotMvDataResponse(2, firstMvResponse));

        knockoutNode(2);

        putToNode(0, 3, "three");
        putToNode(0, 4, "four");
        truncateSolePartitionLogOn(0);

        reanimateNode(2);
        assertThat(firstMvResponse, willSucceedIn(1, TimeUnit.MINUTES));

        knockoutNode(2);
        unwrapIgniteImpl(cluster.node(0)).stopDroppingMessages();
        reanimateNode(2);

        assertThat(getFromNode(2, 1), is("one"));
        assertThat(getFromNode(2, 2), is("two"));
        assertThat(getFromNode(2, 3), is("three"));
        assertThat(getFromNode(2, 4), is("four"));
    }

    /**
     * Adds a listener for the {@link #replicatorLogInspector} to hear the success of the snapshot installation.
     */
    private void listenForSnapshotInstalledSuccessFromLogger(
            int nodeIndexFrom,
            int nodeIndexTo,
            CompletableFuture<Void> snapshotInstallSuccessfullyFuture
    ) {
        String regexp = String.format(
                "Node \\S+%s received InstallSnapshotResponse from %s .+ success=true",
                cluster.nodeName(nodeIndexFrom),
                cluster.nodeName(nodeIndexTo)
        );

        replicatorLogInspector.addHandler(
                evt -> evt.getMessage().getFormattedMessage().matches(regexp),
                () -> snapshotInstallSuccessfullyFuture.complete(null)
        );
    }

    /**
     * This tests the following schenario.
     *
     * <ol>
     *     <li>
     *         A snapshot installation is started from Node A that is a leader because A does not have enough RAFT log to feed a follower
     *         with AppendEntries
     *     </li>
     *     <li>It is cancelled in the middle</li>
     *     <li>Node B is elected as a leader; B has enough log to feed the follower with AppendEntries</li>
     *     <li>The follower gets data from the leader using AppendEntries, not using InstallSnapshot</li>>
     * </ol>
     */
    @Test
    void testChangeLeaderDuringSnapshotInstallationToLeaderWithEnoughLog() throws Exception {
        CompletableFuture<Void> sentSnapshotMetaResponseFormNode0Future = new CompletableFuture<>();

        prepareClusterForInstallingSnapshotToNode2(DEFAULT_STORAGE_ENGINE, cluster -> {
            // Let's hang the InstallSnapshot in the "middle" from the leader with index 0.
            unwrapIgniteImpl(cluster.node(0)).dropMessages(dropSnapshotMetaResponse(sentSnapshotMetaResponseFormNode0Future));
        });

        CompletableFuture<Void> installSnapshotSuccessfulFuture = new CompletableFuture<>();

        listenForSnapshotInstalledSuccessFromLogger(1, 2, installSnapshotSuccessfulFuture);

        // Return node 2.
        reanimateNode(2);

        // Waiting for the InstallSnapshot from node 2 to hang in the "middle".
        assertThat(sentSnapshotMetaResponseFormNode0Future, willSucceedIn(1, TimeUnit.MINUTES));

        // Change the leader to node 1.
        transferLeadershipOnSolePartitionTo(1);

        boolean replicated = waitForCondition(() -> getFromNode(2, 1) != null, 20_000);

        assertTrue(replicated, "Data has not been replicated to node 2 in time");

        // No snapshot must be installed.
        assertFalse(installSnapshotSuccessfulFuture.isDone());

        // Make sure the rebalancing is complete.
        assertThat(getFromNode(2, 1), is("one"));
    }

    /**
     * The replication mechanism must not replicate commands for which schemas are not yet available on the node
     * to which replication happens (in Raft, it means that followers/learners cannot receive commands that they
     * cannot execute without waiting for schemas). This method tests that snapshots bringing such commands are
     * rejected, and that, when metadata catches up, the snapshot gets successfully installed.
     */
    @Test
    void laggingSchemasOnFollowerPreventSnapshotInstallation() throws Exception {
        createTestTableWith3Replicas(DEFAULT_STORAGE_ENGINE);

        // Prepare the scene: force node 0 to be a leader, and node 2 to be a follower.
        final int leaderIndex = 0;
        final int followerIndex = 2;

        transferLeadershipOnSolePartitionTo(leaderIndex);
        transferPrimaryOnSolePartitionTo(leaderIndex);
        cluster.transferLeadershipTo(leaderIndex, MetastorageGroupId.INSTANCE);

        // Block AppendEntries from being accepted on the follower so that the leader will have to use a snapshot.
        blockIncomingAppendEntriesAt(followerIndex);

        // Inhibit the MetaStorage on the follower to make snapshots not eligible for installation.
        WatchListenerInhibitor listenerInhibitor = inhibitMetastorageListenersAt(followerIndex);

        try {
            // Add some data in a schema that is not yet available on the follower
            updateTableSchemaAt(leaderIndex);
            putToTableAt(leaderIndex);

            CountDownLatch installationRejected = installationRejectedLatch();
            CountDownLatch snapshotInstalled = snapshotInstalledLatch(followerIndex);

            // Force InstallSnapshot to be used.
            causeLogTruncationOnSolePartitionLeader(leaderIndex);

            assertTrue(installationRejected.await(20, TimeUnit.SECONDS), "Did not see snapshot installation rejection");

            assertThat("Snapshot was installed before unblocking", snapshotInstalled.getCount(), is(not(0L)));

            listenerInhibitor.stopInhibit();

            assertTrue(snapshotInstalled.await(20, TimeUnit.SECONDS), "Did not see a snapshot installed");
        } finally {
            listenerInhibitor.stopInhibit();
        }
    }

    private void updateTableSchemaAt(int nodeIndex) {
        cluster.doInSession(nodeIndex, session -> {
            session.execute(null, "alter table test add column added int");
        });
    }

    private void putToTableAt(int nodeIndex) {
        KeyValueView<Tuple, Tuple> kvView = cluster.node(nodeIndex)
                .tables()
                .table("test")
                .keyValueView();
        kvView.put(null, Tuple.create().set("key", 1), Tuple.create().set("val", "one"));
    }

    private void blockIncomingAppendEntriesAt(int nodeIndex) {
        BlockingAppendEntriesRequestProcessor blockingProcessorOnFollower = installBlockingAppendEntriesProcessor(nodeIndex);

        blockingProcessorOnFollower.startBlocking();
    }

    private WatchListenerInhibitor inhibitMetastorageListenersAt(int nodeIndex) {
        Ignite nodeToInhibitMetaStorage = cluster.node(nodeIndex);

        WatchListenerInhibitor listenerInhibitor = WatchListenerInhibitor.metastorageEventsInhibitor(nodeToInhibitMetaStorage);
        listenerInhibitor.startInhibit();

        return listenerInhibitor;
    }

    private CountDownLatch installationRejectedLatch() {
        CountDownLatch installationRejected = new CountDownLatch(1);

        copierLogInspector.addHandler(
                event -> event.getMessage().getFormattedMessage().startsWith("Metadata not yet available, rejecting snapshot installation"),
                installationRejected::countDown
        );

        return installationRejected;
    }

    private BlockingAppendEntriesRequestProcessor installBlockingAppendEntriesProcessor(int nodeIndex) {
        RaftServer raftServer = unwrapIgniteImpl(cluster.node(nodeIndex)).raftManager().server();
        RpcServer<?> rpcServer = getFieldValue(raftServer, JraftServerImpl.class, "rpcServer");
        Map<String, RpcProcessor<?>> processors = getFieldValue(rpcServer, IgniteRpcServer.class, "processors");

        AppendEntriesRequestProcessor originalProcessor =
                (AppendEntriesRequestProcessor) processors.get(AppendEntriesRequest.class.getName());
        Executor appenderExecutor = getFieldValue(originalProcessor, RpcRequestProcessor.class, "executor");
        RaftMessagesFactory raftMessagesFactory = getFieldValue(originalProcessor, RpcRequestProcessor.class, "msgFactory");

        BlockingAppendEntriesRequestProcessor blockingProcessor = new BlockingAppendEntriesRequestProcessor(
                appenderExecutor,
                raftMessagesFactory,
                cluster.solePartitionId(TEST_ZONE_NAME, TEST_TABLE_NAME).toString()
        );

        rpcServer.registerProcessor(blockingProcessor);

        return blockingProcessor;
    }

    /**
     * {@link AppendEntriesRequestProcessor} that, when blocking is enabled, blocks all AppendEntriesRequests of
     * the given group (that is, returns EBUSY error code, which makes JRaft repeat them).
     */
    private static class BlockingAppendEntriesRequestProcessor extends AppendEntriesRequestProcessor {
        private final String idOfGroupToBlock;
        private volatile boolean block;

        public BlockingAppendEntriesRequestProcessor(Executor executor,
                RaftMessagesFactory msgFactory, String idOfGroupToBlock) {
            super(executor, msgFactory);

            this.idOfGroupToBlock = idOfGroupToBlock;
        }

        @Override
        public Message processRequest0(RaftServerService service, AppendEntriesRequest request, RpcRequestClosure done) {
            if (block && idOfGroupToBlock.equals(request.groupId())) {
                return RaftRpcFactory.DEFAULT //
                    .newResponse(done.getMsgFactory(), RaftError.EBUSY,
                            "Blocking AppendEntries on '%s'.", request.groupId());
            }

            return super.processRequest0(service, request, done);
        }

        public void startBlocking() {
            block = true;
        }
    }
}
