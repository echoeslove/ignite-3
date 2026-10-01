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

package org.apache.ignite.internal.table.distributed.storage;

import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.lang.IgniteSystemProperties.COLOCATION_FEATURE_FLAG;
import static org.apache.ignite.internal.lang.IgniteSystemProperties.colocationEnabled;
import static org.apache.ignite.internal.table.distributed.storage.InternalTableImpl.collectMultiRowsResponsesWithRestoreOrder;
import static org.apache.ignite.internal.table.distributed.storage.InternalTableImpl.collectRejectedRowsResponses;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.CompletableFutures.emptyListCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.trueCompletedFuture;
import static org.apache.ignite.lang.ErrorGroups.Replicator.REPLICA_TIMEOUT_ERR;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_ALREADY_FINISHED_ERR;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.hlc.TestClockService;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.SingleClusterNodeResolver;
import org.apache.ignite.internal.partition.replicator.network.replication.MultipleRowPkReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.MultipleRowReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadOnlyReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.RequestType;
import org.apache.ignite.internal.partition.replicator.network.replication.ScanRetrieveBatchReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.SingleRowPkReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.SingleRowReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.SwapRowReplicaRequest;
import org.apache.ignite.internal.partitiondistribution.Assignment;
import org.apache.ignite.internal.partitiondistribution.TokenizedAssignments;
import org.apache.ignite.internal.partitiondistribution.TokenizedAssignmentsImpl;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.placementdriver.leases.Lease;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.exception.ReplicaUnavailableException;
import org.apache.ignite.internal.replicator.message.ReadOnlyDirectReplicaRequest;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowEx;
import org.apache.ignite.internal.schema.NullBinaryRow;
import org.apache.ignite.internal.sql.SqlCommon;
import org.apache.ignite.internal.storage.StorageRebalanceException;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.StreamerReceiverRunner;
import org.apache.ignite.internal.table.metrics.TableMetricSource;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.SystemPropertiesExtension;
import org.apache.ignite.internal.testframework.WithSystemProperty;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.PendingTxPartitionEnlistment;
import org.apache.ignite.internal.tx.TransactionIds;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.impl.ReadWriteTransactionImpl;
import org.apache.ignite.internal.tx.impl.TransactionInflights;
import org.apache.ignite.internal.tx.storage.state.TxStateStorage;
import org.apache.ignite.internal.tx.test.TestTransactionIds;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.table.QualifiedName;
import org.apache.ignite.table.QualifiedNameHelper;
import org.apache.ignite.tx.TransactionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * For {@link InternalTableImpl} testing.
 */
@ExtendWith(MockitoExtension.class)
@ExtendWith(SystemPropertiesExtension.class)
public class InternalTableImplTest extends BaseIgniteAbstractTest {
    /** Zone identifier. */
    private static final int ZONE_ID = 1;

    /** Table identifier. */
    private static final int TABLE_ID = 2;

    @Mock
    private PlacementDriver placementDriver;

    @Mock
    private TxManager txManager;

    @Mock
    private ReplicaService replicaService;

    private final HybridClock clock = new HybridClockImpl();

    private final ClockService clockService = new TestClockService(clock);

    private final InternalClusterNode clusterNode = new ClusterNodeImpl(new UUID(1, 1), "node1", new NetworkAddress("host", 3000));

    @BeforeEach
    void setupMocks() {
        lenient().when(placementDriver.awaitPrimaryReplica(any(), any(), anyLong(), any()))
                .then(invocation -> {
                    ReplicationGroupId groupId = invocation.getArgument(0);

                    return completedFuture(
                            new Lease(clusterNode.name(), clusterNode.id(), HybridTimestamp.MIN_VALUE, HybridTimestamp.MAX_VALUE, groupId)
                    );
                });

        lenient().when(txManager.finish(any(), any(), anyBoolean(), anyBoolean(), anyBoolean(), any(), any()))
                .thenReturn(nullCompletedFuture());

        lenient().when(replicaService.invoke(anyString(), any())).then(invocation -> {
            ReplicaRequest request = invocation.getArgument(1);

            if (request instanceof MultipleRowReplicaRequest
                    && ((MultipleRowReplicaRequest) request).requestType() == RequestType.RW_UPSERT_ALL) {
                return nullCompletedFuture();
            }

            if (request instanceof MultipleRowPkReplicaRequest) {
                int rowCount = ((MultipleRowPkReplicaRequest) request).primaryKeys().size();
                return completedFuture(Collections.nCopies(rowCount, null));
            }
            if (request instanceof MultipleRowReplicaRequest) {
                int rowCount = ((MultipleRowReplicaRequest) request).binaryRows().size();
                return completedFuture(Collections.nCopies(rowCount, null));
            }

            if (isSingleRowReplicaRequest(request, RequestType.RW_INSERT)
                    || isSingleRowReplicaRequest(request, RequestType.RW_REPLACE_IF_EXIST)
                    || isSingleRowReplicaRequest(request, RequestType.RW_DELETE_EXACT)) {
                return trueCompletedFuture();
            }
            if (isSingleRowPkReplicaRequest(request, RequestType.RW_DELETE)) {
                return trueCompletedFuture();
            }
            if (request instanceof SwapRowReplicaRequest) {
                return trueCompletedFuture();
            }

            if (request instanceof ScanRetrieveBatchReplicaRequest) {
                return emptyListCompletedFuture();
            }

            return nullCompletedFuture();
        });
    }

    private static boolean isSingleRowReplicaRequest(ReplicaRequest request, RequestType requestType) {
        return request instanceof SingleRowReplicaRequest && ((SingleRowReplicaRequest) request).requestType() == requestType;
    }

    private static boolean isSingleRowPkReplicaRequest(ReplicaRequest request, RequestType requestType) {
        return request instanceof SingleRowPkReplicaRequest && ((SingleRowPkReplicaRequest) request).requestType() == requestType;
    }

    @Test
    void testUpdatePartitionTrackers() {
        InternalTableImpl internalTable = newInternalTable(TABLE_ID, 1);

        // Let's check the empty table.
        assertNull(internalTable.getPartitionSafeTimeTracker(0));
        assertNull(internalTable.getPartitionStorageIndexTracker(0));

        // Let's check the first insert.
        PendingComparableValuesTracker<HybridTimestamp, Void> safeTime0 = mock(PendingComparableValuesTracker.class);
        PendingComparableValuesTracker<Long, Void> storageIndex0 = mock(PendingComparableValuesTracker.class);

        internalTable.updatePartitionTrackers(0, safeTime0, storageIndex0);

        assertSame(safeTime0, internalTable.getPartitionSafeTimeTracker(0));
        assertSame(storageIndex0, internalTable.getPartitionStorageIndexTracker(0));

        verify(safeTime0, never()).close();
        verify(storageIndex0, never()).close();

        // Let's check the new insert.
        PendingComparableValuesTracker<HybridTimestamp, Void> safeTime1 = mock(PendingComparableValuesTracker.class);
        PendingComparableValuesTracker<Long, Void> storageIndex1 = mock(PendingComparableValuesTracker.class);

        internalTable.updatePartitionTrackers(0, safeTime1, storageIndex1);

        assertSame(safeTime1, internalTable.getPartitionSafeTimeTracker(0));
        assertSame(storageIndex1, internalTable.getPartitionStorageIndexTracker(0));

        verify(safeTime0).close();
        verify(storageIndex0).close();
    }

    @Test
    void readOnlyRecipientUsesLocalStablePeerByDefault() {
        InternalTableImpl internalTable = new InternalTableImpl(
                QualifiedNameHelper.fromNormalized(SqlCommon.DEFAULT_SCHEMA_NAME, "test"),
                ZONE_ID,
                TABLE_ID,
                1,
                new SingleClusterNodeResolver(clusterNode),
                txManager,
                mock(MvTableStorage.class),
                mock(TxStateStorage.class),
                replicaService,
                clockService,
                HybridTimestampTracker.atomicTracker(null),
                placementDriver,
                new TransactionInflights(placementDriver, clockService),
                () -> mock(ScheduledExecutorService.class),
                mock(StreamerReceiverRunner.class),
                () -> 10_000L,
                () -> 10_000L,
                colocationEnabled(),
                new TableMetricSource(QualifiedName.fromSimple("test")),
                () -> clusterNode,
                groupId -> true
        );

        when(placementDriver.getAssignments(any(ReplicationGroupId.class), any(HybridTimestamp.class))).thenReturn(completedFuture(
                new TokenizedAssignmentsImpl(Set.of(Assignment.forPeer(clusterNode.name())), 1)
        ));

        assertThat(internalTable.evaluateReadOnlyRecipientNode(0, clock.now()), willBe(equalTo(clusterNode)));

        verify(placementDriver, never()).awaitPrimaryReplica(any(), any(), anyLong(), any());
    }

    @Test
    void readOnlyRecipientFallsBackToPrimaryWhenLocalReplicaIsNotReady() {
        InternalTableImpl internalTable = new InternalTableImpl(
                QualifiedNameHelper.fromNormalized(SqlCommon.DEFAULT_SCHEMA_NAME, "test"),
                ZONE_ID,
                TABLE_ID,
                1,
                new SingleClusterNodeResolver(clusterNode),
                txManager,
                mock(MvTableStorage.class),
                mock(TxStateStorage.class),
                replicaService,
                clockService,
                HybridTimestampTracker.atomicTracker(null),
                placementDriver,
                new TransactionInflights(placementDriver, clockService),
                () -> mock(ScheduledExecutorService.class),
                mock(StreamerReceiverRunner.class),
                () -> 10_000L,
                () -> 10_000L,
                colocationEnabled(),
                new TableMetricSource(QualifiedName.fromSimple("test")),
                () -> clusterNode,
                groupId -> false
        );

        assertThat(internalTable.evaluateReadOnlyRecipientNode(0, clock.now()), willBe(equalTo(clusterNode)));

        verify(placementDriver).awaitPrimaryReplica(any(), any(), anyLong(), any());
    }

    @Test
    void readOnlyGetAllFallsBackToPrimaryWhenLocalReplicaDisappears() {
        InternalTableImpl internalTable = new InternalTableImpl(
                QualifiedNameHelper.fromNormalized(SqlCommon.DEFAULT_SCHEMA_NAME, "test"),
                ZONE_ID,
                TABLE_ID,
                1,
                new SingleClusterNodeResolver(clusterNode),
                txManager,
                mock(MvTableStorage.class),
                mock(TxStateStorage.class),
                replicaService,
                clockService,
                HybridTimestampTracker.atomicTracker(null),
                placementDriver,
                new TransactionInflights(placementDriver, clockService),
                () -> mock(ScheduledExecutorService.class),
                mock(StreamerReceiverRunner.class),
                () -> 10_000L,
                () -> 10_000L,
                colocationEnabled(),
                new TableMetricSource(QualifiedName.fromSimple("test")),
                () -> clusterNode,
                groupId -> true
        );

        when(placementDriver.getAssignments(any(ReplicationGroupId.class), any(HybridTimestamp.class))).thenReturn(completedFuture(
                new TokenizedAssignmentsImpl(Set.of(Assignment.forPeer(clusterNode.name())), 1)
        ));
        when(replicaService.invokeWithoutReplicaWait(eq(clusterNode), any())).thenReturn(failedFuture(
                new ReplicaUnavailableException(new ZonePartitionId(ZONE_ID, 0), clusterNode)
        ));
        when(replicaService.invoke(eq(clusterNode), any())).thenReturn(completedFuture(Collections.singletonList(null)));

        assertThat(
                internalTable.getAll(List.of(createBinaryRow(0)), clock.now(), randomUUID(), randomUUID(), null),
                willCompleteSuccessfully()
        );

        verify(replicaService).invokeWithoutReplicaWait(eq(clusterNode), any());
        verify(replicaService).invoke(eq(clusterNode), any());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void stoppedReplicaIsNotReadAfterAssignmentsWait(boolean multiRow) {
        InternalClusterNode local = new ClusterNodeImpl(randomUUID(), "backup", new NetworkAddress("host", 3001));
        AtomicBoolean ready = new AtomicBoolean(true);
        InternalTableImpl table = new InternalTableImpl(
                QualifiedName.fromSimple("test"), ZONE_ID, TABLE_ID, 1,
                new SingleClusterNodeResolver(clusterNode), txManager,
                mock(MvTableStorage.class), mock(TxStateStorage.class), replicaService,
                clockService, HybridTimestampTracker.atomicTracker(null), placementDriver,
                new TransactionInflights(placementDriver, clockService),
                () -> mock(ScheduledExecutorService.class), mock(StreamerReceiverRunner.class),
                () -> 10_000L, () -> 10_000L, colocationEnabled(),
                new TableMetricSource(QualifiedName.fromSimple("test")), () -> local, groupId -> ready.get()
        );
        CompletableFuture<TokenizedAssignments> assignments = new CompletableFuture<>();
        when(placementDriver.getAssignments(any(ReplicationGroupId.class), any(HybridTimestamp.class))).thenReturn(assignments);
        when(replicaService.invoke(eq(clusterNode), any())).thenReturn(completedFuture(
                multiRow ? Collections.singletonList(null) : null
        ));
        InternalTransaction tx = explicitReadOnlyTransaction();

        CompletableFuture<?> read = multiRow ? table.getAll(List.of(createBinaryRow(0)), tx) : table.get(createBinaryRow(0), tx);
        ready.set(false);
        assignments.complete(new TokenizedAssignmentsImpl(Set.of(Assignment.forPeer(local.name())), 1));

        assertThat(read, willCompleteSuccessfully());
        verify(replicaService, never()).invokeWithoutReplicaWait(eq(local), any());
        verify(replicaService).invoke(eq(clusterNode), any());
        verify(replicaService, never()).invoke(eq(local), any());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void unhealthyReplicaIsNotReadAfterRecipientSelection(boolean multiRow) {
        InternalClusterNode local = new ClusterNodeImpl(randomUUID(), "backup", new NetworkAddress("host", 3001));
        AtomicInteger checks = new AtomicInteger();
        InternalTableImpl table = localReadTable(local, groupId -> checks.incrementAndGet() < 3);
        when(placementDriver.getAssignments(any(ReplicationGroupId.class), any(HybridTimestamp.class))).thenReturn(completedFuture(
                new TokenizedAssignmentsImpl(Set.of(Assignment.forPeer(local.name())), 1)
        ));
        when(replicaService.invoke(eq(clusterNode), any())).thenReturn(completedFuture(
                multiRow ? Collections.singletonList(null) : null
        ));

        InternalTransaction tx = explicitReadOnlyTransaction();

        assertThat(multiRow ? table.getAll(List.of(createBinaryRow(0)), tx) : table.get(createBinaryRow(0), tx),
                willCompleteSuccessfully());
        assertEquals(3, checks.get());
        verify(replicaService, never()).invokeWithoutReplicaWait(eq(local), any());
        verify(replicaService).invoke(eq(clusterNode), any());
    }

    @ParameterizedTest
    @CsvSource({"false,peer", "true,peer", "false,absent", "true,absent", "false,learner", "true,learner",
            "false,empty", "true,empty", "false,error", "true,error", "false,notReady", "true,notReady",
            "false,nullLocal", "true,nullLocal"})
    void explicitReadOnlyRoutesByLocalEligibility(boolean multiRow, String scenario) {
        InternalClusterNode local = new ClusterNodeImpl(randomUUID(), "backup", new NetworkAddress("host", 3001));
        InternalTableImpl table = localReadTable(scenario.equals("nullLocal") ? null : local,
                !scenario.equals("notReady"));
        Set<Assignment> peers = scenario.equals("learner") ? Set.of(Assignment.forLearner(local.name()))
                : scenario.equals("absent") ? Set.of(Assignment.forPeer(clusterNode.name()))
                : Set.of(Assignment.forPeer(local.name()));
        lenient().when(placementDriver.getAssignments(any(ReplicationGroupId.class), any(HybridTimestamp.class)))
                .thenReturn(scenario.equals("error") ? failedFuture(new IllegalStateException("assignments unavailable"))
                        : completedFuture(scenario.equals("empty") ? null : new TokenizedAssignmentsImpl(peers, 1)));
        Object response = multiRow ? Collections.singletonList(null) : null;
        boolean localExpected = scenario.equals("peer");
        if (localExpected) {
            when(replicaService.invokeWithoutReplicaWait(eq(local), any())).thenReturn(completedFuture(response));
        } else {
            when(replicaService.invoke(eq(clusterNode), any())).thenReturn(completedFuture(response));
        }
        InternalTransaction tx = explicitReadOnlyTransaction();
        assertThat(multiRow ? table.getAll(List.of(createBinaryRow(0)), tx) : table.get(createBinaryRow(0), tx),
                willCompleteSuccessfully());
        ArgumentCaptor<ReplicaRequest> sent = ArgumentCaptor.forClass(ReplicaRequest.class);
        if (localExpected) {
            verify(replicaService).invokeWithoutReplicaWait(eq(local), sent.capture());
            verify(replicaService, never()).invoke(any(InternalClusterNode.class), any());
            verify(placementDriver, never()).awaitPrimaryReplica(any(), any(), anyLong(), any());
        } else {
            verify(replicaService).invoke(eq(clusterNode), sent.capture());
            verify(replicaService, never()).invokeWithoutReplicaWait(any(), any());
        }
        assertReadIdentity(tx, sent.getValue());
    }

    @ParameterizedTest
    @CsvSource({"false,unavailable", "true,unavailable", "false,wrapped", "true,wrapped",
            "false,rebalance", "true,rebalance", "false,wrappedRebalance", "true,wrappedRebalance",
            "false,businessError", "true,businessError", "false,missing", "true,missing"})
    void explicitReadOnlyFallbackPreservesSnapshotAndOnlyRetriesUnavailable(boolean multiRow, String scenario) {
        InternalClusterNode local = new ClusterNodeImpl(randomUUID(), "backup", new NetworkAddress("host", 3001));
        InternalTableImpl table = localReadTable(local, true);
        when(placementDriver.getAssignments(any(ReplicationGroupId.class), any(HybridTimestamp.class))).thenReturn(completedFuture(
                new TokenizedAssignmentsImpl(Set.of(Assignment.forPeer(local.name())), 1)));
        Object response = multiRow ? Collections.singletonList(null) : null;
        Throwable failure = scenario.equals("businessError") ? new IllegalArgumentException("bad data")
                : scenario.equals("rebalance") || scenario.equals("wrappedRebalance")
                        ? new StorageRebalanceException("Storage in the process of rebalancing")
                        : new ReplicaUnavailableException(new ZonePartitionId(ZONE_ID, 0), local);
        if (scenario.equals("wrapped") || scenario.equals("wrappedRebalance")) {
            failure = new CompletionException(failure);
        }
        when(replicaService.invokeWithoutReplicaWait(eq(local), any()))
                .thenReturn(scenario.equals("missing") ? completedFuture(response) : failedFuture(failure));
        boolean fallback = scenario.equals("unavailable") || scenario.equals("wrapped")
                || scenario.equals("rebalance") || scenario.equals("wrappedRebalance");
        if (fallback) {
            when(replicaService.invoke(eq(clusterNode), any())).thenReturn(completedFuture(response));
        }
        InternalTransaction tx = explicitReadOnlyTransaction();
        CompletableFuture<?> result = multiRow ? table.getAll(List.of(createBinaryRow(0)), tx) : table.get(createBinaryRow(0), tx);
        if (scenario.equals("businessError")) {
            assertEquals(IllegalArgumentException.class, assertThrows(CompletionException.class, result::join).getCause().getClass());
        } else {
            assertThat(result, willCompleteSuccessfully());
        }
        ArgumentCaptor<ReplicaRequest> localRequest = ArgumentCaptor.forClass(ReplicaRequest.class);
        verify(replicaService).invokeWithoutReplicaWait(eq(local), localRequest.capture());
        assertReadIdentity(tx, localRequest.getValue());
        if (fallback) {
            ArgumentCaptor<ReplicaRequest> primaryRequest = ArgumentCaptor.forClass(ReplicaRequest.class);
            verify(replicaService).invoke(eq(clusterNode), primaryRequest.capture());
            assertReadIdentity(tx, primaryRequest.getValue());
        } else {
            verify(replicaService, never()).invoke(any(InternalClusterNode.class), any());
            verify(placementDriver, never()).awaitPrimaryReplica(any(), any(), anyLong(), any());
        }
    }

    private InternalTransaction explicitReadOnlyTransaction() {
        InternalTransaction tx = mock(InternalTransaction.class);
        lenient().when(tx.isReadOnly()).thenReturn(true);
        lenient().when(tx.readTimestamp()).thenReturn(clock.now());
        lenient().when(tx.id()).thenReturn(TransactionIds.transactionId(clock.now(), 1));
        lenient().when(tx.coordinatorId()).thenReturn(randomUUID());
        lenient().when(tx.getTimeout()).thenReturn(30_000L);
        return tx;
    }

    @ParameterizedTest
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    void assignmentsWaitIsBoundedAndLateCompletionDoesNotRead(boolean multiRow, boolean lateFailure) {
        InternalTableImpl table = localReadTable(clusterNode, true);
        CompletableFuture<TokenizedAssignments> assignments = new CompletableFuture<>();
        when(placementDriver.getAssignments(any(ReplicationGroupId.class), any(HybridTimestamp.class))).thenReturn(assignments);
        InternalTransaction tx = explicitReadOnlyTransaction();
        when(tx.getTimeout()).thenReturn(200L);

        CompletableFuture<?> read = multiRow ? table.getAll(List.of(createBinaryRow(0)), tx) : table.get(createBinaryRow(0), tx);

        ExecutionException failure = assertThrows(ExecutionException.class, () -> read.get(5, SECONDS));
        assertEquals(REPLICA_TIMEOUT_ERR, assertInstanceOf(TransactionException.class, failure.getCause()).code());
        assertFalse(assignments.isDone(), "A timeout must not complete the underlying assignments future");
        if (lateFailure) {
            assignments.completeExceptionally(new IllegalStateException("late assignments failure"));
        } else {
            assignments.complete(new TokenizedAssignmentsImpl(Set.of(Assignment.forPeer(clusterNode.name())), 1));
        }
        verify(replicaService, never()).invokeWithoutReplicaWait(any(), any());
        verify(replicaService, never()).invoke(any(InternalClusterNode.class), any());
        verify(placementDriver, never()).awaitPrimaryReplica(any(), any(), anyLong(), any());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void assignmentsTimeoutDoesNotAffectAnotherReadSharingTheFuture(boolean multiRow) {
        InternalTableImpl table = localReadTable(clusterNode, true);
        CompletableFuture<TokenizedAssignments> assignments = new CompletableFuture<>();
        when(placementDriver.getAssignments(any(ReplicationGroupId.class), any(HybridTimestamp.class))).thenReturn(assignments);
        when(replicaService.invokeWithoutReplicaWait(eq(clusterNode), any()))
                .thenReturn(completedFuture(multiRow ? Collections.singletonList(null) : null));
        InternalTransaction shortTx = explicitReadOnlyTransaction();
        when(shortTx.getTimeout()).thenReturn(200L);
        InternalTransaction longTx = explicitReadOnlyTransaction();

        CompletableFuture<?> shortRead = multiRow
                ? table.getAll(List.of(createBinaryRow(0)), shortTx) : table.get(createBinaryRow(0), shortTx);
        CompletableFuture<?> longRead = multiRow
                ? table.getAll(List.of(createBinaryRow(0)), longTx) : table.get(createBinaryRow(0), longTx);

        assertThrows(ExecutionException.class, () -> shortRead.get(5, SECONDS));
        assertFalse(assignments.isDone());
        assertFalse(longRead.isDone());
        assignments.complete(new TokenizedAssignmentsImpl(Set.of(Assignment.forPeer(clusterNode.name())), 1));
        assertThat(longRead, willCompleteSuccessfully());
        verify(replicaService).invokeWithoutReplicaWait(eq(clusterNode), any());
        verify(placementDriver, never()).awaitPrimaryReplica(any(), any(), anyLong(), any());
    }

    @ParameterizedTest
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    void finishedTransactionDoesNotSendAfterAssignmentsWait(boolean multiRow, boolean primaryRecipient) {
        InternalTableImpl table = localReadTable(clusterNode, true);
        CompletableFuture<TokenizedAssignments> assignments = new CompletableFuture<>();
        when(placementDriver.getAssignments(any(ReplicationGroupId.class), any(HybridTimestamp.class))).thenReturn(assignments);
        InternalTransaction tx = explicitReadOnlyTransaction();
        AtomicBoolean finished = new AtomicBoolean();
        when(tx.isFinishingOrFinished()).thenAnswer(invocation -> finished.get());
        CompletableFuture<?> read = multiRow ? table.getAll(List.of(createBinaryRow(0)), tx) : table.get(createBinaryRow(0), tx);

        finished.set(true);
        assignments.complete(new TokenizedAssignmentsImpl(primaryRecipient ? Set.of() : Set.of(Assignment.forPeer(clusterNode.name())), 1));

        ExecutionException failure = assertThrows(ExecutionException.class, () -> read.get(5, SECONDS));
        assertEquals(TX_ALREADY_FINISHED_ERR, assertInstanceOf(TransactionException.class, failure.getCause()).code());
        verify(replicaService, never()).invokeWithoutReplicaWait(any(), any());
        verify(replicaService, never()).invoke(any(InternalClusterNode.class), any());
        verify(placementDriver, never()).awaitPrimaryReplica(any(), any(), anyLong(), any());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void assignmentsWaitUsesRemainingTransactionTime(boolean multiRow) {
        InternalTableImpl table = localReadTable(clusterNode, true);
        CompletableFuture<TokenizedAssignments> assignments = new CompletableFuture<>();
        when(placementDriver.getAssignments(any(ReplicationGroupId.class), any(HybridTimestamp.class))).thenReturn(assignments);
        InternalTransaction tx = explicitReadOnlyTransaction();
        when(tx.id()).thenReturn(TransactionIds.transactionId(
                new HybridTimestamp(clock.now().getPhysical() - 10_000, 0), 1));
        when(tx.getTimeout()).thenReturn(10_200L);

        CompletableFuture<?> read = multiRow ? table.getAll(List.of(createBinaryRow(0)), tx) : table.get(createBinaryRow(0), tx);

        ExecutionException failure = assertThrows(ExecutionException.class, () -> read.get(5, SECONDS));
        assertEquals(REPLICA_TIMEOUT_ERR, assertInstanceOf(TransactionException.class, failure.getCause()).code());
        assertFalse(assignments.isDone());
        verify(placementDriver, never()).awaitPrimaryReplica(any(), any(), anyLong(), any());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void expiredTransactionDoesNotStartReplicaRouting(boolean multiRow) {
        InternalTableImpl table = localReadTable(clusterNode, true);
        InternalTransaction tx = explicitReadOnlyTransaction();
        when(tx.id()).thenReturn(TransactionIds.transactionId(new HybridTimestamp(clock.now().getPhysical() - 1_000, 0), 1));
        when(tx.getTimeout()).thenReturn(100L);

        CompletableFuture<?> read = multiRow ? table.getAll(List.of(createBinaryRow(0)), tx) : table.get(createBinaryRow(0), tx);

        ExecutionException failure = assertThrows(ExecutionException.class, () -> read.get(5, SECONDS));
        assertEquals(REPLICA_TIMEOUT_ERR, assertInstanceOf(TransactionException.class, failure.getCause()).code());
        verify(placementDriver, never()).getAssignments(any(ReplicationGroupId.class), any(HybridTimestamp.class));
        verify(placementDriver, never()).awaitPrimaryReplica(any(), any(), anyLong(), any());
        verify(replicaService, never()).invokeWithoutReplicaWait(any(), any());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void finishedTransactionDoesNotStartPrimaryFallback(boolean multiRow) {
        InternalTableImpl table = localReadTable(clusterNode, true);
        when(placementDriver.getAssignments(any(ReplicationGroupId.class), any(HybridTimestamp.class))).thenReturn(completedFuture(
                new TokenizedAssignmentsImpl(Set.of(Assignment.forPeer(clusterNode.name())), 1)));
        CompletableFuture<Object> localResponse = new CompletableFuture<>();
        when(replicaService.invokeWithoutReplicaWait(eq(clusterNode), any())).thenReturn(localResponse);
        InternalTransaction tx = explicitReadOnlyTransaction();
        AtomicBoolean finished = new AtomicBoolean();
        when(tx.isFinishingOrFinished()).thenAnswer(invocation -> finished.get());
        CompletableFuture<?> read = multiRow ? table.getAll(List.of(createBinaryRow(0)), tx) : table.get(createBinaryRow(0), tx);

        finished.set(true);
        localResponse.completeExceptionally(new ReplicaUnavailableException(new ZonePartitionId(ZONE_ID, 0), clusterNode));

        ExecutionException failure = assertThrows(ExecutionException.class, () -> read.get(5, SECONDS));
        assertEquals(TX_ALREADY_FINISHED_ERR, assertInstanceOf(TransactionException.class, failure.getCause()).code());
        verify(placementDriver, never()).awaitPrimaryReplica(any(), any(), anyLong(), any());
        verify(replicaService, never()).invoke(any(InternalClusterNode.class), any());
    }

    @ParameterizedTest
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    void finishedTransactionDoesNotSendAfterPrimaryLookup(boolean multiRow, boolean fallback) {
        InternalTableImpl table = localReadTable(clusterNode, fallback);
        if (fallback) {
            when(placementDriver.getAssignments(any(ReplicationGroupId.class), any(HybridTimestamp.class))).thenReturn(completedFuture(
                    new TokenizedAssignmentsImpl(Set.of(Assignment.forPeer(clusterNode.name())), 1)));
            when(replicaService.invokeWithoutReplicaWait(eq(clusterNode), any())).thenReturn(failedFuture(
                    new ReplicaUnavailableException(new ZonePartitionId(ZONE_ID, 0), clusterNode)));
        }
        CompletableFuture<ReplicaMeta> primary = new CompletableFuture<>();
        when(placementDriver.awaitPrimaryReplica(any(), any(), anyLong(), any())).thenReturn(primary);
        InternalTransaction tx = explicitReadOnlyTransaction();
        AtomicBoolean finished = new AtomicBoolean();
        when(tx.isFinishingOrFinished()).thenAnswer(invocation -> finished.get());
        CompletableFuture<?> read = multiRow ? table.getAll(List.of(createBinaryRow(0)), tx) : table.get(createBinaryRow(0), tx);

        finished.set(true);
        primary.complete(new Lease(clusterNode.name(), clusterNode.id(), HybridTimestamp.MIN_VALUE,
                HybridTimestamp.MAX_VALUE, new ZonePartitionId(ZONE_ID, 0)));

        ExecutionException failure = assertThrows(ExecutionException.class, () -> read.get(5, SECONDS));
        assertEquals(TX_ALREADY_FINISHED_ERR, assertInstanceOf(TransactionException.class, failure.getCause()).code());
        verify(replicaService, never()).invoke(any(InternalClusterNode.class), any());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void assignmentsSourceFailureStillUsesPrimary(boolean multiRow) {
        InternalTableImpl table = localReadTable(clusterNode, true);
        when(placementDriver.getAssignments(any(ReplicationGroupId.class), any(HybridTimestamp.class)))
                .thenReturn(failedFuture(new TimeoutException("source lookup failure, not the private wait timer")));
        when(replicaService.invoke(eq(clusterNode), any()))
                .thenReturn(completedFuture(multiRow ? Collections.singletonList(null) : null));
        InternalTransaction tx = explicitReadOnlyTransaction();

        assertThat(multiRow ? table.getAll(List.of(createBinaryRow(0)), tx) : table.get(createBinaryRow(0), tx),
                willCompleteSuccessfully());
        verify(replicaService).invoke(eq(clusterNode), any());
        verify(replicaService, never()).invokeWithoutReplicaWait(any(), any());
    }

    @ParameterizedTest
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    void readOnlyRoutingUsesBeginTimeAndSaturatesDeadline(boolean multiRow, boolean overflow) {
        InternalTableImpl table = localReadTable(clusterNode, true);
        when(placementDriver.getAssignments(any(ReplicationGroupId.class), any(HybridTimestamp.class))).thenReturn(completedFuture(
                new TokenizedAssignmentsImpl(Set.of(Assignment.forPeer(clusterNode.name())), 1)));
        when(replicaService.invokeWithoutReplicaWait(eq(clusterNode), any()))
                .thenReturn(completedFuture(multiRow ? Collections.singletonList(null) : null));
        InternalTransaction tx = explicitReadOnlyTransaction();
        when(tx.readTimestamp()).thenReturn(HybridTimestamp.MIN_VALUE);
        if (overflow) {
            when(tx.getTimeout()).thenReturn(Long.MAX_VALUE);
        }

        assertThat(multiRow ? table.getAll(List.of(createBinaryRow(0)), tx) : table.get(createBinaryRow(0), tx),
                willCompleteSuccessfully());
        ArgumentCaptor<ReplicaRequest> request = ArgumentCaptor.forClass(ReplicaRequest.class);
        verify(replicaService).invokeWithoutReplicaWait(eq(clusterNode), request.capture());
        assertReadIdentity(tx, request.getValue());
    }

    @Test
    void timestampOnlyAssignmentsWaitUsesThirtySecondLimit() {
        InternalTableImpl table = localReadTable(clusterNode, true);
        CompletableFuture<TokenizedAssignments> assignments = new CompletableFuture<>();
        when(placementDriver.getAssignments(any(ReplicationGroupId.class), any(HybridTimestamp.class))).thenReturn(assignments);

        CompletableFuture<?> read = table.evaluateReadOnlyRecipientNode(0, clock.now());

        ExecutionException failure = assertThrows(ExecutionException.class, () -> read.get(35, SECONDS));
        assertEquals(REPLICA_TIMEOUT_ERR, assertInstanceOf(TransactionException.class, failure.getCause()).code());
        assertFalse(assignments.isDone());
        assignments.complete(new TokenizedAssignmentsImpl(Set.of(Assignment.forPeer(clusterNode.name())), 1));
        verify(placementDriver, never()).awaitPrimaryReplica(any(), any(), anyLong(), any());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void explicitlySelectedRecipientIsNotOverriddenByLocalDefault(boolean multiRow) {
        InternalClusterNode local = new ClusterNodeImpl(randomUUID(), "backup", new NetworkAddress("host", 3001));
        InternalTableImpl table = localReadTable(local, true);
        HybridTimestamp timestamp = clock.now();
        UUID transactionId = randomUUID();
        UUID coordinatorId = randomUUID();
        when(replicaService.invoke(eq(clusterNode), any())).thenReturn(completedFuture(
                multiRow ? Collections.singletonList(null) : null));

        assertThat(multiRow
                ? table.getAll(List.of(createBinaryRow(0)), timestamp, transactionId, coordinatorId, clusterNode)
                : table.get(createBinaryRow(0), timestamp, transactionId, coordinatorId, clusterNode), willCompleteSuccessfully());

        ArgumentCaptor<ReplicaRequest> request = ArgumentCaptor.forClass(ReplicaRequest.class);
        verify(replicaService).invoke(eq(clusterNode), request.capture());
        ReadOnlyReplicaRequest read = (ReadOnlyReplicaRequest) request.getValue();
        assertEquals(timestamp, read.readTimestamp());
        assertEquals(transactionId, read.transactionId());
        verify(replicaService, never()).invokeWithoutReplicaWait(any(), any());
        verify(placementDriver, never()).getAssignments(any(ReplicationGroupId.class), any(HybridTimestamp.class));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void implicitReadStillUsesDirectPrimaryRequest(boolean multiRow) {
        InternalClusterNode local = new ClusterNodeImpl(randomUUID(), "backup", new NetworkAddress("host", 3001));
        InternalTableImpl table = localReadTable(local, true);
        InternalTransaction tx = mock(InternalTransaction.class);
        when(txManager.beginImplicitRo(any())).thenReturn(tx);
        when(tx.schemaTimestamp()).thenReturn(clock.now());
        when(tx.finish(anyBoolean(), any(HybridTimestamp.class), anyBoolean(), anyBoolean())).thenReturn(nullCompletedFuture());
        when(replicaService.invoke(eq(clusterNode), any())).thenReturn(completedFuture(
                multiRow ? Collections.singletonList(null) : null));

        assertThat(multiRow ? table.getAll(List.of(createBinaryRow(0)), null) : table.get(createBinaryRow(0), null),
                willCompleteSuccessfully());

        ArgumentCaptor<ReplicaRequest> request = ArgumentCaptor.forClass(ReplicaRequest.class);
        verify(replicaService).invoke(eq(clusterNode), request.capture());
        assertInstanceOf(ReadOnlyDirectReplicaRequest.class, request.getValue());
        verify(replicaService, never()).invokeWithoutReplicaWait(any(), any());
        verify(placementDriver, never()).getAssignments(any(ReplicationGroupId.class), any(HybridTimestamp.class));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void readWriteReadDoesNotEnterLocalReadOnlyRouting(boolean multiRow) {
        InternalClusterNode local = new ClusterNodeImpl(randomUUID(), "backup", new NetworkAddress("host", 3001));
        InternalTableImpl table = localReadTable(local, true);
        InternalTransaction tx = newReadWriteTransaction();
        try {
            assertThat(multiRow ? table.getAll(List.of(createBinaryRow(0)), tx) : table.get(createBinaryRow(0), tx),
                    willCompleteSuccessfully());
            verify(replicaService).invoke(eq(clusterNode.name()), any());
            verify(replicaService, never()).invokeWithoutReplicaWait(any(), any());
            verify(placementDriver, never()).getAssignments(any(ReplicationGroupId.class), any(HybridTimestamp.class));
        } finally {
            tx.rollback();
        }
    }

    private static void assertReadIdentity(InternalTransaction tx, ReplicaRequest request) {
        ReadOnlyReplicaRequest read = (ReadOnlyReplicaRequest) request;
        assertEquals(tx.id(), read.transactionId());
        assertEquals(tx.coordinatorId(), read.coordinatorId());
        assertEquals(tx.readTimestamp(), read.readTimestamp());
    }

    private InternalTableImpl localReadTable(InternalClusterNode local, boolean ready) {
        return localReadTable(local, groupId -> ready);
    }

    private InternalTableImpl localReadTable(InternalClusterNode local, Predicate<ReplicationGroupId> ready) {
        return new InternalTableImpl(
                QualifiedName.fromSimple("test"), ZONE_ID, TABLE_ID, 1,
                new SingleClusterNodeResolver(clusterNode), txManager,
                mock(MvTableStorage.class), mock(TxStateStorage.class), replicaService,
                clockService, HybridTimestampTracker.atomicTracker(null), placementDriver,
                new TransactionInflights(placementDriver, clockService),
                () -> mock(ScheduledExecutorService.class), mock(StreamerReceiverRunner.class),
                () -> 10_000L, () -> 10_000L, colocationEnabled(),
                new TableMetricSource(QualifiedName.fromSimple("test")), () -> local, ready
        );
    }

    private InternalTableImpl newInternalTable(int tableId, int partitionCount) {
        // number of partitions.
        return new InternalTableImpl(
                QualifiedNameHelper.fromNormalized(SqlCommon.DEFAULT_SCHEMA_NAME, "test"),
                ZONE_ID,
                tableId,
                partitionCount, // number of partitions.
                new SingleClusterNodeResolver(clusterNode),
                txManager,
                mock(MvTableStorage.class),
                mock(TxStateStorage.class),
                replicaService,
                mock(ClockService.class),
                HybridTimestampTracker.atomicTracker(null),
                placementDriver,
                new TransactionInflights(placementDriver, clockService),
                () -> mock(ScheduledExecutorService.class),
                mock(StreamerReceiverRunner.class),
                () -> 10_000L,
                () -> 10_000L,
                colocationEnabled(),
                new TableMetricSource(QualifiedName.fromSimple("test"))
        );
    }

    @Test
    void testRowBatchByPartitionId() {
        InternalTableImpl internalTable = newInternalTable(TABLE_ID, 3);

        List<BinaryRowEx> originalRows = List.of(
                // Rows for 0 partition.
                createBinaryRow(0),
                createBinaryRow(0),
                // Rows for 1 partition.
                createBinaryRow(1),
                // Rows for 2 partition.
                createBinaryRow(2),
                createBinaryRow(2),
                createBinaryRow(2)
        );

        // We will get batches for processing and check them.
        Int2ObjectMap<RowBatch> rowBatchByPartitionId = internalTable.toRowBatchByPartitionId(originalRows);

        assertThat(rowBatchByPartitionId.get(0).requestedRows, hasSize(2));
        assertThat(rowBatchByPartitionId.get(0).originalRowOrder, contains(0, 1));

        assertThat(rowBatchByPartitionId.get(1).requestedRows, hasSize(1));
        assertThat(rowBatchByPartitionId.get(1).originalRowOrder, contains(2));

        assertThat(rowBatchByPartitionId.get(2).requestedRows, hasSize(3));
        assertThat(rowBatchByPartitionId.get(2).originalRowOrder, contains(3, 4, 5));

        // Collect the result and check it.
        rowBatchByPartitionId.get(0).resultFuture = completedFuture(List.of(originalRows.get(0), originalRows.get(1)));
        rowBatchByPartitionId.get(1).resultFuture = completedFuture(List.of(originalRows.get(2)));
        rowBatchByPartitionId.get(2).resultFuture = completedFuture(List.of(originalRows.get(3), originalRows.get(4), originalRows.get(5)));

        assertThat(
                collectMultiRowsResponsesWithRestoreOrder(rowBatchByPartitionId.values()),
                willBe(equalTo(originalRows))
        );

        var part1 = new ArrayList<>(2);

        part1.add(null);
        part1.add(new NullBinaryRow());

        rowBatchByPartitionId.get(0).resultFuture = completedFuture(part1);

        var part2 = new ArrayList<>(2);

        part2.add(null);

        rowBatchByPartitionId.get(1).resultFuture = completedFuture(part2);

        var part3 = new ArrayList<>(2);

        part3.add(new NullBinaryRow());
        part3.add(null);
        part3.add(new NullBinaryRow());

        rowBatchByPartitionId.get(2).resultFuture = completedFuture(part3);

        List<BinaryRowEx> rejectedRows = List.of(
                // Rows for 0 partition.
                originalRows.get(0),
                // Rows for 1 partition.
                originalRows.get(2),
                // Rows for 2 partition.
                originalRows.get(4)
        );

        List<RowBatch> partitionOrderBatch = new ArrayList<>();
        partitionOrderBatch.add(rowBatchByPartitionId.get(0));
        partitionOrderBatch.add(rowBatchByPartitionId.get(1));
        partitionOrderBatch.add(rowBatchByPartitionId.get(2));

        assertThat(
                collectRejectedRowsResponses(partitionOrderBatch),
                willBe(equalTo(rejectedRows))
        );
    }

    private static BinaryRowEx createBinaryRow(int colocationHash) {
        BinaryRowEx rowEx = mock(BinaryRowEx.class);

        lenient().when(rowEx.colocationHash()).thenReturn(colocationHash);

        byte[] data = new byte[0];
        lenient().when(rowEx.tupleSlice()).thenReturn(ByteBuffer.wrap(data));
        lenient().when(rowEx.tupleSliceLength()).thenReturn(data.length);

        return rowEx;
    }

    private static BinaryRowEx createBinaryRow() {
        return createBinaryRow(0);
    }

    @ParameterizedTest
    @EnumSource(EnlistingOperation.class)
    @WithSystemProperty(key = COLOCATION_FEATURE_FLAG, value = "true")
    void tableIdGetsEnlisted(EnlistingOperation operation) {
        InternalTable table = newInternalTable(10, 1);
        InternalTransaction transaction = newReadWriteTransaction();

        assertThat(operation.perform(table, transaction), willCompleteSuccessfully());

        transaction.commit();

        PendingTxPartitionEnlistment enlistment = extractSingleEnlistmentForZone();
        assertThat(enlistment.tableIds(), contains(table.tableId()));
    }

    @ParameterizedTest
    @EnumSource(EnlistingOperation.class)
    @WithSystemProperty(key = COLOCATION_FEATURE_FLAG, value = "true")
    void anotherTableIdGetsEnlistedInSameZonePartitionEnlistment(EnlistingOperation operation) {
        InternalTable table1 = newInternalTable(10, 1);
        InternalTable table2 = newInternalTable(11, 1);
        InternalTransaction transaction = newReadWriteTransaction();

        assertThat(table1.upsert(createBinaryRow(), transaction), willCompleteSuccessfully());
        assertThat(operation.perform(table2, transaction), willCompleteSuccessfully());

        transaction.commit();

        PendingTxPartitionEnlistment enlistment = extractSingleEnlistmentForZone();
        assertThat(enlistment.tableIds(), containsInAnyOrder(table1.tableId(), table2.tableId()));
    }

    private PendingTxPartitionEnlistment extractSingleEnlistmentForZone() {
        Map<ReplicationGroupId, PendingTxPartitionEnlistment> capturedEnlistments = extractEnlistmentsFromTxFinish();
        assertThat(capturedEnlistments, is(aMapWithSize(1)));
        PendingTxPartitionEnlistment enlistment = capturedEnlistments.get(new ZonePartitionId(ZONE_ID, 0));
        assertThat(enlistment, is(notNullValue()));
        return enlistment;
    }

    private Map<ReplicationGroupId, PendingTxPartitionEnlistment> extractEnlistmentsFromTxFinish() {
        ArgumentCaptor<Map<ReplicationGroupId, PendingTxPartitionEnlistment>> enlistmentsCaptor = ArgumentCaptor.captor();

        verify(txManager).finish(any(), any(), anyBoolean(), anyBoolean(), anyBoolean(), enlistmentsCaptor.capture(), any());

        return enlistmentsCaptor.getValue();
    }

    private InternalTransaction newReadWriteTransaction() {
        return new ReadWriteTransactionImpl(
                txManager,
                mock(HybridTimestampTracker.class),
                TestTransactionIds.newTransactionId(),
                randomUUID(),
                false,
                10_000,
                colocationEnabled()
        );
    }

    private enum EnlistingOperation {
        GET((table, tx) -> table.get(createBinaryRow(), tx)),
        GET_ALL((table, tx) -> table.getAll(List.of(createBinaryRow()), tx)),
        UPSERT((table, tx) -> table.upsert(createBinaryRow(), tx)),
        UPSERT_ALL((table, tx) -> table.upsertAll(List.of(createBinaryRow()), tx)),
        GET_AND_UPSERT((table, tx) -> table.getAndUpsert(createBinaryRow(), tx)),
        INSERT((table, tx) -> table.insert(createBinaryRow(), tx)),
        INSERT_ALL((table, tx) -> table.insertAll(List.of(createBinaryRow()), tx)),
        REPLACE((table, tx) -> table.replace(createBinaryRow(), tx)),
        REPLACE_EXACT((table, tx) -> table.replace(createBinaryRow(), createBinaryRow(), tx)),
        GET_AND_REPLACE((table, tx) -> table.getAndReplace(createBinaryRow(), tx)),
        DELETE((table, tx) -> table.delete(createBinaryRow(), tx)),
        DELETE_EXACT((table, tx) -> table.deleteExact(createBinaryRow(), tx)),
        GET_AND_DELETE((table, tx) -> table.getAndDelete(createBinaryRow(), tx)),
        DELETE_ALL((table, tx) -> table.deleteAll(List.of(createBinaryRow()), tx)),
        DELETE_ALL_EXACT((table, tx) -> table.deleteAllExact(List.of(createBinaryRow()), tx)),
        SCAN_MV_STORAGE(adaptScan((table, tx) -> table.scan(0, tx))),
        SCAN_INDEX(adaptScan((table, tx) -> table.scan(0, tx, 1, null, null, 0, null)));

        private final BiFunction<InternalTable, InternalTransaction, CompletableFuture<?>> action;

        EnlistingOperation(BiFunction<InternalTable, InternalTransaction, CompletableFuture<?>> action) {
            this.action = action;
        }

        private static BiFunction<InternalTable, InternalTransaction, CompletableFuture<?>> adaptScan(
                BiFunction<InternalTable, InternalTransaction, Publisher<BinaryRow>> func
        ) {
            return (table, tx) -> {
                CompletableFuture<Void> resultFuture = new CompletableFuture<>();

                Publisher<BinaryRow> publisher = func.apply(table, tx);

                publisher.subscribe(new BlackholeSubscriber(resultFuture));

                return resultFuture;
            };
        }

        CompletableFuture<?> perform(InternalTable table, InternalTransaction transaction) {
            return action.apply(table, transaction);
        }
    }
}
