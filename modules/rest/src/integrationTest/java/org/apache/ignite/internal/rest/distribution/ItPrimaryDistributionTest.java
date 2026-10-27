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

package org.apache.ignite.internal.rest.distribution;

import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.ignite.internal.ClusterConfiguration;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.metastorage.server.raft.MetastorageGroupId;
import org.apache.ignite.internal.placementdriver.balance.LeaseBalanceOperation;
import org.apache.ignite.internal.placementdriver.balance.LeaseBalancePlan;
import org.apache.ignite.internal.placementdriver.balance.PrimaryPage;
import org.apache.ignite.internal.placementdriver.balance.PrimarySnapshot;
import org.apache.ignite.internal.placementdriver.balance.PrimarySummary;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.table.NodeUtils;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.TransactionOptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

/** End-to-end Primary REST, durable fencing, timeout reconciliation and transactional correctness. */
@TestMethodOrder(MethodOrderer.MethodName.class)
public class ItPrimaryDistributionTest extends ClusterPerClassIntegrationTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final HttpClient HTTP = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    private final AtomicInteger names = new AtomicInteger();

    @Override
    protected int[] cmgMetastoreNodes() {
        return new int[]{0, 1, 2};
    }

    @Override
    protected void customizeConfiguration(ClusterConfiguration.Builder builder) {
        builder.basePort(29344).baseClientPort(29800).baseHttpPort(29300).baseHttpsPort(29400);
    }

    @BeforeAll
    void enableBalancing() throws Exception {
        patchConfig("ignite.replication.primaryBalanceEnabled=true");
        patchConfig("ignite.replication.leaseAgreementAcceptanceTimeLimitMillis=5000");
    }

    @Test
    void observesAndBalancesDuringContinuousTransactions() throws Exception {
        Scope scope = skewedScope();
        PrimarySummary summary = get(1, "/primary-replicas/summary?zoneId=" + scope.zoneId, PrimarySummary.class);
        assertEquals(8, summary.totalGroups);
        assertEquals(8, summary.nodes.stream().mapToInt(n -> n.primaryCount).max().orElseThrow());
        assertEquals(2, summary.nodes.stream().filter(n -> n.primaryCount == 0).count());
        PrimaryPage first = get(0, "/primary-replicas?zoneId=" + scope.zoneId + "&limit=3", PrimaryPage.class);
        PrimaryPage second = get(2, "/primary-replicas?zoneId=" + scope.zoneId + "&limit=3&offset=3&snapshotId="
                + first.snapshot.snapshotId, PrimaryPage.class);
        assertEquals(first.snapshot.snapshotId, second.snapshot.snapshotId);
        assertEquals(3, first.items.size());
        assertEquals(3, second.items.size());
        assertNotEquals(first.items.get(0).partitionId, second.items.get(0).partitionId);
        LeaseBalancePlan plan = preview(scope.zoneId, 16);
        assertEquals(5, plan.moves.size());
        try (Traffic traffic = new Traffic(scope.table)) {
            LeaseBalanceOperation accepted = execute(1, plan, UUID.randomUUID().toString(), 202);
            LeaseBalanceOperation duplicate = execute(2, plan, accepted.requestId, 202);
            assertEquals(accepted.operationId, duplicate.operationId);
            assertEquals("anonymous", accepted.requestedBy);
            assertTrue(waitForCondition(() -> "SUCCEEDED".equals(operation(accepted.operationId).state), 90_000));
            LeaseBalanceOperation completed = operation(accepted.operationId);
            assertFalse(completed.unresolved);
            assertTrue(completed.plan.moves.stream().allMatch(move -> "SUCCEEDED".equals(move.state)));
            PrimarySnapshot actualSnapshot = impl(0).primaryDistributionManager().snapshot().join();
            for (var move : completed.plan.moves) {
                var actual = actualSnapshot.groups.stream().filter(group -> group.objectId == move.group.objectId
                        && group.partitionId == move.group.partitionId && group.groupType.equals(move.group.groupType))
                        .findFirst().orElseThrow();
                assertEquals(move.targetId, actual.holderId);
                assertTrue(org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestamp(actual.leaseStart)
                        .getPhysical() >= move.oldExpiration);
                assertEquals(move.group.assignmentRevision, actual.assignmentRevision);
                assertEquals(move.group.stablePeers, actual.stablePeers);
            }
            PrimarySummary after = get(2, "/primary-replicas/summary?zoneId=" + scope.zoneId, PrimarySummary.class);
            int min = after.nodes.stream().mapToInt(n -> n.primaryCount).min().orElseThrow();
            int max = after.nodes.stream().mapToInt(n -> n.primaryCount).max().orElseThrow();
            assertTrue(max - min <= 1);
            traffic.verify();
        }
    }

    @Test
    void pdChangeInterruptsUnstartedMovesAndPreservesWrites() throws Exception {
        Scope scope = skewedScope();
        LeaseBalancePlan plan = preview(scope.zoneId, 3);
        assertEquals(3, plan.moves.size());
        try (Traffic traffic = new Traffic(scope.table)) {
            LeaseBalanceOperation accepted = execute(1, plan, UUID.randomUUID().toString(), 202);
            assertTrue(waitForCondition(() -> operation(accepted.operationId).plan.moves.stream()
                    .anyMatch(move -> "WAITING".equals(move.state)), 10_000));
            long oldTerm = operation(accepted.operationId).ownerTerm;
            String leader = CLUSTER.leaderServiceFor(MetastorageGroupId.INSTANCE).getRaftNode().getNodeId().getPeerId().getConsistentId();
            int target = impl(0).name().equals(leader) ? 1 : 0;
            CLUSTER.transferLeadershipTo(target, MetastorageGroupId.INSTANCE);
            assertTrue(waitForCondition(() -> {
                LeaseBalanceOperation current = operation(accepted.operationId);
                return "INTERRUPTED".equals(current.state) && !current.unresolved;
            }, 90_000));
            LeaseBalanceOperation interrupted = operation(accepted.operationId);
            assertTrue(interrupted.ownerTerm > oldTerm);
            org.apache.ignite.internal.lang.ByteArray fence =
                    org.apache.ignite.internal.lang.ByteArray.fromString("primary.balance.v1.fence");
            org.apache.ignite.internal.lang.ByteArray delayed =
                    org.apache.ignite.internal.lang.ByteArray.fromString("hf6.delayed-old-actor");
            assertFalse(impl(0).metaStorageManager().invoke(
                    org.apache.ignite.internal.metastorage.dsl.Conditions.value(fence).eq(
                            String.format(java.util.Locale.ROOT, "%020d", oldTerm).getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                    org.apache.ignite.internal.metastorage.dsl.Operations.put(delayed, new byte[]{1}),
                    org.apache.ignite.internal.metastorage.dsl.Operations.noop()).join());
            assertEquals(2, interrupted.plan.moves.stream().filter(move -> "PENDING".equals(move.state)).count());
            traffic.verify();
        }
    }

    @Test
    void zzPdNodeStopInterruptsTheDurableTaskWithoutReplayingItsRemainingMoves() throws Exception {
        Scope scope = skewedScope();
        CLUSTER.transferLeadershipTo(2, MetastorageGroupId.INSTANCE);
        LeaseBalancePlan plan = preview(scope.zoneId, 3);
        try (Traffic traffic = new Traffic(scope.table, 1)) {
            LeaseBalanceOperation accepted = execute(1, plan, UUID.randomUUID().toString(), 202);
            assertTrue(waitForCondition(() -> operation(accepted.operationId).plan.moves.stream()
                    .anyMatch(move -> "WAITING".equals(move.state)), 10_000));
            UUID stoppedId = impl(2).id();
            CLUSTER.stopNode(2);
            assertTrue(waitForCondition(() -> {
                LeaseBalanceOperation current = operation(accepted.operationId);
                return "INTERRUPTED".equals(current.state) && !current.unresolved;
            }, 90_000));
            LeaseBalanceOperation interrupted = operation(accepted.operationId);
            assertEquals(2, interrupted.plan.moves.stream().filter(move -> "PENDING".equals(move.state)).count());
            if (restartStoppedNode()) {
                UUID previousId = stoppedId;
                CLUSTER.startNode(2);
                assertTrue(waitForCondition(() -> impl(1).logicalTopologyService().localLogicalTopology().nodes().size() == 3, 30_000));
                assertNotEquals(previousId, impl(2).id());
                assertEquals(2, operation(accepted.operationId).plan.moves.stream()
                        .filter(move -> "PENDING".equals(move.state)).count());
            }
            traffic.verify();
        }
    }

    @Test
    void timeoutReportsUnknownBlocksAnotherOperationAndEventuallyReconciles() throws Exception {
        Scope scope = skewedScope();
        LeaseBalancePlan plan = preview(scope.zoneId, 1);
        patchConfig("ignite.replication.primaryBalanceTimeoutMillis=1000");
        try {
            LeaseBalanceOperation accepted = execute(0, plan, UUID.randomUUID().toString(), 202);
            assertTrue(waitForCondition(() -> operation(accepted.operationId).unresolved, 10_000));
            LeaseBalanceOperation timedOut = operation(accepted.operationId);
            assertEquals("PARTIAL", timedOut.state);
            assertEquals("MIGRATION_TIMEOUT", timedOut.reason);
            assertEquals("UNKNOWN", timedOut.plan.moves.get(0).state);
            execute(2, preview(scope.zoneId, 1), UUID.randomUUID().toString(), 409);
            assertTrue(waitForCondition(() -> !operation(accepted.operationId).unresolved, 60_000));
            assertNotEquals("SUCCEEDED", operation(accepted.operationId).state);
        } finally {
            patchConfig("ignite.replication.primaryBalanceTimeoutMillis=30000");
        }
    }

    @Test
    void disabledExecutionAndInvalidPlansAreExplicit() throws Exception {
        Scope scope = skewedScope();
        LeaseBalancePlan plan = preview(scope.zoneId, 1);
        patchConfig("ignite.replication.primaryBalanceEnabled=false");
        try {
            var response = send(0, "POST", "/balance-leases", JSON.writeValueAsString(Map.of(
                    "planId", plan.planId, "requestId", UUID.randomUUID().toString())));
            assertEquals(409, response.statusCode());
            assertTrue(response.body().contains("BALANCING_DISABLED"));
        } finally {
            patchConfig("ignite.replication.primaryBalanceEnabled=true");
        }
        var invalid = send(0, "POST", "/balance-leases/plan", "{\"zoneId\":" + scope.zoneId + ",\"maxMoves\":17}");
        assertEquals(400, invalid.statusCode());
    }

    protected String storageProfile() {
        return "default_aimem";
    }

    protected boolean restartStoppedNode() {
        return false;
    }

    private Scope skewedScope() throws Exception {
        String name = "HF6_" + names.incrementAndGet();
        sql("CREATE ZONE " + name + " (PARTITIONS 8, REPLICAS 3) STORAGE PROFILES ['" + storageProfile() + "']");
        sql("CREATE TABLE " + name + " (id INT PRIMARY KEY, val INT) ZONE " + name);
        sql("INSERT INTO " + name + " VALUES (0, 0), (1, 0)");
        int zoneId = impl(0).catalogManager().activeCatalog(impl(0).clock().nowLong()).zone(name).id();
        assertTrue(waitForCondition(() -> {
            PrimarySnapshot snapshot = impl(0).primaryDistributionManager().snapshot().join();
            return snapshot.complete && snapshot.groups.stream().filter(g -> g.zoneId == zoneId && "ACTIVE".equals(g.state)).count() == 8;
        }, 60_000));
        PrimarySnapshot snapshot = impl(0).primaryDistributionManager().snapshot().join();
        List<IgniteImpl> nodes = CLUSTER.runningNodes().map(org.apache.ignite.internal.TestWrappers::unwrapIgniteImpl)
                .collect(Collectors.toList());
        List<CompletableFuture<Void>> transfers = new ArrayList<>();
        snapshot.groups.stream().filter(g -> g.zoneId == zoneId && !impl(0).name().equals(g.holder)).forEach(group -> {
            transfers.add(CompletableFuture.runAsync(() -> {
                ReplicationGroupId id = "ZONE".equals(group.groupType) ? new ZonePartitionId(group.objectId, group.partitionId)
                        : new TablePartitionId(group.objectId, group.partitionId);
                try {
                    NodeUtils.transferPrimary(nodes, id, impl(0).name());
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }));
        });
        CompletableFuture.allOf(transfers.toArray(new CompletableFuture[0])).get(60, java.util.concurrent.TimeUnit.SECONDS);
        assertTrue(waitForCondition(() -> get(0, "/primary-replicas/summary?zoneId=" + zoneId, PrimarySummary.class)
                .nodes.stream().anyMatch(n -> n.primaryCount == 8), 15_000));
        return new Scope(name, zoneId);
    }

    private static IgniteImpl impl(int index) {
        return unwrapIgniteImpl(CLUSTER.node(index));
    }

    private static LeaseBalancePlan preview(int zoneId, int maxMoves) {
        try {
            var response = send(0, "POST", "/balance-leases/plan",
                    JSON.writeValueAsString(Map.of("zoneId", zoneId, "maxMoves", maxMoves)));
            assertEquals(200, response.statusCode(), response.body());
            return JSON.readValue(response.body(), LeaseBalancePlan.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static LeaseBalanceOperation execute(int node, LeaseBalancePlan plan, String requestId, int status) throws Exception {
        var response = send(node, "POST", "/balance-leases",
                JSON.writeValueAsString(Map.of("planId", plan.planId, "requestId", requestId)));
        assertEquals(status, response.statusCode(), response.body());
        return status == 202 ? JSON.readValue(response.body(), LeaseBalanceOperation.class) : null;
    }

    private static LeaseBalanceOperation operation(String operationId) {
        return get(1, "/balance-leases/operations/" + operationId, LeaseBalanceOperation.class);
    }

    private static <T> T get(int node, String path, Class<T> type) {
        try {
            var response = send(node, "GET", path, "");
            assertEquals(200, response.statusCode(), response.body());
            return JSON.readValue(response.body(), type);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static HttpResponse<String> send(int node, String method, String path, String body) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://localhost:" + (29300 + node)
                        + "/management/v1/distribution" + path)).timeout(Duration.ofSeconds(15))
                .header("Content-Type", "application/json")
                .method(method, body.isEmpty() ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body)).build();
        return HTTP.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private static void patchConfig(String config) throws Exception {
        var response = HTTP.send(HttpRequest.newBuilder(URI.create("http://localhost:29300/management/v1/configuration/cluster"))
                .header("Content-Type", "text/plain").method("PATCH", HttpRequest.BodyPublishers.ofString(config)).build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode(), response.body());
        assertTrue(waitForCondition(() -> CLUSTER.runningNodes().allMatch(node -> {
            try {
                var responseNode = HTTP.send(HttpRequest.newBuilder(URI.create("http://localhost:"
                                + (29300 + CLUSTER.runningNodes().collect(Collectors.toList()).indexOf(node))
                                + "/management/v1/configuration/cluster/ignite.replication"))
                        .build(), HttpResponse.BodyHandlers.ofString());
                String field = config.substring(config.lastIndexOf('.') + 1, config.indexOf('='));
                return JSON.readTree(responseNode.body()).get(field).asText().equals(config.substring(config.indexOf('=') + 1));
            } catch (Exception e) {
                return false;
            }
        }), 10_000));
    }

    private static class Scope {
        final String table;
        final int zoneId;

        Scope(String table, int zoneId) {
            this.table = table;
            this.zoneId = zoneId;
        }
    }

    private static class Traffic implements AutoCloseable {
        private final AtomicBoolean running = new AtomicBoolean(true);
        private final AtomicInteger writes = new AtomicInteger();
        private final AtomicInteger reads = new AtomicInteger();
        private final AtomicInteger retries = new AtomicInteger();
        private final AtomicReference<Throwable> failure = new AtomicReference<>();
        private final String table;
        private final java.util.concurrent.ConcurrentLinkedQueue<Long> latencies = new java.util.concurrent.ConcurrentLinkedQueue<>();
        private final CompletableFuture<Void> writer;
        private final CompletableFuture<Void> reader;

        Traffic(String table) {
            this(table, 2);
        }

        Traffic(String table, int readerIndex) {
            this.table = table;
            RecordView<Tuple> records = impl(1).tables().table(table).recordView();
            writer = CompletableFuture.runAsync(() -> {
                try {
                    while (running.get()) {
                        long started = System.nanoTime();
                        int version = writes.get() + 1;
                        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(30);
                        while (true) {
                            var tx = impl(1).transactions().begin();
                            try {
                                records.upsertAll(tx, List.of(Tuple.create().set("id", 0).set("val", version),
                                        Tuple.create().set("id", 1).set("val", version)));
                                tx.commit();
                                break;
                            } catch (Throwable error) {
                                tx.rollback();
                                if (!knownRollback(error) || System.nanoTime() > deadline) {
                                    throw error;
                                }
                                retries.incrementAndGet();
                                java.util.concurrent.locks.LockSupport.parkNanos(java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(10));
                            }
                        }
                        writes.incrementAndGet();
                        latencies.add(System.nanoTime() - started);
                    }
                } catch (Throwable error) {
                    error.printStackTrace();
                    failure.compareAndSet(null, error);
                }
            });
            reader = CompletableFuture.runAsync(() -> {
                try {
                    while (running.get()) {
                        var tx = impl(readerIndex).transactions().begin(new TransactionOptions().readOnly(true));
                        try {
                            var rows = impl(readerIndex).tables().table(table).recordView().getAll(tx,
                                    List.of(Tuple.create().set("id", 0), Tuple.create().set("id", 1)));
                            assertEquals(rows.get(0).intValue("val"), rows.get(1).intValue("val"));
                            tx.commit();
                            reads.incrementAndGet();
                        } catch (Throwable error) {
                            tx.rollback();
                            throw error;
                        }
                    }
                } catch (Throwable error) {
                    error.printStackTrace();
                    failure.compareAndSet(null, error);
                }
            });
        }

        private static boolean knownRollback(Throwable error) {
            for (Throwable cause = error; cause != null; cause = cause.getCause()) {
                if (cause instanceof org.apache.ignite.tx.TransactionException
                        && ((org.apache.ignite.tx.TransactionException) cause).code()
                        == org.apache.ignite.lang.ErrorGroups.Transactions.TX_PRIMARY_REPLICA_EXPIRED_ERR) {
                    return true;
                }
                if (cause instanceof org.apache.ignite.internal.replicator.exception.PrimaryReplicaMissException) {
                    return true;
                }
            }
            return false;
        }

        void verify() throws Exception {
            assertTrue(waitForCondition(() -> writes.get() > 10 && reads.get() > 10, 20_000));
            assertEquals(null, failure.get());
            List<Long> samples = new ArrayList<>(latencies);
            samples.sort(Long::compareTo);
            double p99 = samples.get(Math.min(samples.size() - 1, (int) (samples.size() * 0.99))) / 1_000_000.0;
            System.out.println("HF6_TRAFFIC writes=" + writes.get() + " consistentReads=" + reads.get()
                    + " retriedTransactions=" + retries.get() + " writeP99Millis=" + p99);
        }

        @Override
        public void close() throws Exception {
            running.set(false);
            CompletableFuture.allOf(writer, reader).get(30, java.util.concurrent.TimeUnit.SECONDS);
            assertEquals(null, failure.get());
            assertEquals(writes.get(), impl(1).tables().table(table).recordView()
                    .get(null, Tuple.create().set("id", 0)).intValue("val"));
        }
    }
}
