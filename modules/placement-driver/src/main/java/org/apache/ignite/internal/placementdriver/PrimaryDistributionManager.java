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

package org.apache.ignite.internal.placementdriver;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.and;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.notExists;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.revision;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.value;
import static org.apache.ignite.internal.metastorage.dsl.Operations.noop;
import static org.apache.ignite.internal.metastorage.dsl.Operations.put;
import static org.apache.ignite.internal.metastorage.dsl.Operations.remove;
import static org.apache.ignite.internal.placementdriver.PlacementDriverManager.PLACEMENTDRIVER_LEASES_KEY;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.components.NodeProperties;
import org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil;
import org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.dsl.Condition;
import org.apache.ignite.internal.metastorage.dsl.Operation;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.partitiondistribution.Assignment;
import org.apache.ignite.internal.partitiondistribution.Assignments;
import org.apache.ignite.internal.placementdriver.balance.LeaseBalanceOperation;
import org.apache.ignite.internal.placementdriver.balance.LeaseBalancePlan;
import org.apache.ignite.internal.placementdriver.balance.LeaseMove;
import org.apache.ignite.internal.placementdriver.balance.PrimaryGroup;
import org.apache.ignite.internal.placementdriver.balance.PrimaryNode;
import org.apache.ignite.internal.placementdriver.balance.PrimaryPage;
import org.apache.ignite.internal.placementdriver.balance.PrimarySnapshot;
import org.apache.ignite.internal.placementdriver.balance.PrimarySummary;
import org.apache.ignite.internal.placementdriver.leases.Lease;
import org.apache.ignite.internal.placementdriver.leases.LeaseBatch;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.configuration.ReplicationConfiguration;
import org.apache.ignite.internal.table.distributed.disaster.DisasterRecoveryManager;

/** Primary observation and manually requested, fenced, one-at-a-time lease transfers. */
public class PrimaryDistributionManager implements IgniteComponent {
    private static final IgniteLogger LOG = Loggers.forClass(PrimaryDistributionManager.class);
    private static final String PREFIX = "primary.balance.v1.";
    static final ByteArray FENCE_KEY = key("fence");
    private static final ByteArray ACTIVE_KEY = key("active");
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final long RETENTION = TimeUnit.HOURS.toMillis(24);
    private final MetaStorageManager meta;
    private final PlacementDriverManager driver;
    private final CatalogManager catalogs;
    private final LogicalTopologyService topology;
    private final ClusterService cluster;
    private final ClockService clock;
    private final NodeProperties properties;
    private final ReplicationConfiguration config;
    private final DisasterRecoveryManager recovery;
    private final ScheduledExecutorService executor;
    private final AtomicBoolean ticking = new AtomicBoolean();
    private final AtomicBoolean refreshing = new AtomicBoolean();
    private volatile boolean stopped;
    private long lastCleanup;
    private final MetricManager metrics;
    private volatile PrimarySummary latestSummary;
    private final java.util.concurrent.atomic.AtomicLong successes = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong timeouts = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong failures = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong durationMillis = new java.util.concurrent.atomic.AtomicLong();

    /** Creates the management component. */
    public PrimaryDistributionManager(MetaStorageManager meta, PlacementDriverManager driver, CatalogManager catalogs,
            LogicalTopologyService topology, ClusterService cluster, ClockService clock, NodeProperties properties,
            ReplicationConfiguration config, DisasterRecoveryManager recovery, MetricManager metrics) {
        this.meta = meta;
        this.driver = driver;
        this.catalogs = catalogs;
        this.topology = topology;
        this.cluster = cluster;
        this.clock = clock;
        this.properties = properties;
        this.config = config;
        this.recovery = recovery;
        this.metrics = metrics;
        executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, cluster.topologyService().localMember().name() + "-primary-balance");
            thread.setDaemon(true);
            return thread;
        });
    }

    @Override
    public CompletableFuture<Void> startAsync(ComponentContext context) {
        metrics.registerSource(new PrimaryDistributionMetricSource(Map.of(
                "LocalPrimaryCount", () -> latestSummary == null ? -1L : latestSummary.nodes.stream()
                        .filter(n -> cluster.topologyService().localMember().name().equals(n.nodeName))
                        .mapToLong(n -> n.onlinePrimaryCount).sum(),
                "ActiveReporter", () -> driver.activeActorTerm() > 0 ? 1L : 0L,
                "WithoutPrimary", () -> driver.activeActorTerm() > 0 && latestSummary != null
                        ? (long) latestSummary.withoutPrimary : -1L,
                "SnapshotStale", () -> latestSummary == null || !latestSummary.complete
                        || clock.current().getPhysical() - latestSummary.observedAt > 10_000 ? 1L : 0L,
                "MigrationsSucceeded", successes::get,
                "MigrationsTimedOut", timeouts::get,
                "MigrationsFailed", failures::get,
                "MigrationDurationMillis", durationMillis::get)));
        metrics.enable(PrimaryDistributionMetricSource.NAME);
        executor.scheduleWithFixedDelay(this::refreshMetrics, 0, 5, TimeUnit.SECONDS);
        executor.scheduleWithFixedDelay(this::tick, 500, 500, TimeUnit.MILLISECONDS);
        return meta.put(capabilityKey(cluster.topologyService().localMember().name()),
                cluster.topologyService().localMember().id().toString().getBytes(UTF_8));
    }

    @Override
    public CompletableFuture<Void> stopAsync(ComponentContext context) {
        stopped = true;
        executor.shutdownNow();
        metrics.disable(PrimaryDistributionMetricSource.NAME);
        return completedFuture(null);
    }

    static byte[] termBytes(long term) {
        return String.format(java.util.Locale.ROOT, "%020d", term).getBytes(UTF_8);
    }

    private static ByteArray key(String suffix) {
        return ByteArray.fromString(PREFIX + suffix);
    }

    private static ByteArray capabilityKey(String node) {
        return key("capability." + node);
    }

    private static ReplicationGroupId groupId(PrimaryGroup group) {
        return "ZONE".equals(group.groupType) ? new ZonePartitionId(group.objectId, group.partitionId)
                : new TablePartitionId(group.objectId, group.partitionId);
    }

    private static ByteArray stableKey(ReplicationGroupId id) {
        return id instanceof ZonePartitionId ? ZoneRebalanceUtil.stablePartAssignmentsKey((ZonePartitionId) id)
                : RebalanceUtil.stablePartAssignmentsKey((TablePartitionId) id);
    }

    private static ByteArray pendingKey(ReplicationGroupId id) {
        return id instanceof ZonePartitionId ? ZoneRebalanceUtil.pendingPartAssignmentsQueueKey((ZonePartitionId) id)
                : RebalanceUtil.pendingPartAssignmentsQueueKey((TablePartitionId) id);
    }

    /** Returns a fresh, non-blocking metadata snapshot, including groups that have never obtained a lease. */
    public CompletableFuture<PrimarySnapshot> snapshot() {
        long started = clock.current().getPhysical();
        Catalog catalog = catalogs.activeCatalog(clock.now().longValue());
        return topology.logicalTopologyOnLeader().thenCompose(top -> {
            Map<ReplicationGroupId, Integer> groups = new java.util.LinkedHashMap<>();
            if (properties.colocationEnabled()) {
                for (CatalogZoneDescriptor zone : catalog.zones()) {
                    for (int part = 0; part < zone.partitions(); part++) {
                        groups.put(new ZonePartitionId(zone.id(), part), zone.id());
                    }
                }
            } else {
                for (CatalogTableDescriptor table : catalog.tables()) {
                    CatalogZoneDescriptor zone = catalog.zone(table.zoneId());
                    for (int part = 0; part < zone.partitions(); part++) {
                        groups.put(new TablePartitionId(table.id(), part), zone.id());
                    }
                }
            }
            Set<ByteArray> keys = new HashSet<>();
            keys.add(PLACEMENTDRIVER_LEASES_KEY);
            groups.keySet().forEach(id -> {
                keys.add(stableKey(id));
                keys.add(pendingKey(id));
            });
            return meta.getAll(keys).thenApply(entries -> buildSnapshot(catalog, top, groups, entries, started));
        });
    }

    private PrimarySnapshot buildSnapshot(Catalog catalog, LogicalTopologySnapshot top,
            Map<ReplicationGroupId, Integer> groups, Map<ByteArray, Entry> entries, long started) {
        Map<String, String> nodes = new TreeMap<>();
        top.nodes().forEach(node -> nodes.put(node.name(), node.id().toString()));
        Entry leaseEntry = entries.get(PLACEMENTDRIVER_LEASES_KEY);
        Map<ReplicationGroupId, Lease> leases = new HashMap<>();
        if (leaseEntry.value() != null) {
            LeaseBatch.fromBytes(leaseEntry.value()).leases().forEach(lease -> leases.put(lease.replicationGroupId(), lease));
        }
        HybridTimestamp now = clock.now();
        boolean complete = catalog.version() == catalogs.activeCatalog(now.longValue()).version()
                && top.version() == topology.localLogicalTopology().version();
        List<PrimaryGroup> result = new ArrayList<>();
        for (Map.Entry<ReplicationGroupId, Integer> group : groups.entrySet()) {
            ReplicationGroupId id = group.getKey();
            Entry stable = entries.get(stableKey(id));
            List<String> peers = stable.value() == null ? List.of() : Assignments.fromBytes(stable.value()).nodes().stream()
                    .filter(Assignment::isPeer).map(Assignment::consistentId).sorted().collect(Collectors.toList());
            complete &= !peers.isEmpty();
            Lease lease = leases.get(id);
            String holder = lease == null ? null : lease.getLeaseholder();
            String holderId = lease == null || lease.getLeaseholderId() == null ? null : lease.getLeaseholderId().toString();
            String state = lease == null ? "MISSING" : !lease.isAccepted() ? "UNACCEPTED"
                    : lease.getStartTime().compareTo(now) > 0 ? "FUTURE"
                    : !clock.before(now, lease.getExpirationTime()) ? "EXPIRED"
                    : !nodes.containsKey(holder) ? "OFFLINE"
                    : !nodes.get(holder).equals(holderId) ? "INSTANCE_MISMATCH" : "ACTIVE";
            int objectId = id instanceof ZonePartitionId ? ((ZonePartitionId) id).zoneId() : ((TablePartitionId) id).tableId();
            int partitionId = id instanceof ZonePartitionId ? ((ZonePartitionId) id).partitionId() : ((TablePartitionId) id).partitionId();
            result.add(new PrimaryGroup(id instanceof ZonePartitionId ? "ZONE" : "TABLE", objectId, group.getValue(), partitionId,
                    holder, holderId, lease == null ? 0 : lease.getStartTime().longValue(),
                    lease == null ? 0 : lease.getExpirationTime().getPhysical(), lease != null && lease.isAccepted(),
                    lease != null && lease.isProlongable(), state, peers, stable.revision(), entries.get(pendingKey(id)).value() != null));
        }
        result.sort(java.util.Comparator.comparing(LeaseBalancePlanner::key));
        long observed = now.getPhysical();
        return new PrimarySnapshot(UUID.randomUUID().toString(), observed, catalog.version(), top.version(), leaseEntry.revision(),
                observed - started <= 5000 ? "FRESH" : "STALE", complete,
                properties.colocationEnabled() ? "ZONE" : "TABLE", nodes, result);
    }

    /** Returns counts without hiding offline holders or zero-primary candidate nodes. */
    public CompletableFuture<PrimarySummary> summary(Integer zoneId) {
        return snapshot().thenApply(snapshot -> summarize(snapshot, zoneId));
    }

    static PrimarySummary summarize(PrimarySnapshot snapshot, Integer zoneId) {
        Map<String, int[]> counts = new TreeMap<>();
        snapshot.nodes.keySet().forEach(node -> counts.put(node, new int[3]));
        Map<Integer, Map<String, Integer>> zones = new TreeMap<>();
        int total = 0;
        int missing = 0;
        for (PrimaryGroup group : snapshot.groups) {
            if (zoneId != null && group.zoneId != zoneId) {
                continue;
            }
            total++;
            Map<String, Integer> zoneCounts = zones.computeIfAbsent(group.zoneId, ignored -> new TreeMap<>());
            for (String peer : group.stablePeers) {
                counts.computeIfAbsent(peer, ignored -> new int[3])[2]++;
                zoneCounts.putIfAbsent(peer, 0);
            }
            if (group.accepted && HybridTimestamp.hybridTimestamp(group.leaseStart).getPhysical() <= snapshot.observedAt
                    && group.leaseExpiration > snapshot.observedAt) {
                counts.computeIfAbsent(group.holder, ignored -> new int[3])[0]++;
                zoneCounts.merge(group.holder, 1, Integer::sum);
                if ("ACTIVE".equals(group.state)) {
                    counts.get(group.holder)[1]++;
                }
            } else {
                missing++;
            }
        }
        List<PrimaryNode> nodes = new ArrayList<>();
        counts.forEach((name, count) -> nodes.add(new PrimaryNode(name, snapshot.nodes.get(name), count[0], count[1], count[2])));
        return new PrimarySummary(snapshot.observedAt, snapshot.catalogVersion, snapshot.topologyVersion, snapshot.leaseRevision,
                snapshot.freshness, snapshot.complete, snapshot.replicationMode, total, missing, nodes, zones);
    }

    /** Returns stable snapshot pagination; the snapshot can be continued from any REST node. */
    public CompletableFuture<PrimaryPage> page(Integer zoneId, String node, String state, String snapshotId, int offset, int limit) {
        if (offset < 0 || limit < 1 || limit > 1000) {
            throw new DistributionException(400, "offset must be nonnegative; limit must be between 1 and 1000");
        }
        CompletableFuture<PrimarySnapshot> future;
        if (snapshotId == null) {
            future = snapshot().thenCompose(snapshot -> meta.put(key("snapshot." + snapshot.snapshotId), encode(snapshot))
                    .thenApply(ignored -> snapshot));
        } else {
            future = read(key("snapshot." + identifier(snapshotId)), PrimarySnapshot.class, 410);
        }
        return future.thenApply(snapshot -> {
            if (clock.current().getPhysical() - snapshot.observedAt > 60_000) {
                throw new DistributionException(410, "SNAPSHOT_EXPIRED");
            }
            List<PrimaryGroup> selected = snapshot.groups.stream()
                    .filter(g -> zoneId == null || g.zoneId == zoneId)
                    .filter(g -> node == null || node.equals(g.holder))
                    .filter(g -> state == null || state.equals(g.state)).collect(Collectors.toList());
            int end = (int) Math.min(selected.size(), (long) offset + limit);
            List<PrimaryGroup> items = offset >= selected.size() ? List.of() : selected.subList(offset, end);
            PrimarySnapshot header = new PrimarySnapshot(snapshot.snapshotId, snapshot.observedAt, snapshot.catalogVersion,
                    snapshot.topologyVersion, snapshot.leaseRevision, snapshot.freshness, snapshot.complete,
                    snapshot.replicationMode, snapshot.nodes, List.of());
            return new PrimaryPage(header, items, end < selected.size() ? end : null);
        });
    }

    /** Generates and stores a preview, without changing a lease. */
    public CompletableFuture<LeaseBalancePlan> plan(int zoneId, int maxMoves) {
        CatalogZoneDescriptor zone = catalogs.activeCatalog(clock.now().longValue()).zone(zoneId);
        if (zone == null) {
            throw new DistributionException(400, "ZONE_NOT_FOUND: " + zoneId);
        }
        if (maxMoves < 1 || maxMoves > 16) {
            throw new DistributionException(400, "maxMoves must be between 1 and 16");
        }
        return snapshot().thenCompose(snapshot -> {
            List<PrimaryGroup> groups = snapshot.groups.stream().filter(g -> g.zoneId == zoneId).collect(Collectors.toList());
            Set<ByteArray> cooldownKeys = groups.stream().map(g -> key("cooldown." + LeaseBalancePlanner.key(g)))
                    .collect(Collectors.toSet());
            return healthy(groups).thenCombine(meta.getAll(cooldownKeys), (healthy, entries) -> {
                Set<String> cooling = new HashSet<>();
                long now = clock.now().getPhysical();
                for (PrimaryGroup group : groups) {
                    Entry cooldown = entries.get(key("cooldown." + LeaseBalancePlanner.key(group)));
                    if (cooldown.value() != null && now - Long.parseLong(new String(cooldown.value(), UTF_8)) < 60_000) {
                        cooling.add(LeaseBalancePlanner.key(group));
                    }
                }
                return LeaseBalancePlanner.plan(snapshot, zoneId, maxMoves, healthy, cooling, now);
            });
        }).thenCompose(plan -> meta.put(key("plan." + plan.planId), encode(plan)).thenApply(ignored -> plan));
    }

    private CompletableFuture<Map<String, Set<String>>> healthy(List<PrimaryGroup> groups) {
        if (groups.isEmpty()) {
            return completedFuture(Map.of());
        }
        Set<String> zones = groups.stream().map(g -> catalogs.activeCatalog(clock.now().longValue()).zone(g.zoneId).name())
                .collect(Collectors.toSet());
        Set<String> nodes = groups.stream().flatMap(g -> g.stablePeers.stream()).collect(Collectors.toSet());
        Set<Integer> parts = groups.stream().map(g -> g.partitionId).collect(Collectors.toSet());
        CompletableFuture<Map<String, Set<String>>> result;
        if (properties.colocationEnabled()) {
            result = recovery.localPartitionStates(zones, nodes, parts).thenApply(states -> {
                Map<String, Set<String>> healthy = new HashMap<>();
                for (PrimaryGroup group : groups) {
                    var byNode = states.get(groupId(group));
                    if (byNode != null) {
                        byNode.entrySet().forEach(e -> {
                            if ("HEALTHY".equals(e.getValue().state.name())) {
                                healthy.computeIfAbsent(LeaseBalancePlanner.key(group), ignored -> new HashSet<>()).add(e.getKey());
                            }
                        });
                    }
                }
                return healthy;
            });
        } else {
            result = recovery.localTablePartitionStates(zones, nodes, parts).thenApply(states -> {
                Map<String, Set<String>> healthy = new HashMap<>();
                for (PrimaryGroup group : groups) {
                    var byNode = states.get(groupId(group));
                    if (byNode != null) {
                        byNode.entrySet().forEach(e -> {
                            if ("HEALTHY".equals(e.getValue().state.name())) {
                                healthy.computeIfAbsent(LeaseBalancePlanner.key(group), ignored -> new HashSet<>()).add(e.getKey());
                            }
                        });
                    }
                }
                return healthy;
            });
        }
        return result.orTimeout(5000, TimeUnit.MILLISECONDS).exceptionally(error -> Map.of());
    }

    /** Accepts an idempotent operation. No lease is changed by the HTTP request itself. */
    public CompletableFuture<LeaseBalanceOperation> execute(String planId, String requestId) {
        return execute(planId, requestId, "internal");
    }

    /** Accepts an operation with the server-established caller identity. */
    public CompletableFuture<LeaseBalanceOperation> execute(String planId, String requestId, String requestedBy) {
        identifier(planId);
        identifier(requestId);
        ByteArray requestKey = key("request." + requestId);
        return meta.get(requestKey).thenCompose(request -> {
            if (request.value() != null) {
                return existingRequest(request, planId);
            }
            if (!config.primaryBalanceEnabled().value()) {
                throw new DistributionException(409, "BALANCING_DISABLED");
            }
            return read(key("plan." + planId), LeaseBalancePlan.class, 409).thenCompose(plan -> {
                if (plan.expiresAt < clock.now().getPhysical()) {
                    throw new DistributionException(409, "PLAN_EXPIRED");
                }
                return snapshot().thenCompose(snapshot -> {
                    validate(plan, snapshot);
                    return checkCapabilities(snapshot).thenCompose(ignored -> {
                        long now = clock.now().getPhysical();
                        LeaseBalanceOperation operation = new LeaseBalanceOperation(1, UUID.randomUUID().toString(), requestId,
                                now, now, 0, "QUEUED", null, plan, false, Map.of(), requestedBy);
                        byte[] bytes = encode(operation);
                        return meta.invoke(and(notExists(ACTIVE_KEY), notExists(requestKey)),
                                List.of(put(ACTIVE_KEY, bytes), put(key("operation." + operation.operationId), bytes),
                                        put(requestKey, bytes)), List.of(noop())).thenCompose(success -> {
                                            if (success) {
                                                return completedFuture(operation);
                                            }
                                            return meta.get(requestKey).thenCompose(existing -> {
                                                if (existing.value() != null) {
                                                    return existingRequest(existing, planId);
                                                }
                                                throw new DistributionException(409, "BALANCE_ALREADY_RUNNING_OR_UNRESOLVED");
                                            });
                                        });
                    });
                });
            });
        });
    }

    private CompletableFuture<LeaseBalanceOperation> existingRequest(Entry entry, String planId) {
        LeaseBalanceOperation existing = decode(entry.value(), LeaseBalanceOperation.class);
        if (!existing.plan.planId.equals(planId)) {
            throw new DistributionException(409, "REQUEST_ID_REUSED_WITH_DIFFERENT_PLAN");
        }
        return operation(existing.operationId);
    }

    /** Returns durable progress from any node. */
    public CompletableFuture<LeaseBalanceOperation> operation(String operationId) {
        return read(key("operation." + identifier(operationId)), LeaseBalanceOperation.class, 410);
    }

    private CompletableFuture<Void> checkCapabilities(PrimarySnapshot snapshot) {
        Set<ByteArray> keys = snapshot.nodes.keySet().stream().map(PrimaryDistributionManager::capabilityKey).collect(Collectors.toSet());
        return meta.getAll(keys).thenAccept(entries -> snapshot.nodes.forEach((node, id) -> {
            Entry capability = entries.get(capabilityKey(node));
            if (capability.value() == null || !id.equals(new String(capability.value(), UTF_8))) {
                throw new DistributionException(409, "HOTFIX6_REQUIRED_ON_ALL_NODES: " + node);
            }
        }));
    }

    private void validate(LeaseBalancePlan plan, PrimarySnapshot snapshot) {
        if (!snapshot.complete || !"FRESH".equals(snapshot.freshness)) {
            throw new DistributionException(503, "INCOMPLETE_OR_STALE_DISTRIBUTION");
        }
        Map<String, PrimaryGroup> groups = snapshot.groups.stream()
                .collect(Collectors.toMap(LeaseBalancePlanner::key, Function.identity()));
        for (LeaseMove move : plan.moves) {
            PrimaryGroup current = groups.get(LeaseBalancePlanner.key(move.group));
            if (current == null || !"ACTIVE".equals(current.state) || current.pending || !current.prolongable
                    || current.assignmentRevision != move.group.assignmentRevision
                    || current.leaseStart != move.group.leaseStart || !move.group.holderId.equals(current.holderId)
                    || !move.targetId.equals(snapshot.nodes.get(move.target)) || !current.stablePeers.contains(move.target)) {
                throw new DistributionException(409, "PLAN_PRECONDITION_CHANGED: " + LeaseBalancePlanner.key(move.group));
            }
        }
    }

    private void refreshMetrics() {
        if (stopped || !refreshing.compareAndSet(false, true)) {
            return;
        }
        try {
            snapshot().orTimeout(5, TimeUnit.SECONDS)
                    .thenAccept(snapshot -> latestSummary = summarize(snapshot, null))
                    .whenComplete((ignored, error) -> refreshing.set(false));
        } catch (RuntimeException error) {
            refreshing.set(false);
            LOG.debug("Primary metrics refresh deferred until metadata is available", error);
        }
    }

    private void tick() {
        long term = driver.activeActorTerm();
        if (stopped || term == 0 || !ticking.compareAndSet(false, true)) {
            return;
        }
        CompletableFuture<Void> processing;
        try {
            processing = meta.getAll(Set.of(ACTIVE_KEY, FENCE_KEY)).thenComposeAsync(entries -> {
                Entry active = entries.get(ACTIVE_KEY);
                Entry fence = entries.get(FENCE_KEY);
                if (!java.util.Arrays.equals(fence.value(), termBytes(term)) || driver.activeActorTerm() != term) {
                    return completedFuture(null);
                }
                if (active.value() == null) {
                    cleanup(Set.of());
                    return completedFuture(null);
                }
                LeaseBalanceOperation operation = decode(active.value(), LeaseBalanceOperation.class);
                cleanup(Set.of(key("operation." + operation.operationId), key("request." + operation.requestId)));
                return advance(active, operation, term);
            }, executor);
        } catch (RuntimeException error) {
            ticking.set(false);
            LOG.debug("Primary balancing poll deferred until metadata is available", error);
            return;
        }
        processing.whenComplete((ignored, error) -> {
            if (error != null && !stopped) {
                LOG.warn("Primary balancing poll failed; durable progress will be reconciled", error);
            }
            ticking.set(false);
        });
    }

    private CompletableFuture<Void> advance(Entry active, LeaseBalanceOperation operation, long term) {
        if (operation.formatVersion != 1) {
            return completedFuture(null);
        }
        if (operation.ownerTerm == 0) {
            return save(active, copy(operation, term, "RUNNING", null, operation.plan, false, Map.of()), term, false);
        }
        return snapshot().thenComposeAsync(snapshot -> {
            Map<String, PrimaryGroup> groups = snapshot.groups.stream()
                    .collect(Collectors.toMap(LeaseBalancePlanner::key, Function.identity()));
            boolean interrupted = operation.ownerTerm != term || !"RUNNING".equals(operation.state);
            for (int index = 0; index < operation.plan.moves.size(); index++) {
                LeaseMove move = operation.plan.moves.get(index);
                if ("WAITING".equals(move.state) || "UNKNOWN".equals(move.state)) {
                    PrimaryGroup actual = groups.get(LeaseBalancePlanner.key(move.group));
                    boolean changed = actual != null && "ACTIVE".equals(actual.state)
                            && (actual.leaseStart != move.group.leaseStart || !move.group.holderId.equals(actual.holderId));
                    if (changed) {
                        boolean success = move.targetId.equals(actual.holderId);
                        LeaseBalancePlan plan = replaceMove(operation.plan, index,
                                move(move, success ? "SUCCEEDED" : "FAILED", move.submittedAt, move.oldExpiration,
                                        success ? null : "LEASE_ASSIGNED_TO_OTHER_NODE"));
                        String nextState = interrupted ? operation.ownerTerm != term ? "INTERRUPTED" : operation.state
                                : success ? "RUNNING" : "PARTIAL";
                        String reason = interrupted ? operation.reason == null ? "PD_CHANGED" : operation.reason
                                : success ? null : "LEASE_ASSIGNED_TO_OTHER_NODE";
                        Map<String, Integer> actualCounts = summarize(snapshot, operation.plan.zoneId).zones
                                .getOrDefault(operation.plan.zoneId, Map.of());
                        return save(active, copy(operation, term, nextState, reason, plan, false, actualCounts),
                                term, interrupted || !success);
                    }
                    long elapsed = clock.now().getPhysical() - move.submittedAt;
                    if ("UNKNOWN".equals(move.state) && operation.ownerTerm == term) {
                        return completedFuture(null);
                    }
                    if (interrupted || elapsed >= config.primaryBalanceTimeoutMillis().value()) {
                        LeaseBalancePlan plan = replaceMove(operation.plan, index,
                                move(move, "UNKNOWN", move.submittedAt, move.oldExpiration,
                                        interrupted ? "PD_CHANGED" : "MIGRATION_TIMEOUT"));
                        return save(active, copy(operation, term, interrupted ? "INTERRUPTED" : "PARTIAL",
                                interrupted ? "PD_CHANGED" : "MIGRATION_TIMEOUT", plan, true, Map.of()), term, false);
                    }
                    return completedFuture(null);
                }
            }
            if (interrupted || !config.primaryBalanceEnabled().value()) {
                return save(active, copy(operation, term, "INTERRUPTED", interrupted ? "PD_CHANGED" : "BALANCING_DISABLED",
                        operation.plan, false, Map.of()), term, true);
            }
            for (int index = 0; index < operation.plan.moves.size(); index++) {
                LeaseMove move = operation.plan.moves.get(index);
                if ("PENDING".equals(move.state)) {
                    return submit(active, operation, snapshot, move, index, term);
                }
            }
            Map<String, Integer> actual = summarize(snapshot, operation.plan.zoneId).zones.getOrDefault(operation.plan.zoneId, Map.of());
            return save(active, copy(operation, term, "SUCCEEDED", null, operation.plan, false, actual), term, true);
        }, executor);
    }

    private CompletableFuture<Void> submit(Entry active, LeaseBalanceOperation operation, PrimarySnapshot snapshot,
            LeaseMove move, int index, long term) {
        if (clock.now().getPhysical() - operation.updatedAt > config.primaryBalanceTimeoutMillis().value()) {
            return save(active, copy(operation, term, "PARTIAL", "SUBMISSION_TIMEOUT", operation.plan, false, Map.of()), term, true);
        }
        Map<String, Integer> counts = summarize(snapshot, operation.plan.zoneId).zones.getOrDefault(operation.plan.zoneId, Map.of());
        if (counts.getOrDefault(move.group.holder, 0) - counts.getOrDefault(move.target, 0) < 2) {
            return save(active, copy(operation, term, "PARTIAL", "DISTRIBUTION_CHANGED", operation.plan, false, counts), term, true);
        }
        LeaseBalancePlan single = replacePlanMoves(operation.plan, List.of(move));
        try {
            validate(single, snapshot);
        } catch (DistributionException changed) {
            return save(active, copy(operation, term, "PARTIAL", changed.getMessage(), operation.plan, false, Map.of()), term, true);
        }
        return healthy(List.of(move.group)).thenComposeAsync(healthy -> {
            if (!healthy.getOrDefault(LeaseBalancePlanner.key(move.group), Set.of()).contains(move.target)) {
                return save(active, copy(operation, term, "PARTIAL", "TARGET_NOT_HEALTHY", operation.plan, false, Map.of()), term, true);
            }
            if (driver.activeActorTerm() != term || !config.primaryBalanceEnabled().value()) {
                return completedFuture(null);
            }
            ReplicationGroupId id = groupId(move.group);
            return meta.get(PLACEMENTDRIVER_LEASES_KEY).thenCompose(entry -> {
                if (stopped || driver.activeActorTerm() != term || !config.primaryBalanceEnabled().value()) {
                    return completedFuture(null);
                }
                Lease lease = entry.value() == null ? null : LeaseBatch.fromBytes(entry.value()).leases().stream()
                        .filter(candidate -> candidate.replicationGroupId().equals(id)).findFirst().orElse(null);
                if (lease == null || lease.getStartTime().longValue() != move.group.leaseStart
                        || !move.group.holderId.equals(lease.getLeaseholderId().toString())) {
                    return completedFuture(null);
                }
                long now = clock.now().getPhysical();
                LeaseBalancePlan plan = replaceMove(operation.plan, index,
                        move(move, "WAITING", now, lease.getExpirationTime().getPhysical(), null));
                LeaseBalanceOperation waiting = copy(operation, term, "RUNNING", null, plan, false, Map.of());
                byte[] bytes = encode(waiting);
                Condition guard = and(guard(active, term), and(revision(stableKey(id)).eq(move.group.assignmentRevision),
                        notExists(pendingKey(id))));
                List<Operation> progress = new ArrayList<>();
                progress.add(put(ACTIVE_KEY, bytes));
                progress.add(put(key("operation." + operation.operationId), bytes));
                progress.add(put(key("cooldown." + LeaseBalancePlanner.key(move.group)), Long.toString(now).getBytes(UTF_8)));
                return driver.transferForBalance(lease, move.target, guard, progress).thenAccept(success -> {
                    if (success) {
                        LOG.info("Primary lease transfer submitted [operationId={}, group={}, target={}, term={}]",
                                operation.operationId, LeaseBalancePlanner.key(move.group), move.target, term);
                    }
                });
            });
        }, executor);
    }

    private static Condition guard(Entry active, long term) {
        return and(value(ACTIVE_KEY).eq(active.value()), value(FENCE_KEY).eq(termBytes(term)));
    }

    private CompletableFuture<Void> save(Entry active, LeaseBalanceOperation operation, long term, boolean release) {
        byte[] bytes = encode(operation);
        return meta.invoke(guard(active, term), List.of(release ? remove(ACTIVE_KEY) : put(ACTIVE_KEY, bytes),
                put(key("operation." + operation.operationId), bytes)), List.of(noop())).thenAccept(success -> {
                    if (success) {
                        LeaseBalanceOperation previous = decode(active.value(), LeaseBalanceOperation.class);
                        if (!previous.unresolved && operation.unresolved && "MIGRATION_TIMEOUT".equals(operation.reason)) {
                            timeouts.incrementAndGet();
                        }
                        for (int i = 0; i < operation.plan.moves.size(); i++) {
                            LeaseMove move = operation.plan.moves.get(i);
                            if (!previous.plan.moves.get(i).state.equals(move.state)) {
                                if ("SUCCEEDED".equals(move.state)) {
                                    successes.incrementAndGet();
                                    durationMillis.addAndGet(Math.max(0, operation.updatedAt - move.submittedAt));
                                } else if ("FAILED".equals(move.state)) {
                                    failures.incrementAndGet();
                                }
                            }
                        }
                    }
                });
    }

    private LeaseBalanceOperation copy(LeaseBalanceOperation old, long term, String state, String reason,
            LeaseBalancePlan plan, boolean unresolved, Map<String, Integer> actual) {
        return new LeaseBalanceOperation(1, old.operationId, old.requestId, old.createdAt, clock.now().getPhysical(),
                term, state, reason, plan, unresolved, actual, old.requestedBy);
    }

    private static LeaseMove move(LeaseMove move, String state, long submitted, long expiration, String reason) {
        return new LeaseMove(move.group, move.target, move.targetId, state, submitted, expiration, reason);
    }

    private static LeaseBalancePlan replaceMove(LeaseBalancePlan plan, int index, LeaseMove move) {
        List<LeaseMove> moves = new ArrayList<>(plan.moves);
        moves.set(index, move);
        return replacePlanMoves(plan, moves);
    }

    private static LeaseBalancePlan replacePlanMoves(LeaseBalancePlan plan, List<LeaseMove> moves) {
        return new LeaseBalancePlan(plan.planId, plan.createdAt, plan.expiresAt, plan.zoneId, plan.maxMoves, plan.reason,
                plan.before, plan.after, moves, plan.skipped, plan.limitedByMaxMoves, plan.balanced);
    }

    private static String identifier(String value) {
        if (value == null || !value.matches("[a-zA-Z0-9_-]{1,100}")) {
            throw new DistributionException(400, "Invalid management identifier");
        }
        return value;
    }

    private <T> CompletableFuture<T> read(ByteArray key, Class<T> type, int missingStatus) {
        return meta.get(key).thenApply(entry -> {
            if (entry.value() == null) {
                throw new DistributionException(missingStatus, "MANAGEMENT_RECORD_NOT_FOUND_OR_EXPIRED");
            }
            return decode(entry.value(), type);
        });
    }

    private static byte[] encode(Object value) {
        try {
            return JSON.writeValueAsBytes(value);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Cannot encode primary management record", e);
        }
    }

    private static <T> T decode(byte[] bytes, Class<T> type) {
        try {
            return JSON.readValue(bytes, type);
        } catch (java.io.IOException e) {
            throw new IllegalStateException("Cannot read primary management record", e);
        }
    }

    private void cleanup(Set<ByteArray> protectedKeys) {
        long now = clock.now().getPhysical();
        if (now - lastCleanup < 60_000) {
            return;
        }
        lastCleanup = now;
        meta.prefix(key("")).subscribe(new Subscriber<>() {
            @Override
            public void onSubscribe(Subscription subscription) {
                subscription.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(Entry entry) {
                if (entry.value() == null || entry.timestamp() == null || protectedKeys.contains(new ByteArray(entry.key()))) {
                    return;
                }
                String suffix = new String(entry.key(), UTF_8).substring(PREFIX.length());
                long ttl = suffix.startsWith("snapshot.") || suffix.startsWith("plan.") ? 120_000 : RETENTION;
                if ((suffix.startsWith("snapshot.") || suffix.startsWith("plan.") || suffix.startsWith("operation.")
                        || suffix.startsWith("request.") || suffix.startsWith("cooldown."))
                        && now - entry.timestamp().getPhysical() > ttl) {
                    meta.invoke(revision(new ByteArray(entry.key())).eq(entry.revision()), remove(new ByteArray(entry.key())), noop());
                }
            }

            @Override
            public void onError(Throwable error) {
                LOG.warn("Primary management cleanup failed", error);
            }

            @Override
            public void onComplete() {
            }
        });
    }
}
