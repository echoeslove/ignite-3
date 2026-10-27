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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.placementdriver.PlacementDriverManager.PLACEMENTDRIVER_LEASES_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.components.NodeProperties;
import org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.partitiondistribution.Assignment;
import org.apache.ignite.internal.partitiondistribution.Assignments;
import org.apache.ignite.internal.placementdriver.balance.LeaseBalancePlan;
import org.apache.ignite.internal.placementdriver.leases.Lease;
import org.apache.ignite.internal.placementdriver.leases.LeaseBatch;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.configuration.ReplicationConfiguration;
import org.apache.ignite.internal.table.distributed.disaster.DisasterRecoveryManager;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.api.Test;

/** Observation must expose missing leases and distinguish restarted holders without relying on name equality. */
class PrimaryDistributionObservationTest {
    @Test
    void enumeratesUnleasedGroupsAndRejectsOldStartupInstance() {
        Fixture fixture = new Fixture();
        var snapshot = fixture.manager.snapshot().join();
        assertTrue(snapshot.complete);
        assertEquals(2, snapshot.groups.size());
        assertEquals("INSTANCE_MISMATCH", snapshot.groups.get(0).state);
        assertEquals("MISSING", snapshot.groups.get(1).state);
        var summary = PrimaryDistributionManager.summarize(snapshot, 20);
        assertEquals(1, summary.withoutPrimary);
        assertEquals(1, summary.nodes.get(0).primaryCount);
        assertEquals(0, summary.nodes.get(0).onlinePrimaryCount);
    }

    @Test
    void missingAssignmentsMakeSnapshotIncompleteInsteadOfHidingTheGroup() {
        Fixture fixture = new Fixture();
        fixture.values.remove(ZoneRebalanceUtil.stablePartAssignmentsKey(new ZonePartitionId(20, 1)));
        var snapshot = fixture.manager.snapshot().join();
        assertFalse(snapshot.complete);
        assertEquals(2, snapshot.groups.size());
    }

    @Test
    void refusesExecutionWhenAnOnlineNodeHasNoHotfixCapabilityMarker() throws Exception {
        Fixture fixture = new Fixture();
        LeaseBalancePlan plan = new LeaseBalancePlan("plan", 1000, 60000, 20, 1, "ALREADY_BALANCED", Map.of(), Map.of(),
                List.of(), Map.of(), false, true);
        fixture.values.put(ByteArray.fromString("primary.balance.v1.plan.plan"), new ObjectMapper().writeValueAsBytes(plan));
        var error = assertThrows(java.util.concurrent.CompletionException.class, () -> fixture.manager.execute("plan", "request").join());
        assertTrue(error.getCause() instanceof DistributionException);
        assertEquals(409, ((DistributionException) error.getCause()).status());
        assertTrue(error.getCause().getMessage().contains("HOTFIX6_REQUIRED_ON_ALL_NODES"));
    }

    private static class Fixture {
        final Map<ByteArray, byte[]> values = new HashMap<>();
        final PrimaryDistributionManager manager;

        Fixture() {
            MetaStorageManager meta = mock(MetaStorageManager.class);
            CatalogManager catalogs = mock(CatalogManager.class);
            Catalog catalog = mock(Catalog.class);
            CatalogZoneDescriptor zone = mock(CatalogZoneDescriptor.class);
            NodeProperties properties = mock(NodeProperties.class);
            LogicalTopologyService topology = mock(LogicalTopologyService.class);
            ClockService clock = mock(ClockService.class);
            when(properties.colocationEnabled()).thenReturn(true);
            when(catalogs.activeCatalog(anyLong())).thenReturn(catalog);
            when(catalog.version()).thenReturn(1);
            when(catalog.zones()).thenReturn(List.of(zone));
            when(zone.id()).thenReturn(20);
            when(zone.partitions()).thenReturn(2);
            LogicalNode restarted = new LogicalNode(UUID.randomUUID(), "n0", NetworkAddress.from("localhost:29344"));
            LogicalTopologySnapshot top = new LogicalTopologySnapshot(1, List.of(restarted));
            when(topology.logicalTopologyOnLeader()).thenReturn(completedFuture(top));
            when(topology.localLogicalTopology()).thenReturn(top);
            HybridTimestamp now = new HybridTimestamp(1000, 0);
            when(clock.now()).thenReturn(now);
            when(clock.current()).thenReturn(now);
            when(clock.before(any(), any())).thenAnswer(i -> ((HybridTimestamp) i.getArgument(0))
                    .compareTo(i.getArgument(1)) < 0);
            Lease lease = new Lease("n0", UUID.randomUUID(), new HybridTimestamp(500, 7), new HybridTimestamp(2000, 0),
                    true, true, null, new ZonePartitionId(20, 0));
            values.put(PLACEMENTDRIVER_LEASES_KEY, new LeaseBatch(List.of(lease)).bytes());
            for (int part = 0; part < 2; part++) {
                values.put(ZoneRebalanceUtil.stablePartAssignmentsKey(new ZonePartitionId(20, part)),
                        Assignments.of(Set.of(Assignment.forPeer("n0")), 1).toBytes());
            }
            when(meta.getAll(any())).thenAnswer(i -> {
                Map<ByteArray, Entry> entries = new HashMap<>();
                for (ByteArray key : (Set<ByteArray>) i.getArgument(0)) {
                    Entry entry = mock(Entry.class);
                    when(entry.value()).thenReturn(values.get(key));
                    when(entry.revision()).thenReturn(1L);
                    entries.put(key, entry);
                }
                return completedFuture(entries);
            });
            when(meta.get(any(ByteArray.class))).thenAnswer(i -> {
                Entry entry = mock(Entry.class);
                when(entry.value()).thenReturn(values.get((ByteArray) i.getArgument(0)));
                return completedFuture(entry);
            });
            ReplicationConfiguration configuration = mock(ReplicationConfiguration.class);
            org.apache.ignite.configuration.ConfigurationValue<Boolean> enabled =
                    mock(org.apache.ignite.configuration.ConfigurationValue.class);
            when(enabled.value()).thenReturn(true);
            when(configuration.primaryBalanceEnabled()).thenReturn(enabled);
            manager = new PrimaryDistributionManager(meta, mock(PlacementDriverManager.class), catalogs, topology,
                    mock(ClusterService.class), clock, properties, configuration,
                    mock(DisasterRecoveryManager.class), mock(MetricManager.class));
        }
    }
}
