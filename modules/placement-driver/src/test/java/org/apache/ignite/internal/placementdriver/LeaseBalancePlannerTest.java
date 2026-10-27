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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.placementdriver.balance.LeaseBalanceOperation;
import org.apache.ignite.internal.placementdriver.balance.LeaseBalancePlan;
import org.apache.ignite.internal.placementdriver.balance.PrimaryGroup;
import org.apache.ignite.internal.placementdriver.balance.PrimarySnapshot;
import org.junit.jupiter.api.Test;

/** Boundary cases missed by floor/ceil-only planning, and durable management value compatibility. */
class LeaseBalancePlannerTest {
    private static final long NOW = 1000;

    @Test
    void balancesBothFloorCeilBoundaryCases() {
        assertCounts(new int[]{4, 2, 2}, List.of(2, 3, 3));
        assertCounts(new int[]{4, 4, 2}, List.of(3, 3, 4));
        assertCounts(new int[]{8, 0, 0}, List.of(2, 3, 3));
        assertCounts(new int[]{4, 19, 9, 0, 0}, List.of(6, 6, 6, 7, 7));
    }

    @Test
    void balancedDistributionDoesNotMove() {
        LeaseBalancePlan plan = plan(snapshot(3, 3, 2), 16, Set.of());
        assertEquals("ALREADY_BALANCED", plan.reason);
        assertTrue(plan.moves.isEmpty());
    }

    @Test
    void respectsMoveLimitAndCooldown() {
        PrimarySnapshot snapshot = snapshot(8, 0, 0);
        LeaseBalancePlan plan = plan(snapshot, 1, Set.of());
        assertEquals(1, plan.moves.size());
        assertTrue(plan.limitedByMaxMoves);
        Set<String> cooling = new HashSet<>();
        snapshot.groups.forEach(g -> cooling.add(LeaseBalancePlanner.key(g)));
        plan = plan(snapshot, 16, cooling);
        assertTrue(plan.moves.isEmpty());
        assertTrue(plan.skipped.values().stream().allMatch("COOLDOWN"::equals));
    }

    @Test
    void cannotMoveToNodeWithoutReplicaOrHealthyState() {
        PrimarySnapshot all = snapshot(8, 0, 0);
        List<PrimaryGroup> groups = new ArrayList<>();
        all.groups.forEach(g -> groups.add(new PrimaryGroup(g.groupType, g.objectId, g.zoneId, g.partitionId,
                g.holder, g.holderId, g.leaseStart, g.leaseExpiration, true, true, "ACTIVE", List.of("n0"), 1, false)));
        PrimarySnapshot constrained = new PrimarySnapshot("s", NOW, 1, 1, 1, "FRESH", true, "ZONE", all.nodes, groups);
        assertTrue(plan(constrained, 16, Set.of()).moves.isEmpty());
        assertTrue(LeaseBalancePlanner.plan(all, 20, 16, Map.of(), Set.of(), NOW).moves.isEmpty());
    }

    @Test
    void excludesPendingAndUnacceptedLeases() {
        PrimarySnapshot all = snapshot(8, 0, 0);
        List<PrimaryGroup> groups = new ArrayList<>();
        all.groups.forEach(g -> groups.add(new PrimaryGroup(g.groupType, g.objectId, g.zoneId, g.partitionId,
                g.holder, g.holderId, g.leaseStart, g.leaseExpiration, true, true, "ACTIVE", g.stablePeers, 1, true)));
        PrimarySnapshot pending = new PrimarySnapshot("s", NOW, 1, 1, 1, "FRESH", true, "ZONE", all.nodes, groups);
        assertTrue(plan(pending, 16, Set.of()).moves.isEmpty());
    }

    @Test
    void missingPrimaryIsNotReportedAsBalanced() {
        PrimarySnapshot base = snapshot(1, 0, 0);
        PrimaryGroup old = base.groups.get(0);
        PrimaryGroup missing = new PrimaryGroup(old.groupType, old.objectId, old.zoneId, old.partitionId,
                null, null, 0, 0, false, false, "MISSING", old.stablePeers, 1, false);
        PrimarySnapshot unavailable = new PrimarySnapshot("s", NOW, 1, 1, 1, "FRESH", true, "ZONE", base.nodes, List.of(missing));
        LeaseBalancePlan plan = plan(unavailable, 16, Set.of());
        assertFalse(plan.balanced);
        assertEquals("PRIMARY_UNAVAILABLE", plan.reason);
        assertEquals("MISSING", plan.skipped.get(LeaseBalancePlanner.key(missing)));
    }

    @Test
    void refusesIncompleteAndInvalidLimits() {
        PrimarySnapshot all = snapshot(8, 0, 0);
        PrimarySnapshot incomplete = new PrimarySnapshot("s", NOW, 1, 1, 1, "FRESH", false, "ZONE", all.nodes, all.groups);
        assertThrows(DistributionException.class, () -> plan(incomplete, 16, Set.of()));
        assertThrows(DistributionException.class, () -> plan(all, 0, Set.of()));
        assertThrows(DistributionException.class, () -> plan(all, 17, Set.of()));
    }

    @Test
    void serializesDurableOperationIncludingFullLeaseToken() throws Exception {
        LeaseBalancePlan plan = plan(snapshot(8, 0, 0), 16, Set.of());
        ObjectMapper mapper = new ObjectMapper();
        LeaseBalanceOperation original = new LeaseBalanceOperation(1, "op", "req", NOW, NOW, 42, "RUNNING", null,
                plan, true, Map.of());
        LeaseBalanceOperation restored = mapper.readValue(mapper.writeValueAsBytes(original), LeaseBalanceOperation.class);
        assertEquals(plan.moves.get(0).group.leaseStart, restored.plan.moves.get(0).group.leaseStart);
        assertEquals(42, restored.ownerTerm);
        assertEquals("internal", restored.requestedBy);
        assertTrue(restored.unresolved);
        assertFalse(restored.plan.moves.isEmpty());
    }

    @Test
    void summaryIncludesZeroPrimaryAndDeduplicatesGroupsInsteadOfTables() {
        PrimarySnapshot snapshot = snapshot(8, 0, 0);
        var summary = PrimaryDistributionManager.summarize(snapshot, 20);
        assertEquals(8, summary.totalGroups);
        assertEquals(3, summary.nodes.size());
        assertEquals(0, summary.nodes.get(1).primaryCount);
        assertEquals(8, summary.nodes.get(1).peerCount);
        assertEquals(8, summary.nodes.stream().mapToInt(n -> n.primaryCount).sum());
    }

    private static void assertCounts(int[] counts, List<Integer> expected) {
        LeaseBalancePlan plan = plan(snapshot(counts), 16, Set.of());
        List<Integer> actual = new ArrayList<>(plan.after.values());
        actual.sort(Integer::compareTo);
        assertEquals(expected, actual);
        Map<String, Integer> progressive = new HashMap<>(plan.before);
        long previous = score(progressive);
        Set<String> moved = new HashSet<>();
        for (var move : plan.moves) {
            assertTrue(moved.add(LeaseBalancePlanner.key(move.group)));
            assertTrue(move.group.stablePeers.contains(move.target));
            progressive.merge(move.group.holder, -1, Integer::sum);
            progressive.merge(move.target, 1, Integer::sum);
            assertTrue(score(progressive) < previous);
            previous = score(progressive);
        }
        assertEquals(plan.after, progressive);
    }

    private static long score(Map<String, Integer> counts) {
        return counts.values().stream().mapToLong(c -> (long) c * c).sum();
    }

    private static LeaseBalancePlan plan(PrimarySnapshot snapshot, int maxMoves, Set<String> cooling) {
        Map<String, Set<String>> healthy = new HashMap<>();
        snapshot.groups.forEach(g -> healthy.put(LeaseBalancePlanner.key(g), Set.copyOf(g.stablePeers)));
        return LeaseBalancePlanner.plan(snapshot, 20, maxMoves, healthy, cooling, NOW);
    }

    private static PrimarySnapshot snapshot(int... counts) {
        Map<String, String> nodes = new java.util.TreeMap<>();
        for (int node = 0; node < counts.length; node++) {
            nodes.put("n" + node, "id" + node);
        }
        List<PrimaryGroup> groups = new ArrayList<>();
        int part = 0;
        for (int node = 0; node < counts.length; node++) {
            for (int i = 0; i < counts[node]; i++) {
                groups.add(new PrimaryGroup("ZONE", 20, 20, part++, "n" + node, "id" + node,
                        new HybridTimestamp(500, 7).longValue(), 2000, true, true, "ACTIVE", new ArrayList<>(nodes.keySet()), 1, false));
            }
        }
        return new PrimarySnapshot("s", NOW, 1, 1, 1, "FRESH", true, "ZONE", nodes, groups);
    }
}
