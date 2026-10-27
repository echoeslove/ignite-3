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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import org.apache.ignite.internal.placementdriver.balance.LeaseBalancePlan;
import org.apache.ignite.internal.placementdriver.balance.LeaseMove;
import org.apache.ignite.internal.placementdriver.balance.PrimaryGroup;
import org.apache.ignite.internal.placementdriver.balance.PrimarySnapshot;

/** Deterministic, assignment-constrained direct-move planner. It never changes assignments. */
public final class LeaseBalancePlanner {
    private LeaseBalancePlanner() {
    }

    /** A stable textual group key, independent of a node's startup instance. */
    public static String key(PrimaryGroup group) {
        return group.groupType + ":" + group.objectId + ":" + group.partitionId;
    }

    /** Produces a preview. Every move strictly decreases the sum of squared primary counts. */
    public static LeaseBalancePlan plan(PrimarySnapshot snapshot, int zoneId, int maxMoves,
            Map<String, Set<String>> healthy, Set<String> cooling, long now) {
        if (maxMoves < 1 || maxMoves > 16) {
            throw new DistributionException(400, "maxMoves must be between 1 and 16");
        }
        if (!snapshot.complete || !"FRESH".equals(snapshot.freshness)) {
            throw new DistributionException(503, "INCOMPLETE_OR_STALE_DISTRIBUTION");
        }
        Map<String, Integer> counts = new TreeMap<>();
        List<PrimaryGroup> groups = new ArrayList<>();
        Map<String, String> skipped = new LinkedHashMap<>();
        for (PrimaryGroup group : snapshot.groups) {
            if (group.zoneId != zoneId) {
                continue;
            }
            groups.add(group);
            for (String peer : group.stablePeers) {
                if (snapshot.nodes.containsKey(peer)) {
                    counts.putIfAbsent(peer, 0);
                }
            }
            if (group.accepted && group.leaseExpiration > now
                    && org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestamp(group.leaseStart).getPhysical() <= now
                    && group.holder != null) {
                counts.merge(group.holder, 1, Integer::sum);
            }
        }
        groups.sort(Comparator.comparing(LeaseBalancePlanner::key));
        Map<String, Integer> before = new TreeMap<>(counts);
        List<LeaseMove> moves = new ArrayList<>();
        Set<String> moved = new java.util.HashSet<>();
        while (moves.size() < maxMoves) {
            PrimaryGroup bestGroup = null;
            String bestTarget = null;
            int bestDifference = 1;
            for (PrimaryGroup group : groups) {
                String groupKey = key(group);
                if (moved.contains(groupKey)) {
                    continue;
                }
                String reason = !"ACTIVE".equals(group.state) ? group.state
                        : group.pending ? "REBALANCING"
                        : !group.prolongable ? "LEASE_TRANSFER_IN_PROGRESS"
                        : cooling.contains(groupKey) ? "COOLDOWN" : null;
                if (reason != null) {
                    skipped.put(groupKey, reason);
                    continue;
                }
                boolean healthyTarget = false;
                for (String target : new java.util.TreeSet<>(group.stablePeers)) {
                    if (target.equals(group.holder) || !snapshot.nodes.containsKey(target)
                            || !healthy.getOrDefault(groupKey, Set.of()).contains(target)) {
                        continue;
                    }
                    healthyTarget = true;
                    int difference = counts.getOrDefault(group.holder, 0) - counts.getOrDefault(target, 0);
                    if (difference > bestDifference) {
                        bestDifference = difference;
                        bestGroup = group;
                        bestTarget = target;
                    }
                }
                if (!healthyTarget) {
                    skipped.put(groupKey, "NO_HEALTHY_TARGET");
                }
            }
            if (bestGroup == null) {
                break;
            }
            moves.add(new LeaseMove(bestGroup, bestTarget, snapshot.nodes.get(bestTarget), "PENDING", 0, 0, null));
            moved.add(key(bestGroup));
            counts.merge(bestGroup.holder, -1, Integer::sum);
            counts.merge(bestTarget, 1, Integer::sum);
        }
        int min = counts.values().stream().mapToInt(Integer::intValue).min().orElse(0);
        int max = counts.values().stream().mapToInt(Integer::intValue).max().orElse(0);
        boolean available = groups.stream().allMatch(group -> "ACTIVE".equals(group.state));
        boolean balanced = max - min <= 1 && available;
        String reason = moves.isEmpty() ? (!available ? "PRIMARY_UNAVAILABLE"
                : balanced ? "ALREADY_BALANCED" : "NO_IMPROVING_DIRECT_MOVE") : "MOVES_PLANNED";
        return new LeaseBalancePlan(UUID.randomUUID().toString(), now, now + 60_000, zoneId, maxMoves,
                reason, before, counts, moves, skipped, moves.size() == maxMoves && !balanced, balanced);
    }
}
