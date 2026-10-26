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

package org.apache.ignite.internal.tx.impl;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.tx.impl.PersistentTxStateVacuumizer.VacuumizableTx;
import org.junit.jupiter.api.Test;

/** Verifies failed persistent vacuum retains work for retry instead of reporting successful cleanup. */
class PersistentTxStateVacuumizerTest extends BaseIgniteAbstractTest {
    @Test
    void failedVacuumCanBeRetriedWithoutLosingTransactionState() {
        ReplicaService replicaService = mock(ReplicaService.class);
        InternalClusterNode node = mock(InternalClusterNode.class);
        ClockService clock = mock(ClockService.class);
        PlacementDriver placementDriver = mock(PlacementDriver.class);
        ReplicaMeta lease = mock(ReplicaMeta.class);
        UUID nodeId = UUID.randomUUID();
        HybridTimestamp now = new HybridTimestamp(100, 0);
        when(node.id()).thenReturn(nodeId);
        when(clock.now()).thenReturn(now);
        when(lease.getLeaseholderId()).thenReturn(nodeId);
        when(lease.getStartTime()).thenReturn(now);
        when(placementDriver.getPrimaryReplica(any(), any())).thenReturn(completedFuture(lease));
        when(replicaService.invoke(any(InternalClusterNode.class), any()))
                .thenReturn(failedFuture(new IllegalStateException("injected vacuum failure")))
                .thenReturn(completedFuture(null));

        var vacuumizer = new PersistentTxStateVacuumizer(replicaService, node, clock, placementDriver);
        UUID txId = UUID.randomUUID();
        var txs = Map.<org.apache.ignite.internal.replicator.ReplicationGroupId, Set<VacuumizableTx>>of(
                new ZonePartitionId(1, 0), Set.of(new VacuumizableTx(txId, 1L)));

        var failed = vacuumizer.vacuumPersistentTxStates(txs).join();
        assertTrue(failed.txnsToVacuum.isEmpty());
        assertEquals(0, failed.vacuumizedPersistentTxnStatesCount);

        var retried = vacuumizer.vacuumPersistentTxStates(txs).join();
        assertEquals(Set.of(txId), retried.txnsToVacuum);
        assertEquals(1, retried.vacuumizedPersistentTxnStatesCount);
    }
}
