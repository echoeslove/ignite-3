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

package org.apache.ignite.internal.raft.server.impl;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.ignite.raft.jraft.Node;
import org.apache.ignite.raft.jraft.core.State;
import org.junit.jupiter.api.Test;

/** Tests the local Raft state gate used by read-only replica routing. */
class JraftServerImplReadEligibilityTest {
    @Test
    void healthyFollowerCanServeLocalRead() {
        Node node = mock(Node.class);
        when(node.getNodeState()).thenReturn(State.STATE_FOLLOWER);
        when(node.lastLogIndex()).thenReturn(1L);

        assertTrue(JraftServerImpl.isHealthyForRead(node));
    }

    @Test
    void snapshotInstallationCannotServeLocalRead() {
        Node node = mock(Node.class);
        when(node.getNodeState()).thenReturn(State.STATE_FOLLOWER);
        when(node.lastLogIndex()).thenReturn(1L);
        when(node.isInstallingSnapshot()).thenReturn(true);

        assertFalse(JraftServerImpl.isHealthyForRead(node));
    }

    @Test
    void uninitializedOrFailedNodeCannotServeLocalRead() {
        for (State state : new State[] {State.STATE_UNINITIALIZED, State.STATE_ERROR, State.STATE_SHUTDOWN}) {
            Node node = mock(Node.class);
            when(node.getNodeState()).thenReturn(state);

            assertFalse(JraftServerImpl.isHealthyForRead(node));
        }

        Node node = mock(Node.class);
        when(node.getNodeState()).thenReturn(State.STATE_FOLLOWER);

        assertFalse(JraftServerImpl.isHealthyForRead(node));
        assertFalse(JraftServerImpl.isHealthyForRead(null));
    }
}
