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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.apache.ignite.internal.raft.server.RaftGroupOptions;
import org.apache.ignite.internal.raft.storage.GroupStoragesDestructionIntents;
import org.apache.ignite.internal.raft.storage.LogStorageFactory;
import org.apache.ignite.internal.raft.storage.impl.LogStorageException;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.raft.jraft.core.NodeImpl;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.option.NodeOptions;
import org.apache.ignite.raft.jraft.option.RaftMetaStorageOptions;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.rpc.impl.RaftGroupEventsClientListener;
import org.apache.ignite.raft.jraft.storage.impl.LocalRaftMetaStorage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Snapshot reset must not erase election state, including at intermediate failure points. */
class JraftServerImplStorageResetTest {
    @TempDir
    Path workDir;

    private final LogStorageFactory logFactory = mock(LogStorageFactory.class);
    private final GroupStoragesDestructionIntents intents = mock(GroupStoragesDestructionIntents.class);
    private final RaftNodeId nodeId = new RaftNodeId(new ZonePartitionId(20, 1), new Peer("local"));
    private JraftServerImpl server;
    private RaftGroupOptions options;
    private Path nodePath;
    private LocalRaftMetaStorage meta;

    @BeforeEach
    void setUp() throws Exception {
        ClusterService service = mock(ClusterService.class);
        when(service.nodeName()).thenReturn("meta-reset-test");
        server = new JraftServerImpl(service, new NodeOptions(), mock(RaftGroupEventsClientListener.class),
                mock(FailureManager.class), intents,
                new GroupStoragesContextResolver(id -> "partitions", Map.of("partitions", workDir),
                        Map.of("partitions", logFactory)));
        options = RaftGroupOptions.forPersistentStores();
        options.serverDataPath(workDir);
        options.setLogStorageFactory(logFactory);
        nodePath = JraftServerImpl.getServerDataPath(workDir, nodeId);
        meta = openMeta();
        assertTrue(meta.setTermAndVotedFor(7, new PeerId("voted-peer")));
        Files.createDirectories(nodePath.resolve("snapshot"));
        Files.writeString(nodePath.resolve("snapshot/old-snapshot"), "old snapshot");
    }

    private LocalRaftMetaStorage openMeta() {
        var storage = new LocalRaftMetaStorage(nodePath.resolve("meta").toString(), new RaftOptions());
        var metaOptions = new RaftMetaStorageOptions();
        metaOptions.setNode(new NodeImpl("meta-reset-test", new PeerId("local")));
        assertTrue(storage.init(metaOptions));
        return storage;
    }

    @Test
    void resetPreservesTermAndVoteWithoutGracefulShutdown() {
        server.resetRaftNodeStoragesForSnapshot(nodeId, options);

        // Deliberately do not call meta.shutdown(): reload the durable state as after an abrupt process death.
        LocalRaftMetaStorage reloaded = openMeta();
        assertEquals(7, reloaded.getTerm());
        assertEquals(new PeerId("voted-peer"), reloaded.getVotedFor());
        assertFalse(Files.exists(nodePath.resolve("snapshot")));
        verify(logFactory).destroyLogStorage(nodeId.nodeIdStringForStorage());
        verifyNoInteractions(intents);
    }

    @Test
    void resetDoesNotOverwriteNewerTermSavedDuringLogReset() {
        doAnswer(invocation -> {
            assertTrue(meta.setTermAndVotedFor(8, new PeerId("new-peer")));
            return null;
        }).when(logFactory).destroyLogStorage(nodeId.nodeIdStringForStorage());

        server.resetRaftNodeStoragesForSnapshot(nodeId, options);

        LocalRaftMetaStorage reloaded = openMeta();
        assertEquals(8, reloaded.getTerm());
        assertEquals(new PeerId("new-peer"), reloaded.getVotedFor());
    }

    @Test
    void failedLogResetPreservesMetaAndSnapshotAndPropagatesError() {
        doThrow(LogStorageException.class).when(logFactory).destroyLogStorage(nodeId.nodeIdStringForStorage());

        assertThrows(IgniteInternalException.class, () -> server.resetRaftNodeStoragesForSnapshot(nodeId, options));

        assertEquals(7, openMeta().getTerm());
        assertTrue(Files.exists(nodePath.resolve("snapshot/old-snapshot")));
        verifyNoInteractions(intents);
    }

    @Test
    void permanentDestructionStillRemovesMetaAndSnapshot() {
        server.destroyRaftNodeStorages(nodeId, options);

        assertFalse(Files.exists(nodePath));
        verify(logFactory).destroyLogStorage(nodeId.nodeIdStringForStorage());
    }

    @Test
    void abruptFailureBetweenLogAndSnapshotResetKeepsDurableElectionState() {
        doAnswer(invocation -> {
            assertTrue(Files.exists(nodePath.resolve("meta/raft_meta")));
            throw new AssertionError("simulated abrupt interruption after log reset");
        }).when(logFactory).destroyLogStorage(nodeId.nodeIdStringForStorage());

        assertThrows(AssertionError.class, () -> server.resetRaftNodeStoragesForSnapshot(nodeId, options));

        LocalRaftMetaStorage reloaded = openMeta();
        assertEquals(7, reloaded.getTerm());
        assertEquals(new PeerId("voted-peer"), reloaded.getVotedFor());
    }
}
