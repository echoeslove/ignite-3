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

package org.apache.ignite.internal.network.recovery;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.DefaultChannelProgressivePromise;
import io.netty.util.concurrent.EventExecutor;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.network.ConstantClusterIdSupplier;
import org.apache.ignite.internal.network.NetworkMessagesFactory;
import org.apache.ignite.internal.network.OutNetworkObject;
import org.apache.ignite.internal.network.RecipientLeftException;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.network.handshake.NoOpHandshakeEventLoopSwitcher;
import org.apache.ignite.internal.network.netty.ChannelCreationListener;
import org.apache.ignite.internal.network.recovery.message.HandshakeRejectionReason;
import org.apache.ignite.internal.network.recovery.message.HandshakeStartMessage;
import org.apache.ignite.internal.network.recovery.message.HandshakeStartV2Message;
import org.apache.ignite.internal.version.DefaultIgniteProductVersionSource;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

class HandshakeStaleRecoveryTest {
    private final NetworkMessagesFactory messages = new NetworkMessagesFactory();
    private final UUID clusterId = UUID.randomUUID();
    private final ClusterNodeImpl local = new ClusterNodeImpl(UUID.randomUUID(), "local", new NetworkAddress("localhost", 1000));
    private final ClusterNodeImpl remote = new ClusterNodeImpl(UUID.randomUUID(), "remote", new NetworkAddress("localhost", 1001));
    private final TopologyService topology = mock(TopologyService.class);
    private final FailureProcessor processor = mock(FailureProcessor.class);
    private final Channel channel = mock(Channel.class);
    private final ChannelHandlerContext context = mock(ChannelHandlerContext.class);
    private final EventExecutor executor = mock(EventExecutor.class);
    private final DefaultIgniteProductVersionSource version = new DefaultIgniteProductVersionSource();

    @BeforeEach
    void setUp() {
        when(topology.logicalTopologyVersion()).thenReturn(5L);
        when(context.channel()).thenReturn(channel);
        when(context.executor()).thenReturn(executor);
        when(executor.inEventLoop()).thenReturn(true);
        when(channel.writeAndFlush(any())).thenAnswer(ignored -> new DefaultChannelProgressivePromise(channel, executor).setSuccess());
    }

    private RecoveryAcceptorHandshakeManager acceptor() {
        RecoveryAcceptorHandshakeManager manager = new RecoveryAcceptorHandshakeManager(local, messages,
                mock(RecoveryDescriptorProvider.class), new NoOpHandshakeEventLoopSwitcher(), ignored -> true,
                new ConstantClusterIdSupplier(clusterId), mock(ChannelCreationListener.class), () -> false, version)
                .withStaleRecovery(topology, processor);
        manager.onInit(context);
        manager.onConnectionOpen();
        return manager;
    }

    @Test
    void legacyProbeReceivesUnchangedLegacyStart() {
        RecoveryAcceptorHandshakeManager manager = acceptor();
        verifyNoInteractions(channel);
        manager.onMessage(messages.probeMessage().dummy((byte) 0).build());
        ArgumentCaptor<OutNetworkObject> sent = ArgumentCaptor.forClass(OutNetworkObject.class);
        verify(channel).writeAndFlush(sent.capture());
        assertEquals(3, sent.getValue().networkMessage().messageType());
        assertInstanceOf(HandshakeStartMessage.class, sent.getValue().networkMessage());
    }

    @ParameterizedTest
    @ValueSource(longs = {0, 4, 5, 6})
    void acceptorOnlySelectsItselfWhenStrictlyBehind(long peerVersion) {
        RecoveryAcceptorHandshakeManager manager = acceptor();
        manager.onMessage(messages.probeMessage().dummy(HandshakeManagerUtils.TOPOLOGY_VERSION_CAPABILITY).build());
        manager.onMessage(messages.handshakeStartResponseV2Message().clientNode(HandshakeManagerUtils.clusterNodeToMessage(remote))
                .connectionId((short) 0).receivedCount(0).topologyVersion(peerVersion).clientClusterId(clusterId)
                .productName(version.productName()).productVersion(version.productVersion().toString()).build());
        if (peerVersion > 5) {
            verify(processor).process(any());
        } else {
            verifyNoInteractions(processor);
        }
    }

    @Test
    void capabilityProbeReceivesSeparateV2WireType() {
        RecoveryAcceptorHandshakeManager manager = acceptor();
        manager.onMessage(messages.probeMessage().dummy(HandshakeManagerUtils.TOPOLOGY_VERSION_CAPABILITY).build());
        ArgumentCaptor<OutNetworkObject> sent = ArgumentCaptor.forClass(OutNetworkObject.class);
        verify(channel).writeAndFlush(sent.capture());
        HandshakeStartV2Message start = assertInstanceOf(HandshakeStartV2Message.class, sent.getValue().networkMessage());
        assertEquals(13, start.messageType());
        assertEquals(5, start.topologyVersion());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void initiatorIgnoresForeignClusterAndLegacyTopology(boolean foreignCluster) {
        RecoveryInitiatorHandshakeManager manager = new RecoveryInitiatorHandshakeManager(local, (short) 0,
                mock(RecoveryDescriptorProvider.class), new NoOpHandshakeEventLoopSwitcher(), ignored -> true,
                new ConstantClusterIdSupplier(clusterId), mock(ChannelCreationListener.class), () -> false, version)
                .withStaleRecovery(topology, processor);
        manager.onInit(context);
        HandshakeStartMessage start = foreignCluster ? messages.handshakeStartV2Message()
                .serverNode(HandshakeManagerUtils.clusterNodeToMessage(remote)).serverClusterId(UUID.randomUUID())
                .productName(version.productName()).productVersion(version.productVersion().toString()).topologyVersion(99).build()
                : messages.handshakeStartMessage().serverNode(HandshakeManagerUtils.clusterNodeToMessage(remote))
                        .serverClusterId(clusterId).productName(version.productName())
                        .productVersion(version.productVersion().toString()).build();
        manager.onMessage(start);
        verifyNoInteractions(processor);
    }

    @Test
    void staleRejectionBecomesRecipientLeftWithoutRestarting() {
        RecoveryInitiatorHandshakeManager manager = new RecoveryInitiatorHandshakeManager(local, (short) 0,
                mock(RecoveryDescriptorProvider.class), new NoOpHandshakeEventLoopSwitcher(), ignored -> false,
                new ConstantClusterIdSupplier(clusterId), mock(ChannelCreationListener.class), () -> false, version)
                .withStaleRecovery(topology, processor);
        manager.onInit(context);
        manager.onMessage(messages.handshakeRejectedMessage().reasonString(HandshakeRejectionReason.STALE_LAUNCH_ID.name())
                .message("stale").build());
        CompletionException error = assertThrows(CompletionException.class, () -> manager.localHandshakeFuture().join());
        assertInstanceOf(RecipientLeftException.class, error.getCause());
        verifyNoInteractions(processor);
    }

    @ParameterizedTest
    @ValueSource(longs = {0, 4, 5, 6})
    void initiatorOnlySelectsItselfWhenStrictlyBehind(long peerVersion) {
        RecoveryInitiatorHandshakeManager manager = new RecoveryInitiatorHandshakeManager(local, (short) 0,
                mock(RecoveryDescriptorProvider.class), new NoOpHandshakeEventLoopSwitcher(), ignored -> true,
                new ConstantClusterIdSupplier(clusterId), mock(ChannelCreationListener.class), () -> false, version)
                .withStaleRecovery(topology, processor);
        manager.onInit(context);
        manager.onMessage(messages.handshakeStartV2Message().serverNode(HandshakeManagerUtils.clusterNodeToMessage(remote))
                .serverClusterId(clusterId).productName(version.productName())
                .productVersion(version.productVersion().toString()).topologyVersion(peerVersion).build());
        if (peerVersion > 5) {
            verify(processor).process(any());
        } else {
            verifyNoInteractions(processor);
        }
    }

    @Test
    void legacyStaleResponseNeverSelectsAnExit() {
        RecoveryAcceptorHandshakeManager manager = acceptor();
        manager.onMessage(messages.probeMessage().dummy((byte) 0).build());
        manager.onMessage(messages.handshakeStartResponseMessage().clientNode(HandshakeManagerUtils.clusterNodeToMessage(remote))
                .connectionId((short) 0).receivedCount(0).build());
        verifyNoInteractions(processor);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void unknownOrForeignClusterCannotSelectAnExit(boolean foreign) {
        RecoveryAcceptorHandshakeManager manager = acceptor();
        manager.onMessage(messages.probeMessage().dummy(HandshakeManagerUtils.TOPOLOGY_VERSION_CAPABILITY).build());
        manager.onMessage(messages.handshakeStartResponseV2Message().clientNode(HandshakeManagerUtils.clusterNodeToMessage(remote))
                .connectionId((short) 0).receivedCount(0).topologyVersion(99).clientClusterId(foreign ? UUID.randomUUID() : null)
                .productName(version.productName()).productVersion(version.productVersion().toString()).build());
        verifyNoInteractions(processor);
    }
}
