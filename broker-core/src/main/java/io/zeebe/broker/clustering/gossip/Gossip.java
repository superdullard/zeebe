package io.zeebe.broker.clustering.gossip;

import static io.zeebe.clustering.gossip.PeerState.ALIVE;
import static io.zeebe.clustering.gossip.PeerState.SUSPECT;
import static io.zeebe.util.buffer.BufferUtil.cloneBuffer;

import java.util.concurrent.CompletableFuture;

import org.agrona.DirectBuffer;
import io.zeebe.broker.clustering.gossip.data.*;
import io.zeebe.broker.clustering.gossip.handler.GossipFragmentHandler;
import io.zeebe.broker.clustering.gossip.protocol.GossipController;
import io.zeebe.broker.clustering.handler.Topology;
import io.zeebe.clustering.gossip.PeerState;
import io.zeebe.clustering.gossip.RaftMembershipState;
import io.zeebe.transport.SocketAddress;
import io.zeebe.util.DeferredCommandContext;
import io.zeebe.util.actor.Actor;

public class Gossip implements Actor
{
    public static final String GOSSIP_FILE_NAME = "gossip.zeebe";

    private final Peer peer;
    private final PeerList peers;

    private final GossipController gossipController;
    private final GossipFragmentHandler fragmentHandler;

    private final DeferredCommandContext commandContext;

    public Gossip(final GossipContext context)
    {
        this.peer = context.getLocalPeer();
        this.peers = context.getPeers();

        this.gossipController = new GossipController(context);
        this.fragmentHandler = new GossipFragmentHandler(this, context.getSubscription());

        this.commandContext = new DeferredCommandContext();
    }

    @Override
    public String name()
    {
        return "gossip";
    }

    @Override
    public int getPriority(long now)
    {
        return PRIORITY_LOW;
    }

    public void open()
    {
        final PeerListIterator iterator = peers.iterator();
        while (iterator.hasNext())
        {
            final Peer peer = iterator.next();
            if (peer.state() == SUSPECT)
            {
                peer.state(ALIVE);
            }

            peers.set(iterator.position(), peer);
        }

        gossipController.open();
    }

    public void close()
    {
        gossipController.close();
    }

    @Override
    public int doWork() throws Exception
    {
        int workcount = 0;

        workcount += commandContext.doWork();

        workcount += gossipController.doWork();
        workcount += fragmentHandler.doWork();

        return workcount;
    }

    public Peer peer()
    {
        return peer;
    }

    public PeerList peers()
    {
        return peers;
    }

    public int onGossipRequest(final DirectBuffer buffer, final int offset, final int length, final int channelId, final long connectionId, final long requestId)
    {
        return gossipController.onGossipRequest(buffer, offset, length, channelId, connectionId, requestId);
    }

    public int onProbeRequest(final DirectBuffer buffer, final int offset, final int length, final int channelId, final long connectionId, final long requestId)
    {
        return gossipController.onProbeRequest(buffer, offset, length, channelId, connectionId, requestId);
    }

    public static String fileName(String directory)
    {
        return directory + GOSSIP_FILE_NAME;
    }

    public CompletableFuture<Topology> getTopology()
    {
        return commandContext.runAsync(future -> {
            final Topology topology = new Topology();

            // force update local peer in peer list to sync local raft changes
            peers.update(peer);

            for (final Peer peer : peers)
            {
                if (PeerState.ALIVE == peer.state())
                {
                    final SocketAddress clientEndpoint = peer.clientEndpoint();

                    topology.brokers().add()
                            .setHost(clientEndpoint.getHostBuffer(), 0, clientEndpoint.hostLength())
                            .setPort(clientEndpoint.port());

                    for (final RaftMembership raftMembership : peer.raftMemberships())
                    {
                        if (RaftMembershipState.LEADER == raftMembership.state())
                        {
                            // TODO(menski): creates garbage
                            final DirectBuffer topicName = cloneBuffer(raftMembership.topicNameBuffer(), 0, raftMembership.topicNameLength());

                            topology.topicLeaders().add()
                                    .setTopicName(topicName, 0, topicName.capacity())
                                    .setPartitionId(raftMembership.partitionId())
                                    .setHost(clientEndpoint.getHostBuffer(), 0, clientEndpoint.hostLength())
                                    .setPort(clientEndpoint.port());
                        }
                    }
                }
            }

            future.complete(topology);
        });
    }

}