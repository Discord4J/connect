package discord4j.connect.rsocket.gateway;

import discord4j.connect.rsocket.gateway.RSocketShardLeaderServer;

import java.net.InetSocketAddress;

public class ExampleRSocketPayloadServer {

    public static void main(String[] args) {
        InetSocketAddress serverAddress = new InetSocketAddress(33444);
        RSocketShardLeaderServer server = new RSocketShardLeaderServer(serverAddress);
        server.start().block().onClose().block();
    }
}
