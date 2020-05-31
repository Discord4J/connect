/*
 * This file is part of Discord4J.
 *
 * Discord4J is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Discord4J is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with Discord4J. If not, see <http://www.gnu.org/licenses/>.
 */

package discord4j.connect.rabbitmq.shared;

import discord4j.common.JacksonResources;
import discord4j.connect.Constants;
import discord4j.connect.common.ConnectGatewayOptions;
import discord4j.connect.common.UpstreamGatewayClient;
import discord4j.connect.rabbitmq.ConnectRabbitMQ;
import discord4j.connect.rabbitmq.gateway.RabbitMQPayloadSink;
import discord4j.connect.rabbitmq.gateway.RabbitMQPayloadSource;
import discord4j.connect.rabbitmq.gateway.RabbitMQSinkMapper;
import discord4j.connect.rabbitmq.gateway.RabbitMQSourceMapper;
import discord4j.connect.rsocket.global.RSocketGlobalRateLimiter;
import discord4j.connect.rsocket.router.RSocketRouter;
import discord4j.connect.rsocket.router.RSocketRouterOptions;
import discord4j.connect.rsocket.shard.RSocketShardCoordinator;
import discord4j.connect.support.LogoutHttpServer;
import discord4j.core.DiscordClient;
import discord4j.core.GatewayDiscordClient;
import discord4j.core.event.dispatch.DispatchEventMapper;
import discord4j.core.object.presence.Presence;
import discord4j.core.shard.InvalidationStrategy;
import discord4j.core.shard.ShardingStrategy;
import discord4j.gateway.intent.Intent;
import discord4j.gateway.intent.IntentSet;
import discord4j.store.api.noop.NoOpStoreService;
import reactor.core.publisher.Mono;
import reactor.rabbitmq.QueueSpecification;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.net.InetSocketAddress;

/**
 * An example distributed Discord4J leader, or a node that is capable of connecting to Discord Gateway and routing
 * its messages to other nodes, across JVM boundaries.
 * <p>
 * In particular, this example covers:
 * <ul>
 *     <li>Connecting to a distributed GlobalRateLimiter for API requests</li>
 *     <li>Connecting to a distributed Router for API requests</li>
 *     <li>Connecting to a distributed ShardCoordinator for connect/IDENTIFY request rate limiting</li>
 *     <li>Connecting to a RabbitMQ broker to send and receive messages from other nodes</li>
 *     <li>Disabling entity cache capabilities to reduce memory footprint, focusing on message routing ONLY</li>
 * </ul>
 */
public class ExampleRabbitLocalCacheLeader {


    public static void main(String[] args) {

        /*
         * Define the location of the Global Router Server (GRS). A GRS combines coordinated routing across API
         * requests while also dealing with the global rate limits.
         *
         * We will use RSocket GRS in this example: see ExampleRSocketGlobalRouterServer
         */
        InetSocketAddress globalRouterServerAddress = new InetSocketAddress(Constants.GLOBAL_ROUTER_SERVER_PORT);

        /*
         * Define the location of the Shard Coordinator Server (SCS). An SCS establishes predictable ordering across
         * multiple leaders attempting to connect to the Gateway.
         *
         * We will use RSocket SCS in this example: see ExampleRSocket
         */
        InetSocketAddress coordinatorServerAddress = new InetSocketAddress(Constants.SHARD_COORDINATOR_SERVER_PORT);

        /*
         * Create a default factory for working with Jackson, this can be reused across the application.
         */
        JacksonResources jackson = JacksonResources.create();

        /*
         * Define the sharding strategy. Refer to the class docs for more details or options.
         */
        ShardingStrategy shardingStrategy = ShardingStrategy.recommended();

        /*
         * Define the key resources for working with RabbitMQ.
         * - ConnectRabbitMQ defines the parameters to a server
         * - RabbitMQSinkMapper will be used to PRODUCE payloads to other nodes
         *      - "createBinarySinkToDirect" will create binary messages, sent to the "payload" queue directly.
         * - RabbitMQSourceMapper will be used to CONSUME payloads from other nodes
         *      - "createBinarySource" will read binary messages
         */
        ConnectRabbitMQ rabbitMQ = ConnectRabbitMQ.createDefault();
        RabbitMQSinkMapper sink = RabbitMQSinkMapper.createBinarySinkToDirect("payload");
        RabbitMQSourceMapper source = RabbitMQSourceMapper.createBinarySource();

        GatewayDiscordClient client = DiscordClient.builder(System.getenv("token"))
                .setJacksonResources(jackson)
                .setGlobalRateLimiter(RSocketGlobalRateLimiter.createWithServerAddress(globalRouterServerAddress))
                .setExtraOptions(o -> new RSocketRouterOptions(o, request -> globalRouterServerAddress))
                .build(RSocketRouter::new)
                .gateway()
                .setSharding(shardingStrategy)
                // Properly coordinate IDENTIFY attempts across all shards
                .setShardCoordinator(RSocketShardCoordinator.createWithServerAddress(coordinatorServerAddress))
                .setDisabledIntents(IntentSet.of(
                        Intent.GUILD_PRESENCES,
                        Intent.GUILD_MESSAGE_TYPING,
                        Intent.DIRECT_MESSAGE_TYPING))
                .setInitialStatus(s -> Presence.invisible())
                // Disable invalidation strategy, event publishing and entity cache to save memory usage
                .setDispatchEventMapper(DispatchEventMapper.discardEvents())
                .setStoreService(new NoOpStoreService())
                .setInvalidationStrategy(InvalidationStrategy.disable())
                // Turn this gateway into a RabbitMQ-based one
                .setExtraOptions(o -> new ConnectGatewayOptions(o,
                        RabbitMQPayloadSink.create(sink, rabbitMQ)
                                .withBeforeSendFunction((rmq, meta) -> rmq.getSender()
                                        .declare(QueueSpecification.queue(meta.getRoutingKey()))
                                        .onErrorResume(t -> Mono.empty())),
                        RabbitMQPayloadSource.create(source, rabbitMQ, "gateway")))
                // UpstreamGatewayClient connects to Discord Gateway and forwards payloads to other nodes
                .login(UpstreamGatewayClient::new)
                .blockOptional()
                .orElseThrow(RuntimeException::new);

        LogoutHttpServer.startAsync(client);
        client.onDisconnect().block();
        rabbitMQ.close();
    }

    private static final Logger log = Loggers.getLogger(ExampleRabbitLocalCacheLeader.class);
}
