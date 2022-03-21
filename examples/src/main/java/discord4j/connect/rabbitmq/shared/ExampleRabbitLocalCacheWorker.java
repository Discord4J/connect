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
import discord4j.common.store.Store;
import discord4j.common.store.legacy.LegacyStoreLayout;
import discord4j.connect.Constants;
import discord4j.connect.common.ConnectGatewayOptions;
import discord4j.connect.common.DownstreamGatewayClient;
import discord4j.connect.rabbitmq.ConnectRabbitMQ;
import discord4j.connect.rabbitmq.ConnectRabbitMQSettings;
import discord4j.connect.rabbitmq.gateway.RabbitMQPayloadSink;
import discord4j.connect.rabbitmq.gateway.RabbitMQPayloadSource;
import discord4j.connect.rabbitmq.gateway.RabbitMQSinkMapper;
import discord4j.connect.rabbitmq.gateway.RabbitMQSourceMapper;
import discord4j.connect.rsocket.global.RSocketGlobalRateLimiter;
import discord4j.connect.rsocket.router.RSocketRouter;
import discord4j.connect.rsocket.router.RSocketRouterOptions;
import discord4j.connect.support.BotSupport;
import discord4j.connect.support.ExtraBotSupport;
import discord4j.connect.support.LogoutHttpServer;
import discord4j.connect.support.NoBotSupport;
import discord4j.core.DiscordClient;
import discord4j.core.GatewayDiscordClient;
import discord4j.core.shard.ShardingStrategy;
import discord4j.store.redis.RedisStoreService;
import io.lettuce.core.RedisClient;
import reactor.core.publisher.Mono;

import java.net.InetSocketAddress;

/**
 * An example distributed Discord4J worker, or a node that is capable of processing payloads coming from leaders,
 * executing API requests and sending payloads back to the leaders, if needed.
 * <p>
 * In particular, this example covers:
 * <ul>
 *     <li>Connecting to a distributed GlobalRateLimiter for API requests</li>
 *     <li>Connecting to a distributed Router for API requests</li>
 *     <li>Connecting to a RabbitMQ broker to send and receive messages from other nodes</li>
 *     <li>Connecting to a redis server to use it as entity cache, using READ-ONLY mode</li>
 *     <li>Shared subscription using {@link ShardingStrategy#single()}: stateless workers reading from every shard</li>
 * </ul>
 */
public class ExampleRabbitLocalCacheWorker {

    public static void main(String[] args) {

        /*
         * Define the location of the Global Router Server (GRS). A GRS combines coordinated routing across API
         * requests while also dealing with the global rate limits.
         *
         * We will use RSocket GRS in this example: see ExampleRSocketGlobalRouterServer
         */
        InetSocketAddress globalRouterServerAddress = new InetSocketAddress(Constants.GLOBAL_ROUTER_SERVER_HOST, Constants.GLOBAL_ROUTER_SERVER_PORT);

        /*
         * Define the redis server that will be used as entity cache.
         */
        RedisClient redisClient = RedisClient.create(Constants.REDIS_CLIENT_URI);

        /*
         * Create a default factory for working with Jackson, this can be reused across the application.
         */
        JacksonResources jackson = JacksonResources.create();

        /*
         * Define the sharding strategy. Workers in this "stateless" configuration should use the single factory.
         * This saves bootstrap efforts by grouping all inbound payloads into one entity (when using
         * DownstreamGatewayClient)
         */
        ShardingStrategy shardingStrategy = ShardingStrategy.single();

        /*
         * Define the key resources for working with RabbitMQ.
         * - ConnectRabbitMQ defines the parameters to a server
         * - RabbitMQSinkMapper will be used to PRODUCE payloads to other nodes
         *      - "createBinarySinkToDirect" will create binary messages, sent to the "gateway" queue directly.
         * - RabbitMQSourceMapper will be used to CONSUME payloads from other nodes
         *      - "createBinarySource" will read binary messages
         */
        ConnectRabbitMQ rabbitMQ;
        if (!Constants.RABBITMQ_HOST.isEmpty()) {
            ConnectRabbitMQSettings settings = ConnectRabbitMQSettings.create().withAddress(Constants.RABBITMQ_HOST, Constants.RABBITMQ_PORT);
            rabbitMQ = ConnectRabbitMQ.createFromSettings(settings);
        } else {
            rabbitMQ = ConnectRabbitMQ.createDefault();
        }
        RabbitMQSinkMapper sinkMapper = RabbitMQSinkMapper.createBinarySinkToDirect("gateway");
        RabbitMQSourceMapper sourceMapper = RabbitMQSourceMapper.createBinarySource();

        GatewayDiscordClient client = DiscordClient.builder(System.getenv("BOT_TOKEN"))
                .setJacksonResources(jackson)
                .setGlobalRateLimiter(RSocketGlobalRateLimiter.createWithServerAddress(globalRouterServerAddress))
                .setExtraOptions(o -> new RSocketRouterOptions(o, request -> globalRouterServerAddress))
                .build(RSocketRouter::new)
                .gateway()
                .setSharding(shardingStrategy)
                // Set a fully capable entity cache as this worker is also performing save tasks
                .setStore(Store.fromLayout(LegacyStoreLayout.of(RedisStoreService.builder()
                        .redisClient(redisClient)
                        .build())))
                // Turn this gateway into a RabbitMQ-based one
                .setExtraOptions(o -> new ConnectGatewayOptions(o,
                        RabbitMQPayloadSink.create(sinkMapper, rabbitMQ),
                        RabbitMQPayloadSource.create(sourceMapper, rabbitMQ, "payload")))
                // DownstreamGatewayClient does not connect to Gateway and receives payloads from other nodes
                .login(DownstreamGatewayClient::new)
                .blockOptional()
                .orElseThrow(RuntimeException::new);

        LogoutHttpServer.startAsync(client);
        if (Boolean.parseBoolean(System.getenv("EXAMPLE_COMMANDS"))) {
            Mono.when(
                    BotSupport.create(client).eventHandlers(),
                    ExtraBotSupport.create(client).eventHandlers()
            ).block();
        } else {
            NoBotSupport.create(client)
                    .eventHandlers()
                    .block();
        }
        rabbitMQ.close();
    }
}
