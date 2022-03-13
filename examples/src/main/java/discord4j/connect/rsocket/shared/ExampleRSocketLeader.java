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

package discord4j.connect.rsocket.shared;

import discord4j.common.JacksonResources;
import discord4j.common.store.Store;
import discord4j.common.store.legacy.LegacyStoreLayout;
import discord4j.connect.Constants;
import discord4j.connect.common.ConnectGatewayOptions;
import discord4j.connect.common.UpstreamGatewayClient;
import discord4j.connect.rsocket.gateway.RSocketJacksonSinkMapper;
import discord4j.connect.rsocket.gateway.RSocketJacksonSourceMapper;
import discord4j.connect.rsocket.gateway.RSocketPayloadSink;
import discord4j.connect.rsocket.gateway.RSocketPayloadSource;
import discord4j.connect.rsocket.global.RSocketGlobalRateLimiter;
import discord4j.connect.rsocket.router.RSocketRouter;
import discord4j.connect.rsocket.router.RSocketRouterOptions;
import discord4j.connect.rsocket.shard.RSocketShardCoordinator;
import discord4j.core.DiscordClient;
import discord4j.core.GatewayDiscordClient;
import discord4j.core.event.dispatch.DispatchEventMapper;
import discord4j.core.shard.ShardingStrategy;
import discord4j.store.jdk.JdkStoreService;
import discord4j.store.redis.RedisStoreService;
import io.lettuce.core.RedisClient;
import reactor.core.publisher.Mono;
import reactor.netty.http.server.HttpServer;
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
 *     <li>Connecting to a RSocket payload server to send messages across boundaries</li>
 *     <li>Using redis as entity cache (write capable in leaders, read-only in workers)</li>
 *     <li>Defining the sharding strategy for the leaders</li>
 * </ul>
 */
public class ExampleRSocketLeader {

    public static void main(String[] args) {

        // define the host and port where the global router is listening to
        // define the host and port where the shard coordinator is listening to
        // define the host and port where the payload server is listening to
        InetSocketAddress globalRouterServerAddress = new InetSocketAddress(Constants.GLOBAL_ROUTER_SERVER_HOST, Constants.GLOBAL_ROUTER_SERVER_PORT);
        InetSocketAddress coordinatorServerAddress = new InetSocketAddress(Constants.SHARD_COORDINATOR_SERVER_HOST, Constants.SHARD_COORDINATOR_SERVER_PORT);
        InetSocketAddress payloadServerAddress = new InetSocketAddress(Constants.PAYLOAD_SERVER_HOST, Constants.PAYLOAD_SERVER_PORT);

        // use a common jackson factory to reuse it where possible
        JacksonResources jackson = JacksonResources.create();

        // use redis to store entity caches
        RedisClient redisClient = RedisClient.create(Constants.REDIS_CLIENT_URI);

        // Select your ShardingStrategy
        // a) use the recommended amount of shards, and connect this leader to all of them into a group
        // b) use a set number of shards, and connect this leader to all of them into a group
        // c) build a custom sharding strategy:
        //      - indexes to filter which shard IDs to connect this leader
        //      - count to set a fixed shard count (or do not set one to use the recommended amount)
        ShardingStrategy recommendedStrategy = ShardingStrategy.recommended();
        ShardingStrategy fixedStrategy = ShardingStrategy.fixed(2);
        ShardingStrategy customStrategy = ShardingStrategy.builder()
                .indices(0, 2) // only connect this leader to shard IDs 0 and 2
                .count(4)      // but still split our bot guilds into 4 shards
                .build();

        // define the GlobalRouterServer as GRL for all nodes in this architecture
        // define the GlobalRouterServer as Router for all request buckets in this architecture
        // create the RSocket capable Router of queueing API requests across boundaries
        // coordinate ws connect and IDENTIFY rate limit across leader nodes using this server
        // define the ConnectGatewayOptions to send payloads across boundaries
        // RSocketPayloadSink: payloads leaders send to workers through the payload server
        // RSocketPayloadSource: payloads workers send to leaders through the payload server
        // we use UpstreamGatewayClient that is capable of using above components to work in a distributed way
        GatewayDiscordClient client = DiscordClient.builder(System.getenv("BOT_TOKEN"))
                .setJacksonResources(jackson)
                .setGlobalRateLimiter(RSocketGlobalRateLimiter.createWithServerAddress(globalRouterServerAddress))
                .setExtraOptions(o -> new RSocketRouterOptions(o, request -> globalRouterServerAddress))
                .build(RSocketRouter::new)
                .gateway()
                .setSharding(recommendedStrategy)
                .setShardCoordinator(RSocketShardCoordinator.createWithServerAddress(coordinatorServerAddress))
//                .setDisabledIntents(IntentSet.of(
//                        Intent.GUILD_PRESENCES,
//                        Intent.GUILD_MESSAGE_TYPING,
//                        Intent.DIRECT_MESSAGE_TYPING))
//                .setInitialStatus(s -> Presence.invisible())

//                .setInvalidationStrategy(InvalidationStrategy.disable())

                .setStore(Store.fromLayout(LegacyStoreLayout.of(RedisStoreService.builder()
                        .redisClient(redisClient)
                        .useSharedConnection(false)
                        .build())))
                .setDispatchEventMapper(DispatchEventMapper.discardEvents())
                .setExtraOptions(o -> new ConnectGatewayOptions(o,
                        new RSocketPayloadSink(payloadServerAddress,
                                new RSocketJacksonSinkMapper(jackson.getObjectMapper(), "inbound")),
                        new RSocketPayloadSource(payloadServerAddress, "outbound",
                                new RSocketJacksonSourceMapper(jackson.getObjectMapper()))))
                .login(UpstreamGatewayClient::new)
                .blockOptional()
                .orElseThrow(RuntimeException::new);

        // Proof of concept allowing leader management via API
        HttpServer.create()
                .port(0) // use an ephemeral port
                .route(routes -> routes
                        .get("/logout",
                                (req, res) -> client.logout()
                                        .then(Mono.from(res.addHeader("content-type", "application/json")
                                                .status(200)
                                                .chunkedTransfer(false)
                                                .sendString(Mono.just("OK")))))
                )
                .bind()
                .doOnNext(facade -> {
                    log.info("*************************************************************");
                    log.info("Server started at {}:{}", facade.host(), facade.port());
                    log.info("*************************************************************");
                    // kill the server on JVM exit
                    Runtime.getRuntime().addShutdownHook(new Thread(facade::disposeNow));
                })
                .subscribe();

        client.onDisconnect().block();
    }

    private static final Logger log = Loggers.getLogger(ExampleRSocketLeader.class);
}
