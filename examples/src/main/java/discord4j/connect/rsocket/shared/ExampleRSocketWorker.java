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
import discord4j.connect.common.DownstreamGatewayClient;
import discord4j.connect.rsocket.gateway.RSocketJacksonSinkMapper;
import discord4j.connect.rsocket.gateway.RSocketJacksonSourceMapper;
import discord4j.connect.rsocket.gateway.RSocketPayloadSink;
import discord4j.connect.rsocket.gateway.RSocketPayloadSource;
import discord4j.connect.rsocket.global.RSocketGlobalRateLimiter;
import discord4j.connect.rsocket.router.RSocketRouter;
import discord4j.connect.rsocket.router.RSocketRouterOptions;
import discord4j.connect.support.BotSupport;
import discord4j.connect.support.ExtraBotSupport;
import discord4j.connect.support.LogoutHttpServer;
import discord4j.connect.support.NoBotSupport;
import discord4j.core.DiscordClient;
import discord4j.core.GatewayDiscordClient;
import discord4j.core.shard.MemberRequestFilter;
import discord4j.core.shard.ShardingStrategy;
import discord4j.store.api.readonly.ReadOnlyStoreService;
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
 *     <li>Connecting to a distributed ShardCoordinator for connect/IDENTIFY request rate limiting</li>
 *     <li>Connecting to a RSocket payload server to send messages across boundaries</li>
 *     <li>Using redis as entity cache (write capable in leaders, read-only in workers)</li>
 *     <li>Shared subscription using {@link ShardingStrategy#single()}: stateless workers reading from every shard.</li>
 * </ul>
 */
public class ExampleRSocketWorker {

    public static void main(String[] args) {

        // define the host and port where the global router is listening to
        // define the host and port where the payload server is listening to
        InetSocketAddress globalRouterServerAddress = new InetSocketAddress(Constants.GLOBAL_ROUTER_SERVER_HOST, Constants.GLOBAL_ROUTER_SERVER_PORT);
        InetSocketAddress payloadServerAddress = new InetSocketAddress(Constants.PAYLOAD_SERVER_HOST, Constants.PAYLOAD_SERVER_PORT);

        // use a common jackson factory to reuse it where possible
        JacksonResources jackson = JacksonResources.create();

        // use redis to store entity caches
        RedisClient redisClient = RedisClient.create(Constants.REDIS_CLIENT_URI);

        // use a "single" strategy where this worker will be capable of reading payloads from every shard
        // - load will be shared across workers
        // - no guarantee (yet..) about receiving payloads from the same shard ID in this worker node
        // - if that is your use case, use ShardingStrategy.recommended()
        ShardingStrategy singleStrategy = ShardingStrategy.single();

        // define the GlobalRouterServer as GRL for all nodes in this architecture
        // define the GlobalRouterServer as Router for all request buckets in this architecture
        // create the RSocket capable Router of queueing API requests across boundaries
        // shard coordinator is not needed by workers: they do not establish Discord Gateway connections
        // disable memberRequests as leader makes them (and we have disabled write access to entity cache)
        // define the ConnectGatewayOptions to send payloads across boundaries
        // RSocketPayloadSink: payloads workers send to leaders through the payload server
        // RSocketPayloadSource: payloads leaders send to workers through the payload server
        // we use DownstreamGatewayClient that is capable of using above components to work in a distributed way
        GatewayDiscordClient client = DiscordClient.builder(System.getenv("BOT_TOKEN"))
                .setJacksonResources(jackson)
                .setGlobalRateLimiter(RSocketGlobalRateLimiter.createWithServerAddress(globalRouterServerAddress))
                .setExtraOptions(o -> new RSocketRouterOptions(o, request -> globalRouterServerAddress))
                .build(RSocketRouter::new)
                .gateway()
                .setSharding(singleStrategy)
                .setMemberRequestFilter(MemberRequestFilter.none())
                .setStore(Store.fromLayout(LegacyStoreLayout.of(new ReadOnlyStoreService(RedisStoreService.builder()
                        .redisClient(redisClient)
                        .build()))))
                .setExtraOptions(o -> new ConnectGatewayOptions(o,
                        new RSocketPayloadSink(payloadServerAddress,
                                new RSocketJacksonSinkMapper(jackson.getObjectMapper(), "outbound")),
                        new RSocketPayloadSource(payloadServerAddress, "inbound",
                                new RSocketJacksonSourceMapper(jackson.getObjectMapper()))))
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
    }
}
