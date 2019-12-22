package discord4j.connect.rsocket.gateway;

import discord4j.common.JacksonResources;
import discord4j.connect.common.ConnectGatewayOptions;
import discord4j.connect.common.DownstreamGatewayClient;
import discord4j.connect.support.BotSupport;
import discord4j.core.DiscordClient;
import discord4j.core.GatewayDiscordClient;
import discord4j.core.shard.ShardingStrategy;
import discord4j.store.api.readonly.ReadOnlyStoreService;
import discord4j.store.redis.RedisStoreService;
import io.lettuce.core.RedisClient;
import reactor.core.publisher.Hooks;

import java.net.InetSocketAddress;

public class ExampleRSocketWorker {

    public static void main(String[] args) {
        Hooks.onOperatorDebug();

        InetSocketAddress serverAddress = new InetSocketAddress(33444);

        JacksonResources jackson = new JacksonResources();
        RedisClient redisClient = RedisClient.create("redis://localhost:6379");

        GatewayDiscordClient client = DiscordClient.builder(System.getenv("token"))
                .setJacksonResources(jackson)
                .build()
                .gateway()
                .setSharding(ShardingStrategy.single()) // required to allow a single downstream reading from all shards
                .setMemberRequest(false) // recommended, leader makes these requests already
                .setStoreService(new ReadOnlyStoreService(
                        new RedisStoreService(redisClient, RedisStoreService.defaultCodec())))
                .setExtraOptions(o -> new ConnectGatewayOptions(o,
                        new RSocketPayloadSink(serverAddress,
                                new RSocketJacksonSinkMapper(jackson.getObjectMapper(), "outbound")),
                        new RSocketPayloadSource(serverAddress, "inbound",
                                new RSocketJacksonSourceMapper(jackson.getObjectMapper()))))
                .connect(DownstreamGatewayClient::new)
                .blockOptional()
                .orElseThrow(RuntimeException::new);

        BotSupport.create(client)
                .eventHandlers()
                .block();
    }
}
