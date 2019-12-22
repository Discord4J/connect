package discord4j.connect.rsocket.gateway;

import discord4j.common.JacksonResources;
import discord4j.connect.common.ConnectGatewayOptions;
import discord4j.connect.common.UpstreamGatewayClient;
import discord4j.core.DiscordClient;
import discord4j.core.shard.ShardingStrategy;
import discord4j.store.redis.RedisStoreService;
import io.lettuce.core.RedisClient;
import reactor.core.publisher.Hooks;

import java.net.InetSocketAddress;

public class ExampleRSocketLeader {

    public static void main(String[] args) {
        Hooks.onOperatorDebug();

        InetSocketAddress serverAddress = new InetSocketAddress(33444);

        JacksonResources jackson = new JacksonResources();

        RedisClient redisClient = RedisClient.create("redis://localhost:6379");

        DiscordClient.builder(System.getenv("token"))
                .setJacksonResources(jackson)
                .build()
                .gateway()
                .setSharding(ShardingStrategy.recommended()) // if using the recommended amount
//                .setSharding(ShardingStrategy.builder() // for a custom strategy
//                        .indexes(0)
//                        .count(2)
//                        .build())
//                .setSharding(ShardingStrategy.fixed(2)) // if using a fixed amount of shards
                .setGuildSubscriptions(false)
                .setStoreService(new RedisStoreService(redisClient, RedisStoreService.defaultCodec()))
                .setExtraOptions(o -> new ConnectGatewayOptions(o,
                        new RSocketPayloadSink(serverAddress,
                                new RSocketJacksonSinkMapper(jackson.getObjectMapper(), "inbound")),
                        new RSocketPayloadSource(serverAddress, "outbound",
                                new RSocketJacksonSourceMapper(jackson.getObjectMapper()))))
                .connect(UpstreamGatewayClient::new)
                .blockOptional()
                .orElseThrow(RuntimeException::new)
                .onDisconnect()
                .block();
    }
}
