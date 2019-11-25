package discord4j.connect.rsocket.gateway;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.BasicPolymorphicTypeValidator;
import discord4j.common.JacksonResources;
import discord4j.core.DiscordClient;
import discord4j.connect.common.ConnectGatewayOptions;
import discord4j.connect.common.UpstreamGatewayClient;
import discord4j.store.redis.JacksonRedisSerializer;
import discord4j.store.redis.RedisStoreService;
import discord4j.store.redis.StoreRedisCodec;
import discord4j.store.redis.StringSerializer;
import io.lettuce.core.RedisClient;
import io.lettuce.core.codec.RedisCodec;

import java.net.InetSocketAddress;

public class ExampleRSocketLeader {

    public static void main(String[] args) {
        InetSocketAddress serverAddress = new InetSocketAddress(33444);

        JacksonResources jackson = new JacksonResources();
        RedisClient redisClient = RedisClient.create("redis://localhost:6379");
        RedisCodec<String, Object> codec = new StoreRedisCodec<>(new StringSerializer(),
                new JacksonRedisSerializer(jackson.getObjectMapper().copy()
                        .setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY)
                        .activateDefaultTyping(BasicPolymorphicTypeValidator.builder()
                                        .allowIfSubType("discord4j.").build(),
                                ObjectMapper.DefaultTyping.NON_FINAL, JsonTypeInfo.As.PROPERTY)));

        DiscordClient.builder(System.getenv("token"))
                .setJacksonResources(jackson)
                .build()
                .gateway()
                .setStoreService(new RedisStoreService(redisClient, codec))
                .setExtraOptions(o -> new ConnectGatewayOptions(o,
                        new RSocketPayloadSink(serverAddress,
                                new RSocketJacksonSinkMapper(jackson.getObjectMapper(), "inbound")),
                        new RSocketPayloadSource(serverAddress, "outbound",
                                new RSocketJacksonSourceMapper(jackson.getObjectMapper()))))
                .connect(UpstreamGatewayClient::new)
                .block()
                .onDisconnect()
                .block();
    }
}
