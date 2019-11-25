package discord4j.connect.rsocket.gateway;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.BasicPolymorphicTypeValidator;
import discord4j.common.JacksonResources;
import discord4j.connect.EventHandler;
import discord4j.core.DiscordClient;
import discord4j.core.GatewayDiscordClient;
import discord4j.core.event.domain.lifecycle.ReadyEvent;
import discord4j.core.event.domain.message.MessageCreateEvent;
import discord4j.connect.common.ConnectGatewayOptions;
import discord4j.connect.common.DownstreamGatewayClient;
import discord4j.rest.entity.data.ApplicationInfoData;
import discord4j.store.api.readonly.ReadOnlyStoreService;
import discord4j.store.redis.JacksonRedisSerializer;
import discord4j.store.redis.RedisStoreService;
import discord4j.store.redis.StoreRedisCodec;
import discord4j.store.redis.StringSerializer;
import io.lettuce.core.RedisClient;
import io.lettuce.core.codec.RedisCodec;
import reactor.core.publisher.Mono;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ExampleRSocketWorker {

    private static final Logger log = Loggers.getLogger(ExampleRSocketWorker.class);

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

        GatewayDiscordClient client = DiscordClient.builder(System.getenv("token"))
                .setJacksonResources(jackson)
                .build()
                .gateway()
                .setStoreService(new ReadOnlyStoreService(new RedisStoreService(redisClient, codec)))
                .setExtraOptions(o -> new ConnectGatewayOptions(o,
                        new RSocketPayloadSink(serverAddress,
                                new RSocketJacksonSinkMapper(jackson.getObjectMapper(), "outbound")),
                        new RSocketPayloadSource(serverAddress, "inbound",
                                new RSocketJacksonSourceMapper(jackson.getObjectMapper()))))
                .connect(DownstreamGatewayClient::new)
                .block();
        assert client != null;

        Mono<Long> ownerId = client.rest().getApplicationInfo()
                .map(ApplicationInfoData::getOwnerId)
                .cache();

        List<EventHandler> eventHandlers = new ArrayList<>();
        eventHandlers.add(new EventHandler.Echo());
        eventHandlers.add(new EventHandler.Status());

        Mono<Void> onReady = client.on(ReadyEvent.class)
                .doOnNext(ready -> log.info("Logged in as {}", ready.getSelf().getUsername()))
                .then();

        Mono<Void> events = client.on(MessageCreateEvent.class, event -> ownerId
                .filter(owner -> {
                    Long author = event.getMessage().getAuthor()
                            .map(u -> u.getId().asLong())
                            .orElse(null);
                    return owner.equals(author);
                })
                .flatMap(id -> Mono.when(eventHandlers.stream()
                        .map(handler -> handler.onMessageCreate(event))
                        .collect(Collectors.toList()))
                ))
                .then();

        Mono.when(onReady, events, client.onDisconnect()).block();
    }
}
