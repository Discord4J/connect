package discord4j.connect.kafka.gateway;

import discord4j.common.JacksonResources;
import discord4j.connect.EventHandler;
import discord4j.connect.kafka.KafkaJacksonSinkMapper;
import discord4j.connect.kafka.KafkaJacksonSourceMapper;
import discord4j.connect.kafka.KafkaPayloadSink;
import discord4j.connect.kafka.KafkaPayloadSource;
import discord4j.core.DiscordClient;
import discord4j.core.GatewayDiscordClient;
import discord4j.core.event.domain.lifecycle.ReadyEvent;
import discord4j.core.event.domain.message.MessageCreateEvent;
import discord4j.connect.common.ConnectGatewayOptions;
import discord4j.connect.common.DownstreamGatewayClient;
import discord4j.rest.entity.data.ApplicationInfoData;
import discord4j.store.api.readonly.ReadOnlyStoreService;
import discord4j.store.redis.RedisStoreService;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import reactor.core.publisher.Mono;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class ExampleKafkaWorker {

    private static final Logger log = Loggers.getLogger(ExampleKafkaWorker.class);

    public static void main(String[] args) {
        String downstreamTopic = System.getenv("downstreamTopic"); // inbound
        String upstreamTopic = System.getenv("upstreamTopic"); // outbound
        Properties props = getKafkaProperties(System.getenv("brokers"));
        JacksonResources jackson = new JacksonResources();

        /*
        the DOWNSTREAM node subscribes to downstreamTopic to receive data from the UPSTREAM node, this the SOURCE
        the DOWNSTREAM node publishes to upstreamTopic to send data to the UPSTREAM node, this is the SINK
         */

        KafkaPayloadSource<String, String> downReceiverSource = new KafkaPayloadSource<>(props, downstreamTopic,
                new KafkaJacksonSourceMapper(jackson.getObjectMapper()));
        KafkaPayloadSink<String, String, Integer> downSenderSink = new KafkaPayloadSink<>(props,
                new KafkaJacksonSinkMapper(jackson.getObjectMapper(), upstreamTopic));

        GatewayDiscordClient client = DiscordClient.builder(System.getenv("token"))
                .setJacksonResources(jackson)
                .build()
                .gateway()
                .setStoreService(new ReadOnlyStoreService(new RedisStoreService()))
                .setExtraOptions(opts -> new ConnectGatewayOptions(opts, downSenderSink, downReceiverSource))
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

    private static Properties getKafkaProperties(String brokers) {
        String serializer = StringSerializer.class.getName();
        String deserializer = StringDeserializer.class.getName();
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("group.id", "consumer");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", deserializer);
        props.put("value.deserializer", deserializer);
        props.put("key.serializer", serializer);
        props.put("value.serializer", serializer);
        return props;
    }
}
