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

package discord4j.gateway;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import discord4j.common.jackson.PossibleModule;
import discord4j.core.DiscordClient;
import discord4j.core.DiscordClientBuilder;
import discord4j.core.event.dispatch.ReadOnlyEventMapperFactory;
import discord4j.core.event.domain.lifecycle.*;
import discord4j.core.event.domain.message.MessageCreateEvent;
import discord4j.core.object.entity.ApplicationInfo;
import discord4j.core.object.entity.Message;
import discord4j.core.object.entity.User;
import discord4j.core.object.presence.Presence;
import discord4j.core.object.util.Snowflake;
import discord4j.gateway.json.GatewayPayload;
import discord4j.store.redis.RedisStoreService;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.function.Tuples;

import java.io.IOException;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

public class KafkaBotTest {

    private static final Logger log = Loggers.getLogger(KafkaBotTest.class);

    private static String token;
    private static Integer shardId;
    private static Integer shardCount;
    private static String brokers;

    @BeforeClass
    public static void initialize() {
        token = System.getenv("token");
        String shardIdValue = System.getenv("shardId");
        String shardCountValue = System.getenv("shardCount");
        if (shardIdValue != null && shardCountValue != null) {
            shardId = Integer.valueOf(shardIdValue);
            shardCount = Integer.valueOf(shardCountValue);
        }
        brokers = System.getenv("brokers");
    }

    @Test
    @Ignore
    public void testUpstreamNode() {
        Properties props = getKafkaProperties();
        ObjectMapper mapper = getObjectMapper();

        String inbound = "inbound";
        String outbound = "outbound";

        SinkMapper<String, String> sinkMapper = getSinkMapper(mapper);
        SourceMapper<String, String> sourceMapper = getSourceMapper(mapper);

        KafkaPayloadSink<String, String> upReceiverSink = new KafkaPayloadSink<>(props, inbound, sinkMapper);
        KafkaPayloadSource<String, String> upSenderSource = new KafkaPayloadSource<>(props, outbound, sourceMapper);

        DiscordClient upstreamClient = new DiscordClientBuilder(token)
            .setGatewayClientFactory(new UpstreamGatewayClientFactory(upReceiverSink, upSenderSource))
            .setStoreService(new RedisStoreService())
            .build();

        upstreamClient.login().block();
    }

    @Test
    @Ignore
    public void testDownstreamNode() {
        Properties props = getKafkaProperties();
        ObjectMapper mapper = getObjectMapper();

        String inbound = "inbound";
        String outbound = "outbound";

        SinkMapper<String, String> sinkMapper = getSinkMapper(mapper);
        SourceMapper<String, String> sourceMapper = getSourceMapper(mapper);

        KafkaPayloadSource<String, String> downReceiverSource = new KafkaPayloadSource<>(props, inbound, sourceMapper);
        KafkaPayloadSink<String, String> downSenderSink = new KafkaPayloadSink<>(props, outbound, sinkMapper);

        DiscordClient downstreamClient = new DiscordClientBuilder(token)
            .setGatewayClientFactory(new DownstreamGatewayClientFactory(downSenderSink, downReceiverSource))
            .setStoreService(new RedisStoreService())
            .setEventMapperFactory(new ReadOnlyEventMapperFactory())
            .build();

        CommandListener commandListener = new CommandListener(downstreamClient);
        commandListener.configure();

        LifecycleListener lifecycleListener = new LifecycleListener(downstreamClient);
        lifecycleListener.configure();

        downstreamClient.login().block();
    }

    private ObjectMapper getObjectMapper() {
        return new ObjectMapper()
            .setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)
            .registerModules(new PossibleModule(), new Jdk8Module());
    }

    private Properties getKafkaProperties() {
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

    private SinkMapper<String, String> getSinkMapper(ObjectMapper mapper) {
        String key = shardId + ":" + shardCount;
        return payload -> {
            try {
                return Tuples.of(key, mapper.writeValueAsString(payload));
            } catch (JsonProcessingException e) {
                log.warn("Unable to serialize {}: {}", payload, e);
                return Tuples.of(key, "");
            }
        };
    }

    private SourceMapper<String, String> getSourceMapper(ObjectMapper mapper) {
        return tuple -> {
            try {
                GatewayPayload<?> payload = mapper.readValue(tuple.getT2(), GatewayPayload.class);
                return Mono.just(payload);
            } catch (IOException e) {
                log.warn("Unable to deserialize {}: {}", tuple.getT2(), e);
                return Mono.empty();
            }
        };
    }

    public static class CommandListener {

        private final DiscordClient client;
        private final AtomicLong ownerId = new AtomicLong();

        public CommandListener(DiscordClient client) {
            this.client = client;
        }

        void configure() {
            Mono<Long> getOwnerId = Mono.justOrEmpty(Optional.of(ownerId.get()).filter(id -> id != 0))
                .switchIfEmpty(client.getApplicationInfo()
                    .map(ApplicationInfo::getOwnerId)
                    .map(Snowflake::asLong));

            Flux.combineLatest(client.getEventDispatcher().on(MessageCreateEvent.class), getOwnerId, Tuples::of)
                .flatMap(tuple -> {
                    Message message = tuple.getT1().getMessage();
                    ownerId.set(tuple.getT2());

                    message.getAuthorId()
                        .filter(id -> tuple.getT2() == id.asLong()) // only accept bot owner messages
                        .flatMap(id -> message.getContent())
                        .ifPresent(content -> {
                            if ("!close".equals(content)) {
                                client.logout();
                            } else if ("!online".equals(content)) {
                                client.updatePresence(Presence.online()).subscribe();
                            } else if ("!dnd".equals(content)) {
                                client.updatePresence(Presence.doNotDisturb()).subscribe();
                            } else if (content.startsWith("!echo ")) {
                                message.getAuthor()
                                    .flatMap(User::getPrivateChannel)
                                    .flatMap(ch -> ch.createMessage(content.substring("!echo ".length())))
                                    .subscribe();
                            }
                        });
                    return Mono.just(tuple.getT1());
                })
                .onErrorContinue()
                .doOnError(t -> log.warn("Something is wrong", t))
                .subscribe();
        }
    }

    public static class LifecycleListener {

        private final DiscordClient client;

        public LifecycleListener(DiscordClient client) {
            this.client = client;
        }

        void configure() {
            client.getEventDispatcher().on(ConnectEvent.class).subscribe();
            client.getEventDispatcher().on(DisconnectEvent.class).subscribe();
            client.getEventDispatcher().on(ReconnectStartEvent.class).subscribe();
            client.getEventDispatcher().on(ReconnectEvent.class).subscribe();
            client.getEventDispatcher().on(ReconnectFailEvent.class).subscribe();
        }

    }
}
