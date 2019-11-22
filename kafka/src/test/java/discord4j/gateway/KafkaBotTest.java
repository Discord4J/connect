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
import discord4j.core.GatewayDiscordClient;
import discord4j.core.event.domain.lifecycle.*;
import discord4j.core.event.domain.message.MessageCreateEvent;
import discord4j.core.object.entity.ApplicationInfo;
import discord4j.core.object.entity.Message;
import discord4j.core.object.entity.User;
import discord4j.core.object.presence.Presence;
import discord4j.core.object.util.Snowflake;
import discord4j.store.api.readonly.ReadOnlyStoreService;
import discord4j.store.redis.RedisStoreService;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.SenderRecord;
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
    private static String brokers;
    private static String downstreamTopic;
    private static String upstreamTopic;

    @BeforeClass
    public static void initialize() {
        token = System.getenv("token");
        brokers = System.getenv("brokers");
        downstreamTopic = "inbound";
        upstreamTopic = "outbound";
    }

    @Test
    @Ignore
    public void testUpstreamNode() {
        Properties props = getKafkaProperties();
        ObjectMapper mapper = getObjectMapper();

        /*
        the UPSTREAM node publishes to downstreamTopic to send data to the DOWNSTREAM node, this is the SINK
        the UPSTREAM node subscribes to upstreamTopic to receive data from the DOWNSTREAM node, this the SOURCE
         */

        KafkaPayloadSink<String, String, Integer> upReceiverSink = new KafkaPayloadSink<>(props,
                payload -> toJson(mapper, payload)
                        .map(json -> SenderRecord.create(
                                new ProducerRecord<>(downstreamTopic, payload.getShardInfo().format(), json),
                                payload.getSessionInfo().getSequence())));
        KafkaPayloadSource<String, String> upSenderSource = new KafkaPayloadSource<>(props, upstreamTopic,
                record -> fromJson(mapper, record.value()));

        DiscordClient upstreamClient = DiscordClient.create(token);

        upstreamClient.gateway()
                .setStoreService(new RedisStoreService())
                .setExtraOptions(opts -> new ConnectGatewayOptions(opts, upReceiverSink, upSenderSource))
                .connect(UpstreamGatewayClient::new)
                .block()
                .onDisconnect()
                .block();
    }

    @Test
    @Ignore
    public void testDownstreamNode() {
        Properties props = getKafkaProperties();
        ObjectMapper mapper = getObjectMapper();

        /*
        the DOWNSTREAM node subscribes to downstreamTopic to receive data from the UPSTREAM node, this the SOURCE
        the DOWNSTREAM node publishes to upstreamTopic to send data to the UPSTREAM node, this is the SINK
         */

        KafkaPayloadSource<String, String> downReceiverSource = new KafkaPayloadSource<>(props, downstreamTopic,
                record -> fromJson(mapper, record.value()));
        KafkaPayloadSink<String, String, Integer> downSenderSink = new KafkaPayloadSink<>(props,
                payload -> toJson(mapper, payload)
                        .map(json -> SenderRecord.create(
                                new ProducerRecord<>(upstreamTopic, payload.getShardInfo().format(), json),
                                payload.getSessionInfo().getSequence())));

        DiscordClient downstreamClient = DiscordClient.create(token);

        GatewayDiscordClient downstreamGateway = downstreamClient.gateway()
                .setStoreService(new ReadOnlyStoreService(new RedisStoreService()))
                .setExtraOptions(opts -> new ConnectGatewayOptions(opts, downSenderSink, downReceiverSource))
                .connect(DownstreamGatewayClient::new)
                .block();

        CommandListener commandListener = new CommandListener(downstreamGateway);
        commandListener.configure();

        LifecycleListener lifecycleListener = new LifecycleListener(downstreamGateway);
        lifecycleListener.configure();

        downstreamClient.login().block();
    }

    private ObjectMapper getObjectMapper() {
        return new ObjectMapper()
                .setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY)
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)
                .registerModules(new PossibleModule(), new Jdk8Module());
    }

    private Mono<String> toJson(ObjectMapper mapper, ConnectPayload payload) {
        return Mono.fromCallable(() -> {
            try {
                return mapper.writeValueAsString(payload);
            } catch (JsonProcessingException e) {
                log.warn("Unable to serialize {}: {}", payload, e);
                return null;
            }
        });
    }

    private Mono<ConnectPayload> fromJson(ObjectMapper mapper, String value) {
        return Mono.fromCallable(() -> {
            try {
                return mapper.readValue(value, ConnectPayload.class);
            } catch (IOException e) {
                log.warn("Unable to deserialize {}: {}", value, e);
                return null;
            }
        });
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

    public static class CommandListener {

        private final GatewayDiscordClient client;
        private final AtomicLong ownerId = new AtomicLong();

        public CommandListener(GatewayDiscordClient client) {
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

                        message.getAuthor()
                                .map(User::getId)
                                .filter(id -> tuple.getT2() == id.asLong()) // only accept bot owner messages
                                .flatMap(id -> message.getContent())
                                .ifPresent(content -> {
                                    if ("!close".equals(content)) {
                                        client.logout();
                                    } else if ("!online".equals(content)) {
                                        client.updatePresence(0, Presence.online()).subscribe();
                                    } else if ("!dnd".equals(content)) {
                                        client.updatePresence(0, Presence.doNotDisturb()).subscribe();
                                    } else if (content.startsWith("!echo ")) {
                                        message.getAuthorAsMember()
                                                .flatMap(User::getPrivateChannel)
                                                .flatMap(ch -> ch.createMessage(content.substring("!echo ".length())))
                                                .subscribe();
                                    }
                                });
                        return Mono.just(tuple.getT1());
                    })
                    .doOnError(t -> log.warn("Something is wrong", t))
                    .subscribe();
        }
    }

    public static class LifecycleListener {

        private final GatewayDiscordClient client;

        public LifecycleListener(GatewayDiscordClient client) {
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
