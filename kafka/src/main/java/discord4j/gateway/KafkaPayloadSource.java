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

import discord4j.gateway.json.GatewayPayload;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOffset;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.function.Tuples;

import java.time.Instant;
import java.util.Collections;
import java.util.Properties;
import java.util.function.Function;

public class KafkaPayloadSource<K, V> implements PayloadSource {

    private static final Logger log = Loggers.getLogger(KafkaPayloadSource.class);

    private final ReceiverOptions<K, V> receiverOptions;
    private final String topic;
    private final SourceMapper<K, V> mapper;

    public KafkaPayloadSource(Properties properties, String topic, SourceMapper<K, V> mapper) {
        this.receiverOptions = ReceiverOptions.create(properties);
        this.topic = topic;
        this.mapper = mapper;
    }

    @Override
    public Flux<?> receive(Function<GatewayPayload<?>, Mono<Void>> processor) {
        ReceiverOptions<K, V> options = receiverOptions.subscription(Collections.singleton(topic))
            .addAssignListener(partitions -> log.debug("Partitions assigned: {}", partitions))
            .addRevokeListener(partitions -> log.debug("Partitions revoked: {}", partitions));
        Flux<ReceiverRecord<K, V>> kafkaFlux = KafkaReceiver.create(options).receive();
        return kafkaFlux
            .doOnNext(record -> {
                ReceiverOffset offset = record.receiverOffset();
                log.debug("Read message from {} offset={} ts={} key={} value={}",
                    offset.topicPartition(),
                    offset.offset(),
                    Instant.ofEpochMilli(record.timestamp()),
                    record.key(),
                    record.value());
                offset.acknowledge();
            })
            .flatMap(record -> mapper.apply(Tuples.of(record.key(), record.value())))
            .flatMap(processor::apply);
    }
}
