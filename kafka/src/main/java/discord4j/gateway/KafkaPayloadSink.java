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
import org.apache.kafka.clients.producer.ProducerRecord;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.function.Tuple2;

import java.util.Properties;

public class KafkaPayloadSink<K, V> implements PayloadSink {

    private static final Logger log = Loggers.getLogger(KafkaPayloadSink.class);

    private final KafkaSender<K, V> sender;
    private final String topic;
    private final SinkMapper<K, V> mapper;

    public KafkaPayloadSink(Properties properties, String topic, SinkMapper<K, V> mapper) {
        SenderOptions<K, V> senderOptions = SenderOptions.create(properties);
        this.sender = KafkaSender.create(senderOptions);
        this.topic = topic;
        this.mapper = mapper;
    }

    @Override
    public Flux<?> send(Flux<GatewayPayload<?>> source) {
        return sender.send(source
            .map(payload -> {
                Tuple2<K, V> mapped = mapper.apply(payload);
                return SenderRecord.create(
                    new ProducerRecord<>(topic, mapped.getT1(), mapped.getT2()), payload.getSequence());
            }))
            .doOnError(e -> log.error("Send failed", e))
            .doOnNext(SenderResult::recordMetadata);
    }

    public void close() {
        sender.close();
    }
}
