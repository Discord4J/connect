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

package discord4j.connect.rabbitmq.gateway;

import com.fasterxml.jackson.databind.ObjectMapper;
import discord4j.connect.common.ConnectPayload;
import discord4j.connect.common.PayloadDestinationMapper;
import discord4j.connect.common.SinkMapper;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.rabbitmq.OutboundMessage;

import java.util.Objects;

import static reactor.function.TupleUtils.function;

/**
 * A higher level mapper that can produce {@link OutboundMessage} instances.
 */
public class RabbitMQSinkMapper implements SinkMapper<OutboundMessage> {

    private final PayloadDestinationMapper<RoutingMetadata> destinationMapper;
    private final SinkMapper<byte[]> contentMapper;

    RabbitMQSinkMapper(PayloadDestinationMapper<RoutingMetadata> destinationMapper,
                       SinkMapper<byte[]> contentMapper) {
        this.destinationMapper = Objects.requireNonNull(destinationMapper);
        this.contentMapper = Objects.requireNonNull(contentMapper);
    }

    /**
     * Create a mapper that uses a binary format and routes directly to the given queue.
     *
     * @param queueName the destination queue
     * @return a binary formatted direct sink mapper
     */
    public static RabbitMQSinkMapper createBinarySinkToDirect(String queueName) {
        return createBinarySinkToDirect(PayloadDestinationMapper.fixed(queueName));
    }

    /**
     * Create a mapper that uses a binary format and routes payloads using a {@link PayloadDestinationMapper}
     * expected to produce routing keys. The default {@code ""} exchange will be used.
     *
     * @param mapper the destination mapper to be applied on every payload
     * @return a binary formatted direct sink mapper
     */
    public static RabbitMQSinkMapper createBinarySinkToDirect(PayloadDestinationMapper<String> mapper) {
        return new RabbitMQSinkMapper(mapper.andThen(publisher ->
                Mono.just("").zipWith(Mono.from(publisher), RoutingMetadata::create)),
                new RabbitMQBinarySinkMapper());
    }

    /**
     * Create a mapper that uses a binary format and routes payloads using a {@link PayloadDestinationMapper}
     * expected to produce routing keys. A given exchange name is used.
     *
     * @param mapper the destination mapper to be applied on every payload
     * @param exchange the destination exchange to use on each payload sent
     * @return a binary formatted sink mapper using a named exchange
     */
    public static RabbitMQSinkMapper createBinarySinkToExchange(PayloadDestinationMapper<String> mapper,
                                                                String exchange) {
        return new RabbitMQSinkMapper(mapper.andThen(publisher ->
                Mono.just(exchange).zipWith(Mono.from(publisher), RoutingMetadata::create)),
                new RabbitMQBinarySinkMapper());
    }

    /**
     * Create a mapper that uses a binary format and routes payloads using a {@link PayloadDestinationMapper}
     * expected to produce {@link RoutingMetadata} instances, defining both exchange and routing key.
     *
     * @param mapper the destination mapper to be applied on every payload
     * @return a binary formatted sink mapper using a customized routing strategy
     */
    public static RabbitMQSinkMapper createBinarySink(PayloadDestinationMapper<RoutingMetadata> mapper) {
        return new RabbitMQSinkMapper(mapper, new RabbitMQBinarySinkMapper());
    }

    /**
     * Change the underlying {@link SinkMapper} this mapper used to convert the content of a payload.
     *
     * @param sinkMapper a custom mapper to use in the new instance
     * @return a new instance using the given parameter
     */
    public RabbitMQSinkMapper withContentMapper(SinkMapper<byte[]> sinkMapper) {
        return new RabbitMQSinkMapper(destinationMapper, sinkMapper);
    }

    /**
     * Change the underlying mapper to use {@link JacksonJsonSinkMapper} for its content.
     *
     * @param objectMapper Jackson resources to use for mapping
     * @return a new instance using the given parameter
     */
    public RabbitMQSinkMapper withJsonContentMapper(ObjectMapper objectMapper) {
        return new RabbitMQSinkMapper(destinationMapper, new JacksonJsonSinkMapper(objectMapper));
    }

    @Override
    public Publisher<OutboundMessage> apply(ConnectPayload payload) {
        return Mono.from(destinationMapper.getDestination(payload))
                .zipWith(Mono.from(contentMapper.apply(payload)))
                .map(function((key, content) -> new OutboundMessage(key.getExchange(), key.getRoutingKey(), content)));
    }
}
