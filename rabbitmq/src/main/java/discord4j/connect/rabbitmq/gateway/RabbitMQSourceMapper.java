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
import com.rabbitmq.client.Delivery;
import discord4j.connect.common.ConnectPayload;
import discord4j.connect.common.SourceMapper;
import org.reactivestreams.Publisher;

/**
 * A higher level mapper that can consume {@link Delivery} instances.
 */
public class RabbitMQSourceMapper implements SourceMapper<Delivery> {

    private final SourceMapper<byte[]> contentMapper;

    RabbitMQSourceMapper(SourceMapper<byte[]> contentMapper) {
        this.contentMapper = contentMapper;
    }

    /**
     * Create a mapper that can consume sources formatted according to {@link RabbitMQBinarySinkMapper}.
     *
     * @return a binary formatted direct source mapper
     */
    public static RabbitMQSourceMapper createBinarySource() {
        return new RabbitMQSourceMapper(new RabbitMQBinarySourceMapper());
    }

    /**
     * Create a mapper that can consume sources formatted according to {@link JacksonJsonSinkMapper}.
     *
     * @param objectMapper Jackson resources to use for mapping
     * @return a JSON formatted direct source mapper
     */
    public static RabbitMQSourceMapper createJsonSource(ObjectMapper objectMapper) {
        return new RabbitMQSourceMapper(new JacksonJsonSourceMapper(objectMapper));
    }

    @Override
    public Publisher<ConnectPayload> apply(Delivery source) {
        return contentMapper.apply(source.getBody());
    }
}
