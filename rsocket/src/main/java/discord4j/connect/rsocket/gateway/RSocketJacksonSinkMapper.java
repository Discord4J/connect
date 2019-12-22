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

package discord4j.connect.rsocket.gateway;

import com.fasterxml.jackson.databind.ObjectMapper;
import discord4j.connect.common.ConnectPayload;
import discord4j.connect.common.SinkMapper;
import io.rsocket.Payload;
import io.rsocket.util.DefaultPayload;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;

/**
 * An implementation of {@link SinkMapper} converting {@link ConnectPayload} instances to RSocket {@link Payload}.
 */
public class RSocketJacksonSinkMapper implements SinkMapper<Payload> {

    private final ObjectMapper mapper;
    private final String topic;

    public RSocketJacksonSinkMapper(ObjectMapper mapper, String topic) {
        this.mapper = mapper;
        this.topic = topic;
    }

    @Override
    public Publisher<Payload> apply(ConnectPayload payload) {
        return Mono.fromCallable(() -> DefaultPayload.create(mapper.writeValueAsBytes(payload),
                ("produce:" + topic).getBytes(StandardCharsets.UTF_8)));
    }
}
