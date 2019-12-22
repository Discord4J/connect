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
import discord4j.connect.common.SourceMapper;
import io.rsocket.Payload;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;

/**
 * An implementation of {@link SourceMapper} converting RSocket {@link Payload} instances to {@link ConnectPayload}.
 */
public class RSocketJacksonSourceMapper implements SourceMapper<Payload> {

    private final ObjectMapper mapper;

    public RSocketJacksonSourceMapper(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public Publisher<ConnectPayload> apply(Payload source) {
        return Mono.fromCallable(() -> {
            ByteBuffer buf = source.getData();
            byte[] array = new byte[buf.remaining()];
            buf.get(array);
            return mapper.readValue(array, ConnectPayload.class);
        });
    }
}
