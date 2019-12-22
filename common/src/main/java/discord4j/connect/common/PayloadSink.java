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

package discord4j.connect.common;

import reactor.core.publisher.Flux;

/**
 * Reactive producer that sends from a {@link ConnectPayload} source.
 */
public interface PayloadSink {

    /**
     * Sends a sequence of messages and returns a {@link Flux} for a response.
     *
     * @param source sequence of messages to send
     * @return a {@link Flux} that can be used to signal a response
     */
    Flux<?> send(Flux<ConnectPayload> source);
}
