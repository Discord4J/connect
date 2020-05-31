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

import org.reactivestreams.Publisher;

/**
 * A function capable of converting a {@link ConnectPayload} into a type used by a {@link PayloadSink} implementation.
 *
 * @param <R> the target type
 */
@FunctionalInterface
public interface SinkMapper<R> {

    /**
     * Transform a single {@link ConnectPayload} into a {@link Publisher} with a target type to be used by an
     * accompanying {@link PayloadSink}.
     *
     * @param payload the message to process
     * @return a reactive response with the target type
     */
    Publisher<R> apply(ConnectPayload payload);
}
