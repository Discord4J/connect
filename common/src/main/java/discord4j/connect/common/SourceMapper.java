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
 * A function capable of converting a type used by a {@link PayloadSource} implementation into a sequence of
 * {@link ConnectPayload} messages.
 *
 * @param <R> the source type
 */
@FunctionalInterface
public interface SourceMapper<R> {

    /**
     * Transform a single source into a {@link Publisher} of {@link ConnectPayload} instances.
     *
     * @param source the source element provided by a {@link PayloadSource}
     * @return a reactive sequence of {@link ConnectPayload} messages
     */
    Publisher<ConnectPayload> apply(R source);
}
