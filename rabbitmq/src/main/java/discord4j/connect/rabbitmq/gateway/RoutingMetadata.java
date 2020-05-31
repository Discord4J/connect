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

import reactor.rabbitmq.OutboundMessage;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A pair of exchange and routingKey for RabbitMQ based payloads.
 */
public class RoutingMetadata {

    private static final Map<Tuple2<String, String>, RoutingMetadata> CACHE = new ConcurrentHashMap<>();

    private final String exchange;
    private final String routingKey;

    RoutingMetadata(String exchange, String routingKey) {
        this.exchange = exchange;
        this.routingKey = routingKey;
    }

    public static RoutingMetadata create(String exchange, String routingKey) {
        return CACHE.computeIfAbsent(Tuples.of(exchange, routingKey),
                t2 -> new RoutingMetadata(t2.getT1(), t2.getT2()));
    }

    public static RoutingMetadata create(OutboundMessage outboundMessage) {
        return CACHE.computeIfAbsent(Tuples.of(outboundMessage.getExchange(), outboundMessage.getRoutingKey()),
                t2 -> new RoutingMetadata(t2.getT1(), t2.getT2()));
    }

    public String getExchange() {
        return exchange;
    }

    public String getRoutingKey() {
        return routingKey;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RoutingMetadata that = (RoutingMetadata) o;
        return Objects.equals(exchange, that.exchange) &&
                Objects.equals(routingKey, that.routingKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(exchange, routingKey);
    }

    @Override
    public String toString() {
        return "RoutingMetadata{" +
                "exchange='" + exchange + '\'' +
                ", routingKey='" + routingKey + '\'' +
                '}';
    }
}
