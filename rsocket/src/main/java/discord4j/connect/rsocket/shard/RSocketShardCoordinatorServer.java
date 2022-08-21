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

package discord4j.connect.rsocket.shard;

import discord4j.common.operator.RateLimitOperator;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.core.RSocketServer;
import io.rsocket.transport.netty.server.CloseableChannel;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.DefaultPayload;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class RSocketShardCoordinatorServer {

    private static final Logger log = Loggers.getLogger(RSocketShardCoordinatorServer.class);

    private final TcpServerTransport serverTransport;
    private final AtomicInteger connected = new AtomicInteger();

    public RSocketShardCoordinatorServer(InetSocketAddress socketAddress) {
        // TODO: allow providing a custom backend - to distribute this server
        this.serverTransport = TcpServerTransport.create(socketAddress);
    }

    public Mono<CloseableChannel> start() {
        Map<String, RateLimitOperator<Payload>> limiters = new ConcurrentHashMap<>(1);
        return RSocketServer.create((setup, sendingSocket) -> Mono.just(socketAcceptor(limiters)))
                .bind(serverTransport);
    }

    private RSocket socketAcceptor(Map<String, RateLimitOperator<Payload>> limiters) {
        return new RSocket() {

            @Override
            public Mono<Payload> requestResponse(Payload payload) {
                String value = payload.getDataUtf8();
                log.debug("[request_response] >: {}", value);
                if (value.startsWith("identify")) {
                    // identify.shard_limiter_key
                    String[] tokens = value.split("\\.");
                    String limiterKey = tokens[1];
                    RateLimitOperator<Payload> limiter = limiters.computeIfAbsent(limiterKey,
                            k -> new RateLimitOperator<>(1, Duration.ofSeconds(6), Schedulers.parallel()));
                    return Mono.just(DefaultPayload.create("identify.success")).transform(limiter);
                } else if (value.equals("request.connected")) {
                    return Mono.fromCallable(connected::get).map(count -> DefaultPayload.create(String.valueOf(count)));
                }
                return Mono.empty();
            }

            @Override
            public Mono<Void> fireAndForget(Payload payload) {
                String value = payload.getDataUtf8();
                log.debug("[request_fnf] >: {}", value);
                if (value.equals("notify.connected")) {
                    return Mono.fromCallable(connected::incrementAndGet).then();
                } else if (value.equals("notify.disconnected")) {
                    return Mono.fromCallable(connected::decrementAndGet).then();
                }
                return Mono.empty();
            }
        };
    }
}
