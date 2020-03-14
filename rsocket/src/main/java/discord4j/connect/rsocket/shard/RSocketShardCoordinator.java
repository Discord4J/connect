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

import discord4j.connect.common.Discord4JConnectException;
import discord4j.connect.rsocket.CachedRSocket;
import discord4j.core.shard.ShardCoordinator;
import discord4j.gateway.PayloadTransformer;
import discord4j.gateway.SessionInfo;
import discord4j.gateway.ShardInfo;
import discord4j.common.retry.ReconnectOptions;
import io.rsocket.util.DefaultPayload;
import reactor.core.publisher.Mono;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.net.InetSocketAddress;

public class RSocketShardCoordinator implements ShardCoordinator {

    private static final Logger log = Loggers.getLogger(RSocketShardCoordinator.class);

    private final CachedRSocket socket;

    public RSocketShardCoordinator(InetSocketAddress socketAddress) {
        this.socket = new CachedRSocket(socketAddress, ctx -> ctx.exception() instanceof Discord4JConnectException,
                ReconnectOptions.create());
    }

    @Override
    public PayloadTransformer getIdentifyLimiter(ShardInfo shardInfo, int shardingFactor) {
        int key = shardInfo.getIndex() % shardingFactor;
        return sequence -> sequence.flatMap(t2 -> socket.withSocket(rSocket ->
                rSocket.requestResponse(DefaultPayload.create("identify:" + key + ":" + t2.getT1().getResponseTime().toString()))
                        .onErrorMap(Discord4JConnectException::new)
                        .doOnNext(payload -> log.debug(">: {}", payload.getDataUtf8())))
                .then(Mono.just(t2.getT2()))
        );
    }

    @Override
    public Mono<Void> publishConnected(ShardInfo shard) {
        return Mono.empty();
    }

    @Override
    public Mono<Void> publishDisconnected(ShardInfo shard, SessionInfo session) {
        return Mono.empty();
    }
}

