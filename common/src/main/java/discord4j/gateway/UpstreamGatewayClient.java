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
package discord4j.gateway;

import discord4j.gateway.json.GatewayPayload;
import discord4j.gateway.json.dispatch.Dispatch;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.time.Duration;
import java.util.function.Function;

public class UpstreamGatewayClient implements GatewayClient {

    private static final Logger log = Loggers.getLogger(UpstreamGatewayClient.class);

    private final DefaultGatewayClient delegate;
    private final PayloadSink sink;
    private final PayloadSource source;
    private final ShardInfo shardInfo;

    public UpstreamGatewayClient(ConnectGatewayOptions gatewayOptions) {
        this.delegate = new DefaultGatewayClient(gatewayOptions);
        this.shardInfo = gatewayOptions.getIdentifyOptions().getShardInfo();
        this.sink = gatewayOptions.getPayloadSink();
        this.source = gatewayOptions.getPayloadSource();
    }

    @Override
    public Mono<Void> execute(String gatewayUrl) {
        Mono<Void> senderFuture = sink.send(receiver().map(this::toConnectPayload))
                .doOnError(t -> log.error("Sender error", t))
                .then();

        Mono<Void> receiverFuture = source.receive(payloadProcessor())
                .doOnError(t -> log.error("Receiver error", t))
                .then();

        return Mono.zip(senderFuture, receiverFuture, delegate.execute(gatewayUrl)).then();
    }

    private Function<ConnectPayload, Mono<Void>> payloadProcessor() {
        FluxSink<GatewayPayload<?>> senderSink = sender();
        return payload -> {
            if (senderSink.isCancelled()) {
                return Mono.error(new IllegalStateException("Sender was cancelled"));
            }
            senderSink.next(payload.getGatewayPayload());
            return Mono.empty();
        };
    }

    private ConnectPayload toConnectPayload(GatewayPayload<?> gatewayPayload) {
        return new ConnectPayload(shardInfo, new SessionInfo(getSessionId(), getSequence()), gatewayPayload);
    }

    @Override
    public Mono<Void> close(boolean allowResume) {
        return delegate.close(allowResume);
    }

    @Override
    public Flux<Dispatch> dispatch() {
        return delegate.dispatch();
    }

    @Override
    public Flux<GatewayPayload<?>> receiver() {
        return delegate.receiver();
    }

    @Override
    public <T> Flux<T> receiver(Function<ByteBuf, Publisher<? extends T>> mapper) {
        return delegate.receiver(mapper);
    }

    @Override
    public FluxSink<GatewayPayload<?>> sender() {
        return delegate.sender();
    }

    @Override
    public Mono<Void> sendBuffer(Publisher<ByteBuf> publisher) {
        return delegate.sendBuffer(publisher);
    }

    @Override
    public String getSessionId() {
        return delegate.getSessionId();
    }

    @Override
    public int getSequence() {
        return delegate.getSequence();
    }

    @Override
    public boolean isConnected() {
        return delegate.isConnected();
    }

    @Override
    public Duration getResponseTime() {
        return delegate.getResponseTime();
    }
}
