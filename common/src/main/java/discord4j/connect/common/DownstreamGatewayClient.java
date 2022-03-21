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

import discord4j.common.close.CloseStatus;
import discord4j.common.close.DisconnectBehavior;
import discord4j.discordjson.json.gateway.Dispatch;
import discord4j.discordjson.json.gateway.Opcode;
import discord4j.gateway.GatewayClient;
import discord4j.gateway.GatewayConnection;
import discord4j.gateway.SessionInfo;
import discord4j.gateway.ShardInfo;
import discord4j.gateway.json.GatewayPayload;
import discord4j.gateway.json.ShardAwareDispatch;
import discord4j.gateway.json.ShardGatewayPayload;
import discord4j.gateway.payload.PayloadReader;
import discord4j.gateway.payload.PayloadWriter;
import discord4j.gateway.retry.GatewayStateChange;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.IllegalReferenceCountException;
import org.reactivestreams.Publisher;
import reactor.core.Scannable;
import reactor.core.publisher.*;
import reactor.core.scheduler.Schedulers;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.concurrent.Queues;
import reactor.util.retry.Retry;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;

/**
 * A {@link GatewayClient} implementation that connects to a {@link PayloadSource} and {@link PayloadSink} to
 * communicate messages across multiple nodes. This implementation does not establish any connection to Discord
 * Gateway and does not provide meaningful state about the actual Gateway connection.
 */
public class DownstreamGatewayClient implements GatewayClient {

    private static final Logger log = Loggers.getLogger(DownstreamGatewayClient.class);
    private static final Logger senderLog = Loggers.getLogger("discord4j.gateway.protocol.sender");
    private static final Logger receiverLog = Loggers.getLogger("discord4j.gateway.protocol.receiver");

    private final Flux<Dispatch> dispatch;
    private final Flux<GatewayPayload<?>> receiver;
    private final Flux<GatewayPayload<?>> sender;

    private final AtomicInteger lastSequence = new AtomicInteger(0);
    private final AtomicInteger shardCount = new AtomicInteger();
    private final AtomicBoolean firstPayload = new AtomicBoolean();
    private final AtomicReference<String> sessionId = new AtomicReference<>("");

    private final Sinks.Many<Dispatch> dispatchSink = Sinks.many().multicast().onBackpressureBuffer(Queues.SMALL_BUFFER_SIZE, false);
    private final Sinks.Many<GatewayPayload<?>> receiverSink = Sinks.many().multicast().onBackpressureBuffer(Queues.SMALL_BUFFER_SIZE, false);
    private final Sinks.Many<GatewayPayload<?>> senderSink = Sinks.many().multicast().onBackpressureBuffer(Queues.SMALL_BUFFER_SIZE, false);

    private final PayloadSink sink;
    private final PayloadSource source;
    private final PayloadReader payloadReader;
    private final PayloadWriter payloadWriter;
    private final ShardInfo initialShardInfo;
    private final boolean filterByIndex;
    private final Sinks.One<Object> closeFuture =  Sinks.one();

    public DownstreamGatewayClient(ConnectGatewayOptions gatewayOptions) {
        this.sink = gatewayOptions.getPayloadSink();
        this.source = gatewayOptions.getPayloadSource();
        this.payloadReader = gatewayOptions.getPayloadReader();
        this.payloadWriter = gatewayOptions.getPayloadWriter();
        this.initialShardInfo = gatewayOptions.getIdentifyOptions().getShardInfo();
        this.filterByIndex = initialShardInfo.getIndex() != 0 || initialShardInfo.getCount() != 1;
        this.shardCount.set(gatewayOptions.getIdentifyOptions().getShardInfo().getCount());
        this.dispatch = dispatchSink.asFlux();
        this.receiver = receiverSink.asFlux();
        this.sender = senderSink.asFlux();
    }

    @Override
    public Mono<Void> execute(String gatewayUrl) {
        return Mono.defer(() -> {
            // Receive from upstream -> Send to user
            Mono<Void> inboundFuture = source.receive(
                    inPayload -> {
                        if (Boolean.TRUE.equals(receiverSink.scan(Scannable.Attr.CANCELLED))) {
                            return Mono.error(new IllegalStateException("Sender was cancelled"));
                        }

                        if (filterByIndex && inPayload.getShard().getIndex() != initialShardInfo.getIndex()) {
                            return Mono.empty();
                        }

                        if (firstPayload.compareAndSet(false, true)) {
                            // TODO: improve state updates for this client
                            while (dispatchSink.tryEmitNext(GatewayStateChange.connected()).isFailure()) {
                                LockSupport.parkNanos(10);
                            }
                        }
                        sessionId.set(inPayload.getSession().getId());
                        shardCount.set(inPayload.getShard().getCount());

                        logPayload(receiverLog, inPayload);

                        return Flux.from(payloadReader.read(Unpooled.wrappedBuffer(inPayload.getPayload().getBytes(StandardCharsets.UTF_8))))
                                .map(payload -> new ShardGatewayPayload<>(payload, inPayload.getShard().getIndex()))
                                .doOnNext(gatewayPayload -> {while (receiverSink.tryEmitNext(gatewayPayload).isFailure()) {LockSupport.parkNanos(10);}})
                                .then();
                    })
                    .then();

            Mono<Void> receiverFuture = receiver.map(this::updateSequence)
                    .doOnNext(this::handlePayload)
                    .then();

            // Receive from user -> Send to upstream
            Mono<Void> senderFuture =
                    sink.send(sender.flatMap(payload -> Flux.from(payloadWriter.write(payload))
                            .map(buf -> {
                                ConnectPayload cp = new ConnectPayload(getShardInfo(payload), getSessionInfo(),
                                        buf.toString(StandardCharsets.UTF_8));
                                safeRelease(buf);
                                logPayload(senderLog, cp);
                                return cp;
                            })
                            .doOnDiscard(ByteBuf.class, DownstreamGatewayClient::safeRelease)))
                            .subscribeOn(Schedulers.newSingle("payload-sender"))
                            .then();

            return Mono.zip(inboundFuture, receiverFuture, senderFuture, closeFuture.asMono())
                    .doOnError(t -> log.error("Gateway client error: {}", t.toString()))
                    .doOnCancel(() -> close(false))
                    .retryWhen(Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(2)).maxBackoff(Duration.ofSeconds(30)))
                    .then();
        });
    }

    private static void safeRelease(ByteBuf buf) {
        if (buf.refCnt() > 0) {
            try {
                buf.release();
            } catch (IllegalReferenceCountException e) {
                if (log.isDebugEnabled()) {
                    log.debug("", e);
                }
            }
        }
    }

    private void logPayload(Logger logger, ConnectPayload payload) {
        if (logger.isTraceEnabled()) {
            logger.trace(payload.toString().replaceAll("(\"token\": ?\")([A-Za-z0-9._-]*)(\")", "$1hunter2$3"));
        }
    }

    private ShardInfo getShardInfo(GatewayPayload<?> payload) {
        if (payload instanceof ShardGatewayPayload) {
            ShardGatewayPayload<?> shardPayload = (ShardGatewayPayload<?>) payload;
            return ShardInfo.create(shardPayload.getShardIndex(), getShardCount());
        }
        return initialShardInfo;
    }

    private SessionInfo getSessionInfo() {
        return SessionInfo.create(getSessionId(), getSequence());
    }

    private GatewayPayload<?> updateSequence(GatewayPayload<?> payload) {
        if (payload.getSequence() != null) {
            lastSequence.set(payload.getSequence());
        }
        return payload;
    }

    private void handlePayload(GatewayPayload<?> payload) {
        if (Opcode.DISPATCH.equals(payload.getOp())) {
            if (payload.getData() != null) {
                if (payload instanceof ShardGatewayPayload) {
                    ShardGatewayPayload<?> shardPayload = (ShardGatewayPayload<?>) payload;
                    while (dispatchSink.tryEmitNext(new ShardAwareDispatch(shardPayload.getShardIndex(), getShardCount(),
                            (Dispatch) payload.getData())).isFailure()) {
                        LockSupport.parkNanos(10);
                    }
                } else {
                    while (dispatchSink.tryEmitNext((Dispatch) payload.getData()).isFailure()) {
                        LockSupport.parkNanos(10);
                    }
                }
            }
        }
    }

    @Override
    public int getShardCount() {
        return shardCount.get();
    }

    @Override
    public Mono<Boolean> isConnected() {
        // TODO: add support for DownstreamGatewayClient::isConnected
        return Mono.just(true);
    }

    @Override
    public Flux<GatewayConnection.State> stateEvents() {
        return Flux.empty();
    }

    @Override
    public Duration getResponseTime() {
        // TODO: add support for DownstreamGatewayClient::getResponseTime
        return Duration.ZERO;
    }

    @Override
    public Mono<CloseStatus> close(boolean allowResume) {
        return Mono.fromRunnable(() -> {
            while (dispatchSink.tryEmitNext(GatewayStateChange.disconnected(DisconnectBehavior.stop(null),
                    allowResume ? CloseStatus.ABNORMAL_CLOSE : CloseStatus.NORMAL_CLOSE)).isFailure()) {
                LockSupport.parkNanos(10);
            }
            senderSink.tryEmitComplete();
            closeFuture.tryEmitEmpty();
        });
    }

    @Override
    public Flux<Dispatch> dispatch() {
        return dispatch.doOnSubscribe(s -> log.info("Subscribed to dispatch sequence"));
    }

    @Override
    public Flux<GatewayPayload<?>> receiver() {
        return receiver;
    }

    @Override
    public <T> Flux<T> receiver(Function<ByteBuf, Publisher<? extends T>> mapper) {
        // have to convert to ByteBuf since we don't directly use it at the downstream level
        return receiver.flatMap(payloadWriter::write).flatMap(mapper);
    }

    @Override
    public Sinks.Many<GatewayPayload<?>> sender() {
        return senderSink;
    }

    @Override
    public Mono<Void> sendBuffer(Publisher<ByteBuf> publisher) {
        return Flux.from(publisher).flatMap(payloadReader::read).doOnNext(gatewayPayload -> { while (senderSink.tryEmitNext(gatewayPayload).isFailure()) {
            LockSupport.parkNanos(10);
        }}).then();
    }

    @Override
    public String getSessionId() {
        return sessionId.get();
    }

    @Override
    public int getSequence() {
        return lastSequence.get();
    }
}
