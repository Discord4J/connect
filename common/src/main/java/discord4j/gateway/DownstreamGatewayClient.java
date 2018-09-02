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
import discord4j.gateway.json.Opcode;
import discord4j.gateway.json.dispatch.Dispatch;
import discord4j.gateway.json.dispatch.Ready;
import reactor.core.publisher.*;
import reactor.core.scheduler.Schedulers;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.concurrent.WaitStrategy;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class DownstreamGatewayClient implements GatewayClient {

    private static final Logger log = Loggers.getLogger(DownstreamGatewayClient.class);

    private final EmitterProcessor<Dispatch> dispatch = EmitterProcessor.create(false);
    private final EmitterProcessor<GatewayPayload<?>> receiver = EmitterProcessor.create(false);
    private final EmitterProcessor<GatewayPayload<?>> sender = EmitterProcessor.create(false);

    private final AtomicInteger lastSequence = new AtomicInteger(0);
    private final AtomicReference<String> sessionId = new AtomicReference<>("");

    private final FluxSink<Dispatch> dispatchSink;
    private final FluxSink<GatewayPayload<?>> receiverSink;
    private final FluxSink<GatewayPayload<?>> senderSink;

    private final PayloadSink sink;
    private final PayloadSource source;
    private final MonoProcessor<Void> closeFuture = MonoProcessor.create(WaitStrategy.parking());

    public DownstreamGatewayClient(PayloadSink sink, PayloadSource source) {
        this.sink = sink;
        this.source = source;
        this.dispatchSink = dispatch.sink(FluxSink.OverflowStrategy.LATEST);
        this.receiverSink = receiver.sink(FluxSink.OverflowStrategy.LATEST);
        this.senderSink = sender.sink(FluxSink.OverflowStrategy.LATEST);
    }

    @Override
    public Mono<Void> execute(String gatewayUrl) {
        return Mono.defer(() -> {
            Mono<Void> inboundFuture = source.receive(payload -> {
                if (receiverSink.isCancelled()) {
                    return Mono.error(new IllegalStateException("Sender was cancelled"));
                }
                receiverSink.next(payload);
                return Mono.empty();
            }).then();

            Mono<Void> receiverFuture = receiver.map(this::updateSequence).doOnNext(this::handlePayload).then();

            Mono<Void> senderFuture = sink.send(sender)
                .subscribeOn(Schedulers.newSingle("payload-sender"))
                .then();

            return Mono.zip(inboundFuture, receiverFuture, senderFuture, closeFuture)
                .doOnError(t -> log.error("Gateway client error: {}", t.toString()))
                .doOnCancel(() -> close(false))
                .then();
        });
    }

    private GatewayPayload<?> updateSequence(GatewayPayload<?> payload) {
        if (payload.getSequence() != null) {
            lastSequence.set(payload.getSequence());
        }
        return payload;
    }

    private void handlePayload(GatewayPayload<?> payload) {
        if (Opcode.DISPATCH.equals(payload.getOp())) {
            if (payload.getData() instanceof Ready) {
                String newSessionId = ((Ready) payload.getData()).getSessionId();
                sessionId.set(newSessionId);
            }
            if (payload.getData() != null) {
                dispatchSink.next((Dispatch) payload.getData());
            }
        }
    }

    @Override
    public void close(boolean reconnect) {
        if (reconnect) {
            senderSink.next(new GatewayPayload<>(Opcode.RECONNECT, null, null, null));
        } else {
            senderSink.next(new GatewayPayload<>());
            senderSink.complete();
            closeFuture.onComplete();
        }
    }

    @Override
    public Flux<Dispatch> dispatch() {
        return dispatch;
    }

    @Override
    public Flux<GatewayPayload<?>> receiver() {
        return receiver;
    }

    @Override
    public FluxSink<GatewayPayload<?>> sender() {
        return senderSink;
    }

    @Override
    public String getSessionId() {
        return sessionId.get();
    }

    @Override
    public int getLastSequence() {
        return lastSequence.get();
    }

}
