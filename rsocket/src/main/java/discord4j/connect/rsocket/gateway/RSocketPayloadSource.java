package discord4j.connect.rsocket.gateway;

import discord4j.common.retry.ReconnectOptions;
import discord4j.connect.common.ConnectPayload;
import discord4j.connect.common.PayloadSource;
import discord4j.connect.common.SourceMapper;
import discord4j.connect.rsocket.ConnectRSocket;
import io.rsocket.Payload;
import io.rsocket.util.DefaultPayload;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.net.InetSocketAddress;
import java.util.function.Function;

/**
 * Implementation of {@link PayloadSource} that is capable of connecting to an RSocket server to receive
 * {@link ConnectPayload} messages.
 */
public class RSocketPayloadSource implements PayloadSource {

    private static final Logger log = Loggers.getLogger(RSocketPayloadSource.class);

    private final SourceMapper<Payload> mapper;
    private final Flux<Payload> inbound;

    public RSocketPayloadSource(InetSocketAddress serverAddress, String topic, SourceMapper<Payload> mapper) {
        ConnectRSocket socket = new ConnectRSocket("pl-source", serverAddress, ctx -> true, ReconnectOptions.create());
        this.mapper = mapper;
        this.inbound = socket.withSocket(
                rSocket -> {
                    UnicastProcessor<Payload> acks = UnicastProcessor.create();
                    acks.onNext(DefaultPayload.create("START", "consume:" + topic));
                    return rSocket.requestChannel(acks)
                            .doOnNext(payload -> acks.onNext(DefaultPayload.create("ACK")));
                })
                .doOnSubscribe(s -> log.info("Begin receiving from server"))
                .doFinally(s -> log.info("Receiver completed after {}", s))
                .share(); // allow multicasting inbound payload
    }

    @Override
    public Flux<?> receive(Function<ConnectPayload, Mono<Void>> processor) {
        return this.inbound.flatMap(mapper::apply).flatMap(processor);
    }
}
