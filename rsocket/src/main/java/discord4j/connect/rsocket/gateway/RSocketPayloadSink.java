package discord4j.connect.rsocket.gateway;

import discord4j.common.retry.ReconnectOptions;
import discord4j.connect.common.ConnectPayload;
import discord4j.connect.common.PayloadSink;
import discord4j.connect.common.SinkMapper;
import discord4j.connect.rsocket.ConnectRSocket;
import io.rsocket.Payload;
import reactor.core.publisher.Flux;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.net.InetSocketAddress;

/**
 * Implementation of {@link PayloadSink} that is capable of connecting to an RSocket server to send
 * {@link ConnectPayload} messages.
 */
public class RSocketPayloadSink implements PayloadSink {

    private static final Logger log = Loggers.getLogger(RSocketPayloadSink.class);

    private final ConnectRSocket socket;
    private final SinkMapper<Payload> mapper;

    public RSocketPayloadSink(InetSocketAddress serverAddress, SinkMapper<Payload> mapper) {
        this.socket = new ConnectRSocket("pl-sink", serverAddress, ctx -> true, ReconnectOptions.create());
        this.mapper = mapper;
    }

    @Override
    public Flux<?> send(Flux<ConnectPayload> source) {
        return socket.withSocket(rSocket -> rSocket.requestChannel(source.flatMap(mapper::apply)))
                .doOnError(e -> log.error("Send failed", e))
                .doOnSubscribe(s -> log.info("Begin sending to server"))
                .doFinally(s -> log.info("Sender completed after {}", s));
    }
}
