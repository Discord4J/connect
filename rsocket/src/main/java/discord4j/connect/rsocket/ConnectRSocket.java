package discord4j.connect.rsocket;

import discord4j.common.retry.ReconnectOptions;
import io.rsocket.RSocket;
import io.rsocket.core.RSocketConnector;
import io.rsocket.transport.netty.client.TcpClientTransport;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.retry.Retry;
import reactor.util.retry.RetryBackoffSpec;

import java.net.InetSocketAddress;
import java.util.function.Function;
import java.util.function.Predicate;

public class ConnectRSocket {

    private static final Logger log = Loggers.getLogger(ConnectRSocket.class);

    private final String name;
    private final RetryBackoffSpec retrySpec;
    private final Mono<RSocket> rSocketMono;

    public ConnectRSocket(String name,
                          InetSocketAddress serverAddress,
                          Predicate<? super Throwable> retryPredicate,
                          ReconnectOptions reconnectOptions) {
        this.name = name;
        this.retrySpec = Retry.backoff(reconnectOptions.getMaxRetries(), reconnectOptions.getFirstBackoff())
                .maxBackoff(reconnectOptions.getMaxBackoffInterval())
                .scheduler(reconnectOptions.getBackoffScheduler())
                .transientErrors(true)
                .filter(retryPredicate);
        this.rSocketMono = RSocketConnector.create()
                .reconnect(retrySpec.doBeforeRetry(signal -> log.debug("[{}] Reconnecting to server (attempt {}): {}",
                        id(), signal.totalRetriesInARow() + 1, signal.failure().toString())))
                .connect(TcpClientTransport.create(serverAddress))
                .doOnSubscribe(s -> log.debug("[{}] Connecting to RSocket server: {}", id(), serverAddress));
    }

    public <T> Flux<T> withSocket(Function<? super RSocket, Publisher<? extends T>> socketFunction) {
        return rSocketMono.flatMapMany(socketFunction)
                .retryWhen(retrySpec.doBeforeRetry(signal ->
                        log.debug("[{}] Retrying action (attempt {}):" + " {}",
                                id(), signal.totalRetriesInARow() + 1, signal.failure().toString())));
    }

    private String id() {
        return name + "-" + Integer.toHexString(hashCode());
    }

}
