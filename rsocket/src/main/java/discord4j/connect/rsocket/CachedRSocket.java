package discord4j.connect.rsocket;

import discord4j.connect.common.Discord4JConnectException;
import discord4j.common.retry.ReconnectContext;
import discord4j.common.retry.ReconnectOptions;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.client.TcpClientTransport;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.retry.Retry;
import reactor.retry.RetryContext;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;

public class CachedRSocket extends AtomicReference<Mono<RSocket>> {

    private static final Logger log = Loggers.getLogger(CachedRSocket.class);

    private final InetSocketAddress serverAddress;
    private final Predicate<? super RetryContext<ReconnectContext>> retryPredicate;
    private final ReconnectOptions reconnectOptions;
    private final ReconnectContext reconnectContext;

    public CachedRSocket(InetSocketAddress serverAddress,
                         Predicate<? super RetryContext<ReconnectContext>> retryPredicate,
                         ReconnectOptions reconnectOptions) {
        this.serverAddress = serverAddress;
        this.retryPredicate = retryPredicate;
        this.reconnectOptions = reconnectOptions;
        this.reconnectContext = new ReconnectContext(reconnectOptions.getFirstBackoff(),
                reconnectOptions.getMaxBackoffInterval());
    }

    private Mono<RSocket> getSocket() {
        return updateAndGet(rSocket -> rSocket != null ? rSocket : createSocket());
    }

    private Mono<RSocket> createSocket() {
        return RSocketFactory.connect()
                .errorConsumer(t -> log.error("Client error: {}", t.toString()))
                .transport(TcpClientTransport.create(serverAddress))
                .start()
                .doOnSubscribe(s -> log.debug("Connecting to RSocket server: {}", serverAddress))
                .cache(rSocket -> Duration.ofHours(1), t -> Duration.ZERO, () -> Duration.ZERO);
    }

    public <T> Flux<T> withSocket(Function<? super RSocket, Publisher<? extends T>> socketFunction) {
        return Mono.defer(this::getSocket)
                .flatMap(rSocket -> {
                    if (rSocket.isDisposed()) {
                        set(null);
                        return Mono.error(new Discord4JConnectException("Lost connection to server"));
                    } else {
                        return Mono.just(rSocket);
                    }
                })
                .flatMapMany(socketFunction)
                .retryWhen(Retry.onlyIf(retryPredicate)
                        .retryMax(reconnectOptions.getMaxRetries())
                        .backoff(reconnectOptions.getBackoff())
                        .jitter(reconnectOptions.getJitter())
                        .withApplicationContext(reconnectContext)
                        .withBackoffScheduler(reconnectOptions.getBackoffScheduler())
                        .doOnRetry(rc -> {
                            set(null);
                            log.info("Reconnecting to server: {}", rc.exception().toString());
                        }));
    }

}
