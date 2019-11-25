package discord4j.connect.rsocket.gateway;

import discord4j.common.LogUtil;
import io.rsocket.*;
import io.rsocket.transport.netty.server.CloseableChannel;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.DefaultPayload;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxProcessor;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.extra.processor.WorkQueueProcessor;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.function.Tuple2;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RSocketShardLeaderServer {

    private static final Logger log = Loggers.getLogger(RSocketShardLeaderServer.class);

    private final TcpServerTransport serverTransport;

    private final Map<String, FluxProcessor<Payload, Payload>> queues = new ConcurrentHashMap<>();
    private final Map<String, FluxSink<Payload>> sinks = new ConcurrentHashMap<>();

    public RSocketShardLeaderServer(InetSocketAddress socketAddress) {
        this.serverTransport = TcpServerTransport.create(socketAddress);
    }

    public Mono<CloseableChannel> start() {
        return RSocketFactory.receive()
                .errorConsumer(t -> log.error("Server error: {}", t.toString()))
                .acceptor((setup, sendingSocket) -> Mono.just(leaderAcceptor(sendingSocket)))
                .transport(serverTransport)
                .start();
    }

    private RSocket leaderAcceptor(RSocket sendingSocket) {
        return new AbstractRSocket() {

            @Override
            public Flux<Payload> requestChannel(Publisher<Payload> payloads) {

                // an incoming payload MUST have routing metadata information
                // <routing>:<topic>
                // "produce:inbound" or "consume:inbound", etc

                // produce case: [payload] [payload] ...
                // consume case: [START] [ACK] [ACK] ...

                String id = Integer.toHexString(System.identityHashCode(sendingSocket));

                return Flux.from(payloads)
                        .switchOnFirst((signal, flux) -> {
                            if (signal.hasValue()) {
                                Payload first = signal.get();
                                String[] command = getCommand(first);
                                String key = command[0];
                                String topic = command[1];
                                if (key.equals("produce")) {
                                    return flux
                                            .map(payload -> {
                                                log.trace("[{}] Produce to {}: {}", id, topic, payload.getDataUtf8());
                                                getSink(topic).next(payload);
                                                return DefaultPayload.create("OK", topic);
                                            });
                                } else if (key.equals("consume")) {
                                    // flux is a sequence of "ACKs" to trigger the next payload
                                    return Flux.defer(() -> getQueue(topic))
                                            .doOnRequest(d -> log.info("[{}] Request {}", id, d))
                                            .limitRate(1)
                                            .doOnNext(payload -> log.info("{}",
                                                    LogUtil.formatValue(payload.getDataUtf8(), 150)))
                                            .zipWith(flux.doOnNext(p -> log.info("[{}] Got {}", id, p.getDataUtf8())))
                                            .map(Tuple2::getT1)
                                            .doOnNext(payload -> {
                                                log.info("[{}] Consume payload from {}", id, command[1]);
                                                if (sendingSocket.availability() < 1.0d) {
                                                    throw new IllegalStateException("Consumer is unavailable");
                                                }
                                            });
                                }
                            }
                            return Flux.error(new IllegalArgumentException(
                                    "Invalid routing: must be produce, consume"));
                        })
                        .doOnSubscribe(s -> log.info("[{}] Starting channel", id))
                        .doFinally(s -> log.info("[{}] Terminating channel after {}", id, s));
            }

            private String[] getCommand(Payload payload) {
                if (!payload.hasMetadata()) {
                    throw new IllegalArgumentException("Missing topic metadata");
                }
                String metadata = payload.getMetadataUtf8();
                String[] tokens = metadata.split(":");
                if (tokens.length != 2 || tokens[1].isEmpty()) {
                    throw new IllegalArgumentException("Invalid topic metadata");
                }
                return tokens;
            }

            private FluxProcessor<Payload, Payload> getQueue(String topic) {
                return queues.computeIfAbsent(topic, k -> {
                    log.info("Creating work queue for topic: {}", topic);
                    return WorkQueueProcessor.<Payload>builder()
                            .autoCancel(false)
                            .share(true)
                            .name(topic)
                            .bufferSize(1024)
                            .build();
                });
            }

            private FluxSink<Payload> getSink(String topic) {
                return sinks.computeIfAbsent(topic, k -> getQueue(k).sink(FluxSink.OverflowStrategy.LATEST));
            }
        };
    }
}
