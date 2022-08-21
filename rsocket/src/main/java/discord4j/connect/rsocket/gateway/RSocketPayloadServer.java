package discord4j.connect.rsocket.gateway;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.core.RSocketServer;
import io.rsocket.transport.netty.server.CloseableChannel;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.DefaultPayload;
import org.reactivestreams.Publisher;
import reactor.core.publisher.*;
import reactor.extra.processor.WorkQueueProcessor;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.function.Tuple2;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * An RSocket-based payload server, holding queues to process messages from multiple sources.
 */
public class RSocketPayloadServer {

    private static final Logger log = Loggers.getLogger(RSocketPayloadServer.class);

    private final TcpServerTransport serverTransport;
    private final Function<String, FluxProcessor<Payload, Payload>> processorFactory;

    private final Map<String, FluxProcessor<Payload, Payload>> queues = new ConcurrentHashMap<>();
    private final Map<String, FluxSink<Payload>> sinks = new ConcurrentHashMap<>();

    public RSocketPayloadServer(InetSocketAddress socketAddress) {
        this(socketAddress, topic -> {
            if (topic.contains("outbound")) {
                log.info("Creating fanout queue: {}", topic);
                return EmitterProcessor.create(1024, false);
            }
            log.info("Creating work queue: {}", topic);
            return WorkQueueProcessor.<Payload>builder()
                    .autoCancel(false)
                    .share(true)
                    .name(topic)
                    .bufferSize(1024)
                    .build();
        });
    }

    public RSocketPayloadServer(InetSocketAddress socketAddress,
                                Function<String, FluxProcessor<Payload, Payload>> processorFactory) {
        this.serverTransport = TcpServerTransport.create(socketAddress);
        this.processorFactory = processorFactory;
    }

    public Mono<CloseableChannel> start() {
        return RSocketServer.create((setup, sendingSocket) -> Mono.just(leaderAcceptor(sendingSocket)))
                .bind(serverTransport);
    }

    private RSocket leaderAcceptor(RSocket sendingSocket) {
        return new RSocket() {

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
                                assert first != null;
                                String[] command = getCommand(first);
                                String key = command[0];
                                String topic = command[1];
                                if (key.equals("produce")) {
                                    return flux.doOnSubscribe(s -> log.debug("[{}] Producing to {}", id, topic))
                                            .map(payload -> {
                                                log.trace("[{}] Produce to {}: {}", id, topic, payload.getDataUtf8());
                                                getSink(topic).next(payload);
                                                return DefaultPayload.create("OK", topic);
                                            });
                                } else if (key.equals("consume")) {
                                    // flux is a sequence of "ACKs" to trigger the next payload
                                    return Flux.defer(() -> getQueue(topic))
                                            .limitRate(1)
                                            .zipWith(flux)
                                            .map(Tuple2::getT1)
                                            .doOnSubscribe(s -> log.debug("[{}] Consuming from {}", id, topic))
                                            .doOnNext(payload -> {
                                                if (sendingSocket.availability() < 1.0d) {
                                                    throw new IllegalStateException("Consumer is unavailable");
                                                }
                                            });
                                }
                            }
                            return Flux.error(new IllegalArgumentException(
                                    "Invalid routing: must be produce, consume"));
                        })
                        .doFinally(s -> log.info("[{}] Terminating channel after {}", id, s));
            }

            private String[] getCommand(Payload payload) {
                if (!payload.hasMetadata()) {
                    throw new IllegalArgumentException("Missing topic metadata");
                }
                String metadata = payload.getMetadataUtf8();
                String[] tokens = metadata.split(":", 2);
                if (tokens.length != 2 || tokens[1].isEmpty()) {
                    throw new IllegalArgumentException("Invalid topic metadata");
                }
                return tokens;
            }

            private FluxProcessor<Payload, Payload> getQueue(String topic) {
                return queues.computeIfAbsent(topic, processorFactory);
            }

            private FluxSink<Payload> getSink(String topic) {
                return sinks.computeIfAbsent(topic, k -> getQueue(k).sink(FluxSink.OverflowStrategy.LATEST));
            }
        };
    }
}
