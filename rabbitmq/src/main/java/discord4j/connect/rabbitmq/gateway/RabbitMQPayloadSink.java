package discord4j.connect.rabbitmq.gateway;

import discord4j.connect.common.*;
import discord4j.connect.rabbitmq.ConnectRabbitMQ;
import discord4j.connect.rabbitmq.ConnectRabbitMQSettings;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.annotation.Nullable;

import java.util.HashSet;
import java.util.Set;

public class RabbitMQPayloadSink implements PayloadSink {

    private static final Logger log = Loggers.getLogger(RabbitMQPayloadSink.class);

    private final SinkMapper<byte[]> mapper;
    private final ConnectRabbitMQ rabbitMQ;
    @Nullable
    private final PayloadDestinationMapper destinationMapper;
    private final String queue;

    private final Set<String> queues;

    /**
     * Simple constructor for immutable building
     *
     * @param mapper the mapper to use for payload mapping
     * @param settings the rabbitmq settings to use for connect
     */
    public RabbitMQPayloadSink(final SinkMapper<byte[]> mapper, final ConnectRabbitMQSettings settings) {
        this(
                "default",
                mapper,
                new ConnectRabbitMQ(settings),
                null
        );
    }

    /**
     * Internal constructor
     *
     * @param queue queueName to use
     * @param mapper the mapper to use for payload mapping
     * @param rabbitMQ the rabbitmq instance
     * @param destinationMapper the destination mapper to use for queue names
     */
    private RabbitMQPayloadSink(final String queue, final SinkMapper<byte[]> mapper,
                                final ConnectRabbitMQ rabbitMQ,
                                final PayloadDestinationMapper destinationMapper) {
        this.rabbitMQ = rabbitMQ;
        this.queue = queue;
        this.mapper = mapper;
        this.destinationMapper = destinationMapper;
        if (this.destinationMapper != null) {
            queues = new HashSet<>();
        } else {
            queues = new HashSet<>(0);
        }
    }

    /**
     * Create a new {@link RabbitMQPayloadSink} with the given queue name
     * <p>
     * Calling this method resets the {@link PayloadDestinationMapper}
     *
     * @param queue the queue name to use
     * @return a new immutable instance
     */
    public RabbitMQPayloadSink setQueueName(final String queue) {
        return new RabbitMQPayloadSink(
                queue,
                mapper,
                rabbitMQ,
                null
        );
    }

    /**
     * Creates a new {@link RabbitMQPayloadSink} with the given destination mapper
     * <p>
     * Calling this method resets the queue name
     *
     * @param destinationMapper the destination mapper to use
     * @return a new immutable instance
     */
    public RabbitMQPayloadSink setDestinationMapper(final PayloadDestinationMapper destinationMapper) {
        return new RabbitMQPayloadSink(
                null,
                mapper,
                rabbitMQ,
                destinationMapper
        );
    }

    @Override
    public Flux<?> send(final Flux<ConnectPayload> source) {
        if (this.destinationMapper == null) {
            return rabbitMQ.declareOutboundQueue(queue)
                    .thenMany(rabbitMQ.sendMany(queue, source.flatMap(mapper::apply)))
                    .doOnError(e -> log.error("Send failed", e))
                    .doOnSubscribe(s -> log.info("Begin sending to server"))
                    .doFinally(s -> log.info("Sender completed after {}", s));
        } else {
            return source
                    .flatMap(payload -> Mono.zip(Mono.just(payload), destinationMapper.getDestination(payload)))
                    .flatMap(tuple -> declareQueue(tuple.getT2()).thenReturn(tuple))
                    .flatMap(tuple -> Mono.zip(Mono.from(mapper.apply(tuple.getT1())), Mono.just(tuple.getT2())))
                    .flatMap(tuple -> rabbitMQ.sendOne(tuple.getT2(), tuple.getT1()))
                    .doOnError(e -> log.error("Send failed", e))
                    .doOnSubscribe(s -> log.info("Begin sending to server"))
                    .doFinally(s -> log.info("Sender completed after {}", s));
        }
    }

    /**
     * Makes sure a "declareQueue" was called for the queue
     *
     * @param queue the queue name
     * @return An empty mono when the task has been finished
     */
    private Mono<Void> declareQueue(final String queue) {
        if (queues.contains(queue)) {
            return Mono.empty();
        }
        queues.add(queue);
        return rabbitMQ.declareOutboundQueue(queue)
                .then();
    }
}
