package discord4j.connect.rabbitmq.gateway;

import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.ShutdownSignalException;
import discord4j.connect.common.ConnectPayload;
import discord4j.connect.common.PayloadSource;
import discord4j.connect.common.SourceMapper;
import discord4j.connect.rabbitmq.ConnectRabbitMQ;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.rabbitmq.ConsumeOptions;
import reactor.rabbitmq.QueueSpecification;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.retry.RetryBackoffSpec;
import reactor.util.retry.RetrySpec;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.function.Function;

/**
 * A RabbitMQ consumer that can process a stream of incoming payloads.
 */
public class RabbitMQPayloadSource implements PayloadSource {

    private static final Logger log = Loggers.getLogger(RabbitMQPayloadSource.class);

    public static final RetryBackoffSpec DEFAULT_RETRY_STRATEGY =
            RetrySpec.fixedDelay(Long.MAX_VALUE, Duration.ofSeconds(1))
                    .filter(t -> !(t instanceof ShutdownSignalException))
                    .doBeforeRetry(retry -> log.info("Consumer retry {} due to {}", retry.totalRetriesInARow(),
                            retry.failure()));

    private final SourceMapper<Delivery> mapper;
    private final ConnectRabbitMQ rabbitMQ;
    private final ConsumeOptions consumeOptions;
    private final Collection<String> queues;
    private final RetryBackoffSpec consumeErrorStrategy;

    RabbitMQPayloadSource(SourceMapper<Delivery> mapper, ConnectRabbitMQ rabbitMQ, ConsumeOptions consumeOptions,
                          Collection<String> queues, RetryBackoffSpec consumeErrorStrategy) {
        this.mapper = mapper;
        this.rabbitMQ = rabbitMQ;
        this.consumeOptions = consumeOptions;
        this.queues = queues;
        this.consumeErrorStrategy = consumeErrorStrategy;
    }

    /**
     * Create a default source using the given parameters, able to subscribe to a list of queues.
     *
     * @param mapper mapper to read {@link Delivery} instances from the received messages
     * @param rabbitMQ RabbitMQ broker abstraction
     * @param queues a list of queues that should be subscribed to
     * @return a source ready to consume payloads
     */
    public static RabbitMQPayloadSource create(SourceMapper<Delivery> mapper,
                                               ConnectRabbitMQ rabbitMQ,
                                               String... queues) {
        return new RabbitMQPayloadSource(mapper, rabbitMQ, new ConsumeOptions(), Arrays.asList(queues),
                DEFAULT_RETRY_STRATEGY);
    }

    /**
     * Create a default source using the given parameters, able to subscribe to a list of queues.
     *
     * @param mapper mapper to read {@link Delivery} instances from the received messages
     * @param rabbitMQ RabbitMQ broker abstraction
     * @param queues a list of queues that should be subscribed to
     * @return a source ready to consume payloads
     */
    public static RabbitMQPayloadSource create(SourceMapper<Delivery> mapper,
                                               ConnectRabbitMQ rabbitMQ,
                                               Collection<String> queues) {
        return new RabbitMQPayloadSource(mapper, rabbitMQ, new ConsumeOptions(), queues, DEFAULT_RETRY_STRATEGY);
    }

    /**
     * Customize the {@link ConsumeOptions} used when consuming each payload.
     *
     * @param consumeOptions options to configure receiving
     * @return a new instance with the given parameter
     */
    public RabbitMQPayloadSource withConsumeOptions(ConsumeOptions consumeOptions) {
        return new RabbitMQPayloadSource(mapper, rabbitMQ, consumeOptions, queues, consumeErrorStrategy);
    }

    /**
     * Customize the retry strategy on consumer errors.
     *
     * @param consumeErrorStrategy a Reactor retrying strategy to be applied on consumer errors
     * @return a new instance with the given parameter
     */
    public RabbitMQPayloadSource withConsumeErrorStrategy(RetryBackoffSpec consumeErrorStrategy) {
        return new RabbitMQPayloadSource(mapper, rabbitMQ, consumeOptions, queues, consumeErrorStrategy);
    }

    @Override
    public Flux<?> receive(Function<ConnectPayload, Mono<Void>> processor) {
        return Flux.fromIterable(queues)
                .flatMap(queue -> Mono.just(queue)
                        .delaySubscription(rabbitMQ.getSender().declare(QueueSpecification.queue(queue))))
                .flatMap(queue -> rabbitMQ.getReceiver().consumeAutoAck(queue, consumeOptions)
                        .retryWhen(consumeErrorStrategy))
                .doOnSubscribe(s -> log.info("Begin receiving from server"))
                .doOnError(e -> log.error("Receive failed", e))
                .doFinally(s -> log.info("Receiver completed after {}", s))
                .share() // allow multi-casting inbound payload
                .flatMap(mapper::apply)
                .flatMap(processor);
    }

}
