package discord4j.connect.rabbitmq.gateway;

import com.rabbitmq.client.ShutdownSignalException;
import discord4j.connect.common.ConnectPayload;
import discord4j.connect.common.PayloadSink;
import discord4j.connect.common.SinkMapper;
import discord4j.connect.rabbitmq.ConnectRabbitMQ;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.rabbitmq.OutboundMessage;
import reactor.rabbitmq.SendOptions;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.retry.RetryBackoffSpec;
import reactor.util.retry.RetrySpec;

import java.time.Duration;
import java.util.Objects;
import java.util.function.BiFunction;

/**
 * A RabbitMQ producer that can send payloads to a broker or cluster.
 */
public class RabbitMQPayloadSink implements PayloadSink {

    private static final Logger log = Loggers.getLogger(RabbitMQPayloadSink.class);

    public static final RetryBackoffSpec DEFAULT_RETRY_STRATEGY =
            RetrySpec.fixedDelay(Long.MAX_VALUE, Duration.ofSeconds(1))
                    .filter(t -> !(t instanceof ShutdownSignalException))
                    .doBeforeRetry(retry -> log.info("Producer retry {} due to {}", retry.totalRetriesInARow(),
                            retry.failure()));

    private final SinkMapper<? extends OutboundMessage> mapper;
    private final ConnectRabbitMQ rabbitMQ;
    private final SendOptions sendOptions;
    private final RetryBackoffSpec sendErrorStrategy;
    private final BiFunction<ConnectRabbitMQ, RoutingMetadata, Mono<?>> onSend;

    RabbitMQPayloadSink(SinkMapper<? extends OutboundMessage> mapper, ConnectRabbitMQ rabbitMQ, SendOptions sendOptions,
                        RetryBackoffSpec sendErrorStrategy,
                        BiFunction<ConnectRabbitMQ, RoutingMetadata, Mono<?>> onSend) {
        this.mapper = mapper;
        this.rabbitMQ = rabbitMQ;
        this.sendOptions = sendOptions;
        this.sendErrorStrategy = sendErrorStrategy;
        this.onSend = onSend;
    }

    /**
     * Create a default sink using an {@link OutboundMessage} mapper.
     *
     * @param mapper mapper to derive {@link OutboundMessage} instances for sending
     * @param rabbitMQ RabbitMQ broker abstraction
     * @return a sink ready for producing payloads
     */
    public static RabbitMQPayloadSink create(SinkMapper<? extends OutboundMessage> mapper,
                                             ConnectRabbitMQ rabbitMQ) {
        return new RabbitMQPayloadSink(mapper, rabbitMQ, new SendOptions(), DEFAULT_RETRY_STRATEGY, null);
    }

    /**
     * Customize the {@link SendOptions} used when producing each payload.
     *
     * @param sendOptions options to configure publishing
     * @return a new instance with the given parameter
     */
    public RabbitMQPayloadSink withSendOptions(SendOptions sendOptions) {
        return new RabbitMQPayloadSink(mapper, rabbitMQ, sendOptions, sendErrorStrategy, onSend);
    }

    /**
     * Customize the retry strategy on sending errors.
     *
     * @param sendErrorStrategy a Reactor retrying strategy to be applied on producer errors
     * @return a new instance with the given parameter
     */
    public RabbitMQPayloadSink withSendErrorStrategy(RetryBackoffSpec sendErrorStrategy) {
        Objects.requireNonNull(sendErrorStrategy);
        return new RabbitMQPayloadSink(mapper, rabbitMQ, sendOptions, sendErrorStrategy, onSend);
    }

    /**
     * Customize the behavior to perform before a payload is sent. Can be used to declare queues, exchanges or
     * bindings. Calling this method will override any previous function. Defaults to no action, leaving you in charge
     * of declaring the right RabbitMQ resources for performance improvement.
     *
     * @param onSend a BiFunction that can be used to apply logic before a payload is sent
     * @return a new instance with the given parameter
     */
    public RabbitMQPayloadSink withBeforeSendFunction(BiFunction<ConnectRabbitMQ, RoutingMetadata, Mono<?>> onSend) {
        Objects.requireNonNull(onSend);
        return new RabbitMQPayloadSink(mapper, rabbitMQ, sendOptions, sendErrorStrategy, onSend);
    }

    @Override
    public Flux<?> send(final Flux<ConnectPayload> source) {
        Publisher<? extends OutboundMessage> messages;
        if (onSend == null) {
            messages = source.flatMap(mapper::apply);
        } else {
            messages = source.flatMap(payload -> Mono.from(mapper.apply(payload)))
                    .flatMap(message -> onSend.apply(rabbitMQ, RoutingMetadata.create(message))
                            .thenReturn(message));
        }
        return rabbitMQ.getSender().sendWithTypedPublishConfirms(messages)
                .doOnSubscribe(s -> log.info("Begin sending to server"))
                .retryWhen(sendErrorStrategy)
                .doOnError(e -> log.error("Send failed", e))
                .doFinally(s -> log.info("Sender completed after {}", s));
    }
}
