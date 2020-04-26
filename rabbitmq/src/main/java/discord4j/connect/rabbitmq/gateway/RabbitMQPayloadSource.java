package discord4j.connect.rabbitmq.gateway;

import com.rabbitmq.client.Address;
import com.rabbitmq.client.Delivery;
import discord4j.connect.common.ConnectPayload;
import discord4j.connect.common.PayloadSource;
import discord4j.connect.common.SourceMapper;
import discord4j.connect.rabbitmq.ConnectRabbitMQ;
import discord4j.connect.rabbitmq.ConnectRabbitMQSettings;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.util.function.Function;

public class RabbitMQPayloadSource implements PayloadSource {

    private static final Logger log = Loggers.getLogger(RabbitMQPayloadSource.class);

    private final SourceMapper<byte[]> mapper;
    private final Flux<byte[]> inbound;

    public RabbitMQPayloadSource(final String queue, final SourceMapper<byte[]> mapper, final ConnectRabbitMQSettings settings) {
        final ConnectRabbitMQ rabbitMQ = new ConnectRabbitMQ(settings);
        this.mapper = mapper;
        this.inbound = rabbitMQ.consume(queue)
            .map(Delivery::getBody)
            .doOnSubscribe(s -> log.info("Begin receiving from server"))
            .doFinally(s -> log.info("Receiver completed after {}", s))
            .share(); // allow multicasting inbound payload
    }

    @Override
    public Flux<?> receive(Function<ConnectPayload, Mono<Void>> processor) {
        return this.inbound.flatMap(mapper::apply).flatMap(processor);
    }

}
