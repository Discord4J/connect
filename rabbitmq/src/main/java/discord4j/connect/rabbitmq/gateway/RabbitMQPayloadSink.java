package discord4j.connect.rabbitmq.gateway;

import discord4j.connect.common.ConnectPayload;
import discord4j.connect.common.PayloadSink;
import discord4j.connect.common.SinkMapper;
import discord4j.connect.rabbitmq.ConnectRabbitMQ;
import discord4j.connect.rabbitmq.ConnectRabbitMQSettings;
import reactor.core.publisher.Flux;
import reactor.util.Logger;
import reactor.util.Loggers;

public class RabbitMQPayloadSink implements PayloadSink {

    private static final Logger log = Loggers.getLogger(RabbitMQPayloadSink.class);

    private final ConnectRabbitMQ rabbitMQ;
    private final SinkMapper<byte[]> mapper;
    private final String queue;

    public RabbitMQPayloadSink(final String queue, final SinkMapper<byte[]> mapper, final ConnectRabbitMQSettings settings) {
        this.rabbitMQ = new ConnectRabbitMQ(settings);
        this.queue = queue;
        this.mapper = mapper;
    }

    @Override
    public Flux<?> send(final Flux<ConnectPayload> source) {
        return rabbitMQ.declareOutboundQueue(queue)
            .thenMany(rabbitMQ.sendMany(queue, source.flatMap(mapper::apply)))
            .doOnError(e -> log.error("Send failed", e))
            .doOnSubscribe(s -> log.info("Begin sending to server"))
            .doFinally(s -> log.info("Sender completed after {}", s));
    }
}
