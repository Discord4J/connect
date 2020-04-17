package discord4j.connect.rabbitmq.gateway;

import com.fasterxml.jackson.databind.ObjectMapper;
import discord4j.connect.common.ConnectPayload;
import discord4j.connect.common.SinkMapper;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

public class RabbitMQJacksonSinkMapper implements SinkMapper<byte[]> {

    private final ObjectMapper objectMapper;

    public RabbitMQJacksonSinkMapper(final ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public Publisher<byte[]> apply(final ConnectPayload payload) {
        return Mono.fromCallable(() -> objectMapper.writeValueAsBytes(payload));
    }

}
