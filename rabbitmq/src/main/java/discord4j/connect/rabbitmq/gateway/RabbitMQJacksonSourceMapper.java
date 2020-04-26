package discord4j.connect.rabbitmq.gateway;

import com.fasterxml.jackson.databind.ObjectMapper;
import discord4j.connect.common.ConnectPayload;
import discord4j.connect.common.SourceMapper;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

public class RabbitMQJacksonSourceMapper implements SourceMapper<byte[]> {

    private final ObjectMapper objectMapper;

    public RabbitMQJacksonSourceMapper(final ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public Publisher<ConnectPayload> apply(final byte[] source) {
        return Mono.fromCallable(() -> objectMapper.readValue(source, ConnectPayload.class));
    }
}
