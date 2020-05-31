package discord4j.connect.rabbitmq.gateway;

import com.fasterxml.jackson.databind.ObjectMapper;
import discord4j.connect.common.ConnectPayload;
import discord4j.connect.common.SinkMapper;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

/**
 * A mapper that can convert a {@link ConnectPayload} into a JSON-formatted byte array using Jackson.
 */
public class JacksonJsonSinkMapper implements SinkMapper<byte[]> {

    private final ObjectMapper objectMapper;

    public JacksonJsonSinkMapper(final ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public Publisher<byte[]> apply(final ConnectPayload payload) {
        return Mono.fromCallable(() -> objectMapper.writeValueAsBytes(payload));
    }

}
