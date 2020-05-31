package discord4j.connect.rabbitmq.gateway;

import com.fasterxml.jackson.databind.ObjectMapper;
import discord4j.connect.common.ConnectPayload;
import discord4j.connect.common.SourceMapper;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

/**
 * A mapper that can convert a byte array source into a {@link ConnectPayload} using Jackson, if the format is JSON. An
 * error is emitted if the deserialization fails.
 */
public class JacksonJsonSourceMapper implements SourceMapper<byte[]> {

    private final ObjectMapper objectMapper;

    public JacksonJsonSourceMapper(final ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public Publisher<ConnectPayload> apply(final byte[] source) {
        return Mono.fromCallable(() -> objectMapper.readValue(source, ConnectPayload.class));
    }
}
