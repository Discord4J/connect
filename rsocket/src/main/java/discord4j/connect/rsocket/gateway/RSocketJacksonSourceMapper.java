package discord4j.connect.rsocket.gateway;

import com.fasterxml.jackson.databind.ObjectMapper;
import discord4j.connect.common.ConnectPayload;
import discord4j.connect.common.SourceMapper;
import io.rsocket.Payload;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;

public class RSocketJacksonSourceMapper implements SourceMapper<Payload> {

    private final ObjectMapper mapper;

    public RSocketJacksonSourceMapper(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public Publisher<ConnectPayload> apply(Payload source) {
        return Mono.fromCallable(() -> {
            ByteBuffer buf = source.getData();
            byte[] array = new byte[buf.remaining()];
            buf.get(array);
            return mapper.readValue(array, ConnectPayload.class);
        });
    }
}
