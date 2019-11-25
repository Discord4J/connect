package discord4j.connect.rsocket.gateway;

import com.fasterxml.jackson.databind.ObjectMapper;
import discord4j.connect.common.ConnectPayload;
import discord4j.connect.common.SinkMapper;
import io.rsocket.Payload;
import io.rsocket.util.DefaultPayload;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;

public class RSocketJacksonSinkMapper implements SinkMapper<Payload> {

    private final ObjectMapper mapper;
    private final String topic;

    public RSocketJacksonSinkMapper(ObjectMapper mapper, String topic) {
        this.mapper = mapper;
        this.topic = topic;
    }

    @Override
    public Publisher<Payload> apply(ConnectPayload payload) {
        return Mono.fromCallable(() -> DefaultPayload.create(mapper.writeValueAsBytes(payload),
                ("produce:" + topic).getBytes(StandardCharsets.UTF_8)));
    }
}
