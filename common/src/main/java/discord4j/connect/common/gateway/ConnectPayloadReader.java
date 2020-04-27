package discord4j.connect.common.gateway;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import discord4j.gateway.json.GatewayPayload;
import discord4j.gateway.payload.JacksonPayloadReader;
import discord4j.gateway.payload.PayloadReader;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;
import reactor.core.Exceptions;
import reactor.core.publisher.Mono;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class ConnectPayloadReader implements PayloadReader {

    private static final Logger log = Loggers.getLogger(JacksonPayloadReader.class);

    private final ObjectMapper mapper;
    private final boolean lenient;

    public ConnectPayloadReader(ObjectMapper mapper) {
        this(mapper, true);
    }

    public ConnectPayloadReader(ObjectMapper mapper, boolean lenient) {
        this.mapper = mapper;
        this.lenient = lenient;
    }

    @Override
    public Mono<GatewayPayload<?>> read(ByteBuf payload) {
        return Mono.create(sink -> {
            try {
                ConnectGatewayPayload<?> value = mapper.readValue(payload.array(), new TypeReference<ConnectGatewayPayload<?>>() {});
                sink.success(value);
            } catch (IOException | IllegalArgumentException e) {
                if (lenient) {
                    // if eof input - just ignore
                    if (payload.readableBytes() > 0) {
                        log.debug("Error while decoding JSON ({}): {}", e.toString(),
                                payload.toString(StandardCharsets.UTF_8));
                    }
                    sink.success();
                } else {
                    sink.error(Exceptions.propagate(e));
                }
            }
        });
    }
}
