package discord4j.connect.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import discord4j.connect.common.ConnectPayload;
import discord4j.connect.common.SourceMapper;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.io.IOException;

public class KafkaJacksonSourceMapper implements SourceMapper<ReceiverRecord<String, String>> {

    private static final Logger log = Loggers.getLogger(KafkaJacksonSourceMapper.class);

    private final ObjectMapper mapper;

    public KafkaJacksonSourceMapper(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public Publisher<ConnectPayload> apply(ReceiverRecord<String, String> source) {
        return fromJson(mapper, source.value());
    }

    private Mono<ConnectPayload> fromJson(ObjectMapper mapper, String value) {
        return Mono.fromCallable(() -> {
            try {
                return mapper.readValue(value, ConnectPayload.class);
            } catch (IOException e) {
                log.warn("Unable to deserialize {}: {}", value, e);
                return null;
            }
        });
    }
}
