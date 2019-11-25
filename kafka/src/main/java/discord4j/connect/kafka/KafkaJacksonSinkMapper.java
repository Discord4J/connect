package discord4j.connect.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import discord4j.connect.common.ConnectPayload;
import discord4j.connect.common.SinkMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.SenderRecord;
import reactor.util.Logger;
import reactor.util.Loggers;

public class KafkaJacksonSinkMapper implements SinkMapper<SenderRecord<String, String, Integer>> {

    private static final Logger log = Loggers.getLogger(KafkaJacksonSinkMapper.class);

    private final ObjectMapper mapper;
    private final String topic;

    public KafkaJacksonSinkMapper(ObjectMapper mapper, String topic) {
        this.mapper = mapper;
        this.topic = topic;
    }

    @Override
    public Publisher<SenderRecord<String, String, Integer>> apply(ConnectPayload payload) {
        return toJson(mapper, payload)
                .map(json -> SenderRecord.create(
                        new ProducerRecord<>(topic, payload.getShard().format(), json),
                        payload.getSession().getSequence()));
    }

    private static Mono<String> toJson(ObjectMapper mapper, ConnectPayload payload) {
        return Mono.fromCallable(() -> {
            try {
                return mapper.writeValueAsString(payload);
            } catch (JsonProcessingException e) {
                log.warn("Unable to serialize {}: {}", payload, e);
                return null;
            }
        });
    }
}
