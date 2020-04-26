package discord4j.connect.common;

import reactor.core.publisher.Mono;

public class ShardAwareDestinationMapper implements PayloadDestinationMapper {

    public final String queueName;

    public ShardAwareDestinationMapper(final String queueName) {
        this.queueName = queueName;
    }

    @Override
    public Mono<String> getDestination(final ConnectPayload source) {
        return Mono.fromCallable(() -> source)
                .map(payload -> payload.getShard().getIndex())
                .map(shardId -> queueName + "-" + shardId);
    }
}
