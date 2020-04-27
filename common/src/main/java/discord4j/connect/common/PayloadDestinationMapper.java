package discord4j.connect.common;

import reactor.core.publisher.Mono;

import java.util.Map;

/**
 * Reactive mapper for destination queue of payloads
 */
@FunctionalInterface
public interface PayloadDestinationMapper {

    /**
     * Takes a payload and specifies the outgoing queue for it
     *
     * @param payload the payload about to process
     * @return the queue name for the payload
     */
    Mono<String> getDestination(ConnectPayload payload);

    /**
     * A shard aware destination-mapper to queue all payloads into a specific queue for their shard
     *
     * @param queuePrefix the prefix for the queue name
     * @return a {@link PayloadDestinationMapper} which returns queues in format {@code queuePrefix + "-" + shardId}
     */
    static PayloadDestinationMapper shardAware(final String queuePrefix) {
        return source -> Mono.fromCallable(() -> source)
                .map(payload -> payload.getShard().getIndex())
                .map(shardId -> queuePrefix + "-" + shardId);
    }

    /**
     * A gateway aware destination-mapper to queue queue all payloads into a specific queue for their gateway-node
     *
     * @param queuePrefix the prefix for the queue name
     * @param gatewayShardMap a map of shardId - gatewayId for resolving the gatewayId by it the given payload shard
     * @return a {@link PayloadDestinationMapper} which returns queues in format {@code queuePrefix + "-" + gatewayId}
     */
    static PayloadDestinationMapper gatewayAware(final String queuePrefix,
                                                 final Map<Integer, Integer> gatewayShardMap) {
        return source -> Mono.fromCallable(() -> source)
                .map(payload -> payload.getShard().getIndex())
                .map(gatewayShardMap::get)
                .map(gatewayId -> queuePrefix + "-" + gatewayId);
    }

}
