package discord4j.connect.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import discord4j.common.JacksonResources;
import discord4j.connect.common.gateway.PartialGatewayPayload;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.Optional;

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

    /**
     * A destination mapper which distributes payloads based on their event type.
     * This method calls {@link PayloadDestinationMapper#eventBased(ObjectMapper, String, Map)} with a new
     * object mapper created with a new {@link JacksonResources#getObjectMapper()}
     *
     * @param fallbackQueue the queue which is used in case there is no other queue defined in queueMap
     * @param queueMap Event-to-Queuename mapping
     * @return a {@link PayloadDestinationMapper} which returns event-specific queues
     */
    static PayloadDestinationMapper eventBased(final String fallbackQueue, final Map<String, String> queueMap) {
        return eventBased(new JacksonResources().getObjectMapper(), fallbackQueue, queueMap);
    }

    /**
     * A destination mapper which distributes payloads based on their event type.
     *
     * @param mapper the {@link ObjectMapper} which is used for deserialization of the payload
     * @param fallbackQueue the queue which is used in case there is no other queue defined in queueMap
     * @param queueMap Event-to-Queuename mapping
     * @return a {@link PayloadDestinationMapper} which returns event-specific queues
     */
    static PayloadDestinationMapper eventBased(final ObjectMapper mapper, final String fallbackQueue,
                                               final Map<String, String> queueMap) {
        return source -> Mono.fromCallable(() -> source)
                .flatMap(payload -> Mono.fromCallable(() -> mapper.readValue(payload.getPayload(),
                        PartialGatewayPayload.class)))
                .map(partialGatewayPayload -> Optional.ofNullable(partialGatewayPayload.getType()))
                .filter(Optional::isPresent)
                .map(event -> queueMap.getOrDefault(event.get(), fallbackQueue))
                .defaultIfEmpty(fallbackQueue);
    }

}
