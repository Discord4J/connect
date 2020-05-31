package discord4j.connect.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import discord4j.common.JacksonResources;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

/**
 * Reactive mapper to aide in producer-side routing of payloads.
 */
@FunctionalInterface
public interface PayloadDestinationMapper<R> {

    /**
     * Takes a payload and specifies the outgoing queue for it
     *
     * @param payload the payload about to process
     * @return the queue name for the payload
     */
    Publisher<R> getDestination(ConnectPayload payload);

    /**
     * A shard aware destination-mapper to queue all payloads into a specific queue for their shard.
     *
     * @param queuePrefix the prefix for the queue name
     * @return a {@link PayloadDestinationMapper} which returns queues in format {@code queuePrefix + shardId}
     */
    static PayloadDestinationMapper<String> shardAware(final String queuePrefix) {
        return source -> Mono.fromCallable(() -> source)
                .map(payload -> payload.getShard().getIndex())
                .map(shardId -> queuePrefix + shardId);
    }

    /**
     * A gateway aware destination-mapper to queue queue all payloads into a specific queue for their gateway-node.
     *
     * @param queuePrefix the prefix for the queue name
     * @param gatewayShardMap a map of shardId to gatewayId for resolving the gatewayId by it the given payload shard
     * @return a {@link PayloadDestinationMapper} which returns queues in format {@code queuePrefix + gatewayId}
     */
    static PayloadDestinationMapper<String> gatewayAware(final String queuePrefix,
                                                         final Map<Integer, Integer> gatewayShardMap) {
        return source -> Mono.fromCallable(() -> source)
                .map(payload -> queuePrefix + gatewayShardMap.get(payload.getShard().getIndex()));
    }

    /**
     * A destination mapper which distributes payloads based on their event type.
     * This method calls {@link PayloadDestinationMapper#eventBased(ObjectMapper, String, Map)} with a new
     * object mapper created with a new {@link JacksonResources#getObjectMapper()}
     *
     * @param fallbackQueue the queue which is used in case there is no other queue defined in queueMap
     * @param queueMap Event-to-Queue name mapping
     * @return a {@link PayloadDestinationMapper} which returns event-specific queues
     */
    static PayloadDestinationMapper<String> eventBased(final String fallbackQueue, final Map<String, String> queueMap) {
        return eventBased(JacksonResources.create().getObjectMapper(), fallbackQueue, queueMap);
    }

    /**
     * A destination mapper which distributes payloads based on their event type.
     *
     * @param mapper the {@link ObjectMapper} which is used for deserialization of the payload
     * @param fallbackQueue the queue which is used in case there is no other queue defined in queueMap
     * @param queueMap Event-to-Queue name mapping
     * @return a {@link PayloadDestinationMapper} which returns event-specific queues
     */
    static PayloadDestinationMapper<String> eventBased(final ObjectMapper mapper, final String fallbackQueue,
                                                       final Map<String, String> queueMap) {
        return source -> Mono.fromCallable(() -> source)
                .flatMap(payload -> Mono.fromCallable(() -> mapper.readValue(payload.getPayload(),
                        PartialGatewayPayload.class)))
                .map(partialGatewayPayload -> Optional.ofNullable(partialGatewayPayload.getType()))
                .filter(Optional::isPresent)
                .map(event -> queueMap.getOrDefault(event.get(), fallbackQueue))
                .defaultIfEmpty(fallbackQueue);
    }

    /**
     * A basic destination mapper that always routes to the same queue.
     *
     * @param queue the queue name
     * @return a {@link PayloadDestinationMapper} which routes queue to the same given {@code queue}
     */
    static PayloadDestinationMapper<String> fixed(final String queue) {
        return source -> Mono.just(queue);
    }

    /**
     * Returns a composed function that first applies this function to its input, and then applies the {@code after}
     * function to the result.
     *
     * @param <V> the type of output of the {@code after} function, and of the composed function
     * @param after the function to apply after this function is applied
     * @return a composed function that first applies this function and then applies the {@code after} function
     */
    default <V> PayloadDestinationMapper<V> andThen(Function<? super Publisher<R>, ? extends Publisher<V>> after) {
        Objects.requireNonNull(after);
        return (ConnectPayload t) -> after.apply(getDestination(t));
    }

}
