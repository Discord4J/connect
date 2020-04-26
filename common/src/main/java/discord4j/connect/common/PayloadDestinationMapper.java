package discord4j.connect.common;

import reactor.core.publisher.Mono;

/**
 * Reactive mapper for destination queue of payloads
 */
public interface PayloadDestinationMapper {

    /**
     * Takes a payload and specifies the outgoing queue for it
     *
     * @param payload the payload about to process
     * @return the queue name for the payload
     */
    Mono<String> getDestination(ConnectPayload payload);

}
