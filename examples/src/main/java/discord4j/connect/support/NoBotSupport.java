package discord4j.connect.support;

import discord4j.core.GatewayDiscordClient;
import discord4j.core.event.domain.lifecycle.ReadyEvent;
import reactor.core.publisher.Mono;
import reactor.util.Logger;
import reactor.util.Loggers;

public class NoBotSupport {

    private static final Logger log = Loggers.getLogger(NoBotSupport.class);

    private final GatewayDiscordClient client;

    public static NoBotSupport create(GatewayDiscordClient client) {
        return new NoBotSupport(client);
    }

    NoBotSupport(GatewayDiscordClient client) {
        this.client = client;
    }

    public Mono<Void> eventHandlers() {
        return readyHandler(client);
    }

    public static Mono<Void> readyHandler(GatewayDiscordClient client) {
        return client.on(ReadyEvent.class)
                .doOnNext(ready -> log.info("Shard [{}] logged in as {}", ready.getShardInfo(), ready.getSelf().getUsername()))
                .then();
    }
}
