package discord4j.connect.rsocket.router;

import discord4j.connect.rsocket.global.RSocketGlobalRateLimiter;
import discord4j.core.DiscordClient;
import reactor.core.publisher.Hooks;

import java.net.InetSocketAddress;

public class ExampleRSocketRestWorker {

    public static void main(String[] args) {
        Hooks.onOperatorDebug();

        // the RSocketServer port
        InetSocketAddress serverAddress = new InetSocketAddress(12123);

        DiscordClient restOnly = DiscordClient.builder(System.getenv("token"))
                .setGlobalRateLimiter(new RSocketGlobalRateLimiter(serverAddress))
                .setExtraOptions(o -> new RSocketRouterOptions(o, request -> serverAddress))
                .build(RSocketRouter::new);
    }
}
