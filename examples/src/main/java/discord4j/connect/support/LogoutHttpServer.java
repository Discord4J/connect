/*
 * This file is part of Discord4J.
 *
 * Discord4J is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Discord4J is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with Discord4J. If not, see <http://www.gnu.org/licenses/>.
 */

package discord4j.connect.support;

import discord4j.core.GatewayDiscordClient;
import reactor.core.publisher.Mono;
import reactor.netty.http.server.HttpServer;
import reactor.util.Logger;
import reactor.util.Loggers;

/**
 * A basic {@link HttpServer} example exposing an endpoint to logout a bot from Discord.
 */
public class LogoutHttpServer {

    private static final Logger log = Loggers.getLogger(LogoutHttpServer.class);

    private final GatewayDiscordClient client;

    public LogoutHttpServer(GatewayDiscordClient client) {
        this.client = client;
    }

    public static void startAsync(GatewayDiscordClient client) {
        new LogoutHttpServer(client).start();
    }

    public void start() {
        HttpServer.create()
                .port(0) // use an ephemeral port
                .route(routes -> routes
                        .get("/logout",
                                (req, res) -> {
                                    return client.logout()
                                            .then(Mono.from(res.addHeader("content-type", "application/json")
                                                    .status(200)
                                                    .chunkedTransfer(false)
                                                    .sendString(Mono.just("OK"))));
                                })
                )
                .bind()
                .doOnNext(facade -> {
                    log.info("*************************************************************");
                    log.info("Server started at {}:{}", facade.host(), facade.port());
                    log.info("*************************************************************");
                    // kill the server on JVM exit
                    Runtime.getRuntime().addShutdownHook(new Thread(() -> facade.disposeNow()));
                })
                .subscribe();
    }
}
