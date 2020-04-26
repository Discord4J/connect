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

package discord4j.connect.rsocket.shared.servers;

import discord4j.connect.rsocket.global.RSocketGlobalRouterServer;
import discord4j.connect.Constants;
import discord4j.rest.request.BucketGlobalRateLimiter;
import discord4j.rest.request.RequestQueueFactory;
import io.rsocket.transport.netty.server.CloseableChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.scheduler.Schedulers;

import java.net.InetSocketAddress;
import java.time.Duration;

/**
 * An example of an {@link RSocketGlobalRouterServer}, capable of providing permits to coordinate REST requests across
 * multiple nodes and also handle global rate limits.
 */
public class ExampleRSocketGlobalRouterServer {

    private static final Logger log = LoggerFactory.getLogger(ExampleRSocketGlobalRouterServer.class);

    public static void main(String[] args) {
        // this server combines a Router and a GlobalRateLimiter servers
        // the server keeps a GRL locally to coordinate requests across nodes
        RSocketGlobalRouterServer routerServer =
                new RSocketGlobalRouterServer(new InetSocketAddress(Constants.GLOBAL_ROUTER_SERVER_PORT),
                        BucketGlobalRateLimiter.create(), Schedulers.parallel(), RequestQueueFactory.buffering());

        // start the server
        routerServer.start()
                .doOnNext(cc -> log.info("Started global router server at {}", cc.address()))
                .retryBackoff(Long.MAX_VALUE, Duration.ofSeconds(1), Duration.ofMinutes(1))
                .flatMap(CloseableChannel::onClose)
                .block();
    }
}
