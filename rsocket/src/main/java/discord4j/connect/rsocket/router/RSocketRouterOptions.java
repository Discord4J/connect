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

package discord4j.connect.rsocket.router;

import discord4j.connect.rsocket.global.RSocketGlobalRouterServer;
import discord4j.rest.request.BucketKey;
import discord4j.rest.request.DiscordWebRequest;
import discord4j.rest.request.RouterOptions;

import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.function.Function;

/**
 * Additional {@link RouterOptions} to configure an {@link RSocketRouter}.
 */
public class RSocketRouterOptions extends RouterOptions {

    private final Function<DiscordWebRequest, InetSocketAddress> requestTransportMapper;

    /**
     * Build a new options instance, based off a parent {@link RouterOptions}.
     *
     * @param parent the original options instance
     * @param requestTransportMapper a mapper to select the proper {@link RSocketRouterServer} or
     * {@link RSocketGlobalRouterServer} from a given {@link DiscordWebRequest}. Use
     * {@link BucketKey#of(DiscordWebRequest)} to identify the Discord API bucket a requests belongs and return the
     * server address.
     */
    public RSocketRouterOptions(RouterOptions parent,
                                Function<DiscordWebRequest, InetSocketAddress> requestTransportMapper) {
        super(parent.getToken(),
                parent.getReactorResources(),
                parent.getExchangeStrategies(),
                parent.getResponseTransformers(),
                parent.getGlobalRateLimiter(),
                parent.getRequestQueueFactory(),
                parent.getDiscordBaseUrl());

        this.requestTransportMapper = Objects.requireNonNull(requestTransportMapper, "requestTransportMapper");
    }

    /**
     * Return a mapper from a {@link DiscordWebRequest} to a {@link RSocketRouterServer} or
     * {@link RSocketGlobalRouterServer} address.
     *
     * @return a function to get a server address
     */
    public Function<DiscordWebRequest, InetSocketAddress> getRequestTransportMapper() {
        return requestTransportMapper;
    }
}
