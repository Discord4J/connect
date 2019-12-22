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

package discord4j.connect.common;

import discord4j.gateway.GatewayClient;
import discord4j.gateway.GatewayOptions;

/**
 * A set of options to configure {@link GatewayClient} instances capable of working in a distributed way.
 */
public class ConnectGatewayOptions extends GatewayOptions {

    private final PayloadSink payloadSink;
    private final PayloadSource payloadSource;

    public ConnectGatewayOptions(GatewayOptions parent, PayloadSink payloadSink, PayloadSource payloadSource) {
        super(parent.getToken(), parent.getReactorResources(), parent.getPayloadReader(), parent.getPayloadWriter(),
                parent.getReconnectOptions(), parent.getIdentifyOptions(), parent.getInitialObserver(),
                parent.getIdentifyLimiter());
        this.payloadSink = payloadSink;
        this.payloadSource = payloadSource;
    }

    public PayloadSink getPayloadSink() {
        return payloadSink;
    }

    public PayloadSource getPayloadSource() {
        return payloadSource;
    }
}
