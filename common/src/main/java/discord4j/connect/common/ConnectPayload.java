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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import discord4j.gateway.SessionInfo;
import discord4j.gateway.ShardInfo;

/**
 * Basic messaging type across Discord4J-connect implementations. Allows wrapping a payload while providing information
 * about its source.
 */
public class ConnectPayload {

    private final ShardInfo shard;
    private final SessionInfo session;
    private final String payload;

    @JsonCreator
    public ConnectPayload(@JsonProperty("shard") ShardInfo shard,
                          @JsonProperty("session") SessionInfo session,
                          @JsonProperty("payload") String payload) {
        this.shard = shard;
        this.session = session;
        this.payload = payload;
    }

    public ShardInfo getShard() {
        return shard;
    }

    public SessionInfo getSession() {
        return session;
    }

    public String getPayload() {
        return payload;
    }

    @Override
    public String toString() {
        return "ConnectPayload{" +
                "shard=" + shard +
                ", session=" + session +
                ", payload='" + payload + '\'' +
                '}';
    }
}
