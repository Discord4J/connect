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

package discord4j.connect.rsocket.shared;

public final class Constants {

    // TODO: use env variables?
    public static int GLOBAL_ROUTER_SERVER_PORT = 33331;
    public static int SHARD_COORDINATOR_SERVER_PORT = 33332;
    public static int PAYLOAD_SERVER_PORT = 33333;
    public static String REDIS_CLIENT_URI = "redis://localhost:6379";

    private Constants() {
    }
}
