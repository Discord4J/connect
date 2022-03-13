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

package discord4j.connect;

import java.net.InetAddress;
import java.net.UnknownHostException;

public final class Constants {


    // TODO: use env variables?
    // also better for kubernetes

    // rabbitmq variables
    private static final String RABBITMQ_HOST_INTERNAL = System.getenv("RABBITMQ_HOST");
    public static final String RABBITMQ_HOST = (null == RABBITMQ_HOST_INTERNAL || RABBITMQ_HOST_INTERNAL.isEmpty()) ? "": RABBITMQ_HOST_INTERNAL;
    // internal default in rabbitmq : -1
    public static final String RABBITMQ_PORT_INTERNAL = System.getenv("RABBITMQ_PORT");
    public static int RABBITMQ_PORT;
    static {
        try {
            RABBITMQ_PORT = (null == RABBITMQ_PORT_INTERNAL || RABBITMQ_PORT_INTERNAL.isEmpty()) ? -1 : Integer.parseInt(RABBITMQ_PORT_INTERNAL);
        } catch (NumberFormatException e) {
            RABBITMQ_PORT = -1;
        }
    }

    // rsocket variables
    private static String LOCALHOST;
    static {
        try {
            LOCALHOST = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            LOCALHOST = "0.0.0.0";
        }
    }
    private static final String RSOCKET_ROUTER_HOST = System.getenv("RSOCKET_ROUTER_HOST");
    private static final String RSOCKET_SHARD_COORDINATOR_HOST = System.getenv("RSOCKET_SHARD_COORDINATOR_HOST");
    private static final String RSOCKET_PAYLOAD_HOST = System.getenv("RSOCKET_PAYLOAD_HOST");

    public static String GLOBAL_ROUTER_SERVER_HOST = (null == RSOCKET_ROUTER_HOST || RSOCKET_ROUTER_HOST.isEmpty()) ? LOCALHOST : RSOCKET_ROUTER_HOST;
    public static String SHARD_COORDINATOR_SERVER_HOST = (null == RSOCKET_SHARD_COORDINATOR_HOST || RSOCKET_SHARD_COORDINATOR_HOST.isEmpty()) ? LOCALHOST : RSOCKET_SHARD_COORDINATOR_HOST;
    public static String PAYLOAD_SERVER_HOST = (null == RSOCKET_PAYLOAD_HOST || RSOCKET_PAYLOAD_HOST.isEmpty()) ? LOCALHOST : RSOCKET_PAYLOAD_HOST;
    public static int GLOBAL_ROUTER_SERVER_PORT = 33331;
    public static int SHARD_COORDINATOR_SERVER_PORT = 33332;
    public static int PAYLOAD_SERVER_PORT = 33333;

    // redis variables
    private static final String CLIENT_URI = System.getenv("REDIS_CLIENT_URI");
    public static String REDIS_CLIENT_URI = (null == CLIENT_URI || CLIENT_URI.isEmpty()) ? "redis://localhost:6379" : CLIENT_URI;

    private Constants() {
    }
}
