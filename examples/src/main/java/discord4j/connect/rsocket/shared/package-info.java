/**
 * Examples to a shared-subscription RSocket distributed Discord4J architecture. A shared subscription has a
 * leader-worker topology where workers consume payloads from every shard, distributing the load.
 */
package discord4j.connect.rsocket.shared;