# RSocket Discord4J Connect Examples

This folder contains examples to set up an RSocket-based implementation of every component required to work with a distributed Discord4J-based bot.

## Components
- D4J Leaders: connect to Gateway and communicates to payload server
- D4J Workers: connect to payload broker and processes messages from leaders
- Distributed global rate limit handling
- Distributed API request queueing (Discord4J Router concept)
- Distributed Gateway connection/IDENTIFY handling
- Distributed payload server

## Usage

Please check `shared` folder for a reference implementation with the following capabilities:

- Leader/Worker topology: multiple leaders connect to Gateway, multiple workers consume payloads.
- Shared subscription means every worker reads from a work queue consuming payloads. There is no guarantee about the shard index received by each worker.

Requires you to start three services for:

- Providing global and API bucket rate limit handling
- Providing websocket connection and IDENTIFY rate limit handling
- Providing payload transport across nodes

- It is possible to split `RSocketGlobalRouterServer` into `RSocketGlobalRateLimiterServer` and `RSocketRouterServer`
- It is possible to split `RSocketRouterServer` each covering a set of Discord's API request buckets
- It is not possible to scale each individual component at this moment (WIP)
