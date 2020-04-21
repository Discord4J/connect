# RabbitMQ Discord4J Connect Examples

This folder contains examples to set up an RSocket-based implementation of every component required to work with a distributed Discord4J-based bot.

## Components
- D4J Leaders: connect to Gateway and communicates to RabbitMQ server
- D4J Workers: connect to RabbitMQ broker and processes messages from leaders

## Usage

Please check `shared` folder for a reference implementation with the following capabilities:

- Leader/Worker topology: multiple leaders connect to Gateway, multiple workers consume payloads.
- Shared subscription means every worker reads from a work queue consuming payloads. There is no guarantee about the shard index received by each worker.

