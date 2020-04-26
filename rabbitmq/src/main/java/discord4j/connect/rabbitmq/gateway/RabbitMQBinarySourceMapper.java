package discord4j.connect.rabbitmq.gateway;

import discord4j.connect.common.ConnectPayload;
import discord4j.connect.common.SourceMapper;
import discord4j.gateway.SessionInfo;
import discord4j.gateway.ShardInfo;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.nio.charset.StandardCharsets;

public class RabbitMQBinarySourceMapper implements SourceMapper<byte[]> {

    /*
    Defined order:
    - Shard Count
    - Shard Index
    - Session Sequence
    - Session Id
    - Payload Count
    - Payload(s)
     */
    @Override
    public Publisher<ConnectPayload> apply(byte[] source) {
        return Mono.fromCallable(() -> {
            try (final DataInputStream dataInputStream = new DataInputStream(new ByteArrayInputStream(source))) {
                final int shardCount = dataInputStream.readInt();
                final int shardIndex = dataInputStream.readInt();
                final int sessionSeq = dataInputStream.readInt();
                final String sessionId = dataInputStream.readUTF();
                final int payloadLength = dataInputStream.readInt();
                final byte[] payloadData = new byte[payloadLength];
                dataInputStream.read(payloadData, 0, payloadLength);
                final String payload = new String(payloadData, StandardCharsets.UTF_8);
                return new ConnectPayload(
                    new ShardInfo(shardIndex, shardCount),
                    new SessionInfo(sessionId, sessionSeq),
                    payload
                );
            }
        });
    }
}
