package discord4j.connect.rabbitmq.gateway;

import discord4j.connect.common.ConnectPayload;
import discord4j.connect.common.SinkMapper;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.charset.StandardCharsets;

/**
 * A mapper that can create a binary (UTF-8) representation of a {@link ConnectPayload}.
 */
public class RabbitMQBinarySinkMapper implements SinkMapper<byte[]> {

    /*
            Defined order:
            - Shard Count
            - Shard Index
            - Session Sequence
            - Session Id
            - Payload Length
            - Payload Bytes
             */
    @Override
    public Publisher<byte[]> apply(ConnectPayload payload) {
        return Mono.fromCallable(() -> {
            try (final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
                try (final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
                    outputStream.writeInt(payload.getShard().getCount());
                    outputStream.writeInt(payload.getShard().getIndex());
                    outputStream.writeInt(payload.getSession().getSequence());
                    outputStream.writeUTF(payload.getSession().getId());
                    final byte[] payloadData = payload.getPayload().getBytes(StandardCharsets.UTF_8);
                    outputStream.writeInt(payloadData.length);
                    outputStream.write(payloadData);
                    return byteArrayOutputStream.toByteArray();
                }
            }
        });
    }

}
