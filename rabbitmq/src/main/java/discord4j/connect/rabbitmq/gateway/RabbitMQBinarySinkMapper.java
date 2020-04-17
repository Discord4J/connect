package discord4j.connect.rabbitmq.gateway;

import discord4j.connect.common.ConnectPayload;
import discord4j.connect.common.SinkMapper;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;

public class RabbitMQBinarySinkMapper implements SinkMapper<byte[]> {

    /*
    Defined order:
    - Shard Count
    - Shard Index
    - Session Sequence
    - Session Id
    - Payload
     */
    @Override
    public Publisher<byte[]> apply(ConnectPayload payload) {
        return Mono.fromCallable(() -> {
            final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);
            outputStream.writeInt(payload.getShard().getCount());
            outputStream.writeInt(payload.getShard().getIndex());
            outputStream.writeInt(payload.getSession().getSequence());
            outputStream.writeUTF(payload.getSession().getId());
            outputStream.writeUTF(payload.getPayload());
            return byteArrayOutputStream.toByteArray();
        });
    }

}
