package discord4j.connect.rabbitmq;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Address;
import com.rabbitmq.client.Delivery;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.rabbitmq.*;
import reactor.util.Logger;
import reactor.util.Loggers;

public class ConnectRabbitMQ {

    private static final Logger log = Loggers.getLogger(ConnectRabbitMQ.class);

    private final Sender sender;
    private final Receiver receiver;

    public ConnectRabbitMQ(final SenderOptions senderOptions, final ReceiverOptions receiverOptions) {
        this.sender = RabbitFlux.createSender(senderOptions);
        this.receiver = RabbitFlux.createReceiver(receiverOptions);
    }

    public ConnectRabbitMQ(final Address... clusterIps) {
        this(
            new SenderOptions().connectionSupplier(connectionFactory -> connectionFactory.newConnection(clusterIps)),
            new ReceiverOptions().connectionSupplier(connectionFactory -> connectionFactory.newConnection(clusterIps))
        );
    }

    public ConnectRabbitMQ(final ConnectRabbitMQSettings settings) {
        this(settings.getSenderOptions(), settings.getReceiverOptions());
    }

    public Flux<OutboundMessageResult> sendMany(final String queue, final Flux<byte[]> data) {
        return sender.sendWithPublishConfirms(data.map(it -> new OutboundMessage("", queue, it)));
    }

    public Mono<AMQP.Queue.DeclareOk> declareOutboundQueue(final String queue) {
        return sender.declareQueue(QueueSpecification.queue(queue));
    }

    public Flux<Delivery> consume(final String queue) {
        return receiver.consumeAutoAck(queue)
            .delaySubscription(sender.declareQueue(QueueSpecification.queue(queue)));
    }

    public void close() {
        sender.close();
        receiver.close();
    }

}
