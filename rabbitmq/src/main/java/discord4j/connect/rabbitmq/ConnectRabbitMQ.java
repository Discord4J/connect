package discord4j.connect.rabbitmq;

import com.rabbitmq.client.Address;
import com.rabbitmq.client.ConnectionFactory;
import reactor.rabbitmq.*;

import java.util.List;

/**
 * An aggregation of a RabbitMQ sender and receiver abstractions.
 */
public class ConnectRabbitMQ {

    private final Sender sender;
    private final Receiver receiver;

    ConnectRabbitMQ(final SenderOptions senderOptions, final ReceiverOptions receiverOptions) {
        this.sender = RabbitFlux.createSender(senderOptions);
        this.receiver = RabbitFlux.createReceiver(receiverOptions);
    }

    /**
     * Create a default RabbitMQ client. Uses the default parameters as given by {@link ConnectionFactory}.
     *
     * @return a default client that can connect to a RabbitMQ broker
     */
    public static ConnectRabbitMQ createDefault() {
        return new ConnectRabbitMQ(new SenderOptions(), new ReceiverOptions());
    }

    /**
     * Create a RabbitMQ client from the given settings object.
     *
     * @param settings a group of settings to customize a client
     * @return a client that can connect to a RabbitMQ broker
     */
    public static ConnectRabbitMQ createFromSettings(ConnectRabbitMQSettings settings) {
        return new ConnectRabbitMQ(settings.getSenderOptions(), settings.getReceiverOptions());
    }

    /**
     * Create a RabbitMQ client that can connect to a broker, using the semantics provided in
     * {@link ConnectionFactory#newConnection(List)}.
     *
     * @param clusterIps known broker addresses to try in order
     * @return a client that can connect to a RabbitMQ broker
     */
    public static ConnectRabbitMQ createWithAddresses(Address... clusterIps) {
        return new ConnectRabbitMQ(
                new SenderOptions().connectionSupplier(connectionFactory -> connectionFactory.newConnection(clusterIps)),
                new ReceiverOptions().connectionSupplier(connectionFactory -> connectionFactory.newConnection(clusterIps))
        );
    }

    /**
     * Return the created RabbitMQ sender for broker operations.
     *
     * @return a RabbitMQ sender
     */
    public Sender getSender() {
        return sender;
    }

    /**
     * Return the created RabbitMQ receiver for broker operations.
     *
     * @return a RabbitMQ receiver
     */
    public Receiver getReceiver() {
        return receiver;
    }

    /**
     * Close and free resources created by this client. Follows the semantics of {@link AutoCloseable#close()}.
     */
    public void close() {
        sender.close();
        receiver.close();
    }

}
