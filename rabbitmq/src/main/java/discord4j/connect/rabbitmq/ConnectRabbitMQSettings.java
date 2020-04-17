package discord4j.connect.rabbitmq;

import com.rabbitmq.client.Address;
import reactor.rabbitmq.ReceiverOptions;
import reactor.rabbitmq.SenderOptions;

public class ConnectRabbitMQSettings {

    /**
     * Creates a new RabbitMQ settings object
     * @return settings object
     */
    public static ConnectRabbitMQSettings create() {
        return new ConnectRabbitMQSettings();
    }

    private final SenderOptions senderOptions;
    private final ReceiverOptions receiverOptions;

    /**
     * Private constructor, use {@link ConnectRabbitMQSettings#create()} instead
     */
    private ConnectRabbitMQSettings() {
        this.senderOptions = new SenderOptions();
        this.receiverOptions = new ReceiverOptions();
    }

    /**
     * Set the RabbitMQs host-and-port list as {@link Address} array
     * @param addresses array of {@link Address} to which RabbitMQ nodes the client should connect
     * @return this builder
     */
    public ConnectRabbitMQSettings withAddresses(final Address... addresses) {
        senderOptions.connectionSupplier(connectionFactory -> connectionFactory.newConnection(addresses));
        receiverOptions.connectionSupplier(connectionFactory -> connectionFactory.newConnection(addresses));
        return this;
    }

    /**
     * Sets the RabbitMQs host list with default ports
     * @param hosts array of hostnames to which RabbitMQ nodes the client should connect on default port
     * @return this builder
     */
    public ConnectRabbitMQSettings withAddresses(final String... hosts) {
        final Address[] addresses = new Address[hosts.length];
        for (int i = 0; i < hosts.length; i++) {
            addresses[i] = new Address(hosts[i]);
        }
        return withAddresses(addresses);
    }

    /**
     * Sets the RabbitMQs host and port
     * @param host hostname to connect to
     * @param port port to connect to
     * @return this builder
     */
    public ConnectRabbitMQSettings withAddress(final String host, final int port) {
        return this.withAddresses(new Address(host, port));
    }

    /**
     * Sets the RabbitMQs host
     * This method will use the default port
     * @param host hostname to connect to
     * @return this builder
     */
    public ConnectRabbitMQSettings withAddress(final String host) {
        return this.withAddresses(new Address(host));
    }

    /**
     * The username to use for authorization
     * @param user username for authorization
     * @return this builder
     */
    public ConnectRabbitMQSettings withUser(final String user) {
        senderOptions.getConnectionFactory().setUsername(user);
        receiverOptions.getConnectionFactory().setUsername(user);
        return this;
    }

    /**
     * The password to use for authorization
     * @param password password for authorization
     * @return this builder
     */
    public ConnectRabbitMQSettings withPassword(final String password) {
        senderOptions.getConnectionFactory().setPassword(password);
        receiverOptions.getConnectionFactory().setPassword(password);
        return this;
    }

    /**
     * Internal method to retrieve the whole settings object for the RabbitMQ sender
     * @return SenderOptions
     */
    SenderOptions getSenderOptions() {
        return senderOptions;
    }

    /**
     * Internal method to retrieve the whole settings object for the RabbitMQ receiver
     * @return ReceiverOptions
     */
    ReceiverOptions getReceiverOptions() {
        return receiverOptions;
    }
}
