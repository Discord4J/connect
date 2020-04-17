package discord4j.connect.rabbitmq;

import com.rabbitmq.client.Address;
import reactor.rabbitmq.ReceiverOptions;
import reactor.rabbitmq.SenderOptions;

public class ConnectRabbitMQSettings {

    public static ConnectRabbitMQSettings create() {
        return new ConnectRabbitMQSettings();
    }

    private final SenderOptions senderOptions;
    private final ReceiverOptions receiverOptions;

    private ConnectRabbitMQSettings() {
        this.senderOptions = new SenderOptions();
        this.receiverOptions = new ReceiverOptions();
    }

    public ConnectRabbitMQSettings withAddresses(final Address... addresses) {
        senderOptions.connectionSupplier(connectionFactory -> connectionFactory.newConnection(addresses));
        receiverOptions.connectionSupplier(connectionFactory -> connectionFactory.newConnection(addresses));
        return this;
    }

    public ConnectRabbitMQSettings withAddresses(final String... hosts) {
        final Address[] addresses = new Address[hosts.length];
        for (int i = 0; i < hosts.length; i++) {
            addresses[i] = new Address(hosts[i]);
        }
        return withAddresses(addresses);
    }

    public ConnectRabbitMQSettings withAddress(final String host, final int port) {
        return this.withAddresses(new Address(host, port));
    }

    public ConnectRabbitMQSettings withAddress(final String host) {
        return this.withAddresses(new Address(host));
    }

    public ConnectRabbitMQSettings withUser(final String user) {
        senderOptions.getConnectionFactory().setUsername(user);
        receiverOptions.getConnectionFactory().setUsername(user);
        return this;
    }

    public ConnectRabbitMQSettings withPassword(final String password) {
        senderOptions.getConnectionFactory().setPassword(password);
        receiverOptions.getConnectionFactory().setPassword(password);
        return this;
    }

    SenderOptions getSenderOptions() {
        return senderOptions;
    }

    ReceiverOptions getReceiverOptions() {
        return receiverOptions;
    }
}
