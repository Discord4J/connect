package discord4j.gateway;

public class ConnectGatewayOptions extends GatewayOptions {

    private final PayloadSink payloadSink;
    private final PayloadSource payloadSource;

    public ConnectGatewayOptions(GatewayOptions parent, PayloadSink payloadSink, PayloadSource payloadSource) {
        super(parent.getToken(), parent.getReactorResources(), parent.getPayloadReader(), parent.getPayloadWriter(),
                parent.getReconnectOptions(), parent.getIdentifyOptions(), parent.getInitialObserver(),
                parent.getIdentifyLimiter());
        this.payloadSink = payloadSink;
        this.payloadSource = payloadSource;
    }

    public PayloadSink getPayloadSink() {
        return payloadSink;
    }

    public PayloadSource getPayloadSource() {
        return payloadSource;
    }
}
