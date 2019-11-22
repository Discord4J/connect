package discord4j.gateway;

import discord4j.gateway.json.GatewayPayload;

public class ConnectPayload {

    private final ShardInfo shardInfo;
    private final SessionInfo sessionInfo;
    private final GatewayPayload<?> gatewayPayload;

    public ConnectPayload(ShardInfo shardInfo, SessionInfo sessionInfo, GatewayPayload<?> gatewayPayload) {
        this.shardInfo = shardInfo;
        this.sessionInfo = sessionInfo;
        this.gatewayPayload = gatewayPayload;
    }

    public ShardInfo getShardInfo() {
        return shardInfo;
    }

    public SessionInfo getSessionInfo() {
        return sessionInfo;
    }

    public GatewayPayload<?> getGatewayPayload() {
        return gatewayPayload;
    }
}
