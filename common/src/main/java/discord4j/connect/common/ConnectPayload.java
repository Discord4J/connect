package discord4j.connect.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import discord4j.gateway.SessionInfo;
import discord4j.gateway.ShardInfo;
import discord4j.gateway.json.GatewayPayload;

public class ConnectPayload {

    private final ShardInfo shard;
    private final SessionInfo session;
    private final GatewayPayload<?> payload;

    @JsonCreator
    public ConnectPayload(@JsonProperty("shard") ShardInfo shard,
                          @JsonProperty("session") SessionInfo session,
                          @JsonProperty("payload") GatewayPayload<?> payload) {
        this.shard = shard;
        this.session = session;
        this.payload = payload;
    }

    public ShardInfo getShard() {
        return shard;
    }

    public SessionInfo getSession() {
        return session;
    }

    public GatewayPayload<?> getPayload() {
        return payload;
    }
}
