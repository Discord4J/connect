package discord4j.connect.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import discord4j.gateway.SessionInfo;
import discord4j.gateway.ShardInfo;

public class ConnectPayload {

    private final ShardInfo shard;
    private final SessionInfo session;
    private final String payload;

    @JsonCreator
    public ConnectPayload(@JsonProperty("shard") ShardInfo shard,
                          @JsonProperty("session") SessionInfo session,
                          @JsonProperty("payload") String payload) {
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

    public String getPayload() {
        return payload;
    }

    @Override
    public String toString() {
        return "ConnectPayload{" +
                "shard=" + shard +
                ", session=" + session +
                ", payload='" + payload + '\'' +
                '}';
    }
}
