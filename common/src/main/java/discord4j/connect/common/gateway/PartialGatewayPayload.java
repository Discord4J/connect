package discord4j.connect.common.gateway;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import reactor.util.annotation.Nullable;

public class PartialGatewayPayload {

    private int op;
    @JsonProperty("d")
    @Nullable
    private JsonNode data;
    @JsonProperty("s")
    @Nullable
    private Integer sequence;
    @JsonProperty("t")
    @Nullable
    private String type;

    public int getOp() {
        return op;
    }

    @Nullable
    public JsonNode getData() {
        return data;
    }

    @Nullable
    public Integer getSequence() {
        return sequence;
    }

    @Nullable
    public String getType() {
        return type;
    }
}
