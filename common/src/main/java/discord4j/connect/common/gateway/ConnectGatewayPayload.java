package discord4j.connect.common.gateway;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import discord4j.discordjson.json.gateway.Opcode;
import discord4j.discordjson.json.gateway.PayloadData;
import discord4j.gateway.json.GatewayPayload;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.annotation.Nullable;

import java.util.function.Function;

@JsonDeserialize(using = ConnectPayloadDeserializer.class)
@JsonIgnoreProperties({ "data" })
public class ConnectGatewayPayload<T extends PayloadData> extends GatewayPayload<T> {

    private static final Logger log = Loggers.getLogger(ConnectGatewayPayload.class);

    @JsonProperty("d")
    @Nullable
    private JsonNode rawData;
    private Function<JsonNode, T> dispatchDeserializer;
    private T cachedData;

    public ConnectGatewayPayload(Opcode<T> op, @Nullable T data, @Nullable Integer sequence, @Nullable String type) {
        super(op, data, sequence, type);
    }

    public ConnectGatewayPayload(Opcode<T> op, @Nullable JsonNode rawData, @Nullable Integer sequence,
                                 @Nullable String type, Function<JsonNode, T> dispatchDeserializer) {
        super(op, null, sequence, type);
        this.rawData = rawData;
        this.dispatchDeserializer = dispatchDeserializer;
    }

    public JsonNode getRawData() {
        return rawData;
    }

    @Override
    public T getData() {
        final T data = super.getData();
        if (data != null) {
            return data;
        }
        if (rawData == null && cachedData == null) {
            return null;
        }
        if (cachedData != null) {
            return cachedData;
        }
        cachedData = dispatchDeserializer.apply(rawData);
        dispatchDeserializer = null; // free reference to the function
        return cachedData;
    }

    @Override
    public boolean isDataPresent() {
        return super.getData() != null || rawData != null;
    }
}
