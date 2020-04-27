package discord4j.connect.common.gateway;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import discord4j.discordjson.json.gateway.*;
import discord4j.gateway.json.dispatch.EventNames;
import reactor.util.annotation.Nullable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class ConnectPayloadDeserializer extends StdDeserializer<ConnectGatewayPayload<?>> {

    private static final String OP_FIELD = "op";
    private static final String D_FIELD = "d";
    private static final String T_FIELD = "t";
    private static final String S_FIELD = "s";

    private static final Map<String, Class<? extends Dispatch>> dispatchTypes = new HashMap<>();

    static {
        dispatchTypes.put(EventNames.READY, Ready.class);
        dispatchTypes.put(EventNames.RESUMED, Resumed.class);
        dispatchTypes.put(EventNames.CHANNEL_CREATE, ChannelCreate.class);
        dispatchTypes.put(EventNames.CHANNEL_UPDATE, ChannelUpdate.class);
        dispatchTypes.put(EventNames.CHANNEL_DELETE, ChannelDelete.class);
        dispatchTypes.put(EventNames.CHANNEL_PINS_UPDATE, ChannelPinsUpdate.class);
        dispatchTypes.put(EventNames.GUILD_CREATE, GuildCreate.class);
        dispatchTypes.put(EventNames.GUILD_UPDATE, GuildUpdate.class);
        dispatchTypes.put(EventNames.GUILD_DELETE, GuildDelete.class);
        dispatchTypes.put(EventNames.GUILD_BAN_ADD, GuildBanAdd.class);
        dispatchTypes.put(EventNames.GUILD_BAN_REMOVE, GuildBanRemove.class);
        dispatchTypes.put(EventNames.GUILD_EMOJIS_UPDATE, GuildEmojisUpdate.class);
        dispatchTypes.put(EventNames.GUILD_INTEGRATIONS_UPDATE, GuildIntegrationsUpdate.class);
        dispatchTypes.put(EventNames.GUILD_MEMBER_ADD, GuildMemberAdd.class);
        dispatchTypes.put(EventNames.GUILD_MEMBER_REMOVE, GuildMemberRemove.class);
        dispatchTypes.put(EventNames.GUILD_MEMBER_UPDATE, GuildMemberUpdate.class);
        dispatchTypes.put(EventNames.GUILD_MEMBERS_CHUNK, GuildMembersChunk.class);
        dispatchTypes.put(EventNames.GUILD_ROLE_CREATE, GuildRoleCreate.class);
        dispatchTypes.put(EventNames.GUILD_ROLE_UPDATE, GuildRoleUpdate.class);
        dispatchTypes.put(EventNames.GUILD_ROLE_DELETE, GuildRoleDelete.class);
        dispatchTypes.put(EventNames.MESSAGE_CREATE, MessageCreate.class);
        dispatchTypes.put(EventNames.MESSAGE_UPDATE, MessageUpdate.class);
        dispatchTypes.put(EventNames.MESSAGE_DELETE, MessageDelete.class);
        dispatchTypes.put(EventNames.MESSAGE_DELETE_BULK, MessageDeleteBulk.class);
        dispatchTypes.put(EventNames.MESSAGE_REACTION_ADD, MessageReactionAdd.class);
        dispatchTypes.put(EventNames.MESSAGE_REACTION_REMOVE, MessageReactionRemove.class);
        dispatchTypes.put(EventNames.MESSAGE_REACTION_REMOVE_ALL, MessageReactionRemoveAll.class);
        dispatchTypes.put(EventNames.PRESENCE_UPDATE, PresenceUpdate.class);
        dispatchTypes.put(EventNames.TYPING_START, TypingStart.class);
        dispatchTypes.put(EventNames.USER_UPDATE, UserUpdate.class);
        dispatchTypes.put(EventNames.VOICE_STATE_UPDATE, VoiceStateUpdateDispatch.class);
        dispatchTypes.put(EventNames.VOICE_SERVER_UPDATE, VoiceServerUpdate.class);
        dispatchTypes.put(EventNames.WEBHOOKS_UPDATE, WebhooksUpdate.class);
        dispatchTypes.put(EventNames.INVITE_CREATE, InviteCreate.class);
        dispatchTypes.put(EventNames.INVITE_DELETE, InviteDelete.class);

        // Ignored
        dispatchTypes.put(EventNames.PRESENCES_REPLACE, null);
        dispatchTypes.put(EventNames.GIFT_CODE_UPDATE, null);
    }

    public ConnectPayloadDeserializer() {
        super(ConnectGatewayPayload.class);
    }

    @Override
    public ConnectGatewayPayload<?> deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        final ObjectCodec codec = p.getCodec();
        JsonNode payload = codec.readTree(p);

        int op = payload.get(OP_FIELD).asInt();
        String t = payload.get(T_FIELD).asText();
        Integer s = payload.get(S_FIELD).isNull() ? null : payload.get(S_FIELD).intValue();

        Class<? extends PayloadData> payloadType = getPayloadType(op, t);
        JsonNode rawData = payload.get(D_FIELD);
        if (op != Opcode.DISPATCH.getRawOp()) {
            PayloadData data = payloadType == null ? null : codec.treeToValue(rawData, payloadType);
            return new ConnectGatewayPayload(Opcode.forRaw(op), data, s, t);
        }
        Function<JsonNode, ?> deserializer = jsonNode -> {
            try {
                return payloadType == null ? null : codec.treeToValue(rawData, payloadType);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
            return null;
        };
        return new ConnectGatewayPayload(Opcode.forRaw(op), rawData, s, t, deserializer);
    }

    @Nullable
    private static Class<? extends PayloadData> getPayloadType(int op, String t) {
        if (op == Opcode.DISPATCH.getRawOp()) {
            if (!dispatchTypes.containsKey(t)) {
                throw new IllegalArgumentException("Attempt to deserialize payload with unknown event type: " + t);
            }
            return dispatchTypes.get(t);
        }

        Opcode<?> opcode = Opcode.forRaw(op);
        if (opcode == null) {
            throw new IllegalArgumentException("Attempt to deserialize payload with unknown op: " + op);
        }
        return opcode.getPayloadType();
    }
}