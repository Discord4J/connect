package discord4j.connect.common.gateway;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import discord4j.discordjson.json.gateway.*;
import discord4j.gateway.json.GatewayPayload;
import discord4j.gateway.json.dispatch.EventNames;
import discord4j.gateway.payload.JacksonPayloadReader;
import discord4j.gateway.payload.PayloadReader;
import io.netty.buffer.ByteBuf;
import reactor.core.Exceptions;
import reactor.core.publisher.Mono;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.annotation.Nullable;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class ConnectPayloadReader implements PayloadReader {

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

    private static final Logger log = Loggers.getLogger(JacksonPayloadReader.class);

    private final ObjectMapper mapper;
    private final boolean lenient;

    public ConnectPayloadReader(ObjectMapper mapper) {
        this(mapper, true);
    }

    public ConnectPayloadReader(ObjectMapper mapper, boolean lenient) {
        this.mapper = mapper;
        this.lenient = lenient;
    }

    @Override
    public Mono<GatewayPayload<?>> read(ByteBuf payload) {
        return Mono.create(sink -> {
            try {
                PartialGatewayPayload partial = mapper.readValue(payload.array(), PartialGatewayPayload.class);
                Class<? extends PayloadData> payloadClass = getPayloadType(partial.getOp(), partial.getType());

                Function<JsonNode, ?> dataDeserializer = jsonNode -> {
                    try {
                        return mapper.treeToValue(jsonNode, payloadClass);
                    } catch (JsonProcessingException e) {
                        e.printStackTrace();
                    }
                    return null;
                };

                ConnectGatewayPayload<?> value = new ConnectGatewayPayload(Opcode.forRaw(partial.getOp()),
                        partial.getData(), partial.getSequence(), partial.getType(), dataDeserializer);

                sink.success(value);
            } catch (IOException | IllegalArgumentException e) {
                if (lenient) {
                    // if eof input - just ignore
                    if (payload.readableBytes() > 0) {
                        log.debug("Error while decoding JSON ({}): {}", e.toString(),
                                payload.toString(StandardCharsets.UTF_8));
                    }
                    sink.success();
                } else {
                    sink.error(Exceptions.propagate(e));
                }
            }
        });
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
