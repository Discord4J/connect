package gateway.payload;

import com.fasterxml.jackson.databind.ObjectMapper;
import discord4j.common.JacksonResources;
import discord4j.connect.common.gateway.ConnectPayloadReader;
import discord4j.discordjson.json.gateway.PayloadData;
import discord4j.gateway.json.GatewayPayload;
import discord4j.gateway.payload.JacksonPayloadReader;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.openjdk.jmh.annotations.*;

import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Fork(5)
public class ConnectPayloadReaderBenchmark {

    private ByteBuf dispatch;
    private JacksonPayloadReader defaultPayloadReader;
    private ConnectPayloadReader connectPayloadReader;

    @Setup
    public void setup() {
        dispatch = ByteBufUtil.writeUtf8(new UnpooledByteBufAllocator(false), DISPATCH_GUILD_CREATE);
        final ObjectMapper objectMapper = new JacksonResources().getObjectMapper();
        defaultPayloadReader = new JacksonPayloadReader(objectMapper);
        connectPayloadReader = new ConnectPayloadReader(objectMapper);
    }

    @Benchmark
    public GatewayPayload<?> connectLazyLoad() {
        final GatewayPayload<?> payload = connectPayloadReader.read(dispatch).block();
        dispatch.resetReaderIndex();
        return payload;
    }

    @Benchmark
    public PayloadData connectFull() {
        final GatewayPayload<?> payload = connectPayloadReader.read(dispatch).block();
        dispatch.resetReaderIndex();
        return payload.getData();
    }

    @Benchmark
    public PayloadData defaultFull() {
        final GatewayPayload<?> payload = defaultPayloadReader.read(dispatch).block();
        dispatch.resetReaderIndex();
        return payload.getData();
    }

    private static final String DISPATCH_GUILD_CREATE = "{\"t\":\"GUILD_CREATE\",\"s\":3,\"op\":0," +
            "\"d\":{\"roles\":[{\"position\":0,\"permissions\":37215809,\"name\":\"@everyone\",\"mentionable\":false," +
            "\"managed\":false,\"id\":\"142630296464916480\",\"hoist\":false,\"color\":0},{\"position\":9," +
            "\"permissions\":3411520,\"name\":\"AIRHORN SOLUTIONS\",\"mentionable\":false,\"managed\":false," +
            "\"id\":\"191201774659567616\",\"hoist\":false,\"color\":0},{\"position\":8,\"permissions\":3411520," +
            "\"name\":\"AIRHORN SOLUTIONS\",\"mentionable\":false,\"managed\":false,\"id\":\"197777019587657728\"," +
            "\"hoist\":false,\"color\":0},{\"position\":7,\"permissions\":12922447,\"name\":\"Botwyniel\"," +
            "\"mentionable\":false,\"managed\":false,\"id\":\"197778022865043457\",\"hoist\":false,\"color\":0}," +
            "{\"position\":6,\"permissions\":473169469,\"name\":\"SirBroBot ⚔\",\"mentionable\":false," +
            "\"managed\":true,\"id\":\"282914756929781760\",\"hoist\":false,\"color\":0},{\"position\":5," +
            "\"permissions\":2146958975,\"name\":\"admin\",\"mentionable\":false,\"managed\":false," +
            "\"id\":\"305373357866745858\",\"hoist\":false,\"color\":0},{\"position\":4,\"permissions\":37031488," +
            "\"name\":\"Discedia\",\"mentionable\":false,\"managed\":true,\"id\":\"305510896493527041\"," +
            "\"hoist\":false,\"color\":0},{\"position\":1,\"permissions\":37215808,\"name\":\"DOTA GÓWNO\"," +
            "\"mentionable\":true,\"managed\":false,\"id\":\"316669830453395468\",\"hoist\":false," +
            "\"color\":10181046},{\"position\":1,\"permissions\":36982336,\"name\":\"UB3R-B0T\"," +
            "\"mentionable\":false,\"managed\":true,\"id\":\"329392056961204236\",\"hoist\":false,\"color\":0}," +
            "{\"position\":1,\"permissions\":475775,\"name\":\"Mantaro\",\"mentionable\":false,\"managed\":true," +
            "\"id\":\"331536178371362817\",\"hoist\":false,\"color\":0},{\"position\":1,\"permissions\":301264471," +
            "\"name\":\"Pancake\",\"mentionable\":false,\"managed\":true,\"id\":\"331536367647457301\"," +
            "\"hoist\":false,\"color\":0},{\"position\":1,\"permissions\":262728,\"name\":\"Gnar-bot\"," +
            "\"mentionable\":false,\"managed\":true,\"id\":\"331536746506354688\",\"hoist\":false,\"color\":0}," +
            "{\"position\":1,\"permissions\":53816912,\"name\":\"Candy Music\",\"mentionable\":false," +
            "\"managed\":true,\"id\":\"396749205231632385\",\"hoist\":false,\"color\":0},{\"position\":1," +
            "\"permissions\":3632720,\"name\":\"Shadbot\",\"mentionable\":false,\"managed\":true," +
            "\"id\":\"396749569863581697\",\"hoist\":false,\"color\":0},{\"position\":1,\"permissions\":3444288," +
            "\"name\":\"Discord Echo\",\"mentionable\":false,\"managed\":true,\"id\":\"407642471015645186\"," +
            "\"hoist\":false,\"color\":0},{\"position\":1,\"permissions\":35073600,\"name\":\"Audio recorder\"," +
            "\"mentionable\":false,\"managed\":true,\"id\":\"407642857772679168\",\"hoist\":false,\"color\":0}," +
            "{\"position\":1,\"permissions\":37056065,\"name\":\"JukeBot\",\"mentionable\":false,\"managed\":true," +
            "\"id\":\"409815638337388564\",\"hoist\":false,\"color\":0},{\"position\":1,\"permissions\":262728," +
            "\"name\":\"Logger\",\"mentionable\":false,\"managed\":true,\"id\":\"410538056358035476\"," +
            "\"hoist\":false,\"color\":0},{\"position\":1,\"permissions\":37215809,\"name\":\"new role\"," +
            "\"mentionable\":false,\"managed\":false,\"id\":\"412321525480423427\",\"hoist\":false,\"color\":0}]," +
            "\"premium_tier\":0,\"members\":[{\"user\":{\"username\":\"Sh4q\",\"id\":\"219430801526226945\"," +
            "\"discriminator\":\"5836\",\"avatar\":\"44a8eeb997c0b57ceb479a58d10c3966\"}," +
            "\"roles\":[\"316669830453395468\"],\"mute\":false,\"joined_at\":\"2017-02-10T16:06:28.534000+00:00\"," +
            "\"hoisted_role\":null,\"deaf\":false},{\"user\":{\"username\":\"Kevcio\",\"id\":\"156050903449862144\"," +
            "\"discriminator\":\"0146\",\"avatar\":\"880d0f963d8094c3d30245f51e4419bf\"}," +
            "\"roles\":[\"305373357866745858\"],\"premium_since\":null,\"nick\":null,\"mute\":false," +
            "\"joined_at\":\"2020-03-19T18:02:44.304504+00:00\",\"hoisted_role\":null,\"deaf\":false}," +
            "{\"user\":{\"username\":\"TheKoreK\",\"id\":\"142630151430209538\",\"discriminator\":\"3855\"," +
            "\"avatar\":\"a964825b074ddeeec3f4c884bd1f5caa\"},\"roles\":[\"305373357866745858\"],\"mute\":false," +
            "\"joined_at\":\"2016-01-29T14:01:52.623000+00:00\",\"hoisted_role\":null,\"deaf\":false}," +
            "{\"user\":{\"username\":\"Sh4q\",\"id\":\"219430801526226945\",\"discriminator\":\"5836\"," +
            "\"avatar\":\"44a8eeb997c0b57ceb479a58d10c3966\"},\"roles\":[\"316669830453395468\"],\"mute\":false," +
            "\"joined_at\":\"2017-02-10T16:06:28.534000+00:00\",\"hoisted_role\":null,\"deaf\":false}," +
            "{\"user\":{\"username\":\"Kevcio\",\"id\":\"156050903449862144\",\"discriminator\":\"0146\"," +
            "\"avatar\":\"880d0f963d8094c3d30245f51e4419bf\"},\"roles\":[\"305373357866745858\"]," +
            "\"premium_since\":null,\"nick\":null,\"mute\":false,\"joined_at\":\"2020-03-19T18:02:44.304504+00:00\"," +
            "\"hoisted_role\":null,\"deaf\":false},{\"user\":{\"username\":\"TheKoreK\"," +
            "\"id\":\"142630151430209538\",\"discriminator\":\"3855\"," +
            "\"avatar\":\"a964825b074ddeeec3f4c884bd1f5caa\"},\"roles\":[\"305373357866745858\"],\"mute\":false," +
            "\"joined_at\":\"2016-01-29T14:01:52.623000+00:00\",\"hoisted_role\":null,\"deaf\":false}," +
            "{\"user\":{\"username\":\"Shadbot\",\"id\":\"331146243596091403\",\"discriminator\":\"4564\"," +
            "\"bot\":true,\"avatar\":\"210d873cf4232ba43046ab8cc92dbd7d\"},\"roles\":[\"396749569863581697\"]," +
            "\"mute\":false,\"joined_at\":\"2017-12-30T19:41:08.699000+00:00\",\"hoisted_role\":null," +
            "\"deaf\":false}],\"banner\":null,\"emojis\":[{\"roles\":[],\"require_colons\":true," +
            "\"name\":\"pawko_creepy\",\"managed\":false,\"id\":\"305408262549798912\",\"available\":true," +
            "\"animated\":false},{\"roles\":[],\"require_colons\":true,\"name\":\"pawko_p\",\"managed\":false," +
            "\"id\":\"305408318225121281\",\"available\":true,\"animated\":false},{\"roles\":[]," +
            "\"require_colons\":true,\"name\":\"pawko_pogchamp\",\"managed\":false,\"id\":\"305408369706008576\"," +
            "\"available\":true,\"animated\":false},{\"roles\":[],\"require_colons\":true,\"name\":\"jozek_mlody\"," +
            "\"managed\":false,\"id\":\"305408417181204480\",\"available\":true,\"animated\":false},{\"roles\":[]," +
            "\"require_colons\":true,\"name\":\"jozek_swag\",\"managed\":false,\"id\":\"305408480183844896\"," +
            "\"available\":true,\"animated\":false},{\"roles\":[],\"require_colons\":true,\"name\":\"pawel_dumny\"," +
            "\"managed\":false,\"id\":\"305413160050098176\",\"available\":true,\"animated\":false},{\"roles\":[]," +
            "\"require_colons\":true,\"name\":\"pawel_oko\",\"managed\":false,\"id\":\"305413178123223041\"," +
            "\"available\":true,\"animated\":false},{\"roles\":[],\"require_colons\":true," +
            "\"name\":\"pawel_westwood\",\"managed\":false,\"id\":\"305413187958996992\",\"available\":true," +
            "\"animated\":false},{\"roles\":[],\"require_colons\":true,\"name\":\"pawel_zamyslony\"," +
            "\"managed\":false,\"id\":\"305413201258872842\",\"available\":true,\"animated\":false},{\"roles\":[]," +
            "\"require_colons\":true,\"name\":\"pawel_zdziwony\",\"managed\":false,\"id\":\"305413237858631682\"," +
            "\"available\":true,\"animated\":false},{\"roles\":[],\"require_colons\":true,\"name\":\"pawko_matma\"," +
            "\"managed\":false,\"id\":\"305415078562234380\",\"available\":true,\"animated\":false},{\"roles\":[]," +
            "\"require_colons\":true,\"name\":\"seba_menel1\",\"managed\":false,\"id\":\"305415105171161098\"," +
            "\"available\":true,\"animated\":false},{\"roles\":[],\"require_colons\":true,\"name\":\"seba_menel2\"," +
            "\"managed\":false,\"id\":\"305415127333863424\",\"available\":true,\"animated\":false},{\"roles\":[]," +
            "\"require_colons\":true,\"name\":\"seba_znudzony\",\"managed\":false,\"id\":\"305415143469088769\"," +
            "\"available\":true,\"animated\":false},{\"roles\":[],\"require_colons\":true,\"name\":\"poka_sowe\"," +
            "\"managed\":false,\"id\":\"310836167777976330\",\"available\":true,\"animated\":false},{\"roles\":[]," +
            "\"require_colons\":true,\"name\":\"lucyn_weeabo\",\"managed\":false,\"id\":\"315531252621115392\"," +
            "\"available\":true,\"animated\":false},{\"roles\":[],\"require_colons\":true,\"name\":\"pawel_zorro\"," +
            "\"managed\":false,\"id\":\"315531258723696642\",\"available\":true,\"animated\":false},{\"roles\":[]," +
            "\"require_colons\":true,\"name\":\"pawel_pijak\",\"managed\":false,\"id\":\"315531261567303681\"," +
            "\"available\":true,\"animated\":false}],\"verification_level\":0,\"lazy\":true," +
            "\"icon\":\"6b88842ef13e8ef4320f16212fc7418b\",\"presences\":[],\"large\":false,\"vanity_url_code\":null," +
            "\"unavailable\":false,\"region\":\"eu-central\",\"member_count\":35,\"system_channel_id\":null," +
            "\"afk_channel_id\":null,\"application_id\":null,\"name\":\"bydlaki\",\"explicit_content_filter\":0," +
            "\"rules_channel_id\":null,\"joined_at\":\"2017-12-30T19:41:08.699000+00:00\"," +
            "\"preferred_locale\":\"en-US\",\"owner_id\":\"142630151430209538\",\"system_channel_flags\":0," +
            "\"afk_timeout\":300,\"voice_states\":[{\"user_id\":\"142630151430209538\",\"suppress\":false," +
            "\"session_id\":\"0bbe1f462ac81fc2b2ef50d72087b64f\",\"self_video\":false,\"self_mute\":true," +
            "\"self_deaf\":false,\"mute\":false,\"deaf\":false,\"channel_id\":\"142630296464916481\"}," +
            "{\"user_id\":\"156050903449862144\",\"suppress\":false," +
            "\"session_id\":\"2d7b20e8113a7368b00c43e2ab024e9a\",\"self_video\":false,\"self_mute\":false," +
            "\"self_deaf\":false,\"mute\":false,\"deaf\":false,\"channel_id\":\"142630296464916481\"}," +
            "{\"user_id\":\"219430801526226945\",\"suppress\":false," +
            "\"session_id\":\"ac716893b36ba317ef28ef0b48ddc777\",\"self_video\":false,\"self_mute\":false," +
            "\"self_deaf\":false,\"mute\":false,\"deaf\":false,\"channel_id\":\"142630296464916481\"}]," +
            "\"public_updates_channel_id\":null,\"premium_subscription_count\":0,\"discovery_splash\":null," +
            "\"mfa_level\":0,\"default_message_notifications\":0,\"splash\":null,\"features\":[]," +
            "\"description\":null,\"channels\":[{\"type\":0,\"topic\":\"\",\"rate_limit_per_user\":0,\"position\":0," +
            "\"permission_overwrites\":[{\"type\":\"role\",\"id\":\"142630296464916480\",\"deny\":268435472," +
            "\"allow\":313345},{\"type\":\"member\",\"id\":\"331146243596091403\",\"deny\":0,\"allow\":805829713}]," +
            "\"parent_id\":null,\"nsfw\":true,\"name\":\"dupa\",\"last_pin_timestamp\":null," +
            "\"last_message_id\":\"699031231924404234\",\"id\":\"142630296464916480\"},{\"user_limit\":0,\"type\":2," +
            "\"position\":0,\"permission_overwrites\":[],\"name\":\"General\",\"id\":\"142630296464916481\"," +
            "\"bitrate\":64000},{\"user_limit\":0,\"type\":2,\"position\":1," +
            "\"permission_overwrites\":[{\"type\":\"role\",\"id\":\"142630296464916480\",\"deny\":0,\"allow\":0}]," +
            "\"name\":\"aaa\",\"id\":\"142633167663267840\",\"bitrate\":64000}],\"id\":\"142630296464916480\"}}";

}
