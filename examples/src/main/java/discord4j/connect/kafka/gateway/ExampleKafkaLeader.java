package discord4j.connect.kafka.gateway;

import discord4j.common.JacksonResources;
import discord4j.connect.kafka.KafkaJacksonSinkMapper;
import discord4j.connect.kafka.KafkaJacksonSourceMapper;
import discord4j.connect.kafka.KafkaPayloadSink;
import discord4j.connect.kafka.KafkaPayloadSource;
import discord4j.core.DiscordClient;
import discord4j.connect.common.ConnectGatewayOptions;
import discord4j.connect.common.UpstreamGatewayClient;
import discord4j.store.redis.RedisStoreService;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ExampleKafkaLeader {

    public static void main(String[] args) {
        String downstreamTopic = System.getenv("downstreamTopic"); // inbound
        String upstreamTopic = System.getenv("upstreamTopic"); // outbound
        Properties props = getKafkaProperties(System.getenv("brokers"));
        JacksonResources jackson = new JacksonResources();

        KafkaPayloadSink<String, String, Integer> upReceiverSink = new KafkaPayloadSink<>(props,
                new KafkaJacksonSinkMapper(jackson.getObjectMapper(), downstreamTopic));
        KafkaPayloadSource<String, String> upSenderSource = new KafkaPayloadSource<>(props, upstreamTopic,
                new KafkaJacksonSourceMapper(jackson.getObjectMapper()));

        DiscordClient.builder(System.getenv("token"))
                .setJacksonResources(jackson)
                .build()
                .gateway()
                .setStoreService(new RedisStoreService())
                .setExtraOptions(opts -> new ConnectGatewayOptions(opts, upReceiverSink, upSenderSource))
                .connect(UpstreamGatewayClient::new)
                .block()
                .onDisconnect()
                .block();
    }

    private static Properties getKafkaProperties(String brokers) {
        String serializer = StringSerializer.class.getName();
        String deserializer = StringDeserializer.class.getName();
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("group.id", "consumer");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", deserializer);
        props.put("value.deserializer", deserializer);
        props.put("key.serializer", serializer);
        props.put("value.serializer", serializer);
        return props;
    }
}
