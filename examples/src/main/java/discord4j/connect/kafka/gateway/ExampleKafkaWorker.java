package discord4j.connect.kafka.gateway;

import discord4j.common.JacksonResources;
import discord4j.connect.common.ConnectGatewayOptions;
import discord4j.connect.common.DownstreamGatewayClient;
import discord4j.connect.kafka.KafkaJacksonSinkMapper;
import discord4j.connect.kafka.KafkaJacksonSourceMapper;
import discord4j.connect.kafka.KafkaPayloadSink;
import discord4j.connect.kafka.KafkaPayloadSource;
import discord4j.connect.support.BotSupport;
import discord4j.core.DiscordClient;
import discord4j.core.GatewayDiscordClient;
import discord4j.store.api.readonly.ReadOnlyStoreService;
import discord4j.store.redis.RedisStoreService;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ExampleKafkaWorker {

    public static void main(String[] args) {
        String downstreamTopic = System.getenv("downstreamTopic"); // inbound
        String upstreamTopic = System.getenv("upstreamTopic"); // outbound
        Properties props = getKafkaProperties(System.getenv("brokers"));
        JacksonResources jackson = new JacksonResources();

        /*
        the DOWNSTREAM node subscribes to downstreamTopic to receive data from the UPSTREAM node, this the SOURCE
        the DOWNSTREAM node publishes to upstreamTopic to send data to the UPSTREAM node, this is the SINK
         */

        KafkaPayloadSource<String, String> downReceiverSource = new KafkaPayloadSource<>(props, downstreamTopic,
                new KafkaJacksonSourceMapper(jackson.getObjectMapper()));
        KafkaPayloadSink<String, String, Integer> downSenderSink = new KafkaPayloadSink<>(props,
                new KafkaJacksonSinkMapper(jackson.getObjectMapper(), upstreamTopic));

        GatewayDiscordClient client = DiscordClient.builder(System.getenv("token"))
                .setJacksonResources(jackson)
                .build()
                .gateway()
                .setStoreService(new ReadOnlyStoreService(new RedisStoreService()))
                .setExtraOptions(opts -> new ConnectGatewayOptions(opts, downSenderSink, downReceiverSource))
                .connect(DownstreamGatewayClient::new)
                .blockOptional()
                .orElseThrow(RuntimeException::new);

        BotSupport.create(client)
                .eventHandlers()
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
