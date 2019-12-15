package discord4j.connect;

import discord4j.connect.support.BotSupport;
import discord4j.core.DiscordClientBuilder;
import discord4j.core.shard.ShardingStrategy;
import discord4j.store.jdk.JdkStoreService;
import reactor.blockhound.BlockHound;

public class ExampleSingleWithConnection {

    public static void main(String[] args) {
        BlockHound.install();
        DiscordClientBuilder.create(System.getenv("token"))
                .build()
                .gateway()
                .setStoreService(new JdkStoreService())
                .setSharding(ShardingStrategy.fixed(1))
                .withConnection(client -> BotSupport.create(client).eventHandlers())
                .block();
    }
}
