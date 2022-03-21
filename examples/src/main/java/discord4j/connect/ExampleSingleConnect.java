package discord4j.connect;

import discord4j.common.store.Store;
import discord4j.common.store.legacy.LegacyStoreLayout;
import discord4j.connect.support.BotSupport;
import discord4j.core.DiscordClientBuilder;
import discord4j.core.GatewayDiscordClient;
import discord4j.core.shard.ShardingStrategy;
import discord4j.store.jdk.JdkStoreService;

public class ExampleSingleConnect {

    public static void main(String[] args) {
        GatewayDiscordClient client = DiscordClientBuilder.create(System.getenv("BOT_TOKEN"))
                .build()
                .gateway()
                .setStore(Store.fromLayout(LegacyStoreLayout.of(new JdkStoreService())))
                .setSharding(ShardingStrategy.fixed(1))
                .login()
                .blockOptional()
                .orElseThrow(RuntimeException::new);

        BotSupport.create(client)
                .eventHandlers()
                .block();
    }
}
