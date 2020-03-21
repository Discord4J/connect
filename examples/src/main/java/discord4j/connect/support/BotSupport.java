package discord4j.connect.support;

import discord4j.core.GatewayDiscordClient;
import discord4j.core.event.domain.lifecycle.ReadyEvent;
import discord4j.core.event.domain.message.MessageCreateEvent;
import discord4j.core.object.entity.Message;
import discord4j.core.object.presence.Activity;
import discord4j.core.object.presence.Presence;
import discord4j.core.object.util.Snowflake;
import discord4j.discordjson.json.ApplicationInfoData;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A support class to provide bot message responses.
 */
public class BotSupport {

    private static final Logger log = Loggers.getLogger(BotSupport.class);

    private final GatewayDiscordClient client;

    public static BotSupport create(GatewayDiscordClient client) {
        return new BotSupport(client);
    }

    BotSupport(GatewayDiscordClient client) {
        this.client = client;
    }

    public Mono<Void> eventHandlers() {
        return Mono.when(readyHandler(client), commandHandler(client));
    }

    public static Mono<Void> readyHandler(GatewayDiscordClient client) {
        return client.on(ReadyEvent.class)
                .doOnNext(ready -> log.info("Logged in as {}", ready.getSelf().getUsername()))
                .then();
    }

    public static Mono<Void> commandHandler(GatewayDiscordClient client) {
        Mono<Long> ownerId = client.rest().getApplicationInfo()
                .map(ApplicationInfoData::owner)
                .map(user -> Long.parseUnsignedLong(user.id()))
                .cache();

        List<EventHandler> eventHandlers = new ArrayList<>();
        eventHandlers.add(new Echo());
        eventHandlers.add(new Status());
        eventHandlers.add(new StatusEmbed());

        return client.on(MessageCreateEvent.class, event -> ownerId
                .filter(owner -> {
                    Long author = event.getMessage().getAuthor()
                            .map(u -> u.getId().asLong())
                            .orElse(null);
                    return owner.equals(author);
                })
                .flatMap(id -> Mono.when(eventHandlers.stream()
                        .map(handler -> handler.onMessageCreate(event))
                        .collect(Collectors.toList()))
                ))
                .then();
    }

    public static class Echo extends EventHandler {

        @Override
        public Mono<Void> onMessageCreate(MessageCreateEvent event) {
            Message message = event.getMessage();
            return Mono.justOrEmpty(message.getContent())
                    .filter(content -> content.startsWith("!echo "))
                    .map(content -> content.substring("!echo ".length()))
                    .flatMap(source -> message.getChannel()
                            .flatMap(channel -> channel.createMessage(source)))
                    .then();
        }
    }

    public static class StatusEmbed extends EventHandler {

        @Override
        public Mono<Void> onMessageCreate(MessageCreateEvent event) {
            Message message = event.getMessage();
            return Mono.justOrEmpty(message.getContent())
                    .filter(content -> content.equals("!status"))
                    .flatMap(source -> message.getChannel()
                            .publishOn(Schedulers.boundedElastic())
                            .flatMap(channel -> channel.createEmbed(spec -> {
                                spec.setThumbnail(event.getClient().getSelf()
                                        .blockOptional()
                                        .orElseThrow(RuntimeException::new)
                                        .getAvatarUrl());
                                spec.addField("Self ID", event.getClient().getSelfId().blockOptional()
                                        .orElse(Snowflake.of(0L)).asString(), false);
                                spec.addField("Servers", event.getClient().getGuilds().count()
                                        .blockOptional()
                                        .orElse(-1L)
                                        .toString(), false);
                            })))
                    .then();
        }
    }

    public static class Status extends EventHandler {

        @Override
        public Mono<Void> onMessageCreate(MessageCreateEvent event) {
            Message message = event.getMessage();
            return Mono.justOrEmpty(message.getContent())
                    .filter(content -> content.startsWith("!status "))
                    .map(content -> {
                        String[] tokens = content.split(" ");
                        String status = tokens.length > 1 ? tokens[1] : "";
                        String activity = tokens.length > 2 ? tokens[2] : null;
                        if (status.equalsIgnoreCase("online")) {
                            return activity != null ? Presence.online(Activity.playing(activity)) : Presence.online();
                        } else if (status.equalsIgnoreCase("dnd")) {
                            return activity != null ? Presence.doNotDisturb(
                                    Activity.playing(activity)) : Presence.doNotDisturb();
                        } else if (status.equalsIgnoreCase("idle")) {
                            return activity != null ? Presence.idle(Activity.playing(activity)) : Presence.idle();
                        } else {
                            return Presence.online();
                        }
                    })
                    .flatMap(presence -> event.getClient().updatePresence(presence))
                    .then();
        }
    }

    public abstract static class EventHandler {

        public abstract Mono<Void> onMessageCreate(MessageCreateEvent event);
    }
}
