package discord4j.connect;

import discord4j.core.event.domain.message.MessageCreateEvent;
import discord4j.core.object.entity.Message;
import discord4j.core.object.presence.Presence;
import reactor.core.publisher.Mono;

public abstract class EventHandler {

    public abstract Mono<Void> onMessageCreate(MessageCreateEvent event);

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

    public static class Status extends EventHandler {

        @Override
        public Mono<Void> onMessageCreate(MessageCreateEvent event) {
            Message message = event.getMessage();
            return Mono.justOrEmpty(message.getContent())
                    .filter(content -> content.startsWith("!status "))
                    .map(content -> {
                        String status = content.substring("!status ".length());
                        if (status.equalsIgnoreCase("online")) {
                            return Presence.online();
                        } else if (status.equalsIgnoreCase("dnd")) {
                            return Presence.doNotDisturb();
                        } else {
                            return Presence.idle();
                        }
                    })
                    .flatMap(presence -> event.getClient().updatePresence(presence))
                    .then();
        }
    }
}
