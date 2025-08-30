package org.example.akka.actor.dmcc;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import static org.example.akka.message.CognexCommand.*;
import org.example.akka.message.CognexCommand;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import org.example.akka.event.SystemEvent;

import java.util.HashSet;
import java.util.Set;

import org.example.akka.metrics.ActorMetricsInterceptor;



public class CognexDataManActor extends AbstractBehavior<CognexCommand> {

    private final Set<ActorRef<SystemEvent.CognexEvent>> listeners = new HashSet<>();

    private CognexDataManActor(ActorContext<CognexCommand> context) {
        super(context);
    }

    public static Behavior<CognexCommand> create() {
        Behavior<CognexCommand> core = Behaviors.setup(CognexDataManActor::new);
        return ActorMetricsInterceptor.wrap("CognexDataManActor", CognexCommand.class, core);
    }


    @Override
    public Receive<CognexCommand> createReceive() {
        return newReceiveBuilder()
                .onMessage(NotifyScannedCode.class, this::onNotifyScannedCode)
                .onMessage(CognexCommand.RegisterListener.class, msg -> {
                    listeners.add(msg.listener());
                    getContext().watchWith(msg.listener(), new CognexCommand.ListenerDied(msg.listener()));
                    return this;
                })
                .onMessage(CognexCommand.UnregisterListener.class, msg -> {
                    listeners.remove(msg.listener());
                    return this;
                })
                .onMessage(CognexCommand.ListenerDied.class, msg -> {
                    listeners.remove(msg.listener());
                    return this;
                })
                .onMessage(CognexCommand.Connect.class, msg -> {
                    getContext().getLog().info("Cognex connected (no-op)");
                    return this;
                })
                .onMessage(CognexCommand.Disconnect.class, msg -> {
                    getContext().getLog().info("Cognex disconnected (no-op)");
                    return this;
                })


                .build();
    }
    //This is the only Method which is used
    private Behavior<CognexCommand> onNotifyScannedCode(CognexCommand.NotifyScannedCode msg) {
        if (msg.code() == null || msg.code().isBlank()) {
            getContext().getLog().warn("Empty or null code received, ignoring.");
            return this; //
        }
        for (ActorRef<SystemEvent.CognexEvent> listener : listeners) {
            listener.tell(new SystemEvent.CognexEvent.CodeScanned(msg.resource(), msg.code()));
        }
        return this;
    }
}
