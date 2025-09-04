package org.example.akka.bench;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;

public class Worker extends AbstractBehavior<Ping> {

    public static Behavior<Ping> create() {
        return Behaviors.setup(Worker::new);
    }

    private Worker(ActorContext<Ping> ctx) {
        super(ctx);
    }

    @Override
    public Receive<Ping> createReceive() {
        return newReceiveBuilder()
                .onMessage(Ping.class, this::onPing)
                .build();
    }

    private Behavior<Ping> onPing(Ping msg) {
        ActorRef<Pong> replyTo = msg.replyTo();
        System.out.println("Worker reply with sentAtNanos = " + msg.id());
        replyTo.tell(new Pong(msg.id()));
        return this;
    }
}
