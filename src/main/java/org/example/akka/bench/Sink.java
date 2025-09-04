package org.example.akka.bench;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.*;
import java.util.concurrent.TimeUnit;

public class Sink extends AbstractBehavior<Pong> {

    private final ActorRef<Flooder.Command> flooder;
    public static Behavior<Pong> create(ActorRef<Flooder.Command> flooder) { return Behaviors.setup(ctx->
            new Sink(ctx,flooder)); }
    private Sink(ActorContext<Pong> ctx, ActorRef<Flooder.Command> flooder) { super(ctx); this.flooder=flooder; }

    @Override
    public Receive<Pong> createReceive() {
        return newReceiveBuilder().onMessage(Pong.class, this::onPong).build();
    }

    private Behavior<Pong> onPong(Pong p) {

        long rttMs = (System.nanoTime() - p.id()) / 1_000_000;
        BenchMetrics.RTT.record(rttMs, TimeUnit.MILLISECONDS);
        BenchMetrics.COMPLETED.increment();
        System.out.println("Sink Ack sentAtNanos = " + p.id());
        flooder.tell(new Flooder.Ack(p.id()));
        return this;
    }
}

