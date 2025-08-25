package org.example.akka.bench;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.*;
import java.util.concurrent.TimeUnit;

public class Sink extends AbstractBehavior<Pong> {
    public static Behavior<Pong> create() { return Behaviors.setup(Sink::new); }
    private Sink(ActorContext<Pong> ctx) { super(ctx); }

    @Override
    public Receive<Pong> createReceive() {
        return newReceiveBuilder().onMessage(Pong.class, this::onPong).build();
    }

    private Behavior<Pong> onPong(Pong p) {

        long rttMs = (System.nanoTime() - p.sentAtNanos()) / 1_000_000;
        BenchMetrics.RTT.record(rttMs, TimeUnit.MILLISECONDS);
        BenchMetrics.COMPLETED.increment();
        return this;
    }
}

