package org.example.akka.adapters;

import akka.actor.typed.*;
import akka.actor.typed.javadsl.*;
import akka.actor.typed.javadsl.AskPattern;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.example.akka.bench.BenchMetrics;
import org.example.akka.bench.Ping;
import org.example.akka.bench.Pong;
import org.example.akka.message.RangeObserverCommand;

public class RangeObserverBenchAdapter extends AbstractBehavior<Ping> {
    private final ActorRef<RangeObserverCommand> observer;
    private final Scheduler scheduler;

    public static Behavior<Ping> create(ActorRef<RangeObserverCommand> observer){
        return Behaviors.setup(ctx -> new RangeObserverBenchAdapter(ctx, observer));
    }
    private RangeObserverBenchAdapter(ActorContext<Ping> ctx, ActorRef<RangeObserverCommand> observer){
        super(ctx);
        this.observer = observer;
        this.scheduler = ctx.getSystem().scheduler();
    }

    @Override public Receive<Ping> createReceive() {
        return newReceiveBuilder().onMessage(Ping.class, this::onPing).build();
    }

    private Behavior<Ping> onPing(Ping p){
        long t0 = System.nanoTime();
        AskPattern.<RangeObserverCommand,Boolean>ask(
                observer,
                replyTo -> new RangeObserverCommand.StartObservingBench(replyTo),
                Duration.ofSeconds(5),
                scheduler
        ).whenComplete((ok, ex) -> {
            long ms = (System.nanoTime() - t0) / 1_000_000L;
            BenchMetrics.RTT.record(ms, TimeUnit.MILLISECONDS);
            if (ex != null) BenchMetrics.TIMEOUTS.increment(); else BenchMetrics.COMPLETED.increment();
            // به Flooder علامت بده که این پینگ تمام شد
            p.replyTo().tell(new Pong(p.id()));
        });
        return this;
    }
}
