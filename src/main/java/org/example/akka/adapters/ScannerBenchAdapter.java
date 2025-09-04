package org.example.akka.adapters;

// ScannerBenchAdapter.java

import akka.actor.typed.*;
import akka.actor.typed.javadsl.*;
import akka.actor.typed.javadsl.AskPattern;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.example.akka.bench.BenchMetrics;
import org.example.akka.bench.Ping;
import org.example.akka.bench.Pong;
import org.example.akka.message.ScannerCommand;

public class ScannerBenchAdapter extends AbstractBehavior<Ping> {
    private final ActorRef<ScannerCommand> scanner;
    private final Scheduler scheduler;

    public static Behavior<Ping> create(ActorRef<ScannerCommand> scanner){
        return Behaviors.setup(ctx -> new ScannerBenchAdapter(ctx, scanner));
    }
    private ScannerBenchAdapter(ActorContext<Ping> ctx, ActorRef<ScannerCommand> scanner){
        super(ctx); this.scanner=scanner; this.scheduler=ctx.getSystem().scheduler();
    }

    @Override public Receive<Ping> createReceive() {
        return newReceiveBuilder().onMessage(Ping.class, this::onPing).build();
    }

    private Behavior<Ping> onPing(Ping p) {
        long t0 = System.nanoTime();

        AskPattern.<ScannerCommand, ScannerCommand.ConnectedStatus>ask(
                        scanner,
                        ScannerCommand.QueryIsConnected::new,
                        Duration.ofSeconds(2),
                        scheduler
                )
                // اگر لازم داری به Boolean نگاشتش کن:
                //.thenApply(cs -> cs.connected())      // یا cs.isConnected() بسته به تعریف کلاس/record
                .whenComplete((cs, ex) -> {
                    long ms = (System.nanoTime() - t0) / 1_000_000L;
                    BenchMetrics.RTT.record(ms, TimeUnit.MILLISECONDS);

                    if (ex != null) {
                        BenchMetrics.TIMEOUTS.increment();
                    } else {
                        // اگر خواستی نتیجه را هم لحاظ کنی:
                        // boolean ok = cs.connected();  // یا cs.isConnected()
                        BenchMetrics.COMPLETED.increment();
                    }

                    // مهم: به Flooder بگو این پینگ تمام شد
                    p.replyTo().tell(new Pong(p.id()));
                });

        return this;
    }

}
