package org.example.akka.bench;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import org.example.akka.message.ScannerCommand;

import java.time.Duration;

// TriggerScanLoadGen.java
public class TriggerScanLoadGen extends AbstractBehavior<TriggerScanLoadGen.Cmd> {
    public interface Cmd {}
    private enum Tick implements Cmd { INSTANCE; }

    private final ActorRef<ScannerCommand> scanner;
    private final akka.actor.typed.Scheduler scheduler;
    private akka.actor.Cancellable ticker;

    public static Behavior<Cmd> create(ActorRef<ScannerCommand> scanner, int qps){
        return Behaviors.setup(ctx -> new TriggerScanLoadGen(ctx, scanner, qps));
    }

    private TriggerScanLoadGen(ActorContext<Cmd> ctx, ActorRef<ScannerCommand> scanner, int qps){
        super(ctx);
        this.scanner = scanner;
        this.scheduler = ctx.getSystem().scheduler();

        int tickMs = 10;
        int burst = Math.max(1, (int)(qps * tickMs / 1000.0));
        this.ticker = scheduler.scheduleAtFixedRate(
                Duration.ZERO, Duration.ofMillis(tickMs),
                () -> {
                    for (int i=0; i<burst; i++) scanner.tell(new ScannerCommand.TriggerScan());
                },
                ctx.getSystem().executionContext()
        );
    }

    @Override public Receive<Cmd> createReceive() {
        return newReceiveBuilder().onMessage(Tick.class, m -> this).build();
    }
}
