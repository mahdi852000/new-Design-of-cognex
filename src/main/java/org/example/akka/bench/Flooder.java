package org.example.akka.bench;

import akka.actor.Cancellable;
import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.Scheduler;
import akka.actor.typed.javadsl.*;
import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Flooder extends AbstractBehavior<Flooder.Command> {
    public interface Command {}
    public record Start(int qps, int durationSec, ActorRef<Ping> target) implements Command {}
    public record Ramp(int startQps, int stepQps, int maxQps, int stepDurationSec, ActorRef<Ping> target) implements Command {}
    private enum NextStep implements Command { INSTANCE; }
    private static final Logger LOG = LoggerFactory.getLogger(Flooder.class);

    private final Scheduler scheduler;
    private Cancellable ticker;

    private final akka.actor.typed.ActorRef<Pong> sink;


    // state for ramp
    private int currentQps, stepQps, maxQps, stepDurationSec;
    private ActorRef<Ping> currentTarget;

    public static Behavior<Command> create() { return Behaviors.setup(Flooder::new); }
    private Flooder(ActorContext<Command> ctx) { super(ctx); this.scheduler = ctx.getSystem().scheduler();
        this.sink = ctx.spawn(Sink.create(), "sink");}

    @Override public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(Start.class, this::onStart)
                .onMessage(Ramp.class, this::onRamp)
                .onMessage(NextStep.class, m -> { onNextStep(); return this; })
                .build();
    }

    private Behavior<Command> onStart(Start m) {
        startRound(m.qps(), m.durationSec(), m.target());
        return this;
    }

    private Behavior<Command> onRamp(Ramp r) {
        this.currentQps = r.startQps();
        this.stepQps = r.stepQps();
        this.maxQps = r.maxQps();
        this.stepDurationSec = r.stepDurationSec();
        this.currentTarget = r.target();
        getContext().getLog().info("Ramp start: {} -> {} (step {}), {}s/step", currentQps, maxQps, stepQps, stepDurationSec);
        startRound(currentQps, stepDurationSec, currentTarget);
        return this;
    }

    private void onNextStep() {
        int next = currentQps + stepQps;
        if (currentTarget != null && next <= maxQps) {
            currentQps = next;
            getContext().getLog().info("Ramp next step: {} qps", currentQps);
            startRound(currentQps, stepDurationSec, currentTarget);
        } else {
            getContext().getLog().info("Ramp finished.");
        }
    }

        private void startRound(int qps, int durationSec, ActorRef<Ping> target) {
            if (ticker != null && !ticker.isCancelled()) ticker.cancel();

            final ActorRef<Command> self = getContext().getSelf();

            final int tickMillis = 10;
            final long tickNanos = tickMillis * 1_000_000L;
            final int burst = Math.max(1, (int)(qps * tickMillis / 1000L));

            ticker = scheduler.scheduleAtFixedRate(
                    Duration.ZERO, Duration.ofNanos(tickNanos),
                    () -> {
                        long now = System.nanoTime();

                        for (int i = 0; i < burst; i++) {
                            target.tell(new Ping(now, sink));
                        }
                    },
                    getContext().getSystem().executionContext()
            );

            scheduler.scheduleOnce(
                    Duration.ofSeconds(durationSec),
                    () -> {
                        if (ticker != null) ticker.cancel();
                        LOG.info("Round done at {} qps (burst per {}ms = {})", qps, tickMillis, burst);
                        self.tell(NextStep.INSTANCE);
                    },
                    getContext().getSystem().executionContext()
            );
        }

    }

