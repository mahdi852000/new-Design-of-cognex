package org.example.akka.bench;

import akka.actor.Cancellable;
import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.Scheduler;
import akka.actor.typed.javadsl.*;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import akka.actor.typed.MailboxSelector;

public class Flooder extends AbstractBehavior<Flooder.Command> {

    // ===== Messages =====
    public interface Command {}
    public record Start(int qps, int durationSec, ActorRef<Ping> target) implements Command {}
    public record Ramp(int startQps, int stepQps, int maxQps, int stepDurationSec, ActorRef<Ping> target) implements Command {}
    private enum NextStep implements Command { INSTANCE; }
    public record Timeout(long id) implements Command {}
    public record Ack(long id) implements Command {}

    private static final Logger LOG = LoggerFactory.getLogger(Flooder.class);

    // ===== State =====
    private final Scheduler scheduler;
    private Cancellable ticker;
    private final ActorRef<Pong> sink;

    private final Map<Long, Cancellable> timeouts = new ConcurrentHashMap<>();
    private static final AtomicLong counter = new AtomicLong();

    // ramp state
    private int currentQps, stepQps, maxQps, stepDurationSec;
    private ActorRef<Ping> currentTarget;

    // ===== Factory =====
    public static Behavior<Command> create() {
        return Behaviors.setup(Flooder::new);
    }

    private Flooder(ActorContext<Command> ctx) {
        super(ctx);
        this.scheduler = ctx.getSystem().scheduler();
        this.sink = ctx.spawn(Sink.create(ctx.getSelf()), "sink",
                MailboxSelector.bounded(1000));
    }

    // ===== Behavior =====
    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(Start.class, this::onStart)
                .onMessage(Ramp.class, this::onRamp)
                .onMessage(NextStep.class, m -> { onNextStep(); return this; })
                .onMessage(Timeout.class, this::onTimeout)
                .onMessage(Ack.class, this::onAck)
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
        LOG.info("Ramp start: {} -> {} (step {}), {}s/step", currentQps, maxQps, stepQps, stepDurationSec);
        startRound(currentQps, stepDurationSec, currentTarget);
        return this;
    }

    private void onNextStep() {
        int next = currentQps + stepQps;
        if (currentTarget != null && next <= maxQps) {
            currentQps = next;
            LOG.info("Ramp next step: {} qps", currentQps);
            startRound(currentQps, stepDurationSec, currentTarget);
        } else {
            LOG.info("Ramp finished.");
        }
    }

    private Behavior<Command> onAck(Ack ack) {
        Cancellable timeout = timeouts.remove(ack.id());
        if (timeout != null) timeout.cancel();
        return this;
    }

    private Behavior<Command> onTimeout(Timeout t) {
        BenchMetrics.TIMEOUTS.increment();
        getContext().getLog().warn("Timeout detected for ping id={}", t.id());
        return this;
    }

    // ===== Round logic =====
    private void startRound(int qps, int durationSec, ActorRef<Ping> target) {
        if (ticker != null && !ticker.isCancelled()) ticker.cancel();

        final ActorRef<Command> self = getContext().getSelf();
        final int tickMillis = 10;
        final long tickNanos = tickMillis * 1_000_000L;
        final int burst = Math.max(1, (int)(qps * tickMillis / 1000L));

        ticker = scheduler.scheduleAtFixedRate(
                Duration.ZERO, Duration.ofNanos(tickNanos),
                () -> {
                    for (int i = 0; i < burst; i++) {
                        long sentAt = System.nanoTime();
                        target.tell(new Ping(sentAt, sink));
                        

                        Cancellable timeout = scheduler.scheduleOnce(
                                Duration.ofSeconds(5),
                                () -> self.tell(new Timeout(sentAt)),
                                getContext().getSystem().executionContext()
                        );
                        timeouts.put(sentAt, timeout);
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
