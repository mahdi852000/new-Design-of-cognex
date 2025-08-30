package org.example.akka.actor.dmcc;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.PostStop;
import akka.actor.typed.javadsl.*;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import org.example.akka.config.RangeObserverConfig;

import org.example.akka.extra.FakeDataManSystem;
import org.example.akka.message.RangeObserverCommand;
import org.example.akka.message.Response;
import org.example.akka.message.ScannerCommand;
import org.example.akka.metrics.Metrics;

import org.example.akka.metrics.ActorMetricsInterceptor;
import org.example.akka.metrics.MetricsServer;

import io.micrometer.core.instrument.*;


import java.time.Duration;

public class RangeObserverActor extends AbstractBehavior<RangeObserverCommand> {

    public final ActorRef<Metrics.Event> metricsRef;
    private final RangeObserverConfig config;
    private final TimerScheduler<RangeObserverCommand> timers;
    private int cmId;
    private final long[] measurements = new long[5];
    private int pos = 0;
    private Boolean occupation = Boolean.FALSE;
    private static final Object TICK_KEY = new Object();
    private final Duration tickInterval;

    // ===== Micrometer fields =====
    private final MeterRegistry reg = MetricsServer.registry();
    private final DistributionSummary dmccLatency;
    private final Counter occupationTrue;
    private final Counter occupationFalse;
    private volatile boolean occupied;            //For Gauge


    private final boolean simulateNoise = true;
    private final MeanRevertingInt synth = new MeanRevertingInt(90, 90.0, 0.08, 5.0);

    private RangeObserverActor (
            ActorContext<RangeObserverCommand> context,
            TimerScheduler<RangeObserverCommand> timers,
            RangeObserverConfig config,
            Duration tickInterval,
            ActorRef<Metrics.Event> metrics,
            ActorRef<Metrics.Event> metricsRef) {
        super(context);
        this.timers = timers;
        this.config = config;
        this.cmId = config.cmId;
        this.tickInterval=tickInterval !=null ? tickInterval : Duration.ofSeconds(5);
        this.metricsRef = metricsRef;

        getContext().getLog().info("metricsRef = {}", config.metricsRef);
        this.occupied = Boolean.TRUE.equals(this.occupation);

        final String actorLabel = "RangeObserverActor";

        this.dmccLatency = DistributionSummary.builder("dmcc_latency_ms")
                .baseUnit("ms")
                .tag("actor", actorLabel)
                .publishPercentileHistogram()
                .register(reg);

        this.occupationTrue = Counter.builder("occupation_change_total")
                .tag("actor", actorLabel)
                .tag("new_state", "true")
                .register(reg);

        this.occupationFalse = Counter.builder("occupation_change_total")
                .tag("actor", actorLabel)
                .tag("new_state", "false")
                .register(reg);

        Gauge.builder("occupation_state", () -> this.occupied ? 1 : 0)
                .tag("actor", actorLabel)
                .register(reg);
    }

    public static Behavior<RangeObserverCommand> create(RangeObserverConfig config) {
        Behavior<RangeObserverCommand> core =
                Behaviors.withTimers(timers ->
                        Behaviors.setup(ctx ->
                                new RangeObserverActor(
                                        ctx, timers, config, java.time.Duration.ofSeconds(5),
                                        config.metricsRef, config.metricsRef
                                )
                        )
                );

        return ActorMetricsInterceptor.wrap("RangeObserverActor", RangeObserverCommand.class, core);
    }



    @Override
    public  Receive<RangeObserverCommand> createReceive() {
        return newReceiveBuilder()
                .onMessage(RangeObserverCommand.StartObserving.class, this::onStartObservingRange)
                .onMessage(RangeObserverCommand.StopObserving.class, this::onStopObservingRange)
                .onMessage(RangeObserverCommand.Tick.class, this::onTick)
                .onMessage(RangeObserverCommand.ScanCode.class, this::onScanCode)
                .onSignal(PostStop.class, sig -> {
                            timers.cancelAll();
                            getContext().getLog().info("Observer PostStop: all timers cancelled");
                            return Behaviors.same();})
                .build();
    }
    // For Testing Purpose
    public static Behavior<RangeObserverCommand> createWithFakeSensor(
            double simulatedDistance,
            ActorRef<ScannerCommand> scannerActor,
            ActorRef<String> scanReceiver,
            Duration tickInterval, ActorRef<Metrics.Event> metrics) {

        FakeDataManSystem fakeDmcc = new FakeDataManSystem(simulatedDistance, scanReceiver);
        RangeObserverConfig config = new RangeObserverConfig(
                                fakeDmcc,
                                0,
                                80L,
                                100L,
                                120L,
                                scannerActor,
                                "fakeUri",
                                "fakeHost",
                                0,
                                scanReceiver,
                                metrics
                        );
                return Behaviors.withTimers(timers->
                        Behaviors.setup(ctx->
                                new RangeObserverActor(ctx,timers,config,tickInterval,metrics,metrics )));

    }

    private Behavior<RangeObserverCommand> onScanCode(RangeObserverCommand.ScanCode msg) {
        config.scanReceiver.tell(msg.code());
        getContext().getLog().info("Received scan code: {}",msg.code() );
        return this;
    }

    private Behavior<RangeObserverCommand> onStartObservingRange(RangeObserverCommand.StartObserving startObserving) {
        timers.startTimerAtFixedRate(TICK_KEY, new RangeObserverCommand.Tick(), this.tickInterval);
        //For test
        getContext().getSelf().tell(new RangeObserverCommand.Tick());
        getContext().getLog().info("Range Observing Started");
        return this;
    }

    private Behavior<RangeObserverCommand> onStopObservingRange(RangeObserverCommand.StopObserving stopObserving) {
        timers.cancel(TICK_KEY);
        timers.cancelAll();
        getContext().getLog().info("Range Observing Stopped");
        return this;
    }

    private Behavior<RangeObserverCommand> onTick(RangeObserverCommand.Tick tick) {
        try {
            long startNs = System.nanoTime();
            Response r = config.dmcc.sendCommand("GET HEIGHT-SENSOR.CURRENT-MEASUREMENT", cmId++,
                    true);

            long elapsedNs = System.nanoTime() - startNs;
            long latencyMs = Math.max(1, elapsedNs / 1_000_000L);
            if (r == null) {
                getContext().getLog().warn("Received null Response from DMCC");
                return this;
            }
           if (r.result() == null) {
                getContext().getLog().warn("Null result from DMCC");
                return this;
            }

            String s = r.result().trim();
            long measurement;
            try {
                measurement = Long.parseLong(s);
            } catch (NumberFormatException e) {
                measurement = Math.round(Double.parseDouble(s));
            }


            if (simulateNoise) {
                measurement = synth.next();
            }
            measurements[pos] = measurement;
            pos = (pos + 1) % measurements.length;
            double avg = java.util.Arrays.stream(measurements)
                    .filter(m -> m > 0)
                    .average()
                    .orElse(0.0);

            boolean occ = Boolean.TRUE.equals(occupation);
            long ts = System.currentTimeMillis();

            //For debug
            getContext().getLog().debug("avg={}, rangeMin={}, rangeMax={}", avg, config.rangeMin, config.rangeMax);
            getContext().getLog().debug("Avg(5)={}, measurement={}", avg, measurement);

            getContext().getLog().info("METRICS record avg={} meas={} occ={}", avg, measurement, occ);

            if (metricsRef != null) {
                metricsRef.tell(new Metrics.DmccLatency(latencyMs, ts));
                metricsRef.tell(new Metrics.Trigger(true, ts));
                metricsRef.tell(new Metrics.Record(ts, avg, measurement, occ));
            }
            if (config.rangeMin < avg && avg < config.rangeMax) {
                if (occupation == null || !occupation) {
                    occupation = true;
                    config.scannerActor.tell(new ScannerCommand.SetOccupation(true));

                    //For Debugging
                    getContext().getLog().info("Trigger condition met. Occupation ON (within range).");

                    getContext().getLog().info("Occupation changed to ON");
                }
            } else if (avg > config.rangeOff) {
                if (occupation == null || Boolean.TRUE.equals(occupation)) {
                    occupation = false;
                    config.scannerActor.tell(new ScannerCommand.SetOccupation(false));
                    getContext().getLog().info("Occupation changed to OFF");
                }
            }
            else {
                //For debugging purpose
                getContext().getLog().info("Trigger condition NOT met. No scan triggered.");
            }
                //For debugging purpose
            getContext().getLog().debug("Tick received");

        } catch (Throwable t) {
            getContext().getLog().error("Error during range observation: {}", t.getMessage(), t);
            }
        getContext().getLog().debug("Starting DMCC scanner at URI={} host={} port={}", config.uri, config.host,config.port);
        return this;
    }

    private static final class MeanRevertingInt {
        private double v; //Final Rounded Result
        private final double mu;      // Mean reversion center (e.g., 90)
        private final double kappa;   // Strength of pull toward the center (0..1)
        private final double sigma;   // Strength of random noise
        private final java.util.concurrent.ThreadLocalRandom rnd =
                java.util.concurrent.ThreadLocalRandom.current();

        MeanRevertingInt(int start, double mu, double kappa, double sigma) {
            this.v = start; this.mu = mu; this.kappa = kappa; this.sigma = sigma;
        }
        int next() {
            // Discrete Ornstein–Uhlenbeck: v = v + k*(mu - v) + noise
            v = v + kappa * (mu - v) + rnd.nextGaussian() * sigma;
            // Randomized quantization to produce integer output while preserving drift
            long lo = (long) Math.floor(v);
            double frac = v - lo;
            return (rnd.nextDouble() < frac) ? (int) (lo + 1) : (int) lo;
        }
    }

}


