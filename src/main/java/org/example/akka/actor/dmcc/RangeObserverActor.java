package org.example.akka.actor.dmcc;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.*;
import org.example.akka.config.RangeObserverConfig;

import org.example.akka.extra.FakeDataManSystem;
import org.example.akka.message.RangeObserverCommand;
import org.example.akka.message.Response;
import org.example.akka.message.ScannerCommand;

import java.time.Duration;

public class RangeObserverActor extends AbstractBehavior<RangeObserverCommand> {


    private final RangeObserverConfig config;
    private final TimerScheduler<RangeObserverCommand> timers;
    private int cmId;
    private final long[] measurements = new long[5];
    private int pos = 0;
    private Boolean occupation = Boolean.FALSE;
    private static final Object TICK_KEY = new Object();

    private final Duration tickInterval;


    private RangeObserverActor (
            ActorContext<RangeObserverCommand> context,
            TimerScheduler<RangeObserverCommand> timers,
            RangeObserverConfig config,
            Duration tickInterval) {
        super(context);
        this.timers = timers;
        this.config = config;
        this.cmId = config.cmId;
        this.tickInterval=tickInterval !=null ? tickInterval : Duration.ofSeconds(5);
    }

    public static Behavior<RangeObserverCommand> create(RangeObserverConfig config) {
        return Behaviors.withTimers(timers->
                Behaviors.setup(
                        ctx-> new RangeObserverActor(ctx, timers,config,Duration.ofSeconds(5))));
    }

    @Override
    public  Receive<RangeObserverCommand> createReceive() {
        return newReceiveBuilder()
                .onMessage(RangeObserverCommand.StartObserving.class, this::onStartObservingRange)
                .onMessage(RangeObserverCommand.StopObserving.class, this::onStopObservingRange)
                .onMessage(RangeObserverCommand.Tick.class, this::onTick)
                .onMessage(RangeObserverCommand.ScanCode.class, this::onScanCode)
                .build();
    }
    // For Testing Purpose
    public static Behavior<RangeObserverCommand> createWithFakeSensor(
            double simulatedDistance,
            ActorRef<ScannerCommand> scannerActor,
            ActorRef<String> scanReceiver,
            Duration tickInterval) {

        FakeDataManSystem fakeDmcc = new FakeDataManSystem(simulatedDistance, scanReceiver);
        RangeObserverConfig config = new RangeObserverConfig(
                                fakeDmcc,
                                0,
                                10L,
                                100L,
                                120L,
                                scannerActor,
                                "fakeUri",
                                "fakeHost",
                                0,
                                scanReceiver
                        );
                return Behaviors.withTimers(timers->
                        Behaviors.setup(ctx->
                                new RangeObserverActor(ctx,timers,config,tickInterval)));

    }

    private Behavior<RangeObserverCommand> onScanCode(RangeObserverCommand.ScanCode msg) {
        config.scanReceiver.tell(msg.code());
        getContext().getLog().info("Received scan code: {}",msg.code() );
        return this;
    }

    private Behavior<RangeObserverCommand> onStartObservingRange(RangeObserverCommand.StartObserving startObserving) {
        timers.startTimerAtFixedRate(TICK_KEY, new RangeObserverCommand.Tick(), this.tickInterval);
        getContext().getLog().info("Range Observing Started");
        return this;
    }

    private Behavior<RangeObserverCommand> onStopObservingRange(RangeObserverCommand.StopObserving stopObserving) {
        timers.cancel(TICK_KEY);
        getContext().getLog().info("Range Observing Stopped");
        return this;
    }

    private Behavior<RangeObserverCommand> onTick(RangeObserverCommand.Tick tick) {
        try {
            Response r = config.dmcc.sendCommand("GET HEIGHT-SENSOR.CURRENT-MEASUREMENT", cmId++, true);
            if (r == null) {
                getContext().getLog().warn("Received null Response from DMCC");
                return this;
            }

           if (r.result() == null) {
                getContext().getLog().warn("Null result from DMCC");
                return this;
            }

            long measurement = Long.parseLong(r.result());
            measurements[pos] = measurement;
            pos = (pos + 1) % measurements.length;

            double avg = java.util.Arrays.stream(measurements)
                    .filter(m -> m > 0)
                    .average()
                    .orElse(0.0);
            //For debug
            getContext().getLog().info("avg={}, rangeMin={}, rangeMax={}", avg, config.rangeMin, config.rangeMax);
            getContext().getLog().info("Avg(5)={}, measurement={}", avg, measurement);

            if (config.rangeMin < avg && avg < config.rangeMax) {
                if (occupation == null || !occupation) {
                    occupation = true;
                    config.scannerActor.tell(new ScannerCommand.SetOccupation(true));

                   // config.scanReceiver.tell(String.valueOf(measurement));

                    //For Debugging
                    getContext().getLog().info("Trigger condition met. Occupation ON (within range).");
                   /* config.scannerActor.tell(new ScannerCommand.TriggerScan()); //This is my Question! is this what we want?
                    getContext().getLog().info("Trigger condition met. Occupation ON (within range). TriggerScan sent.");*/
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
            getContext().getLog().info("Tick received");

        } catch (Throwable t) {
            getContext().getLog().error("Error during range observation: {}", t.getMessage(), t);
            }

        getContext().getLog().debug("Starting DMCC scanner at URI={} host={} port={}", config.uri, config.host,config.port);
        return this;
    }
}
