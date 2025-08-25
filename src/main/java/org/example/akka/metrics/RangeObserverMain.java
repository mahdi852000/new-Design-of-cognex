package org.example.akka.metrics;
import akka.actor.typed.*;
import akka.actor.typed.javadsl.*;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;

import org.example.akka.message.RangeObserverCommand;

public class RangeObserverMain {

    public static void main(String[] args) throws Exception {

        MetricsServer.start(9402);


        Counter restarts = Counter.builder("akka_range_observer_restarts")
                .tag("actor", "reliableObserver")
                .description("Number of restarts")
                .register(Metrics.globalRegistry);


        Behavior<RangeObserverCommand> faultyBehavior = Behaviors.supervise(
                Behaviors.<RangeObserverCommand>setup(ctx ->
                        new AbstractBehavior<RangeObserverCommand>(ctx) {
                            @Override
                            public Receive<RangeObserverCommand> createReceive() {
                                return newReceiveBuilder()
                                        .onMessage(RangeObserverCommand.Tick.class, t -> {
                                            throw new RuntimeException("fail");
                                        })
                                        .onSignal(PreRestart.class, sig -> {
                                            restarts.increment();
                                            System.out.println("Actor restarted, counter=" + restarts.count());
                                            return this;
                                        })
                                        .build();
                            }
                        })
        ).onFailure(RuntimeException.class, SupervisorStrategy.restart());

        ActorSystem<RangeObserverCommand> system =
                ActorSystem.create(faultyBehavior, "reliableObserver");


        for (int i = 0; i < 5000; i++) {
            system.tell(new RangeObserverCommand.Tick());
            Thread.sleep(2000);
        }

        System.out.println("Demo finished. Check http://localhost:9401/metrics");
        Thread.sleep(30000);
        system.terminate();
        MetricsServer.stop();
    }
}

