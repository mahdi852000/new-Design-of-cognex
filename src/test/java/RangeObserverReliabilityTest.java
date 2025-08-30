import akka.actor.testkit.typed.javadsl.ActorTestKit;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.SupervisorStrategy;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Counter;
import org.example.akka.metrics.MetricsServer;


import org.example.akka.config.RangeObserverConfig;
import org.example.akka.extra.FakeDataManSystem;
import org.example.akka.message.RangeObserverCommand;
import org.example.akka.message.ScannerCommand;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class RangeObserverReliabilityTest {

    private static final ActorTestKit testKit = ActorTestKit.create();

    //For Graph
    private static final Counter RESTARTS =
            Counter.builder("akka_range_observer_restarts")
                    .tag("actor","reliableObserver")
                    .description("Number of restarts due to failures")
                    .register(Metrics.globalRegistry);


    @BeforeAll
    static void up() {

        MetricsServer.start(9402);
    }

    @AfterAll
    static void tearDown(){
        testKit.shutdownTestKit();
        MetricsServer.stop();
    }


    @Test
    public void dummyTestToCheckSetup(){
        System.out.println("TestKit is working fine.");
    }

    @Test
    public void testObserverRestartsAfterFailure() throws InterruptedException {

        TestProbe<ScannerCommand> scannerProbe = testKit.createTestProbe();
        TestProbe<String> scanReceiverProbe = testKit.createTestProbe();


        RangeObserverConfig config = new RangeObserverConfig(
                new FakeDataManSystem(50,testKit.createTestProbe(String.class).getRef()),
                123,
                10L,
                100L,
                5L,
                scannerProbe.getRef(),
                "rangeObserver1",
                "localhost",
                1234,
                scanReceiverProbe.getRef()
        );
        Behavior<RangeObserverCommand> faultyBehavior = Behaviors.supervise(
                Behaviors.<RangeObserverCommand>setup(ctx ->
                        new AbstractBehavior<RangeObserverCommand>(ctx) {
                            @Override
                            public Receive<RangeObserverCommand> createReceive() {
                                return newReceiveBuilder()
                                        .onMessage(RangeObserverCommand.Tick.class, tick -> {
                                            throw new RuntimeException("Simulated failure");
                                        })
                                        .onMessage(RangeObserverCommand.StartObserving.class, msg -> {
                                            System.out.println("Restarted after failure. Start received.");
                                            scanReceiverProbe.ref().tell("started");
                                            return this;
                                        })
                                        .onSignal(akka.actor.typed.PreRestart.class, sig ->
                                        { RESTARTS.increment(); return this; })
                                       /* .onSignal(akka.actor.typed.PreRestart.class, sig -> {
                                            Counter.builder("akka_range_observer_restarts")
                                                    .tag("actor", "reliableObserver")
                                                    .register(Metrics.globalRegistry)
                                                    .increment();
                                            return this;
                                        })*/
                                        .build();

                            }
                        }
                )
        ).onFailure(RuntimeException.class, SupervisorStrategy.restart());

        ActorRef<RangeObserverCommand> observer =
                testKit.spawn(faultyBehavior, "reliableObserver");

        for (int i = 0; i < 6000; i++) {
            observer.tell(new RangeObserverCommand.Tick());
            Thread.sleep(1000);
        }

        observer.tell(new RangeObserverCommand.Tick());

        Thread.sleep(2000);
        observer.tell(new RangeObserverCommand.StartObserving());
        scanReceiverProbe.expectMessage("started");

        Thread.sleep(12_000);

    }
}

