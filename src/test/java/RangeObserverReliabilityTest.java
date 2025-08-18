import akka.actor.testkit.typed.javadsl.ActorTestKit;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.SupervisorStrategy;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import org.example.akka.config.RangeObserverConfig;
import org.example.akka.extra.FakeDataManSystem;
import org.example.akka.message.RangeObserverCommand;
import org.example.akka.message.ScannerCommand;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

public class RangeObserverReliabilityTest {

    private static final ActorTestKit testKit = ActorTestKit.create();

    @AfterAll
    static void tearDown(){
        testKit.shutdownTestKit();
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
                                        .build();
                            }
                        }
                )
        ).onFailure(RuntimeException.class, SupervisorStrategy.restart());

        ActorRef<RangeObserverCommand> observer =
                testKit.spawn(faultyBehavior, "reliableObserver");
        observer.tell(new RangeObserverCommand.Tick());

        Thread.sleep(2000);
        observer.tell(new RangeObserverCommand.StartObserving());
        scanReceiverProbe.expectMessage("started");

    }
}

