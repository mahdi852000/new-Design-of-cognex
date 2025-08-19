import akka.actor.testkit.typed.javadsl.ActorTestKit;
import akka.actor.typed.ActorRef;

import org.example.akka.actor.dmcc.RangeObserverActor;
import org.example.akka.message.RangeObserverCommand;
import org.example.akka.message.ScannerCommand;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import akka.actor.testkit.typed.javadsl.TestProbe;
import org.slf4j.LoggerFactory;
import java.time.Duration;
import org.slf4j.Logger;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.example.akka.actor.dmcc.ScannerActor;
import org.example.akka.config.ScannerActorConfig;
import org.example.akka.extra.FakeDataManSystem;


public class RangeObserverScannerIntegrationTest {
    public static  final Logger log = LoggerFactory.getLogger(
            RangeObserverScannerIntegrationTest.class);

    static ActorTestKit testKit;

    @BeforeAll
    static void setup(){
        testKit=ActorTestKit.create();
    }


    @AfterAll
    static void cleanup(){
        testKit.shutdownTestKit();
    }

    @Test
    void shouldStopTriggeringScanWhenStopped() throws InterruptedException {
        TestProbe<ScannerCommand> scannerProbe = testKit.createTestProbe();
        TestProbe<String> scanReceiverProbe = testKit.createTestProbe();

        double simulatedDistance = 25.0;
        Duration tickInterval = Duration.ofMillis(100);

        ActorRef<RangeObserverCommand> observer = testKit.spawn(
                RangeObserverActor.createWithFakeSensor(
                        simulatedDistance,
                        scannerProbe.getRef(),
                        scanReceiverProbe.getRef(),
                        tickInterval,
                        null
                        )
        );

        observer.tell(new RangeObserverCommand.StartObserving());

        scannerProbe.awaitAssert(Duration.ofSeconds(3), () -> {
            scannerProbe.expectMessageClass(ScannerCommand.SetOccupation.class);
            return null;
        });

       /* ScannerCommand.TriggerScan trigger =
                scannerProbe.expectMessageClass(ScannerCommand.TriggerScan.class, Duration.ofSeconds(2));
        log.info("Received TriggerScan: {}", trigger);*/
        /*scannerProbe.expectMessageClass(ScannerCommand.SetOccupation.class,Duration.ofSeconds(2));
        TriggerScan msg = scannerProbe.expectMessageClass(TriggerScan.class,Duration.ofSeconds(2));
        log.info("Received TriggerScan: {}", msg);*/
        observer.tell(new RangeObserverCommand.StopObserving());
        scannerProbe.expectNoMessage(Duration.ofMillis(3000));
    }
    @Test
    void observerToScanner_autoTrigger_onceWhenOccupied() {
        TestProbe<String> sink = testKit.createTestProbe();
        ActorRef<ScannerCommand> scanner = testKit.spawn(
                ScannerActor.create(new ScannerActorConfig(
                        1,
                        new FakeDataManSystem(50, sink.getRef()),
                        new DummyListener(),
                        new DummyResource(),
                        "localhost",
                        5000,
                        testKit.createTestProbe(org.example.akka.message.CognexCommand.class).getRef(),
                        true,
                        sink.getRef(),
                        false
                )),
                "scanner-it"
        );
        scanner.tell(new ScannerCommand.IsConnected(true));
        scanner.tell(new ScannerCommand.SwitchMode(ScannerCommand.Mode.AUTO));
        ActorRef<RangeObserverCommand> obs = testKit.spawn(
                RangeObserverActor.createWithFakeSensor(
                        50.0,
                        scanner,
                        sink.getRef(),
                        Duration.ofMillis(100),
                        null),
                "observer-it"
        );
        obs.tell(new RangeObserverCommand.StartObserving());

        TestProbe<ScannerCommand.OccupationStatus> occProbe = testKit.createTestProbe();
        occProbe.awaitAssert(Duration.ofSeconds(3), () -> {
            scanner.tell(new ScannerCommand.QueryOccupation(occProbe.getRef()));
            assertTrue(occProbe.receiveMessage().occupied());
            return null;
        });

        assertEquals("SCAN_CODE_FROM_ACTOR", sink.receiveMessage(Duration.ofSeconds(3)));
    }
}

