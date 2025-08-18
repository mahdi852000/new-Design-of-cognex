import akka.actor.testkit.typed.javadsl.ActorTestKit;
import akka.actor.testkit.typed.javadsl.TestProbe;
import org.example.akka.config.ScannerActorConfig;
import org.example.akka.extra.FakeDataManSystem;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import akka.actor.typed.ActorRef;
import org.example.akka.actor.dmcc.RangeObserverActor;
import org.example.akka.actor.dmcc.ScannerActor;

import org.example.akka.message.CognexCommand;
import org.example.akka.message.RangeObserverCommand;
import org.example.akka.message.ScannerCommand;


import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class ScannerConsoleFlowTest {
    private static ActorTestKit testKit;

    @BeforeAll
    static void setup() {
        testKit = ActorTestKit.create();
    }

    @AfterAll
    static void teardown() {
        testKit.shutdownTestKit();
    }
    @Test
    void console_operator_manual_then_auto_then_manual() {

        TestProbe<String> consoleOut = testKit.createTestProbe(); // only for ConsoleAdapter messages
        TestProbe<String> scanOut    = testKit.createTestProbe(); // only for ScannerOutput

        // Scanner + Observer
        var cfg = new ScannerActorConfig(
                1,
                new FakeDataManSystem(50, scanOut.getRef()),
                new DummyListener(),
                new DummyResource(),
                "localhost",
                5000,
                testKit.createTestProbe(CognexCommand.class).getRef(),
                true,
                scanOut.getRef(),
                false
        );
        ActorRef<ScannerCommand> scanner = testKit.spawn(ScannerActor.create(cfg));

        ActorRef<RangeObserverCommand> observer = testKit.spawn(
                RangeObserverActor.createWithFakeSensor(50.0, scanner, scanOut.getRef(), Duration.ofMillis(500)));
        scanner.tell(new ScannerCommand.RegisterObserver(observer));


        ActorRef<String> console = testKit.spawn(
                org.example.akka.console.ConsoleAdapterActor.create(scanner, consoleOut.getRef())
        );

        console.tell("occ?");
        assertEquals("occupation=false", consoleOut.receiveMessage(Duration.ofSeconds(2)));

        // MANUAL: trigger
        console.tell("trigger");
        consoleOut.expectMessage("trigger sent");
        assertEquals("SCAN_CODE_FROM_ACTOR", scanOut.receiveMessage(Duration.ofSeconds(2)));

        scanner.tell(new ScannerCommand.IsConnected(true));


        // AUTO
        console.tell("mode auto");
        consoleOut.expectMessage("OK: mode=AUTO");


        //observe
       /* console.tell("start");
        consoleOut.expectMessage("started");*/

        consoleOut.awaitAssert(Duration.ofSeconds(3), () -> {
            console.tell("occ?");
            assertEquals("occupation=true", consoleOut.receiveMessage());
            return null;
        });
        console.tell("occ?");
        String occAns = consoleOut.receiveMessage(Duration.ofSeconds(2));
        if (!occAns.equals("occupation=true")) {
            console.tell("occ?");
            occAns = consoleOut.receiveMessage(Duration.ofSeconds(2));
        }
        assertEquals("occupation=true", occAns);

        String autoCode = scanOut.receiveMessage(Duration.ofSeconds(3));
        assertNotNull(autoCode);


        console.tell("mode manual");
        consoleOut.expectMessage("OK: mode=MANUAL");


        scanOut.expectNoMessage(Duration.ofMillis(500));

        console.tell("trigger");
        consoleOut.expectMessage("trigger sent");
        assertEquals("SCAN_CODE_FROM_ACTOR", scanOut.receiveMessage(Duration.ofSeconds(2)));
    }


}
