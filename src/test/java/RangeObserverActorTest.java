import akka.actor.testkit.typed.javadsl.*;
import akka.actor.typed.ActorRef;
import org.example.akka.actor.dmcc.RangeObserverActor;
import org.example.akka.config.RangeObserverConfig;
import org.example.akka.extra.DataManSystem;
import org.example.akka.message.RangeObserverCommand;
import org.example.akka.message.Response;
import org.example.akka.message.ScannerCommand;
import org.junit.jupiter.api.*;
import org.mockito.Mockito;

import java.io.IOException;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class RangeObserverActorTest {

    private static ActorTestKit testKit;
    private static final int cmId = 1;
    private static final long rangeMin = 10L;
    private static final long rangeMax = 100L;
    private static final long rangeOff = 120L;
    private static final String uri = "fakeUri";
    private static final String host = "localhost";
    private static final int port = 5000;


    @BeforeAll
    static void setup() {
        testKit = ActorTestKit.create();
    }

    @AfterAll
    static void tearDown() {
        testKit.shutdownTestKit();
    }

    @Test
    void testRangeObserverActorBehavior() throws IOException {

        DataManSystem dmccMock = Mockito.mock(DataManSystem.class);

        Response responseMock = Mockito.mock(Response.class);
        when(dmccMock.sendCommand(anyString(), anyInt(), anyBoolean())).thenReturn(responseMock);
        when(responseMock.result()).thenReturn("50");
        TestProbe<ScannerCommand> scannerProbe = testKit.createTestProbe();
        TestProbe<String> scanReceiverProbe = testKit.createTestProbe();
        RangeObserverConfig config = new RangeObserverConfig(
                dmccMock, cmId, rangeMin, rangeMax, rangeOff,
                scannerProbe.getRef(), uri, host, port, scanReceiverProbe.getRef()
        );
        ActorRef<RangeObserverCommand> rangeObserverActor = testKit.spawn(
                RangeObserverActor.create(config)
        );

        rangeObserverActor.tell(new RangeObserverCommand.StartObserving());
        rangeObserverActor.tell(new RangeObserverCommand.Tick());

        ScannerCommand.SetOccupation setOcc = scannerProbe.expectMessageClass(
                ScannerCommand.SetOccupation.class);
        assertTrue(setOcc.occupied());

        scannerProbe.expectNoMessage(Duration.ofMillis(300));


        rangeObserverActor.tell(new RangeObserverCommand.StopObserving());

        scannerProbe.expectNoMessage(Duration.ofMillis(300));



        verify(dmccMock, atLeastOnce()).sendCommand(anyString(), anyInt(), anyBoolean());
    }


    @Test
    void testRangeObserverReceivesScanCode() {

        // ScannerActor mock
        TestProbe<ScannerCommand> scannerProbe = testKit.createTestProbe();

        // Probe for receiving scan results
        TestProbe<String> scanResultReceiver = testKit.createTestProbe();

        DataManSystem dmcc = mock(DataManSystem.class);

        RangeObserverConfig config = new RangeObserverConfig(
                dmcc, 1, 100L, 200L, 300L,
                scannerProbe.getRef(), "uri", "host", 1234, scanResultReceiver.getRef()

        );
        ActorRef<RangeObserverCommand> observer = testKit.spawn(
                RangeObserverActor.create(config)
        );


        String scannedCode = "abc123";
        observer.tell(new RangeObserverCommand.ScanCode(scannedCode));

        String receivedCode = scanResultReceiver.receiveMessage();
        assertEquals(scannedCode, receivedCode);
    }

}
