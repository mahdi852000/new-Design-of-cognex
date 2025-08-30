import akka.actor.testkit.typed.javadsl.ActorTestKit;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import lombok.extern.slf4j.Slf4j;
import net.enilink.komma.core.*;
import org.example.akka.actor.dmcc.RangeObserverActor;
import org.example.akka.actor.dmcc.ScannerActor;
import org.example.akka.config.ScannerActorConfig;
import org.example.akka.extra.*;
import org.example.akka.message.*;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;


import java.io.IOException;
import java.time.Duration;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * This test class validates the behavior of ScannerActor and its integration with RangeObserverActor
 * using Akka Typed's ActorTestKit. It includes:
 * - Unit tests for verifying state transitions (connection, occupation).
 * - Integration test with RangeObserverActor to simulate real-time height updates.
 * - Use of dummy/mock implementations for external dependencies (DMCC, resource, listener).
 * - Ensures scanner actor's proper interaction with CognexCommand and scan receivers.
 */
@Slf4j
public class ScannerActorTest {

    static final ActorTestKit testKit = ActorTestKit.create();
    private ActorRef<ScannerCommand> scannerActor;
    private final TestProbe<String> scanReceiverProbe = testKit.createTestProbe();

    private ActorRef<ScannerCommand> spawnScannerActor(SystemConnector.Listener listener) {
        TestProbe<CognexCommand> fakeCognexActor = testKit.createTestProbe(CognexCommand.class);
        ScannerActorConfig config = new ScannerActorConfig(
                1,
                new DummyDMCC(),
                listener,
                new DummyResource(),
                "localhost",
                5000,
                fakeCognexActor.getRef(),
                true,
                scanReceiverProbe.getRef(),
                false
        );
        return testKit.spawn(ScannerActor.create(config), "Scanner-" + UUID.randomUUID());
    }

    @AfterAll
    static void tearDown() {
        testKit.shutdownTestKit();
    }

    /**
     * - Verifies that ScannerActor reports "not connected" immediately after being spawned.
     * - Spawns the actor with a DummyListener, sends QueryIsConnected via a TestProbe,
     * - receives ConnectedStatus, and asserts status() is false.
     * - Console/log output confirms the returned status is false and the test passes.
     * - The CoordinatedShutdown info log after the assertion is expected during ActorSystem teardown.
     */

    @Test
    public void testQueryIsConnectedShouldReturnFalseInitially() {
        scannerActor = spawnScannerActor(new DummyListener());
        TestProbe<ScannerCommand.ConnectedStatus> probe = testKit.createTestProbe();
        scannerActor.tell(new ScannerCommand.QueryIsConnected(probe.getRef()));
        ScannerCommand.ConnectedStatus result = probe.receiveMessage();
        assertFalse(result.status());
        log.info("Actor Status is : {}", result.status());//However this is always false
    }

    /**
     * - Verifies that ScannerActor correctly updates and reports its occupation state.
     * - Sends SetOccupation(true) and, using a TestProbe with awaitAssert, queries state
     *   until OccupationStatus.occupied() is true.
     * - Then sends SetOccupation(false) and queries again until occupied() is false.
     * - Uses awaitAssert to handle asynchronous state propagation in the actor system.
     * - Test logs show QueryOccupation responses toggling from true to false; CoordinatedShutdown
     *   info after completion is expected during ActorSystem teardown.
     */
    @Test
    public void testSetAndQueryOccupation() {
        scannerActor = spawnScannerActor(new DummyListener());
        TestProbe<ScannerCommand.OccupationStatus> probe = testKit.createTestProbe();

        scannerActor.tell(new ScannerCommand.SetOccupation(true));
        probe.awaitAssert(Duration.ofSeconds(3), () -> {
            scannerActor.tell(new ScannerCommand.QueryOccupation(probe.getRef()));
            assertTrue(probe.receiveMessage().occupied());
            return null;
        });

        scannerActor.tell(new ScannerCommand.SetOccupation(false));
        probe.awaitAssert(Duration.ofSeconds(3), () -> {
            scannerActor.tell(new ScannerCommand.QueryOccupation(probe.getRef()));
            assertFalse(probe.receiveMessage().occupied());
            return null;
        });
    }
    /**
     * - Verifies that ScannerActor transitions to "connected" after receiving Connect.
     * - Mocks URI/IReference/IResource so the actor can resolve its endpoint details.
     * - Spawns a fake Cognex actor and asserts a CognexCommand.Connect is emitted,
     *   indicating an attempted hardware connection.
     * - Uses a TestProbe with awaitAssert to query connection status until true.
     * - Logs confirm the flow: onConnect() called → Attempting to connect… → Connected successfully.
     * - The CoordinatedShutdown info log after success is expected during ActorSystem teardown.
     */
    @Test
    public void testOnConnectShouldUpdateConnectionStatus() {
        URI mockUri = mock(URI.class);
        when(mockUri.toString()).thenReturn("urn:dummy");

        IReference mockRef = mock(IReference.class);
        when(mockRef.getURI()).thenReturn(mockUri);

        IResource mockResource = mock(IResource.class);
        when(mockResource.getSingle(any())).thenReturn(mockRef);
        when(mockResource.getReference()).thenReturn(mockRef);
        when(mockResource.getURI()).thenReturn(mockUri);

        TestProbe<ScannerCommand.ConnectedStatus> probe = testKit.createTestProbe();
        TestProbe<CognexCommand> fakeCognexActor = testKit.createTestProbe(CognexCommand.class);
        TestProbe<String> scanReceiverProbe = testKit.createTestProbe();

        ScannerActorConfig config = new ScannerActorConfig(
                1,
                new DummyDMCC(),
                new DummyListener(),
                mockResource,
                "localhost",
                5000,
                fakeCognexActor.getRef(),
                true,
                scanReceiverProbe.getRef(),
                false
        );

        scannerActor = testKit.spawn(ScannerActor.create(config), "Scanner-" + UUID.randomUUID());
        scannerActor.tell(new ScannerCommand.Connect());

        // Spawn a ScannerActor instance and send a Connect command.
        // We expect the actor to send a CognexCommand.Connect message to the fake Cognex actor,
        // indicating it is attempting to establish a connection.
        // Then we query the connection status and assert that the actor reports it as connected.
        fakeCognexActor.expectMessageClass(CognexCommand.Connect.class);
        log.info("hey rooozegar");

        probe.awaitAssert(Duration.ofSeconds(3), () -> {
            scannerActor.tell(new ScannerCommand.QueryIsConnected(probe.getRef()));
            assertTrue(probe.receiveMessage().status());
            return null;
        });
    }
    /**
     * - Verifies that ScannerActor flips its connection status on OnConnect/OnDisconnect.
     * - Spawns the actor, sends OnConnect, then uses a TestProbe + awaitAssert to
     *   query until ConnectedStatus.status() becomes true.
     * - Sends OnDisconnect and again queries until status() becomes false.
     * - awaitAssert handles asynchronous state propagation in the actor system.
     * - Logs confirm the flow: "DMCC is connected" → "External DMCC injected" →
     *   "Handling OnDisconnect" → "Disconnected from DMCC" → "Scheduled reconnect attempt".
     * - The CoordinatedShutdown info log is expected during ActorSystem teardown.
     */
    @Test
    public void testOnDisconnectShouldUpdateConnectionStatus() throws IOException {

    scannerActor = spawnScannerActor(new DummyListener());
        scannerActor.tell(new ScannerCommand.OnConnect());

        TestProbe<ScannerCommand.ConnectedStatus> probe = testKit.createTestProbe();
        // Wait until actor has processed OnConnect and status becomes true
        probe.awaitAssert(Duration.ofSeconds(3), ()-> {
            scannerActor.tell(new ScannerCommand.QueryIsConnected(probe.getRef()));
            assertTrue(probe.receiveMessage().status());
            return null;
        });
        scannerActor.tell(new ScannerCommand.OnDisconnect());
        // Wait until actor has processed OnDisconnect and status becomes false
        probe.awaitAssert(Duration.ofSeconds(3), () -> {
            scannerActor.tell(new ScannerCommand.QueryIsConnected(probe.getRef()));
            assertFalse(probe.receiveMessage().status());
            return null;
        });    }
    /**
     * - Verifies end-to-end integration between RangeObserverActor and ScannerActor.
     * - Spawns ScannerActor with a FakeDataManSystem that emits "SCAN_CODE_FROM_ACTOR" to the sink.
     * - Marks the scanner as connected (IsConnected(true)) so scans are permitted.
     * - Spawns RangeObserverActor with a fake sensor reading of 50.0 and starts observing.
     * - Sends Tick; the observer computes avg=50 within [10,100], sets occupation ON, and issues TriggerScan.
     * - Asserts the sink receives the expected payload "SCAN_CODE_FROM_ACTOR" within 3 seconds.
     * - Logs confirm the sequence: "Connection is Ok" → observing started → avg/range details
     *   → trigger condition met → TriggerScan sent → "Scanner triggered to scan."
     * - CoordinatedShutdown info after completion is expected during ActorSystem teardown.
     */
    @Test
    public void testRangeObserverToScannerActorIntegration() {
        TestProbe<String> sink = testKit.createTestProbe();

        ActorRef<ScannerCommand> scanner = testKit.spawn(
                ScannerActor.create(new ScannerActorConfig(
                        1,
                        new FakeDataManSystem(50, sink.getRef()),
                        new DummyListener(),
                        new DummyResource(),
                        "localhost", 5000,
                        testKit.createTestProbe(CognexCommand.class).getRef(),
                        true,
                        sink.getRef(),
                        false
                ))
        );
        scanner.tell(new ScannerCommand.IsConnected(true));
        scanner.tell(new ScannerCommand.SwitchMode(ScannerCommand.Mode.AUTO));

        ActorRef<RangeObserverCommand> obs = testKit.spawn(
                RangeObserverActor.createWithFakeSensor(50.0, scanner, sink.getRef(), Duration.ofMillis(100), null)
        );

        obs.tell(new RangeObserverCommand.StartObserving());
        obs.tell(new RangeObserverCommand.Tick());
        assertEquals("SCAN_CODE_FROM_ACTOR", sink.receiveMessage(Duration.ofSeconds(3)));
}
}