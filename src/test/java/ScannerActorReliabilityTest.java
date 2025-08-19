import akka.actor.testkit.typed.javadsl.*;
import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import net.enilink.komma.core.IReference;
import org.example.akka.actor.dmcc.RangeObserverActor;
import org.example.akka.actor.dmcc.ScannerActor;
import org.example.akka.config.ScannerActorConfig;
import org.example.akka.extra.*;
import org.example.akka.message.*;
import org.junit.jupiter.api.*;
import static org.mockito.Mockito.*;

import java.net.URISyntaxException;
import java.time.Duration;
import static org.junit.jupiter.api.Assertions.*;

public class ScannerActorReliabilityTest {

    private static ActorTestKit testKit;

    @BeforeAll
    static void setup() {
        testKit = ActorTestKit.create();
    }

    @AfterAll
    static void teardown() {
        testKit.shutdownTestKit();
    }
    /**
     * Verifies that the ScannerActor properly notifies registered listeners
     * when its occupation status changes.
     * <p>
     * The test sets up a fake RangeObserver that triggers scan commands and occupation state changes.
     * It then spawns a ScannerActor with a mocked SystemConnector.Listener and ensures that:
     *   - When the actor receives a SetOccupation(true), the listener is notified with the correct state.
     *   - When the actor receives a SetOccupation(false), the listener is again notified appropriately.
     * <p>
     * This test ensures the correct interaction between ScannerActor and its listeners
     * regarding occupation state transitions, which is critical for systems that depend
     * on sensor status awareness (e.g., safety or task scheduling).
     */

    @Test
    public void testOccupationChangeNotifiesListeners() throws URISyntaxException {
        TestProbe<ScannerCommand> scannerProbe = testKit.createTestProbe();
        TestProbe<String> scanReceiver = testKit.createTestProbe();

        Behavior<RangeObserverCommand> behavior = createWithFakeSensor(
                55.0, // only for test
                scannerProbe.getRef(),
                scanReceiver.getRef(),
                Duration.ofSeconds(2) // Tick interval
        );

        ActorRef<RangeObserverCommand> actor = testKit.spawn(behavior);
        actor.tell(new RangeObserverCommand.StartObserving());

        scannerProbe.expectMessageClass(ScannerCommand.SetOccupation.class);
      //  scannerProbe.expectMessageClass(ScannerCommand.TriggerScan.class);

        TestProbe<ScannerCommand> listenerProbe = testKit.createTestProbe();
        TestProbe<String> dummyReceiver = testKit.createTestProbe();
        SystemConnector.Listener listener = mock(SystemConnector.Listener.class);
        doAnswer(invocation -> {
            boolean occupied = invocation.getArgument(0);
            listenerProbe.getRef().tell(new ScannerCommand.SetOccupation(occupied));
            System.out.println("onOccupationChanged called");
            return null;
        }).when(listener).onOccupationChanged(anyBoolean());

        ActorRef<ScannerCommand> scannerActor = testKit.spawn(
                ScannerActor.create(new ScannerActorConfig(
                        1,
                        new FakeDataManSystem(50, testKit.createTestProbe(String.class).getRef()),
                        listener,
                        new DummyResource(),
                        "localhost",
                        5000,
                        testKit.createTestProbe(CognexCommand.class).getRef(),
                        true,
                        dummyReceiver.ref(),
                        false
                ))
        );
        System.out.println("Sending SetOccupation(true)");
        scannerActor.tell(new ScannerCommand.SetOccupation(true));
        ScannerCommand received1 = listenerProbe.receiveMessage(Duration.ofSeconds(3));
        System.out.println("Received: " + received1);
        assertInstanceOf(ScannerCommand.SetOccupation.class, received1);
        assertTrue(((ScannerCommand.SetOccupation) received1).occupied());

        scannerActor.tell(new ScannerCommand.SetOccupation(false));
        ScannerCommand received2 = listenerProbe.receiveMessage(Duration.ofSeconds(3));
        assertInstanceOf(ScannerCommand.SetOccupation.class, received2);
        assertFalse(((ScannerCommand.SetOccupation) received2).occupied());
    }

    /**
     * Tests that the ScannerActor correctly processes a TriggerScan command
     * and sends the scan result to the configured receiver.
     * <p>
     * The test sets up a ScannerActor with a FakeDataManSystem that simulates a scan delay.
     * It then sends a TriggerScan command and verifies that a scan result is received
     * by the dummyReceiver probe.
     * <p>
     * This test validates the core scan triggering behavior of ScannerActor,
     * ensuring that scan requests are processed and results are delivered as expected.
     * This functionality is fundamental to the actor’s role in interacting with scanning hardware.
     */

    @Test
    public void testTriggerScanCommand() {
        // Create the probe that will receive scan results
        TestProbe<String> dummyReceiver = testKit.createTestProbe();

        // Create a probe for RangeObserver (even if unused here)
        TestProbe<RangeObserverCommand> rangeObserverProbe = testKit.createTestProbe();

        // Create a probe for Cognex commands (even if not directly used in assertions)
        TestProbe<CognexCommand> cognexCommandProbe = testKit.createTestProbe();
        TestProbe<String> probe = testKit.createTestProbe();
        // Define scanner config
        ScannerActorConfig config = new ScannerActorConfig(
                1,
                new FakeDataManSystem(50, probe.getRef()),  // Simulated scan delay
                new DummyListener(),                   // Optional stub listener
                new DummyResource(),                   // Optional stub resource
                "localhost",
                5000,
                cognexCommandProbe.getRef(),
                true,                                       // simulateConnected
                dummyReceiver.getRef(),                                  // This probe will get scan result
                false,                                                   // manual trigger
                rangeObserverProbe.getRef(),                              // Optional observer probe
                null);

        // Spawn the actor
        ActorRef<ScannerCommand> scannerActor = testKit.spawn(
                ScannerActor.create(config)
        );

        // Tell actor to trigger a scan
        scannerActor.tell(new ScannerCommand.TriggerScan());

        // Wait for scan result from dummyReceiver
        System.out.println("Waiting for scan result...");
        String scanResult = dummyReceiver.receiveMessage(Duration.ofSeconds(5));

        // Assert and print the result
        assertNotNull(scanResult);
        System.out.println("Received scan result: " + scanResult);
        assertEquals("SCAN_CODE_FROM_ACTOR", scanResult);

    }
    /**
     * Tests the connect-disconnect flow of the ScannerActor with a simulated retry mechanism.
     * <p>
     * The test simulates a failure on the first connection attempt and a successful
     * connection on the second attempt. It then verifies that after disconnecting,
     * the actor reports it is no longer connected.
     */
    @Test
    public void testConnectDisconnectFlow_withRetrySuccess() {

        TestProbe<String> dummyReceiver = testKit.createTestProbe();
        DataManSystem retryingDmcc = new DataManSystem(new DummyConnector()) {
            private boolean firstAttempt = true;
            private boolean connected = false;

            @Override
            public boolean connect() {
                if (firstAttempt) {
                    firstAttempt = false;
                    System.out.println("Simulated failure on first connect");
                    return false;
                }
                connected = true;
                System.out.println("Simulated success on second connect");
                return true;
            }

            @Override
            public boolean connected() {           // [KEEP]
                return connected;
            }
            public boolean disconnect()  {
                return connected = false;
            }
            @Override
            public Response sendCommand(String command, Integer id, boolean log) {
                return new Response("140", false, id);
            }
        };
        ScannerActorConfig config = new ScannerActorConfig(
                1,
                retryingDmcc,
                new DummyListener(),
                new DummyResource(),
                "localhost",
                5000,
                testKit.createTestProbe(CognexCommand.class).getRef(),
                /* simulateConnected */ false,
                dummyReceiver.ref(),
                false
        );


        ActorRef<ScannerCommand> scannerActor = testKit.spawn(ScannerActor.create(config));
        scannerActor.tell(new ScannerCommand.Connect());

        TestProbe<ScannerCommand.ConnectedStatus> probe = testKit.createTestProbe();
        probe.awaitAssert(Duration.ofSeconds(5), () -> {
            scannerActor.tell(new ScannerCommand.QueryIsConnected(probe.ref()));
            assertTrue(probe.receiveMessage(Duration.ofSeconds(1)).status());
            return null;
        });


        scannerActor.tell(new ScannerCommand.Disconnect());

        probe.awaitAssert(Duration.ofSeconds(5), () -> {
            scannerActor.tell(new ScannerCommand.QueryIsConnected(probe.ref()));
            assertFalse(probe.receiveMessage(Duration.ofSeconds(1)).status());
            return null;
        });

        // Thread.sleep(2500);

        // TestProbe<ScannerCommand.ConnectedStatus> replyProbe = testKit.createTestProbe();
        // scannerActor.tell(new ScannerCommand.QueryIsConnected(replyProbe.ref()));
        // ScannerCommand.ConnectedStatus status = replyProbe.receiveMessage();
        // assertNotNull(status);
        // assertFalse(status.status());
    }

    /**
     * Tests that the ScannerActor correctly enqueues a DTO and replies with success.
     * <p>
     * The test sends an Enqueue command with a dummy DTO and a reply probe,
     * and verifies that the actor responds with a Boolean `true` value.
     * <p>
     * This ensures that the actor's internal queueing mechanism accepts new scan requests
     * and acknowledges the enqueue operation properly.
     * <p>
     * This behavior is essential for managing scan jobs and coordinating scan execution flow.
     */
    @Test
    public void testEnqueueAndReply() {
        TestProbe<String> dummyReceiver = testKit.createTestProbe();
        ActorRef<ScannerCommand> scannerActor = testKit.spawn(createTestScannerActor(testKit, dummyReceiver.ref()));

        TestProbe<Boolean> replyProbe = testKit.createTestProbe();

        IDTO dummyDto = new DummyDTO();
        scannerActor.tell(new ScannerCommand.Enqueue(dummyDto, replyProbe.ref()));

        Boolean response = replyProbe.receiveMessage(Duration.ofSeconds(1));
        System.out.println("Enqueue response received: " + response);

        assertNotNull(response);
        assertTrue(response);
    }

    public static Behavior<ScannerCommand> createTestScannerActor(ActorTestKit testKit,
                                                                  ActorRef<String> dummyReceiver
                                        ) {
        return ScannerActor.create(new ScannerActorConfig(
                1,
                new FakeDataManSystem(50, testKit.createTestProbe(String.class).getRef()),
                new DummyListener(),
                new DummyResource(),
                "localhost",
                5000,
                testKit.createTestProbe(CognexCommand.class).getRef(),
                true,
                dummyReceiver,
                false
        ));
    }

   /* private Behavior<ScannerCommand> createTestScannerActor(
            ActorTestKit testKit,
            ActorRef<String> receiver,
            DataManSystem dmcc
    ) {
        ScannerActorConfig config = new ScannerActorConfig(()->dmcc,receiver)
        return ScannerActor.create(() -> dmcc, receiver);
    }*/

    /**
     * Tests the behavior of mocked IResource and its associated references.
     * <p>
     * This unit test creates a mock IResource, IReference, and URI,
     * and verifies that the nested method calls return the expected fake URI string.
     * <p>
     * The goal is to ensure that dummyResource.getReference().getURI().toString()
     * yields the correct mocked value ("fake://scanner"), which can be useful
     * for testing components that rely on semantic resource references
     * without requiring real implementations of IResource or IReference.
     * <p>
     * This test is particularly helpful for verifying mock-based setups used
     * in other integration tests involving semantic resources or URIs.
     */
    // Dummy Resource Implementation
    @Test
    void testWithDummyResource() {
        // create faked URI
        net.enilink.komma.core.URI dummyUri = mock(net.enilink.komma.core.URI.class);
        when(dummyUri.toString()).thenReturn("fake://scanner");

        // create faked IReference
        IReference dummyRef = mock(IReference.class);
        when(dummyRef.getURI()).thenReturn(dummyUri);

        // create faked IResource
        IResource dummyResource = mock(IResource.class);
        when(dummyResource.getReference()).thenReturn(dummyRef);
        when(dummyRef.getURI()).thenReturn(dummyUri);
        
        assertEquals("fake://scanner", dummyResource.getReference().getURI().toString());
    }

    // Dummy DTO for testing
    static class DummyDTO extends IDTO {
        @Override
        public String toString() {
            return "dummy";
        }
    }
    private Behavior<RangeObserverCommand> createWithFakeSensor(
            double fixedDistance,
            ActorRef<ScannerCommand> scannerActor,
            ActorRef<String> scanReceiver,
            Duration tickInterval
    ) { return RangeObserverActor.createWithFakeSensor(
            fixedDistance,scannerActor,scanReceiver,tickInterval, null);
    }
}
