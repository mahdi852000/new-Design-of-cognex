import org.example.akka.extra.DataManSystem;
import org.example.akka.extra.Request;
import org.example.akka.message.Response;

/**
 * Simulates the DMCC (DataMan) system.
 * - Can return fake scan height values on each request.
 * - Mocks connection state.
 * Used to decouple ScannerActor tests from the real DMCC device.
 */
class DummyDMCC extends DataManSystem {
    private boolean connected = false;
    private final long[] simulatedHeights;
    private int index = 0;

    public DummyDMCC() {
        super(new DummyConnector());
        this.simulatedHeights = new long[]{140};
    }

    @Override public boolean disconnect() { connected = false; return true;}


    public DummyDMCC(long[] simulatedHeights) {
        super(new DummyConnector());
        this.simulatedHeights = simulatedHeights;
    }

    public Response send(Request request) {
        long value = simulatedHeights[index % simulatedHeights.length];
        index++;
        return new Response(Long.toString(value), false, request.getId());
    }

    public Response sendCommand(String command, Integer id, boolean log) {
        long value = simulatedHeights[index % simulatedHeights.length];
        index++;
        return new Response(Long.toString(value), false, id);
    }

    public boolean connected() { return connected; }
    public boolean connect() { return connected = true; }
}