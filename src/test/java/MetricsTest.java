
import akka.actor.testkit.typed.javadsl.ActorTestKit;
import akka.actor.typed.ActorRef;
import org.example.akka.metrics.Metrics;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;


import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MetricsTest {
    static ActorTestKit testKit;

    @BeforeAll
    static void setup() { testKit = ActorTestKit.create(); }

    @AfterAll
    static void tearDown() { testKit.shutdownTestKit(); }

    @Test
    void countersIncrementBeforeRecord() throws Exception {
        Path csv = Files.createTempFile("metrics-", ".csv");
        ActorRef<Metrics.Event> metrics = testKit.spawn(Metrics.create(csv, Duration.ofMillis(20)));

        long ts = System.currentTimeMillis();
        metrics.tell(new Metrics.Trigger(true, ts));
        metrics.tell(new Metrics.Record(ts, 1.0, 90, true));

        Thread.sleep(80);

        List<String> lines = Files.readAllLines(csv);
        String last = lines.get(lines.size() - 1);
        String[] c = last.split(",");
        assertEquals("1", c[4]);
    }
}
