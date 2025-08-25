package org.example.akka.bench;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import org.example.akka.metrics.MetricsServer;

public final class BenchMetrics {

    public static final Counter COMPLETED = Counter.builder("bench_completed_total")
            .description("completed pings")
            .register(MetricsServer.registry());

    public static final Counter TIMEOUTS = Counter.builder("bench_timeouts_total")
            .description("ask timeouts")
            .register(MetricsServer.registry());

    public static final Timer RTT = Timer.builder("bench_rtt_ms")
            .description("end-to-end latency ms")
            .publishPercentiles(0.5, 0.95, 0.99)
            .publishPercentileHistogram(true)
            .register(MetricsServer.registry());

    private BenchMetrics() {}
}
