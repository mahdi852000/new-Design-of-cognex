package org.example.akka.metrics;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.binder.system.UptimeMetrics;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

public final class MetricsServer {
    private static volatile PrometheusMeterRegistry REGISTRY;
    private static volatile HttpServer SERVER;


    public static synchronized PrometheusMeterRegistry registry() {
        if (REGISTRY == null) {
            REGISTRY = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);

            Metrics.addRegistry(REGISTRY);

            //CPU/RAM/Threads/Uptime
            new ClassLoaderMetrics().bindTo(REGISTRY);
            new JvmMemoryMetrics().bindTo(REGISTRY);
            new JvmGcMetrics().bindTo(REGISTRY);
            new JvmThreadMetrics().bindTo(REGISTRY);
            new ProcessorMetrics().bindTo(REGISTRY);  // process_cpu_usage / system_cpu_usage
            new UptimeMetrics().bindTo(REGISTRY);

        }
        return REGISTRY;
    }

    public static synchronized void start(int port) {
        if (SERVER != null) return;
        try {

            SERVER = HttpServer.create(new InetSocketAddress("0.0.0.0", port), 0);
            SERVER.createContext("/metrics", new MetricsHandler(registry()));
            SERVER.setExecutor(null);
            SERVER.start();
            System.out.println("[metrics] exposed at http://127.0.0.1:" + port + "/metrics");
        } catch (IOException e) {
            throw new RuntimeException("Failed to start metrics server", e);
        }
    }

    public static synchronized void stop() {
        if (SERVER != null) {
            SERVER.stop(0);
            SERVER = null;
            System.out.println("[metrics] stopped");
        }
    }

    private static final class MetricsHandler implements HttpHandler {
        private final PrometheusMeterRegistry reg;
        MetricsHandler(PrometheusMeterRegistry reg) { this.reg = reg; }

        @Override public void handle(HttpExchange ex) throws IOException {
            byte[] body = reg.scrape().getBytes(StandardCharsets.UTF_8);
            ex.getResponseHeaders().add("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
            ex.sendResponseHeaders(200, body.length);
            try (OutputStream os = ex.getResponseBody()) { os.write(body); }
        }
    }
}
