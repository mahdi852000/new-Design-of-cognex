package org.example;

import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.Behaviors;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import kamon.Kamon;
import kamon.prometheus.PrometheusReporter;
import org.example.akka.actor.dmcc.RangeObserverActor;
import org.example.akka.actor.dmcc.ScannerActor;
import org.example.akka.config.ScannerActorConfig;
import org.example.akka.extra.FakeDataManSystem;
import org.example.akka.extra.NoopListener;
import org.example.akka.extra.NoopResource;
import org.example.akka.message.CognexCommand;
import org.example.akka.message.RangeObserverCommand;
import org.example.akka.message.ScannerCommand;
import org.example.akka.metrics.MetricsServer;

public class Main {
    public static void main(String[] args) {

        /*Config forceProm = ConfigFactory.parseString(
                "kamon.prometheus.embedded-server { hostname = \"0.0.0.0\", port = 9101 }"
        );
        Config config = forceProm.withFallback(ConfigFactory.load());

        Kamon.init(config);
        Kamon.registerModule("prometheus", new PrometheusReporter("kamon.prometheus", config));
        System.out.println("Prometheus metrics at: http://localhost:9101/metrics");*/

      //  MetricsServer.start(Integer.getInteger("METRICS_PORT", 9402));

      /*  int port = Integer.getInteger("METRICS_PORT", 9401);
        try {
            org.example.akka.metrics.MetricsServer.start(port);
            System.out.println("[metrics] listening on http://127.0.0.1:" + port + "/metrics");
        } catch (Throwable t) {
            System.err.println("[metrics] FAILED: " + t);
            t.printStackTrace();
        }*/
       // MetricsServer.start(port);
      //  MetricsServer.registry().counter("app_startup_total").increment();
      //  System.out.println(">>> metrics up at http://localhost:" + port + "/metrics");


        // === 2) ساخت ActorSystem ===
        ActorSystem<Void> system = ActorSystem.create(Behaviors.setup(ctx -> {
            var metrics = ctx.spawn(
                    org.example.akka.metrics.Metrics.create(
                            java.nio.file.Paths.get("metrics.csv"),
                            java.time.Duration.ofSeconds(1)
                    ),
                    "metrics"
            );

            ActorRef<String> printer = ctx.spawn(
                    Behaviors.receiveMessage(msg -> {
                        System.out.println(msg);
                        return Behaviors.same();
                    }),
                    "printer"
            );

            ActorRef<CognexCommand> cognex =
                    ctx.spawn(Behaviors.<CognexCommand>ignore(), "cognex");

            var scannerCfg = new ScannerActorConfig(
                    1,
                    new FakeDataManSystem(50, printer),
                    new NoopListener(),
                    new NoopResource(),
                    "localhost",
                    5000,
                    cognex,
                    true,
                    printer,
                    false
            );

            ActorRef<ScannerCommand> scanner =
                    ctx.spawn(ScannerActor.create(scannerCfg), "scanner");

            ActorRef<RangeObserverCommand> observer =
                    ctx.spawn(
                            RangeObserverActor.createWithFakeSensor(
                                    50.0, scanner, printer, java.time.Duration.ofMillis(5000), metrics
                            ),
                            "observer"
                    );

            scanner.tell(new ScannerCommand.RegisterObserver(observer));

            ActorRef<String> console =
                    ctx.spawn(org.example.akka.console.ConsoleAdapterActor.create(scanner, printer), "console");

            new Thread(() -> {
                try (var br = new java.io.BufferedReader(new java.io.InputStreamReader(System.in))) {
                    System.out.println("Type 'help' to see commands. Type 'exit' to quit.");
                    String line;
                    while ((line = br.readLine()) != null) {
                        if (line.equalsIgnoreCase("exit") || line.equalsIgnoreCase("quit")) break;
                        console.tell(line);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    ctx.getSystem().terminate();
                }
            }, "console-reader").start();

            return Behaviors.empty();
        }), "app");

        // === 3) Shutdown Hook
       /* Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try { Kamon.stop(); } catch (Exception ignored) {}
            system.terminate();
        }));*/
        try { Thread.currentThread().join(); } catch (InterruptedException ignored) {}
    }
}
