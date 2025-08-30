package org.example;

import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.Behaviors;

import org.example.akka.actor.dmcc.RangeObserverActor;
import org.example.akka.actor.dmcc.ScannerActor;
import org.example.akka.config.ScannerActorConfig;
import org.example.akka.extra.FakeDataManSystem;
import org.example.akka.extra.NoopListener;
import org.example.akka.extra.NoopResource;
import org.example.akka.message.CognexCommand;
import org.example.akka.message.RangeObserverCommand;
import org.example.akka.message.ScannerCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Main {
    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(Main.class);
         /* ActorSystem<Void> system = */
                  ActorSystem.create(Behaviors.setup(ctx -> {
            var metrics = ctx.spawn(
                    org.example.akka.metrics.Metrics.create(
                            java.nio.file.Paths.get("metrics.csv"),
                            java.time.Duration.ofSeconds(1)
                    ),
                    "metrics"
            );

            ActorRef<String> printer = ctx.spawn(
                    Behaviors.receiveMessage(msg -> {
                        logger.info("Received message: {}", msg);
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
                    logger.info("Type 'help' to see commands. Type 'exit' to quit.");
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

        try { Thread.currentThread().join(); } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
