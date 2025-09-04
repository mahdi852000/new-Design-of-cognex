package org.example.akka.bench;

import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.MailboxSelector;
import akka.actor.typed.javadsl.Behaviors;

import java.nio.file.Paths;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import kamon.Kamon;

import org.example.akka.actor.dmcc.ScannerActor;
import org.example.akka.adapters.RangeObserverBenchAdapter;
import org.example.akka.adapters.ScannerBenchAdapter;
import org.example.akka.config.RangeObserverConfig;
import org.example.akka.config.ScannerActorConfig;
import org.example.akka.extra.DataManSystem;
import org.example.akka.extra.Request;
import org.example.akka.extra.SystemConnector;
import org.example.akka.message.CognexCommand;
import org.example.akka.message.RangeObserverCommand;
import org.example.akka.message.Response;
import org.example.akka.message.ScannerCommand;
import org.example.akka.metrics.Metrics;

public class BenchMain {

    public static void main(String[] args) {

        // --- Kamon Prometheus ---
       /* System.setProperty("kamon.prometheus.start-embedded-http-server", "true");
        System.setProperty("kamon.prometheus.embedded-server.hostname", "0.0.0.0");
        System.setProperty("kamon.prometheus.embedded-server.port", "9191");*/
        Kamon.init();

        // --- Empty root system ---
        ActorSystem<Void> system = ActorSystem.create(Behaviors.empty(), "Bench");

        // --- Dummy connector + DMCC ---
        SystemConnector dummyConn = new SystemConnector() {
            @Override public boolean connect() { System.out.println("Dummy connect called"); return true; }
            @Override public boolean connected() { return true; }
            @Override public boolean disconnect() { System.out.println("Dummy disconnect called"); return true; }
            @Override public Response send(Request request) {
                System.out.println("Dummy send called: " + request);
                return new Response.NoResponse();
            }
            @Override public boolean addListener(Listener listener) { System.out.println("Dummy listener added"); return true; }
            @Override public boolean removeListener(Listener listener) { System.out.println("Dummy listener removed"); return true; }
        };

        DataManSystem dummyDmcc = new DataManSystem(dummyConn) {
            @Override public boolean connect() { System.out.println("Dummy connect called"); return true; }
            @Override public boolean connected() { return true; }
            @Override public boolean disconnect() { System.out.println("Dummy disconnect called"); return true; }
            public Response sendCommand(String cmd, int cmId, boolean useCheckSum) {
                System.out.println("Dummy sendCommand: " + cmd);
                return new Response.NoResponse();
            }
        };

        // --- Metrics actor ---
        ActorRef<Metrics.Event> metrics =
                system.systemActorOf(
                        Metrics.create(
                                Paths.get("metrics", "bench-metrics.csv"),
                                Duration.ofSeconds(10)
                        ),
                        "metrics",
                        MailboxSelector.fromConfig("akka.actor.mailbox.bounded-mailbox")
                );

        // --- Cognex + dummy scan receiver ---
        ActorRef<CognexCommand> cognex =
                system.systemActorOf(
                        Behaviors.<CognexCommand>receiveMessage(msg -> {
                            system.log().info("CognexActor got: {}", msg);
                            return Behaviors.same();
                        }),
                        "cognex",
                        MailboxSelector.fromConfig("akka.actor.mailbox.bounded-mailbox")
                );

        ActorRef<String> dummyScanReceiver =
                system.systemActorOf(
                        Behaviors.<String>receiveMessage(msg -> {
                            system.log().info("Dummy scanReceiver got: {}", msg);
                            return Behaviors.same();
                        }),
                        "dummy-scan-receiver",
                        MailboxSelector.fromConfig("akka.actor.mailbox.bounded-mailbox")
                );

        // --- Scanner actor ---
        ScannerActorConfig scannerConfig = new ScannerActorConfig(
                0,                    // cmId
                dummyDmcc,            // dmcc
                null,                 // listener
                null,                 // delegate
                "127.0.0.1",          // host
                23,                   // port
                cognex,               // cognex actor
                false,                // isExternalDmcc
                dummyScanReceiver,    // scanReceiver
                false,                // useCheckSum
                null,                 // rangeObserverActor (wired below)
                metrics               // metrics
        );

        ActorRef<ScannerCommand> scanner =
                system.systemActorOf(
                        ScannerActor.create(scannerConfig),
                        "scanner1",
                        MailboxSelector.fromConfig("akka.actor.mailbox.bounded-mailbox")
                );

        // --- RangeObserver actor ---
        RangeObserverConfig roConfig = new RangeObserverConfig(
                dummyDmcc, 0, 0L, 100L, 5L,
                scanner,
                "scanner-uri",
                "127.0.0.1", 23,
                dummyScanReceiver,
                metrics
        );

        ActorRef<RangeObserverCommand> observer =
                system.systemActorOf(
                        org.example.akka.actor.dmcc.RangeObserverActor.create(roConfig),
                        "observer1",
                        MailboxSelector.fromConfig("akka.actor.mailbox.bounded-mailbox")
                );

        // --- Bench adapters (هدف فلودر) ---
        ActorRef<Ping> scannerAdapter =
                system.systemActorOf(
                        ScannerBenchAdapter.create(scanner),
                        "scanner-bench-adapter",
                        MailboxSelector.fromConfig("akka.actor.mailbox.bounded-mailbox")
                );

        system.systemActorOf(
                TriggerScanLoadGen.create(scanner, /*qps*/ 500),
                "scan-loadgen",
                MailboxSelector.fromConfig("akka.actor.mailbox.bounded-mailbox")
        );
     /*   system.systemActorOf(Flooder.create(), "flooder-scan", MailboxSelector.bounded(1000))
                .tell(new Flooder.Start(200, 30, scannerAdapter));*/

        ActorRef<Ping> roAdapter =
                system.systemActorOf(
                        RangeObserverBenchAdapter.create(observer),
                        "observer-bench-adapter",
                        MailboxSelector.fromConfig("akka.actor.mailbox.bounded-mailbox")
                );

        // --- Flooder ---
        ActorRef<Flooder.Command> floodScan =
                system.systemActorOf(Flooder.create(), "flooder-scan", MailboxSelector.bounded(1000));
        ActorRef<Flooder.Command> floodRO =
                system.systemActorOf(Flooder.create(), "flooder-ro",   MailboxSelector.bounded(1000));

        floodScan.tell(new Flooder.Start(500, 30, scannerAdapter));
        floodRO.tell(new Flooder.Start(300, 30, roAdapter));


        // --- (اختیاری) پریم‌کردن اسکنر برای تست ---
        system.scheduler().scheduleOnce(
                Duration.ofSeconds(1),
                () -> scanner.tell(new ScannerCommand.Connect()),
                system.executionContext()
        );

        // زنده نگه‌دار
        new CompletableFuture<Void>().join();
    }
}
