import akka.actor.typed.*;
import akka.actor.typed.javadsl.*;
import net.enilink.komma.core.IReference;
import org.example.akka.actor.dmcc.CognexDataManActor;
import org.example.akka.actor.dmcc.RangeObserverActor;
import org.example.akka.message.RangeObserverCommand;

import org.example.akka.actor.dmcc.ScannerActor;
import org.example.akka.config.ScannerActorConfig;
import org.example.akka.event.SystemEvent;
import org.example.akka.extra.*;
import org.example.akka.message.*;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.example.akka.metrics.Metrics;
import java.nio.file.Paths;
import java.time.Duration;


import java.util.Locale;

import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/**
 * Console-driven integration harness for Scanner/Cognex/Range flow.
 * Run it and type commands in the terminal (see help below).
 *
 * This is meant as a MANUAL, interactive integration test — not a JUnit test.
 */
public class ConsoleScannerIT {

    public static void main(String[] args) throws Exception {
        Behavior<Void> root = Behaviors.setup(ctx -> {
            var metrics = ctx.spawn(
                    Metrics.create(Paths.get("metrics.csv"), Duration.ofSeconds(1)),
                    "metrics"
            );
            org.example.akka.metrics.MetricsServer.start(9402);

            ActorSystem<?> system=ctx.getSystem();
            // === Print sinks ===
            ActorRef<String> scanSink = ctx.spawn(printString("[scan] "), "scan-sink");
            ActorRef<SystemEvent.CognexEvent> cognexSink = ctx.spawn(printCognex(), "cognex-sink");

            // === Cognex actor ===
            ActorRef<CognexCommand> cognexActor = ctx.spawn(CognexDataManActor.create(), "cognex");
            // Register sink so we see scanned codes
            cognexActor.tell(new CognexCommand.RegisterListener(cognexSink));

            // === DMCC fake (simulated) ===
            // Sends any scan result string to scanSink; you can still trigger via ScannerActor.TriggerScan
            DataManSystem dmcc = new DataManSystem(new DummyConnector()) {
                private boolean ok = false;
                private long meas = 95;
                @Override public boolean connect() { ok=true; return true;}
                @Override public boolean connected() { return ok; }
                @Override public boolean disconnect() { ok = false; return false; }
                @Override public Response sendCommand(String cmd, Integer id, boolean log) {
                    if (cmd.startsWith("GET HEIGHT-SENSOR.CURRENT-MEASUREMENT")) {

                        return new Response(String.valueOf(meas), false, id);
                    }
                    if (cmd.equals("UPTIME")) {
                        return new Response("12345", false, id);
                    }
                    return new Response("0", false, id);
                }

                @Override public Response sendCommand(String cmd) {
                    if (cmd.equals("UPTIME")) return new Response("12345", false, 0);
                    return new Response("0", false, 0);
                }
            };
            // === Listener just logs occupancy/connect events ===
            SystemConnector.Listener listener = new SystemConnector.Listener() {
                @Override public void onMessage(Response response) { /* unused in this harness */ }
                @Override public void onConnect() { System.out.println("[listener] DMCC connected"); }
                @Override public void onDisconnect() { System.out.println("[listener] DMCC disconnected"); }
                @Override public void onOccupationChanged(boolean occupied) {
                    System.out.println("[listener] occupation=" + occupied);
                }
            };

            // === Delegate resource (dummy) ===
            // If your project already provides DummyResource, this will work out-of-the-box.
            IResource delegate = mock(IResource.class);

            IReference refMin = LOGISTICS.NAMAESPACE_URI.appendLocalPart("triggerRangeMin");
            IReference refMax = LOGISTICS.NAMAESPACE_URI.appendLocalPart("triggerRangeMax");
            IReference refOff = LOGISTICS.NAMAESPACE_URI.appendLocalPart("triggerRangeOff");
            IReference refHB  = LOGISTICS.NAMAESPACE_URI.appendLocalPart("heartbeat");

            when(delegate.getSingle(refMin)).thenReturn("90");
            when(delegate.getSingle(refMax)).thenReturn("100");
            when(delegate.getSingle(refOff)).thenReturn("110");
            when(delegate.getSingle(refHB)).thenReturn("true");

            URI fakeUri = URIs.createURI("urn:scanner:fake1");
            IReference fakeRef = mock(IReference.class);
            when(fakeRef.getURI()).thenReturn(fakeUri);
            when(delegate.getReference()).thenReturn(fakeRef);
            when(delegate.getURI()).thenReturn(fakeUri);

            // === Scanner actor ===
            ScannerActorConfig config = new ScannerActorConfig(
                    1,              // cmId
                    dmcc,
                    listener,
                    delegate,
                    "localhost",
                    5000,
                    cognexActor,
                    true,  // simulateConnected: allow TriggerScan etc.
                    scanSink,           // where the scan code goes (we print it)
                    false               // manual trigger by default
            );
            ActorRef<ScannerCommand> scanner = ctx.spawn(ScannerActor.create(config), "scanner");
            ActorRef<RangeObserverCommand> observer = ctx.spawn(
                    RangeObserverActor.createWithFakeSensor(
                            90.0,
                            scanner,
                            scanSink,
                            java.time.Duration.ofMillis(5000),
                            metrics
                    ),
                    "observer"
            );
            scanner.tell(new ScannerCommand.RegisterObserver(observer));

            // === REPL (stdin) on a blocking thread ===
            new Thread(() -> repl(scanner, cognexActor, scanSink,system), "console-repl").start();
            return Behaviors.empty();
        });

        ActorSystem<Void> system = ActorSystem.create(root, "console-it");
    }
    // ===== Console REPL =====
    private static void repl(ActorRef<ScannerCommand> scanner,
                             ActorRef<CognexCommand> cognex,
                             ActorRef<String> scanSink,
                             ActorSystem<?> system) {
        printHelp();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(System.in))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.trim().split("\\s+", 2);
                if (parts.length == 0 || parts[0].isBlank()) continue;
                String cmd = parts[0].toLowerCase(Locale.ROOT);
                String arg = parts.length > 1 ? parts[1] : "";
                switch (cmd) {
                    case "help" -> printHelp();
                    case "exit", "quit" -> { System.out.println("bye"); System.exit(0); }
                    // Scanner control
                    case "connect" -> scanner.tell(new ScannerCommand.Connect());
                    case "onconnect" -> scanner.tell(new ScannerCommand.OnConnect());
                    case "disconnect" -> scanner.tell(new ScannerCommand.Disconnect());
                    case "start" -> scanner.tell(new ScannerCommand.Start());
                    case "stop" -> scanner.tell(new ScannerCommand.Stop());
                  //  case "trigger" -> scanner.tell(new ScannerCommand.TriggerScan());
                    case "sendtrigger" -> scanner.tell(new ScannerCommand.SendTrigger());
                //    case "trigger" -> {
                     //   scanner.tell(new ScannerCommand.ManualTriggerScan());
                  //  }
                    // case "setocc" -> {
                    //    boolean occ = arg.equalsIgnoreCase("true") || arg.equalsIgnoreCase("on");
                      //  scanner.tell(new ScannerCommand.SetOccupation(occ));
                   // }
                    case "setocc" -> {
                        boolean occ = arg.equalsIgnoreCase("true")
                                || arg.equalsIgnoreCase("on");

                        scanner.tell(new ScannerCommand.ManualSetOccupation(occ));
                        System.out.println("requested: set occupation = " + occ + " (manual mode only)");
                    }
                    case "qocc" -> {
                        // reply comes as a message; we create an inline temp actor to print it
                        var probe = tempPrinter((ScannerCommand.OccupationStatus s) ->
                                System.out.println("occupation=" + s.occupied()));
                        scanner.tell(new ScannerCommand.QueryOccupation(probe));
                    }
                    case "qconn" -> {
                        var probe = tempPrinter((ScannerCommand.ConnectedStatus s) ->
                                System.out.println("connected=" + s.status()));
                        scanner.tell(new ScannerCommand.QueryIsConnected(probe));
                    }

                    // Cognex side
                    case "cognex.register" -> {
                        // register the scanSink as a CognexEvent listener printer
                        var sink = tempPrinter((SystemEvent.CognexEvent e) -> System.out.println("[cognex] " + e));
                        cognex.tell(new CognexCommand.RegisterListener(sink));
                        System.out.println("registered a temporary Cognex listener");
                    }
                    case "cognex.notify" -> {
                        // Manually simulate a scanned code that flows through Cognex actor
                        var code = arg.isBlank() ? "TEST-CODE" : arg;
                        cognex.tell(new CognexCommand.NotifyScannedCode(new DummyResource(), code));
                    }
                    // Utilities for manual checks
                    case "pause" -> {
                        long parsed = 1000L;
                        try { parsed = Long.parseLong(arg); } catch (Exception ignored) {}
                        final long delayMs = parsed;
                        scanner.tell(new ScannerCommand.Stop());
                        system.scheduler().scheduleOnce(
                                java.time.Duration.ofMillis(delayMs),
                                () -> scanner.tell(new ScannerCommand.Start()),
                                system.executionContext()
                        );
                        System.out.println("paused observing for ~" + delayMs + " ms");
                    }
                    case "mode" -> {
                        String m = arg.toLowerCase(Locale.ROOT);
                        if ("manual".equals(m)) {
                            scanner.tell(new ScannerCommand.SwitchMode(ScannerCommand.Mode.MANUAL));
                            System.out.println("OK: mode=MANUAL");
                        } else if ("auto".equals(m)) {
                            scanner.tell(new ScannerCommand.SwitchMode(ScannerCommand.Mode.AUTO));
                            System.out.println("OK: mode=AUTO");
                        } else {
                            System.out.println("usage: mode manual | mode auto");
                        }
                    }
                    default -> System.out.println("unknown command. type 'help'");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void printHelp() {
        System.out.println("\nCommands:\n" +
                "  help                       - show commands\n" +
                "  exit|quit                  - terminate\n" +
                "  connect / onconnect        - connect DMCC (explicit/shortcut)\n" +
                "  disconnect                 - disconnect DMCC\n" +
                "  start / stop               - start/stop scanner workflow\n" +
                "  mode manual | mode auto    - switch mode\n" +
             //   "  trigger                    - trigger a scan (manual mode only)\n" +
                "  sendtrigger                - force low-level trigger (ignores mode)\n" +
                "  setocc on|off              - set occupation (manual mode only)\n" +
                "  qocc                       - query occupation\n" +
                "  qconn                      - query connection\n" +
                "  cognex.register            - add a temp listener to print Cognex events\n" +
                "  cognex.notify <CODE>       - simulate NotifyScannedCode to Cognex actor\n" +
                "  pause <ms>                 - sleep to allow timers/messages to flow\n");
    }

    // ===== Small helper behaviors =====
    private static Behavior<String> printString(String prefix) {
        return Behaviors.receiveMessage(msg -> {
            System.out.println(prefix + msg);
            return Behaviors.same();
        });
    }

    private static Behavior<SystemEvent.CognexEvent> printCognex() {
        return Behaviors.receiveMessage(evt -> {
            System.out.println("[cognex-event] " + evt);
            return Behaviors.same();
        });
    }

    // Create a temporary actor that prints a single reply type, then stops after a short timeout
    private static <T> ActorRef<T> tempPrinter(java.util.function.Consumer<T> onMsg) {
        return ActorSystem.create(
                Behaviors.withTimers(timers -> Behaviors.<T>receive((ctx, msg) -> {
                    onMsg.accept(msg);
                    return Behaviors.same();
                })),
                "tmp-" + java.util.UUID.randomUUID());
    }
}
