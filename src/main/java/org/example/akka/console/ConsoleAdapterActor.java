package org.example.akka.console;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.*;
import org.example.akka.message.ScannerCommand;

import java.time.Duration;
import java.util.Locale;

import static akka.actor.typed.javadsl.AskPattern.ask;

public class ConsoleAdapterActor extends AbstractBehavior<String> {

    private final ActorRef<ScannerCommand> scanner;
    private final ActorRef<String> out;
    private final Duration timeout = Duration.ofSeconds(10);

    public static Behavior<String> create(ActorRef<ScannerCommand> scanner, ActorRef<String> out) {
        return Behaviors.setup(ctx -> new ConsoleAdapterActor(ctx, scanner, out));
    }

    private ConsoleAdapterActor(ActorContext<String> ctx,
                                ActorRef<ScannerCommand> scanner,
                                ActorRef<String> out) {
        super(ctx);
        this.scanner = scanner;
        this.out = out;
    }

    @Override
    public Receive<String> createReceive() {
        return newReceiveBuilder()
                .onMessage(String.class, this::onLine)
                .build();
    }

    private Behavior<String> onLine(String raw) {
        String line = raw.trim().toLowerCase(Locale.ROOT);
        switch (line) {
            case "help" -> out.tell(help());

            case "mode auto" -> {
                scanner.tell(new ScannerCommand.SwitchMode(ScannerCommand.Mode.AUTO));
                out.tell("OK: mode=AUTO");
            }
            case "mode manual" -> {
                scanner.tell(new ScannerCommand.SwitchMode(ScannerCommand.Mode.MANUAL));
                out.tell("OK: mode=MANUAL");
            }
            case "mode?" -> {
                ask(scanner, ScannerCommand.QueryMode::new, timeout, getContext().getSystem().scheduler())
                        .whenComplete((ms, ex) ->
                                out.tell(ex == null ? ("mode=" + ms.mode()) : ("mode? failed: " + ex.getMessage())));
            }
            case "occ?" -> {
                ask(scanner, ScannerCommand.QueryOccupation::new, timeout, getContext().getSystem().scheduler())
                        .whenComplete((os, ex) ->
                                out.tell(ex == null ? ("occupation=" + os.occupied()) : ("occ? failed: " + ex.getMessage())));
            }

            case "connected?" -> {
                ask(scanner, ScannerCommand.QueryIsConnected::new, timeout, getContext().getSystem().scheduler())
                        .whenComplete((st, ex) ->
                                out.tell(ex == null ? ("connected=" + st.status()) : ("connected? failed: " + ex.getMessage())));
            }
            case "trigger" -> {
                ask(scanner, ScannerCommand.QueryMode::new, timeout, getContext().getSystem().scheduler())
                        .whenComplete((ms, ex) -> {
                            if (ex != null) { out.tell("trigger failed: " + ex.getMessage()); return; }
                            if (ms.mode() == ScannerCommand.Mode.MANUAL) {
                                scanner.tell(new ScannerCommand.ManualTriggerScan());
                                out.tell("trigger sent");
                            } else {
                                out.tell("ignored: trigger is manual-only (mode=" + ms.mode() + ")");
                            }
                        });
            }

            case "setocc true" -> {
                ask(scanner, ScannerCommand.QueryMode::new, timeout, getContext().getSystem().scheduler())
                        .whenComplete((ms, ex) -> {
                            if (ex != null) { out.tell("setocc failed: " + ex.getMessage()); return; }
                            if (ms.mode() == ScannerCommand.Mode.MANUAL) {
                                scanner.tell(new ScannerCommand.ManualSetOccupation(true));
                                out.tell("occupation := true");
                            } else {
                                out.tell("ignored: setocc is manual-only (mode=" + ms.mode() + ")");
                            }
                        });
            }


            case "connect" -> {
                scanner.tell(new ScannerCommand.OnConnect());
                out.tell("connecting");
            }
            case "disconnect" -> {
                scanner.tell(new ScannerCommand.Disconnect());
                out.tell("disconnecting");
            }
            case "start" -> {
                scanner.tell(new ScannerCommand.Start());
                out.tell("started");
            }
            case "stop" -> {
                scanner.tell(new ScannerCommand.Stop());
                out.tell("stopped");
            }
            case "exit", "quit" -> {
                scanner.tell(new ScannerCommand.Stop());
                out.tell("stopped. bye");
            }

            default -> out.tell("unknown command. type: help");
        }
        return this;
    }

    private static String help() {
        return String.join("\n",
                "commands:",
                "  help",
                "  start",
                "  stop",
                "  connect",
                "  disconnect",
                "  mode auto | mode manual | mode?",
                "  trigger",
                "  setocc true | setocc false",
                "  occ?",
                "  connected?",
                "  exit | quit"
        );
    }
}
