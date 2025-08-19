import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.Behaviors;
import org.example.akka.actor.dmcc.RangeObserverActor;
import org.example.akka.actor.dmcc.ScannerActor;
import org.example.akka.config.ScannerActorConfig;
import org.example.akka.message.CognexCommand;
import org.example.akka.extra.FakeDataManSystem;
import org.example.akka.message.RangeObserverCommand;
import org.example.akka.message.ScannerCommand;

public class AppMain {
    public static void main(String[] args) {
        ActorSystem<Void> system = ActorSystem.create(Behaviors.setup(ctx -> {
           /* var metrics = ctx.spawn(
                    Metrics.create(Paths.get("metrics.csv"), Duration.ofSeconds(1)),
                    "metrics"
            );*/
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
            var config = new ScannerActorConfig(
                    1,
                    new FakeDataManSystem(50, printer),
                    new DummyListener(),
                    new DummyResource(),
                    "localhost",
                    5000,
                    cognex,
                    true,
                    printer,
                    false
            );
            ActorRef<ScannerCommand> scanner =
                    ctx.spawn(ScannerActor.create(config), "scanner");

            ActorRef<RangeObserverCommand> observer =
                    ctx.spawn(
                            RangeObserverActor.createWithFakeSensor(
                                    50.0, scanner, printer, java.time.Duration.ofMillis(5000),metrics                            ),
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
                     //   if (line.trim().equalsIgnoreCase("exit")) break;
                        if (line.equalsIgnoreCase("exit") ||
                                line.equalsIgnoreCase("quit")) break;
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
    }
}
