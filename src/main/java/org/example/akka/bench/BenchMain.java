package org.example.akka.bench;

import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.Behavior;
import akka.actor.typed.Props;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Routers;


import org.example.akka.metrics.MetricsServer;

public class BenchMain {
    public static void main(String[] args) {

        MetricsServer.start(9402);
        System.out.println("Metrics server started");

        ActorSystem<Void> system = ActorSystem.create(Behaviors.empty(), "Bench");

        Behavior<Ping> pool = Routers.pool(10, Worker.create());
        ActorRef<Ping> router = system.systemActorOf(pool, "workers",Props.empty());

        ActorRef<Flooder.Command> flooder = system.systemActorOf(Flooder.create(), "flood", Props.empty());
       // flooder.tell(new Flooder.Start(60_000, 30, router));
        flooder.tell(new Flooder.Ramp(1, 5, 5000, 5, router));
      //  flooder.tell(new Flooder.Ramp(1, 15, 500, 15, router));
      //  flooder.tell(new Flooder.Ramp(1, 50, 5000, 10, router));
       // flooder.tell(new Flooder.Ramp(100, 300, 600_000_0, 5, router));
        system.getWhenTerminated().toCompletableFuture().join();

    }
}

