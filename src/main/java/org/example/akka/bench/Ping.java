package org.example.akka.bench;
import akka.actor.typed.ActorRef;

public record Ping(long sentAtNanos, ActorRef<Pong> replyTo) {
}
