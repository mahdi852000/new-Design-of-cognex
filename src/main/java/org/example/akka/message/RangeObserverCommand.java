package org.example.akka.message;

import akka.actor.typed.ActorRef;

public interface RangeObserverCommand {
    public record StartObserving () implements RangeObserverCommand{}
    public record StopObserving () implements RangeObserverCommand{}
    public record Tick() implements RangeObserverCommand{}
    public record ScanCode(String code) implements RangeObserverCommand{}

    //For Bench
    public static final class StartObservingBench implements RangeObserverCommand {
        public final ActorRef<Boolean> replyTo;
        public StartObservingBench(ActorRef<Boolean> replyTo) { this.replyTo = replyTo; }
    }

}
