package org.example.akka.message;
import akka.actor.typed.ActorRef;
import org.example.akka.event.SystemEvent;
import org.example.akka.extra.IResource;

public interface CognexCommand {
    record RegisterListener(ActorRef<SystemEvent.CognexEvent> listener) implements CognexCommand {}
    record UnregisterListener(ActorRef<SystemEvent.CognexEvent> listener) implements CognexCommand {}
    record ListenerDied(ActorRef<SystemEvent.CognexEvent> listener) implements CognexCommand {}
    record Connect() implements CognexCommand {}
    record NotifyScannedCode(IResource resource, String code) implements CognexCommand {}
    record Disconnect() implements CognexCommand {}



}

