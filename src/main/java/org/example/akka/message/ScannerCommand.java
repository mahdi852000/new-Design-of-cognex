package org.example.akka.message;

import akka.actor.typed.ActorRef;

public interface ScannerCommand {
    public record GetOccupation() implements ScannerCommand {}
    public record SetOccupation(boolean occupied) implements ScannerCommand {}
    public record Start() implements ScannerCommand{}
    public record Stop() implements ScannerCommand{}
    public record SendTrigger() implements ScannerCommand{}
    public record Connect() implements ScannerCommand{}
    public record OnMessage(Response response) implements ScannerCommand{}
    public record OnConnect() implements ScannerCommand{}
    public record OnDisconnect() implements ScannerCommand{}
    public record IsConnected(boolean value) implements ScannerCommand{}
    public record Enqueue(IDTO dto,ActorRef<Boolean> replyTo) implements ScannerCommand {}
    public record RegisterEventListener(ScannerEventListener listener) implements ScannerCommand{}
    public record UnregisterEventListener(ScannerEventListener listener) implements ScannerCommand{}
    public record IsConnectedToDMCC(boolean value) implements ScannerCommand {}

    public record OccupationStatus(boolean occupied) implements ScannerCommand{}
    public record QueryOccupation(ActorRef<OccupationStatus> replyTo) implements ScannerCommand{}
    public record QueryIsConnected(ActorRef<ConnectedStatus> replyTo) implements ScannerCommand {}
    public record ConnectedStatus(boolean connected, String source) {
        public ConnectedStatus(boolean connected) {
                this(connected,null);
            }
            public boolean status(){
                return connected;
            }
    }
    public record Disconnect() implements ScannerCommand{}

    //TriggerScan for Test Purpose
    public record TriggerScan() implements ScannerCommand {}


    public interface ScannerEventListener{
        public void onOccupationChange(ScannerCommand scannerCommand , boolean occupied);
        public void onCodeScanned(ScannerCommand scannerCommand , String code);

    }

    enum Mode { AUTO, MANUAL }
    record SwitchMode(Mode mode) implements ScannerCommand {}
    record ToggleMode() implements ScannerCommand {}
    record QueryMode(akka.actor.typed.ActorRef<ModeStatus> replyTo) implements ScannerCommand {}
    record ModeStatus(Mode mode) implements ScannerCommand {}


    record RegisterObserver(ActorRef<RangeObserverCommand> ref)
            implements ScannerCommand {}

    record ManualTriggerScan() implements ScannerCommand {}
    record ManualSetOccupation(boolean occupied) implements ScannerCommand {}


}



