package org.example.akka.config;

import akka.actor.typed.ActorRef;
import org.example.akka.extra.DataManSystem;
import org.example.akka.message.ScannerCommand;

import org.example.akka.metrics.Metrics;


public class RangeObserverConfig {
    public final DataManSystem dmcc;
    public final int cmId;
    public final Long rangeMin, rangeMax, rangeOff;
    public final ActorRef<ScannerCommand> scannerActor;
    public final String uri, host;
    public final int port;
    public final ActorRef<String> scanReceiver;
    public final ActorRef<Metrics.Event> metricsRef;

    public RangeObserverConfig(
            DataManSystem dmcc,
            int cmId,
            Long rangeMin,
            Long rangeMax,
            Long rangeOff,
            ActorRef<ScannerCommand> scannerActor,
            String uri,
            String host,
            int port,
            ActorRef<String> scanReceiver,
            ActorRef<Metrics.Event> metricsRef
    ) {
        this.dmcc = dmcc;
        this.cmId = cmId;
        this.rangeMin = rangeMin;
        this.rangeMax = rangeMax;
        this.rangeOff = rangeOff;
        this.scannerActor = scannerActor;
        this.uri = uri;
        this.host = host;
        this.port = port;
        this.scanReceiver = scanReceiver;
        this.metricsRef = metricsRef;
    }
    public RangeObserverConfig(
            DataManSystem dmcc,
            int cmId,
            long rangeMin,
            long rangeMax,
            long rangeOff,
            ActorRef<ScannerCommand> scannerActor,
            String uri,
            String host,
            int port,
            ActorRef<String> scanReceiver
    ) {
        this(dmcc, cmId, rangeMin, rangeMax, rangeOff,
                scannerActor, uri, host, port, scanReceiver,
                null /* metricsRef = null */);
    }
}
