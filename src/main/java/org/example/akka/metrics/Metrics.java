package org.example.akka.metrics;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.TimerScheduler;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.nio.file.StandardOpenOption.*;

public final class Metrics {

    // ===== Events =====
    public interface Event {}
    public record Record(long ts, double avg, long measurement, boolean occupied) implements Event {}
    public record Trigger(boolean auto, long ts) implements Event {}
    public record DmccLatency(long ms, long ts) implements Event {}
    public record Error(String type, long ts) implements Event {}
    private record Flush() implements Event {}

    private Metrics() {}



    // ===== Actor factory =====
    public static Behavior<Event> create(Path csv, Duration flushEvery) {
        return Behaviors.withTimers((TimerScheduler<Event> timers) ->
                Behaviors.setup(ctx -> new AbstractBehavior<Event>(ctx) {

                    final List<String> buffer = new ArrayList<>(256);
                    long autoCnt = 0, manualCnt = 0;
                    long lastLatency = -1;
                    final Map<Long,Long> latencyByTs = new  HashMap<>();
                    boolean headerWritten;

                    {
                        try {
                            if (csv.getParent() != null) Files.createDirectories(csv.getParent());
                            headerWritten = Files.exists(csv) && Files.size(csv) > 0;
                        } catch (IOException e) {
                            headerWritten = false;
                            ctx.getLog().warn("metrics init failed: {}", e.toString());
                        }
                        if (!headerWritten) {


                            write(csv, "ts,avg,measurement,occupied,autoCnt,manualCnt,dmccLatencyMs,autoCntAtRecord,manualCntAtRecord,error\n");


                            headerWritten = true;
                        }
                        timers.startTimerAtFixedRate(new Flush(), flushEvery);
                    }

                    @Override
                    public akka.actor.typed.javadsl.Receive<Event> createReceive() {
                        return newReceiveBuilder()
                                .onMessage(Record.class, r -> {
                                    Long lat = latencyByTs.remove(r.ts());
                                    String latStr = (lat == null) ? "" : Long.toString(lat);
                                    buffer.add(String.join(",",
                                            Long.toString(r.ts()),
                                            Double.toString(r.avg()),
                                            Double.toString((double) r.measurement()),
                                            Boolean.toString(r.occupied()),
                                            Long.toString(autoCnt),
                                            Long.toString(manualCnt),
                                            Long.toString(lastLatency),
                                            Long.toString(autoCnt),
                                            Long.toString(manualCnt)
                                    ));
                                    return this;
                                })
                                .onMessage(Trigger.class, t -> {
                                    if (t.auto()) autoCnt++; else manualCnt++;
                                    return this;
                                })
                                .onMessage(DmccLatency.class, d -> { lastLatency = d.ms(); return this; })
                                .onMessage(Flush.class, f -> { flush(csv); return this; })
                               //For debugging
                                .onMessage(Trigger.class, t -> {
                                    long beforeA = autoCnt, beforeM = manualCnt;
                                    if (t.auto()) autoCnt++; else manualCnt++;
                                    ctx.getLog().info("METRICS/TRIGGER auto={} ts={} cnt {}->{} / {}->{}",
                                            t.auto(), t.ts(), beforeA, autoCnt, beforeM, manualCnt);
                                    return this;
                                })
                                .onMessage(Record.class, r -> {
                                    ctx.getLog().info("METRICS/RECORD ts={} snapshot autoCnt={} manualCnt={}",
                                            r.ts(), autoCnt, manualCnt);
                                    // ... append row ...
                                    return this;
                                })
                                .onMessage(Error.class, e -> {
                                    ctx.getLog().warn("METRICS/ERROR type={} ts={}", e.type(), e.ts());
                                    buffer.add(String.join(",",
                                            Long.toString(e.ts()),
                                            "NaN",               // avg خالی
                                            "-1",                // measurement خالی
                                            "false",             // occupied پیش‌فرض
                                            Long.toString(autoCnt),
                                            Long.toString(manualCnt),
                                            Long.toString(lastLatency),
                                            "ERROR-" + e.type(), // برای اینکه توی CSV مشخص باشه
                                            ""                   // extra
                                    ));
                                    return this;
                                })

                                .build();
                    }
                    void flush(Path csv) {
                        if (buffer.isEmpty()) return;
                        write(csv, String.join("\n", buffer) + "\n");
                        buffer.clear();
                    }
                    void write(Path csv, String chunk) {
                        try {
                            Files.writeString(csv, chunk, CREATE, WRITE, APPEND);
                        } catch (IOException e) {
                            ctx.getLog().warn("metrics write failed: {}", e.toString());
                        }
                    }
                })
        );
    }
}
