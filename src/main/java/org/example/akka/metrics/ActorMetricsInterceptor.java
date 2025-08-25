package org.example.akka.metrics;

import akka.actor.typed.Behavior;
import akka.actor.typed.Signal;
import akka.actor.typed.PostStop;
import akka.actor.typed.TypedActorContext;
import akka.actor.typed.BehaviorInterceptor;     // ← همین یکی
import akka.actor.typed.javadsl.Behaviors;       // ← فقط برای intercept()

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public final class ActorMetricsInterceptor<T> extends BehaviorInterceptor<T, T> {
    private final String actorName;
    private final MeterRegistry reg;

    private final Counter started;
    private final Counter stopped;
    private final Counter failures;

    private final Map<String, Timer> latencyByMsg = new ConcurrentHashMap<>();
    private final Map<String, Counter> processedByMsg = new ConcurrentHashMap<>();
    private final Map<String, Counter> errorsByMsg = new ConcurrentHashMap<>();


    @SuppressWarnings("unchecked")
    public static <T> Behavior<T> wrapAll(String actorName, Behavior<T> behavior) {
        return Behaviors.intercept(() -> new ActorMetricsInterceptor<>(actorName, (Class<T>)(Class<?>)Object.class), behavior);
    }


    public static <T> Behavior<T> wrap(String actorName, Class<T> msgClass, Behavior<T> behavior) {
        return Behaviors.intercept(() -> new ActorMetricsInterceptor<>(actorName, msgClass), behavior);
    }

    private ActorMetricsInterceptor(String actorName, Class<T> interceptClass) {
        super(interceptClass);
        this.actorName = actorName;
        this.reg = MetricsServer.registry();
        this.started  = Counter.builder("akka_actor_started_total").tag("actor", actorName).register(reg);
        this.stopped  = Counter.builder("akka_actor_stopped_total").tag("actor", actorName).register(reg);
        this.failures = Counter.builder("akka_actor_failures_total").tag("actor", actorName).register(reg);
    }

    @Override
    public Behavior<T> aroundStart(TypedActorContext<T> ctx,
                                   BehaviorInterceptor.PreStartTarget<T> target) {
        started.increment();
        return target.start(ctx);
    }

    @Override
    public Behavior<T> aroundReceive(TypedActorContext<T> ctx,
                                     T msg,
                                     BehaviorInterceptor.ReceiveTarget<T> target) {
        final String msgType = msg.getClass().getSimpleName();

        final Timer timer = latencyByMsg.computeIfAbsent(
                msgType,
                k -> Timer.builder("akka_actor_message_latency")
                        .tag("actor", actorName).tag("message", k)
                        .publishPercentileHistogram().register(reg)
        );
        final Counter processed = processedByMsg.computeIfAbsent(
                msgType,
                k -> Counter.builder("akka_actor_messages_processed_total")
                        .tag("actor", actorName).tag("message", k).register(reg)
        );
        final Counter errors = errorsByMsg.computeIfAbsent(
                msgType,
                k -> Counter.builder("akka_actor_message_errors_total")
                        .tag("actor", actorName).tag("message", k).register(reg)
        );

        long begin = System.nanoTime();
        try {
            Behavior<T> next = target.apply(ctx, msg);
            timer.record(System.nanoTime() - begin, TimeUnit.NANOSECONDS);
            processed.increment();
            return next;
        } catch (Throwable t) {
            failures.increment();
            errors.increment();
            throw t;
        }
    }

    @Override
    public Behavior<T> aroundSignal(TypedActorContext<T> ctx,
                                    Signal signal,
                                    BehaviorInterceptor.SignalTarget<T> target) {
        if (signal instanceof PostStop) {
            stopped.increment();
        }
        return target.apply(ctx, signal);
    }
}
