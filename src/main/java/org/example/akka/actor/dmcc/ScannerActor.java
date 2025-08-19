package org.example.akka.actor.dmcc;
import akka.actor.typed.Terminated;
import akka.actor.typed.javadsl.*;
import org.example.akka.config.RangeObserverConfig;
import org.example.akka.config.ScannerActorConfig;
import org.example.akka.message.RangeObserverCommand;
import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import net.enilink.komma.core.IReference;
import org.example.akka.extra.*;
import org.example.akka.message.*;
import org.example.akka.message.Response;
import org.example.akka.metrics.Metrics;
import org.example.akka.necessary.BehaviorDelegateResponse;
import org.example.akka.necessary.GetBehaviorDelegate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.example.akka.extra.TcpConnector;
import org.example.akka.utils.ScannerUtils;
import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

public class ScannerActor extends AbstractBehavior<ScannerCommand> implements Behaviour<IResource>, TcpConnector{

    protected static final Logger logger = LoggerFactory.getLogger(ScannerActor.class);
    private final ScannerActorConfig config;
    private int cmId = 0;
    private DataManSystem dmcc;
    boolean heartbeat = false;
    private Boolean occupation = Boolean.FALSE;
    private SystemConnector.Listener listener;
    private final IResource delegate;
    private ActorRef<RangeObserverCommand> rangeObserverActor;
    private boolean isRangeObserving = false;
    private enum ObservingSource { NONE, EXTERNAL, INTERNAL }
    private ObservingSource observingSource = ObservingSource.NONE;
    private boolean connected = false;
    private Collection<ScannerEventListener> listeners = new CopyOnWriteArrayList<>();
    boolean useCheckSum = false;
    public final ActorRef<CognexCommand> cognexActor;
    private boolean triggeredWhileOccupied = false;
    private ScannerCommand.Mode mode = ScannerCommand.Mode.MANUAL;
    private ActorRef<RangeObserverCommand> observerRef = null;
    final ActorRef<Metrics.Event> metrics;
    long lastCommandStartNs = -1;
    private enum ConnectionState {
        DISCONNECTED,
        CONNECTING,
        CONNECTED,
        RECONNECTING
    }

    private ConnectionState connectionState = ConnectionState.DISCONNECTED;
    private int retryCount = 0;
    private static final int MAX_RETRIES = 5;
    private static final Duration RETRY_INTERVAL = Duration.ofSeconds(2);


    public ScannerActor(ActorContext<ScannerCommand> context, ScannerActorConfig config,
                        ActorRef<CognexCommand> cognexActor, ActorRef<Metrics.Event> metrics) {
        super(context);
        this.config=config;
        this.dmcc = config.dmcc;
        this.metrics = metrics;
        this.heartbeat = false;
        this.occupation = false;
        this.listener = config.listener;
        this.delegate=config.delegate;
        this.connected=false;
        this.isRangeObserving=false;
        this.listeners= new CopyOnWriteArrayList<>();
        this.cognexActor = cognexActor;

    }
        public static Behavior<ScannerCommand> create(ScannerActorConfig config)   {
            return Behaviors.setup(ctx->
                    new ScannerActor(ctx,config, config.cognexActor,config.metricsRef));
        }

    private Behavior<ScannerCommand> onGetBehaviorDelegate(GetBehaviorDelegate msg) {
        Object delegate = ((Behaviour<IResource>) this).getBehaviourDelegate();
        msg.replyTo.tell(new BehaviorDelegateResponse(delegate));
        return this;
    }

    private ReceiveBuilder<ScannerCommand> connectionHandlers(ReceiveBuilder<ScannerCommand> builder) {
        return builder
                .onMessage(ScannerCommand.Connect.class, this::onConnect)
                .onMessage(ScannerCommand.OnDisconnect.class, this::onOnDisconnect)
                .onMessage(ScannerCommand.OnConnect.class, this::onOnConnect)
                .onMessage(ScannerCommand.IsConnected.class, this::onIsConnected)
                .onMessage(ScannerCommand.IsConnectedToDMCC.class, this::onIsConnectedToDMCC)
                .onMessage(ScannerCommand.QueryIsConnected.class, this::onQueryIsConnected)
                .onMessage(ScannerCommand.Disconnect.class, this::onDisconnect);
    }

    private ReceiveBuilder<ScannerCommand> scanHandlers(ReceiveBuilder<ScannerCommand> builder) {
        return builder
                .onMessage(ScannerCommand.Start.class, this::onStart)
                .onMessage(ScannerCommand.Stop.class, this::onStop)
                .onMessage(ScannerCommand.SendTrigger.class, this::onSendTrigger)
                .onMessage(ScannerCommand.TriggerScan.class, this::onTriggerScan)
                .onMessage(ScannerCommand.OnMessage.class, this::onOnMessage);
    }

    private ReceiveBuilder<ScannerCommand> listenerHandlers(ReceiveBuilder<ScannerCommand> builder) {
        return builder
                .onMessage(ScannerCommand.RegisterEventListener.class, this::onRegisterEventListener)
                .onMessage(ScannerCommand.UnregisterEventListener.class, this::onUnregisterEventListener);
    }

    private ReceiveBuilder<ScannerCommand> occupationHandlers(ReceiveBuilder<ScannerCommand> builder) {
        return builder
                .onMessage(ScannerCommand.SetOccupation.class, this::onSetOccupation)
                .onMessage(ScannerCommand.GetOccupation.class, this::onGetOccupation)
                .onMessage(ScannerCommand.QueryOccupation.class, this::onQueryOccupation);
    }

    private ReceiveBuilder<ScannerCommand> miscHandlers(ReceiveBuilder<ScannerCommand> builder) {
        return builder
                .onMessage(ScannerCommand.Enqueue.class, this::onEnqueue)
                .onMessage(GetBehaviorDelegate.class, this::onGetBehaviorDelegate);
    }
    private ReceiveBuilder<ScannerCommand> modeHandler(ReceiveBuilder<ScannerCommand> builder) {
        return builder
                .onMessage(ScannerCommand.RegisterObserver.class, msg -> {
                    this.observerRef = msg.ref();
                    getContext().getLog().info("RangeObserver registered: {}", observerRef);
                    return this;
                })
                .onMessage(ScannerCommand.SwitchMode.class, this::onSwitchMode)
                .onMessage(ScannerCommand.ToggleMode.class, m ->
                        onSwitchMode(new ScannerCommand.SwitchMode(
                                mode == ScannerCommand.Mode.AUTO ? ScannerCommand.Mode.MANUAL : ScannerCommand.Mode.AUTO)))
                .onMessage(ScannerCommand.QueryMode.class, q -> {
                    q.replyTo().tell(new ScannerCommand.ModeStatus(mode));
                    return this;
                })
                .onMessage(ScannerCommand.ManualTriggerScan.class, m ->
                        (mode == ScannerCommand.Mode.MANUAL)
                                ? onTriggerScan(new ScannerCommand.TriggerScan())
                                : this)
                .onMessage(ScannerCommand.ManualSetOccupation.class, m ->
                        (mode == ScannerCommand.Mode.MANUAL)
                                ? onSetOccupation(new ScannerCommand.SetOccupation(m.occupied()))
                                : this);
    }
    @Override
    public Receive<ScannerCommand> createReceive() {
        ReceiveBuilder<ScannerCommand> builder = newReceiveBuilder();
        connectionHandlers(builder);
        scanHandlers(builder);
        listenerHandlers(builder);
        occupationHandlers(builder);
        miscHandlers(builder);
        modeHandler(builder);
        builder.onSignal(Terminated.class, this::onTerminated);
        return builder.build();
    }
    private void startObserver() {
        if (isRangeObserving) return;
        if (observerRef != null) {
            observerRef.tell(new RangeObserverCommand.StartObserving());
            observingSource = ObservingSource.EXTERNAL;
            isRangeObserving = true;
            getContext().getLog().info("RangeObserver (observerRef) observing started");
        } else if (rangeObserverActor != null) {
            rangeObserverActor.tell(new RangeObserverCommand.StartObserving());
            observingSource = ObservingSource.INTERNAL;
            isRangeObserving = true;
            getContext().getLog().info("RangeObserverActor observing started");
        } else {
            getContext().getLog().info("No observer to start (external/internal not set)");
        }
    }
    private void stopObserver() {
        if (!isRangeObserving) return;
        switch (observingSource) {
            case EXTERNAL -> { if (observerRef != null) observerRef.tell(new RangeObserverCommand.StopObserving()); }
            case INTERNAL -> { if (rangeObserverActor != null) rangeObserverActor.tell(new RangeObserverCommand.StopObserving()); }
            case NONE     -> {}
        }
        isRangeObserving = false;
        observingSource = ObservingSource.NONE;
        getContext().getLog().info("Observing stopped");
    }
    private Behavior<ScannerCommand> onSwitchMode(ScannerCommand.SwitchMode msg) {
        if (this.mode == msg.mode()) return this;

        this.mode = msg.mode();
        getContext().getLog().info("Mode -> {}", this.mode);

        triggeredWhileOccupied = false;

        if (this.mode == ScannerCommand.Mode.AUTO) {
            if (!isRangeObserving) startObserver();
        } else {
            stopObserver();
        }
        return this;
    }
   /* private Behavior<ScannerCommand> onSwitchMode(ScannerCommand.SwitchMode msg) {
        if (msg.mode() == ScannerCommand.Mode.AUTO) {
            if (mode != ScannerCommand.Mode.AUTO) {
                mode = ScannerCommand.Mode.AUTO;
                if (observerRef != null) observerRef.tell(new RangeObserverCommand.StartObserving());
                getContext().getLog().info("Mode -> AUTO");
            }
        } else {
            if (mode != ScannerCommand.Mode.MANUAL) {
                mode = ScannerCommand.Mode.MANUAL;
                if (observerRef != null) observerRef.tell(new RangeObserverCommand.StopObserving());
                getContext().getLog().info("Mode -> MANUAL");
            }
        }
        return this;
    }*/
   private Behavior<ScannerCommand> onTriggerScan(ScannerCommand.TriggerScan msg) {
       logger.info("Scanner triggered to scan.");

       long ts = System.currentTimeMillis();
       if (metrics != null) metrics.tell(new Metrics.Trigger(false, ts));

       if (config.scanReceiver != null) {
           config.scanReceiver.tell("SCAN_CODE_FROM_ACTOR");
       }


       if (mode == ScannerCommand.Mode.MANUAL) {
           return Behaviors.same();
       }

       if (connectionState == ConnectionState.CONNECTED && !isRangeObserving) {
           if (rangeObserverActor != null) {
               rangeObserverActor.tell(new RangeObserverCommand.StartObserving());
               isRangeObserving = true;
               logger.info("RangeObserverActor observing started (auto-resume by trigger)");
           } else {
               Optional<Long> rangeMax = ScannerUtils.getProperty(delegate, Long.class, "triggerRangeMax");
               Optional<Long> rangeMin = ScannerUtils.getProperty(delegate, Long.class, "triggerRangeMin");
               Optional<Long> rangeOff = ScannerUtils.getProperty(delegate, Long.class, "triggerRangeOff");
               if (rangeMin.isPresent() && rangeMax.isPresent() && rangeOff.isPresent()) {
                   RangeObserverConfig rangeConfig = new RangeObserverConfig(
                           dmcc, cmId, rangeMin.get(), rangeMax.get(), rangeOff.get(),
                           getContext().getSelf(),
                           ((net.enilink.komma.core.IReference) delegate.getReference()).getURI().toString(),
                           config.host, config.port, config.scanReceiver, this.metrics
                   );
                   rangeObserverActor = getContext().spawn(RangeObserverActor.create(rangeConfig), "rangeObserver-" + cmId);
                   getContext().watch(rangeObserverActor);
                   rangeObserverActor.tell(new RangeObserverCommand.StartObserving());
                   isRangeObserving = true;
                   logger.info("RangeObserverActor created & observing started (auto-resume by trigger)");
               } else {
                   logger.info("Auto-resume skipped: range config not present.");
               }
           }
       }
       return Behaviors.same();
   }

    private Behavior<ScannerCommand>onQueryOccupation(ScannerCommand.QueryOccupation msg) {
        boolean occ= Boolean.TRUE.equals(this.occupation);
        msg.replyTo().tell(new ScannerCommand.OccupationStatus(occupation));// This has been created for TEST purpose
        getContext().getLog().debug("📥 [ScannerActor] Received QueryOccupation, responding with {}", occupation);
        return this;
    }
    private Behavior<ScannerCommand> onDisconnect(ScannerCommand.Disconnect msg) {
        if (rangeObserverActor != null && isRangeObserving) {
            rangeObserverActor.tell(new RangeObserverCommand.StopObserving());
            isRangeObserving = false;
            observingSource = ObservingSource.NONE;
            getContext().getLog().info("RangeObserverActor stopped due to disconnect");
        }
        triggeredWhileOccupied = false;
        occupation = false;
        if (listener != null) {
            try { listener.onOccupationChanged(false); }
            catch (Throwable ignore) {}
        }
        if (dmcc != null) {
            try { if (listener != null) dmcc.removeListener(listener); }
            catch (Throwable t) { getContext().getLog().warn("Failed to remove listener: {}", t.toString()); }
            try { if (dmcc.connected()) dmcc.disconnect(); }
            catch (Throwable t) { getContext().getLog().warn("Failed to disconnect DMCC: {}", t.toString()); }
        }
        connected = false;
        connectionState = ConnectionState.DISCONNECTED;
        retryCount = 0;
        cognexActor.tell(new CognexCommand.Disconnect());
        getContext().getLog().info("DMCC disconnected; state -> DISCONNECTED");
        return this;
    }
    /*   private Behavior<ScannerCommand> onDisconnect(ScannerCommand.Disconnect msg) {

        if (rangeObserverActor != null && isRangeObserving) {
            rangeObserverActor.tell(new RangeObserverCommand.StopObserving());
            isRangeObserving = false;
            getContext().getLog().info("RangeObserverActor stopped due to disconnect");
        }
        if (dmcc != null) {
            try {
                if (listener != null) dmcc.removeListener(listener);
            } catch (Throwable t) {
                getContext().getLog().warn("Failed to remove listener: {}", t.toString());
            }
            try {
                if (dmcc.connected()) dmcc.disconnect();
            } catch (Throwable t) {
                getContext().getLog().warn("Failed to disconnect DMCC: {}", t.toString());
            }
        }
        connected = false;
        connectionState = ConnectionState.DISCONNECTED;
        retryCount = 0;
        cognexActor.tell(new CognexCommand.Disconnect());
        getContext().getLog().info("DMCC disconnected; state -> DISCONNECTED");
        return this;
    }*/
    private Behavior <ScannerCommand> onGetOccupation (ScannerCommand.GetOccupation msg) {
        getContext().getLog().info("Check being Occupied");
        boolean isOccupied =  occupation !=null && occupation;
        getContext().getLog().info("Current occupation status: {}", isOccupied);
        return this;
    }
    private Behavior<ScannerCommand> onSetOccupation(ScannerCommand.SetOccupation msg) {
        boolean occupied = msg.occupied();

        if (occupation == null || occupied != occupation) {
            occupation =occupied;
            listener.onOccupationChanged(occupied);

            if (occupied && connected && mode == ScannerCommand.Mode.AUTO && !triggeredWhileOccupied) {
                getContext().getSelf().tell(new ScannerCommand.TriggerScan());
                triggeredWhileOccupied = true;
            }
            //Can be removed
           /*
            if (occupied && connected && !triggeredWhileOccupied) {
                getContext().getSelf().tell(new ScannerCommand.TriggerScan());
                triggeredWhileOccupied = true;
            }*/
            if (!occupied) {
                triggeredWhileOccupied = false;
            }
        }
        return this;
    }
    private Behavior<ScannerCommand> onOnDisconnect(ScannerCommand.OnDisconnect msg) {
        getContext().getLog().info("Handling OnDisconnect. Current state: {}", connectionState);

        if (rangeObserverActor != null && isRangeObserving) {
            rangeObserverActor.tell(new RangeObserverCommand.StopObserving());
            isRangeObserving = false;
            observingSource = ObservingSource.NONE;

        }

        if (dmcc != null && dmcc.connected()) {
            dmcc.disconnect();
            connected = false;
            getContext().getLog().info("Disconnected from DMCC.");
        }

        connectionState = ConnectionState.RECONNECTING;
        retryCount = 1;
        scheduleReconnect();
        return this;
    }
    private Behavior<ScannerCommand> onConnect (ScannerCommand.Connect msg) {
        getContext().getLog().info("onConnect() called. Current state: {}", connectionState);
        if(connectionState == ConnectionState.CONNECTED) {
            getContext().getLog().info("Already connected. Ignoring connect request.");
            return this;
        }
        if(connectionState==ConnectionState.DISCONNECTED){
            retryCount=0;
        }
        connectionState = ConnectionState.CONNECTING;
        getContext().getLog().info("Attempting to connect...");
        try {
            dmcc.connect();
            if (dmcc.connected()) {
                connectionState = ConnectionState.CONNECTED;
                this.connected = true;
                getContext().getLog().info("Connected successfully.");
                cognexActor.tell(new CognexCommand.Connect());
            } else {
                connectionState = ConnectionState.RECONNECTING;
                retryCount = 1;
                getContext().getLog().warn("Initial connection failed. Will retry...");
                scheduleReconnect();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return this;
    }
    private Behavior<ScannerCommand> onOnMessage (ScannerCommand.OnMessage msg) {
        Response response = msg.response();
        getContext().getLog().info("some Messages 2");
        return Behaviors.same();
    }
    private Behavior<ScannerCommand> onEnqueue (ScannerCommand.Enqueue msg) {
        IDTO dto = msg.dto();
        ActorRef<Boolean> replyTo = msg.replyTo();//(What is supposed to do, apparently nothing here)
        getContext().getLog().info("Received Enqueue command with DTO: {}", dto);
        replyTo.tell(true);
        getContext().getLog().info("Enqueue result sent: true");
        return Behaviors.same();
    }
    private Behavior<ScannerCommand> onIsConnectedToDMCC (ScannerCommand.IsConnectedToDMCC msg){
        this.connected=msg.value();
        if(connected){
            getContext().getLog().info(" DMCC connection is OK.");
        } else  {
            getContext().getLog().info(" DMCC connection FAILED.");
        }
        return this;
    }
    private Behavior<ScannerCommand> onIsConnected (ScannerCommand.IsConnected msg) {
        this.connected = msg.value();
        if(connected){
            logger.info("Connection is Ok");
        } else {
            logger.warn("Connection failed");
        }
        return this;
    }
    private Behavior<ScannerCommand> onRegisterEventListener (ScannerCommand.RegisterEventListener msg) {
        ScannerCommand.ScannerEventListener listener = msg.listener();
        if(null!=listener){
            listeners.add((ScannerEventListener) listener);
            getContext().getLog().info("Listener registered: {}", listener);
        }
        return this;
    }
    private Behavior<ScannerCommand> onUnregisterEventListener (ScannerCommand.UnregisterEventListener msg) {

        ScannerCommand.ScannerEventListener listener = msg.listener();
        if (listener != null) {
            listeners.remove(listener);
            getContext().getLog().info("Listener unregistered: {}", listener);
        }
        return this;
    }
    private Behavior<ScannerCommand> onTerminated(Terminated sig) {
        if (sig.getRef().equals(rangeObserverActor)) {
            logger.warn("rangeObserverActor is terminated!");
            rangeObserverActor = null;
            isRangeObserving = false;
        }
        return this;
    }
    private Behavior<ScannerCommand> onStop(ScannerCommand.Stop msg) {
        if (observerRef != null) {
            observerRef.tell(new RangeObserverCommand.StopObserving());
           // isRangeObserving = false;
            logger.info("RangeObserver (observerRef) stopped observing");
        }
        if (rangeObserverActor != null && isRangeObserving) {
            rangeObserverActor.tell(new RangeObserverCommand.StopObserving());
           // isRangeObserving = false;
            logger.info("RangeObserverActor stopped observing");
        } else {
            logger.info("RangeObserverActor not observing; stop ignored");
        }
        isRangeObserving = false;
        observingSource = ObservingSource.NONE;
        return Behaviors.same();
    }
    private Behavior<ScannerCommand> onSendTrigger (ScannerCommand.SendTrigger msg) {
        if(connected){
            getContext().getLog().info("DMCC is connected, sending trigger...");
        } else {
            getContext().getLog().info("DMCC is NOT connected. Trigger skipped.");
        }
        return this;
    }
    private Behavior<ScannerCommand> onOnConnect (ScannerCommand.OnConnect msg) {
        if(dmcc.connected()) return this;
        getContext().getLog().info("DMCC is connected");

        String uri =((IReference) getBehaviourDelegate()).getURI().toString();
        heartbeat = Boolean.TRUE.equals(org.example.akka.utils.ScannerUtils.getProperty
                (delegate,Boolean.class , "heartbeat"));
        if(!dmcc.connected()) {
                if(config.isExternalDmcc){
                    getContext().getLog().info("External DMCC injected, skipping override.");
                } else {
                    TcpSystemConnector conn = new TcpSystemConnector(host(), port()).useHeartBeat(heartbeat);
                    dmcc = new DataManSystem(conn);
                    getContext().getLog().info("dmcc instance is: {}", dmcc.getClass());
                }
            }
        listener = new SystemConnector.Listener() {
            @Override
            public void onMessage(Response response) {
                String code = response.result();
                logger.info("gateway-scan got code={} at source{}", code, uri);
                config.cognexActor.tell(new CognexCommand.NotifyScannedCode(getBehaviourDelegate(),code));
                listeners.forEach(l ->
                        l.onCodeScanned(getBehaviourDelegate(),code));
            }
            @Override
            public void onConnect() {
                try {
                    logger.info("gateway-scan connected source={}", uri);
                    dmcc.sendCommand("SET COM.DMCC-RESPONSE 1", cmId++, useCheckSum );
                } catch (IOException ioe) {
                    ioe.printStackTrace();
                }
            }
            @Override
            public void onDisconnect() {
                logger.info("gateway-scan disconnected source={}", uri);
                if(null!=rangeObserverActor) {
                    rangeObserverActor.tell(new RangeObserverCommand.StopObserving());
                    isRangeObserving=false;
                    observingSource = ObservingSource.NONE;

                    logger.info("Sent StopObserving to RangeObserverActor");
                };
                if(null!=dmcc){
                    dmcc.connect();
                }
            }
            @Override
            public void onOccupationChanged(boolean occupied) {

            }
        };
        dmcc.addListener(listener);
        dmcc.connect();
        this.connected=true;
        return this;
    }
    private Behavior<ScannerCommand> onQueryIsConnected(ScannerCommand.QueryIsConnected msg) {
        boolean status = dmcc != null && dmcc.connected();
        msg.replyTo().tell(new ScannerCommand.ConnectedStatus(status));
        return this;
    }
    // BUG: QueryIsConnected replies with 'this.connected'
    //instead of the freshly computed 'status'.
    //If 'this.connected' is out of sync with the actual DMCC link, the response can be stale/incorrect.
    // FIX: reply with the computed 'status' (dmcc != null && dmcc.connected()).
    private Behavior<ScannerCommand>onStart(ScannerCommand.Start msg) {
        logger.info("Starting " + getBehaviourDelegate());
        //((IReference) getBehaviourDelegate()).getURI().toString();
        IReference ref = ((IReference) getBehaviourDelegate() instanceof IReference) ? (IReference)
        getBehaviourDelegate() : null;
        String uri = (ref != null && ref.getURI() != null) ? ref.getURI().toString() : "UNKNOWN";
        logger.info("Starting DMCC scanner at URI= {} host = {} port= {}", uri, host(), port());
        logger.info("Starting observation for scanner URI = {}", uri);
        try {   
            Response r = dmcc.sendCommand("UPTIME");
            if (r instanceof Response.NoResponse) {
                logger.warn("No response from UPTIME, triggering disconnect");
                getContext().getSelf().tell(new ScannerCommand.OnDisconnect());
                getContext().getSelf().tell(new ScannerCommand.Connect());
                return this;
            }
            logger.info("UPTIME response = {}", r);
            if (observerRef != null) {
                if (!isRangeObserving) {
                    triggeredWhileOccupied = false;
                    observerRef.tell(new RangeObserverCommand.StartObserving());
                    isRangeObserving = true;
                    observingSource = ObservingSource.EXTERNAL;
                    logger.info("RangeObserver (observerRef) observing started");
                } else {
                    logger.info("RangeObserver (observerRef) already observing; start ignored");
                }
                return this;
            }
            Optional <Long> rangeMax = ScannerUtils.getProperty(delegate,Long.class, "triggerRangeMax");
            Optional <Long> rangeMin = ScannerUtils.getProperty(delegate,Long.class, "triggerRangeMin");
            Optional <Long> rangeOff = ScannerUtils.getProperty(delegate,Long.class, "triggerRangeOff");
            boolean checkRange = rangeMin.isPresent() && rangeMax.isPresent() && rangeOff.isPresent();
            if (!checkRange) {
                logger.info("Range check not configured");
                return this;
            }
            if (rangeObserverActor != null) {
                if (!isRangeObserving) {
                    triggeredWhileOccupied = false;
                    rangeObserverActor.tell(new RangeObserverCommand.StartObserving());
                    isRangeObserving = true;
                    logger.info("RangeObserverActor observing started (reused)");
                } else {
                    logger.info("RangeObserverActor already observing; start ignored");
                }
                return this;
            }
            // BUG: Optional variables (rangeMin/rangeMax/rangeOff) are checked against null.
            // Optionals are never null, so this condition is always true and may start range
            // observation with missing config.
            // FIX: use .isPresent() on each Optional and proceed only when all are present.
            Optional<RangeObserverConfig> rangeConfigOpt =
                    rangeMin.flatMap(min ->
                            rangeMax.flatMap(max ->
                                    rangeOff.map(off -> new RangeObserverConfig(
                                            dmcc, cmId, min, max, off,
                                            getContext().getSelf(), uri, config.host, config.port, config.scanReceiver,
                                            this.metrics
                                    ))
                            )
                    );

            if (rangeConfigOpt.isPresent()) {
                RangeObserverConfig rangeConfig = rangeConfigOpt.get();
                rangeObserverActor = getContext().spawn(
                        RangeObserverActor.create(rangeConfig),
                        "rangeObserver-" + cmId
                );
                getContext().watch(rangeObserverActor);
                logger.info("RangeObserverActor created");

                triggeredWhileOccupied = false;

                rangeObserverActor.tell(new RangeObserverCommand.StartObserving());
                isRangeObserving = true;
                logger.info("RangeObserverActor observing started");
            } else {
                logger.warn("Range observation skipped — one or more range properties were missing.");
            }
        } catch (Throwable t) {
            logger.error("Failed to start:", t);
        }
        return this;
    }
    private void scheduleReconnect() {
        if(retryCount>MAX_RETRIES) {
            getContext().getLog().warn("Max reconnect attempts reached. Switching to DISCONNECTED.");
            connectionState=ConnectionState.DISCONNECTED;
            return;
        }
        getContext().getSystem().scheduler().scheduleOnce(
                RETRY_INTERVAL,
                ()->getContext().getSelf().tell(new ScannerCommand.Connect()),
                        getContext().getSystem().executionContext());
        getContext().getLog().info("Scheduled reconnect attempt {} after {} seconds",
                retryCount, RETRY_INTERVAL.getSeconds());
                retryCount++;
    }
    @Override
    public IResource getBehaviourDelegate() {
        return this.delegate;
    }

    @Override
    public String host() {
        return this.config.host;
    }

    @Override
    public int port() {
        return this.config.port;
    }
}


