package org.example.akka.extra;

import org.example.akka.extra.SystemConnector;
import org.example.akka.message.Response;

/**
 * No-operation implementation of SystemConnector.Listener.
 * Safe to use in production AppMain when no real listener is required.
 */
public class NoopListener implements SystemConnector.Listener {
    @Override public void onMessage(Response response) { }
    @Override public void onConnect() { }
    @Override public void onDisconnect() { }
    @Override public void onOccupationChanged(boolean occupied) { }
}
