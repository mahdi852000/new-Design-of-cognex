package org.example.akka.extra;
import org.example.akka.message.Response;
import java.io.IOException;

public class DataManSystem {
    private SystemConnector conn;
    
    public DataManSystem(SystemConnector conn) {this.conn=conn;}
    public boolean connected() {return conn.connected();}
    public boolean connect(){return conn.connect();}

    public void addListener(SystemConnector.Listener listener) {
        conn.addListener(listener);
    }

    public void removeListener(SystemConnector.Listener listener) {conn.removeListener(listener);
    }
    
    public Response sendCommand(String command, Integer id, boolean useCheckSum) throws IOException {
    if(!this.connected()) {
        System.out.println("sendCommand: not connected, returning NoResponse");
        return new Response.NoResponse();
    }else {
        Response r = conn.send(new Request(command).id(id).useCheckSum(useCheckSum));
        System.out.println("sendCommand: got from conn = " + r);
        return r != null ? r : new Response.NoResponse();
        }
    }
    public Response sendCommand(String command) throws IOException {
        return sendCommand(command,null, false);
    }
    public boolean disconnect() {return conn.disconnect();}
}
