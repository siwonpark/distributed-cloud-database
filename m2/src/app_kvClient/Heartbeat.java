package app_kvClient;

import client.KVStore;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static java.lang.Thread.sleep;

public class Heartbeat implements Runnable{

    private Logger logger = Logger.getRootLogger();
    private KVStore kvStore;
    private boolean continueProbing;
    private Set<ClientSocketListener> listeners;
    // How often should we send a heartbeat to the server?
    private final int HEARTBEAT_FREQ_MS = 2000;

    public Heartbeat(KVStore kvStore){
        this.kvStore = kvStore;
        this.continueProbing = true;
        listeners = new HashSet<ClientSocketListener>();


    }

    /**
     * Initializes and starts the client connection.
     * Loops until the connection is closed or aborted by the client.
     */
    public void run() {
        try {
            while(continueProbing) {
                kvStore.heartbeat();
                sleep(HEARTBEAT_FREQ_MS);
            }
        } catch (IOException ioe) {
            /* connection either terminated by the client or lost due to
             * network problems*/
            if (continueProbing) {
                for (ClientSocketListener listener : listeners) {
                    listener.handleStatus(ClientSocketListener.SocketStatus.CONNECTION_LOST);
                }
                logger.error("Error! Connection lost!");
            }
        } catch (InterruptedException e) {
            logger.warn("Heartbeat thread interrupted prematurely");
        }
    }

    /**
     * Add a listener to this heartbeat
     * @param listener The listener to add
     */
    public void addListener(ClientSocketListener listener){
        listeners.add(listener);
    }

    /**
     * Stop probing the server
     */
    public void stopProbing(){
        continueProbing = false;
    }

}
