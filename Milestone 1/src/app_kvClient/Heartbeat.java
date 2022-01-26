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
                sleep(3000);
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

    public void addListener(ClientSocketListener listener){
        listeners.add(listener);
    }

    public void stopProbing(){
        continueProbing = false;
    }

}
