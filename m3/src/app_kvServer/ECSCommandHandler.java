package app_kvServer;

import org.apache.log4j.Logger;
import shared.KVAdminMessage;
import shared.KVAdminMessage.OperationType;

public class ECSCommandHandler {
    private KVServer server;
    private Logger logger = Logger.getRootLogger();

    /**
     * Instantiate an ECS command handler for KVServer server
     * @param server The server for which this handler is created
     */
    public ECSCommandHandler(KVServer server){
        this.server = server;
    }

    public void handleCommand(KVAdminMessage data){
        OperationType op = data.getOperationType();

        // ACKS to ECS are performed inside each individual call
        switch(op){
            case INIT:
                server.update(data.getMetadata());
                break;
            case START:
                server.startServer();
                break;
            case STOP:
                server.stopServer();
                break;
            case SHUT_DOWN:
                server.shutDown();
                break;
            case METADATA:
                server.update(data.getMetadata());
                break;
            case LOCK_WRITE:
                server.lockWrite();
                break;
            case UNLOCK_WRITE:
                server.unlockWrite();
                break;
            case MOVE_DATA:
                server.moveData(data.getMoveRange(), data.getTargetNode());
                break;
            case FORCE_CONSISTENCY:
                server.forceConsistency();
            default:
                String errorMsg = "Request contained a status unknown to the server: " + op;
                logger.error(errorMsg);
                break;
        }
    }
}
