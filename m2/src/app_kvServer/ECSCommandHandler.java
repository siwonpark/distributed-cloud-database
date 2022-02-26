package app_kvServer;

import org.apache.log4j.Logger;
import shared.ZKData;
import shared.ZKData.OperationType;
import shared.messages.KVMessage;

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

    public void handleCommand(ZKData data){
        OperationType op = data.getOperationType();

        switch(op){
            case INIT:
                server.update(data.getMetadata());
            case START:
                server.startServer();
            case STOP:
                server.stopServer();
            case SHUT_DOWN:
                server.shutDown();
            case METADATA:
                server.update(data.getMetadata());
            case LOCK_WRITE:
                server.lockWrite();
            case UNLOCK_WRITE:
                server.unlockWrite();
            case MOVE_DATA:
                server.moveData(data.getMoveRange(), data.getTargetNode());
            default:
                String errorMsg = "Request contained a status unknown to the server: " + op;
                logger.error(errorMsg);
                break;
        }
    }
}
