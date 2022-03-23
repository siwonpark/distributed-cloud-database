package app_kvServer;

import ecs.ECSNode;
import org.apache.log4j.Logger;
import shared.communication.CommModule;
import shared.messages.KVMessage;
import shared.messages.Message;
import app_kvServer.KVServer.ReplicationMsg;

import java.io.IOException;

public class ReplicationMsgSender implements Runnable {
    private Logger logger = Logger.getRootLogger();

    private CommModule commModule;
    private ECSNode dest;
    private ReplicationMsg rpmsg;
    public ReplicationMsgSender(ECSNode dest ,ReplicationMsg rpmsg) {
        this.dest = dest;
        this.rpmsg = rpmsg;
    }

    @Override
    public void run() {
        boolean sendSuccess = false;
        while(!sendSuccess){//have to retry until success 
            sendSuccess = true;
            commModule = new CommModule(dest.getNodeHost(), dest.getNodePort());
            try {
                commModule.connect();
            } catch (IOException e) {
                logger.error("Could not connect to destination server to perform replication");
                return;
            }
            
            
            KVMessage.StatusType msgType;
            switch(rpmsg.type){
                case REPLICATE_MIDDLE_REPLICA:
                    msgType = KVMessage.StatusType.REPLICATE_TO_MIDDLE_REPLICA;
                    break;
                case REPLICATE_TAIL:
                    msgType = KVMessage.StatusType.REPLICATE_TO_TAIL;
                    break;
                case ACK_FROM_MIDDLE_REPLICA:
                    msgType = KVMessage.StatusType.REPLICATION_ACK_FROM_MIDDLE_REPLICA;
                    break;
                case ACK_FROM_TAIL:
                    msgType = KVMessage.StatusType.REPLICATION_ACK_FROM_TAIL;
                    break;
                default:
                    logger.error("Wrong Replication Message Type");
                    return;
            }
            Message msg = new Message(rpmsg.key, rpmsg.value, rpmsg.sequence, msgType);            

            try {
                commModule.sendMessage(msg);
                Message response = commModule.receiveMessage();
                if (response.getStatus() != KVMessage.StatusType.REPLICATION_MESSAGE_SENDED) {
                    logger.error(String.format("Unable to send replication message: key %s and value %s to server port %s", rpmsg.key, rpmsg.value, dest.getNodePort()));
                    sendSuccess = false;
                }
            } catch (IOException e) {
                logger.error("Could not connect to destination server to perform replication");
                sendSuccess = false;
            }
        }
    }
}

