package app_kvServer;

import ecs.ECSNode;
import org.apache.log4j.Logger;
import shared.communication.CommModule;
import shared.messages.KVMessage;
import shared.messages.Message;

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
        commModule = new CommModule(dest.getNodeHost(), dest.getNodePort());
        try {
            commModule.connect();
        } catch (IOException e) {
            logger.error(String.format("Could not connect to destination server to perform replication key %s, value %s and sequence %d to server port %s server name: %s\n the error is %s", 
            rpmsg.key, rpmsg.value, rpmsg.sequence, dest.getNodePort(), dest.getNodeName(), e));
            return;
        }
        
        
        KVMessage.StatusType msgType;
        switch(rpmsg.type){
            case REPLICATE_MIDDLE_REPLICA:
                logger.info(String.format("send an REPLICATE_MIDDLE_REPLICA message to server %s, k: %s, v: %s, seq: %d", dest.getNodeName(), rpmsg.key, rpmsg.value, rpmsg.sequence));
                msgType = KVMessage.StatusType.REPLICATE_TO_MIDDLE_REPLICA;
                break;
            case REPLICATE_TAIL:
                logger.info(String.format("send an REPLICATE_TAIL message to server %s, k: %s, v: %s, seq: %d", dest.getNodeName(), rpmsg.key, rpmsg.value, rpmsg.sequence));
                msgType = KVMessage.StatusType.REPLICATE_TO_TAIL;
                break;
            case ACK_FROM_MIDDLE_REPLICA:
                logger.info(String.format("send an ACK_FROM_MIDDLE_REPLICA message to server %s, k: %s, v: %s, seq: %d", dest.getNodeName(), rpmsg.key, rpmsg.value, rpmsg.sequence));
                msgType = KVMessage.StatusType.REPLICATION_ACK_FROM_MIDDLE_REPLICA;
                break;
            case ACK_FROM_TAIL:
                logger.info(String.format("send an ACK_FROM_TAIL message to server %s, k: %s, v: %s, seq: %d", dest.getNodeName(), rpmsg.key, rpmsg.value, rpmsg.sequence));
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
            if (response.getStatus() != KVMessage.StatusType.REPLICATION_MESSAGE_SEND) {
                logger.error(String.format("Unable to send replication message: key %s, value %s and sequence %d to server port %s", rpmsg.key, rpmsg.value, rpmsg.sequence, dest.getNodePort()));
            }
        } catch (IOException e) {
            logger.error(String.format("Could not connect to destination server to perform replication key %s, value %s and sequence %d to server port %s server name: %s\n the error is %s", 
            rpmsg.key, rpmsg.value, rpmsg.sequence, dest.getNodePort(), dest.getNodeName(), e));
        }

        try {
            commModule.disconnect();
        } catch (IOException e) {
            logger.error("Could not disconnect server");
        }
    }
}

