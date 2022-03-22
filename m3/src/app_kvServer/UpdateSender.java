package app_kvServer;

import ecs.ECSNode;
import org.apache.log4j.Logger;
import shared.communication.CommModule;
import shared.messages.KVMessage;
import shared.messages.Message;
import app_kvServer.KVServer.Update;
import app_kvServer.KVServer.Update.UpdateType;

import java.io.IOException;
import java.util.ArrayList;

public class UpdateSender implements Runnable {
    private Logger logger = Logger.getRootLogger();

    private CommModule commModule;
    private ECSNode dest;
    private Update update;
    public UpdateSender(ECSNode dest ,Update update) {
        this.dest = dest;
    }

    @Override
    public void run() {
        boolean sendSuccess = true;
        commModule = new CommModule(dest.getNodeHost(), dest.getNodePort());
        try {
            commModule.connect();
        } catch (IOException e) {
            logger.error("Could not connect to destination server to perform replication");
            return;
        }
        
        Message msg;
        if(update.type == UpdateType.MIDDLE){
            msg = new Message(update.key, update.value, update.sequence, KVMessage.StatusType.REPLICATION_MIDDLE);
        }else if(update.type == UpdateType.TAIL){
            msg = new Message(update.key, update.value, update.sequence, KVMessage.StatusType.REPLICATION_TAIL);
        }else{
            logger.error("Wrong Update Type");
            return;
        }


        try {
            commModule.sendMessage(msg);
            Message response = commModule.receiveMessage();
            if (response.getStatus() != KVMessage.StatusType.PUT_SUCCESS) {
                logger.error(String.format("Unable to replication key %s and value %s to server port %s", update.key, update.value, dest.getNodePort()));
                sendSuccess = false;
            }
        } catch (IOException e) {
            logger.error("Could not connect to destination server to perform replication");
            sendSuccess = false;
        }
        if(sendSuccess){
            
        }   
    }
}

