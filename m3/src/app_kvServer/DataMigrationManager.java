package app_kvServer;


import ecs.ECSNode;
import org.apache.log4j.Logger;
import persistence.DataBase;
import shared.communication.CommModule;
import shared.messages.KVMessage;
import shared.messages.Message;

import java.io.IOException;
import java.util.ArrayList;
import static shared.PrintUtils.DELETE_STRING;

public class DataMigrationManager implements Runnable {
    private Logger logger = Logger.getRootLogger();

    // Used for server->server communication when moving data
    private CommModule commModule;
    private ECSNode destServer;
    private DataBase db;
    private String migrationHashRangeStart;
    private String migrationHashRangeEnd;
    private ZKWatcher zkWatcher;
    private boolean deleteOldKeys;


    public DataMigrationManager(ECSNode destServer, String[] migrationHashRange, DataBase db, ZKWatcher zkWatcher, boolean deleteOldKeys){
        this.destServer = destServer;
        this.migrationHashRangeStart = migrationHashRange[0];
        this.migrationHashRangeEnd = migrationHashRange[1];
        this.db = db;
        this.zkWatcher = zkWatcher;
        this.deleteOldKeys = deleteOldKeys;
    }

    @Override
    public void run() {
        boolean migrationSuccess = true;
        ArrayList<ArrayList<String>> dataToMigrate = db.getData(migrationHashRangeStart, migrationHashRangeEnd);
        if(destServer != null){
            commModule = new CommModule(destServer.getNodeHost(), destServer.getNodePort());
            try {
                commModule.connect();
            } catch (IOException e) {
                logger.error("Could not connect to destination server to perform data migration");
                return;
            }

            // Delete null values before performing migration
            db.batchDeleteNull();
            /*
            * An arraylist of the data to migrate.
            * At the inner level, ArrayList<String> is an array of two
            * elements, index 0 = key, index 1 = value
            */
            for (ArrayList<String> keyValue : dataToMigrate){
                String key = keyValue.get(0);
                String value = keyValue.get(1);
                Message msg = new Message(key, value, KVMessage.StatusType.DATA_MIGRATION);
                try {
                    commModule.sendMessage(msg);
                    Message response = commModule.receiveMessage();
                    if (response.getStatus() != KVMessage.StatusType.PUT_SUCCESS) {
                        logger.error(String.format("Unable to migrate key %s and value %s to server port %s",
                                key, value, this.destServer.getNodePort()));
                        migrationSuccess = false;
                    }
                } catch (IOException e) {
                    logger.error("Could not connect to destination server to perform data migration");
                    migrationSuccess = false;
                }
            }
        }

        if (migrationSuccess){
            if(destServer!=null){
                logger.info("Successfully migrated keys" + dataToMigrate);
            }else{
                logger.info("Begin deleting keys" + dataToMigrate);
            }
            // Delete all the keys that were migrated, since migration was successful
            if(deleteOldKeys || destServer == null){
                for (ArrayList<String> keyValue : dataToMigrate) {
                    String key = keyValue.get(0);
                    db.put(key, DELETE_STRING);
                 }
                 //We want to remove the keys that were migrated
                 db.batchDeleteNull();
            }
            // Send ACK to Zookeeper
            zkWatcher.setData();
        }
        // If the migration failed, then we do not send ACK.
    }
}
