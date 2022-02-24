package app_kvServer;


import ecs.ECSNode;
import org.apache.log4j.Logger;
import persistence.DataBase;
import shared.MetadataUtils;
import shared.communication.CommModule;
import shared.messages.KVMessage;
import shared.messages.Message;

import java.io.IOException;
import java.util.ArrayList;
import java.util.TreeMap;

public class DataMigrationManager implements Runnable {
    private Logger logger = Logger.getRootLogger();

    // Used for server->server communication when moving data
    private CommModule commModule;
    private String destServerName;
    private TreeMap<String, ECSNode> metadata;
    private DataBase db;
    private String migrationHashRangeStart;
    private String migrationHashRangeEnd;


    public DataMigrationManager(String destServerName, TreeMap<String, ECSNode> metadata,
                                String[] migrationHashRange, DataBase db){
        this.destServerName = destServerName;
        this.metadata = metadata;
        this.migrationHashRangeStart = migrationHashRange[0];
        this.migrationHashRangeEnd = migrationHashRange[1];
        this.db = db;
    }

    @Override
    public void run() {
        ECSNode destServer
                = MetadataUtils.getServerNode(destServerName, metadata);
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
        ArrayList<ArrayList<String>> dataToMigrate =
                db.getData(migrationHashRangeStart, migrationHashRangeEnd);

        boolean migrationSuccess = true;

        for (ArrayList<String> keyValue : dataToMigrate){
            String key = keyValue.get(0);
            String value = keyValue.get(1);
            Message msg = new Message(key, value, KVMessage.StatusType.DATA_MIGRATION);

            try {
                commModule.sendMessage(msg);
                Message response = commModule.receiveMessage();
                if (response.getStatus() != KVMessage.StatusType.PUT_SUCCESS) {
                    logger.error(String.format("Unable to migrate key %s and value %s to server %s",
                            key, value, destServerName));
                    migrationSuccess = false;
                } else {
                    db.put(key, null);
                }
            } catch (IOException e) {
                logger.error("Could not connect to destination server to perform data migration");
            } finally {
                // We want to remove the keys that were migrated
                db.batchDeleteNull();
            }
        }
        // TODO: send ACK to ECS
        if (migrationSuccess){
            // send ACK to ECS that moveData was successful
        } else{
            // error msg? Idk
        }
    }
}
