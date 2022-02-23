package ecs;

import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.Watcher.Event.EventType;
import shared.ZKData;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

public class ZKWatcher implements Watcher {
    private ZooKeeper zooKeeper;
    private Logger logger = Logger.getRootLogger();
    private String ROOT_PATH = "/ecs";

    public ZooKeeper connect() throws IOException {
        zooKeeper = new ZooKeeper("localhost:2181", 1000, this);

        try {
            create(ROOT_PATH, null);
        } catch (Exception e) {
            logger.error("Root znode was unable to be created");
        }
        return zooKeeper;
    }

    public void create(String path, ZKData data) throws InterruptedException, KeeperException, IOException {
        byte[] dataBytes = serializeData(data);

        zooKeeper.create(path, dataBytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    @Override
    public void process(WatchedEvent event) {
        logger.info("WATCHER NOTIFICATION!");
        if (event == null) {
            return;
        }

        // Get connection status
        Event.KeeperState keeperState = event.getState();
        // Event type
        EventType eventType = event.getType();
        // Affected path
        String path = event.getPath();

        logger.info("Connection status:\t" + keeperState.toString());
        logger.info("Event type:\t" + eventType.toString());

        if (KeeperState.SyncConnected == keeperState) {
            // Successfully connected to ZK server
            if (EventType.None == eventType) {
                logger.info("Successfully connected to ZK server!");
            }
            //Create node
            else if (EventType.NodeCreated == eventType) {
                logger.info("Node creation");
            }
            //Update node
            else if (EventType.NodeDataChanged == eventType) {
                logger.info("Node data update");
            }
            //Update child nodes
            else if (EventType.NodeChildrenChanged == eventType) {
                logger.info("Child node change");
            }
            //Delete node
            else if (EventType.NodeDeleted == eventType) {
                logger.info("node " + path + " Deleted");
            }
        } else if (KeeperState.Disconnected == keeperState) {
            logger.info("And ZK Server Disconnected");
        } else if (KeeperState.AuthFailed == keeperState) {
            logger.info("Permission check failed");
        } else if (KeeperState.Expired == keeperState) {
            logger.info("Session failure");
        }
    }

    public void close() throws InterruptedException {
        zooKeeper.close();
    }

    public byte[] serializeData(ZKData data) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream out = new ObjectOutputStream(bos);
        out.writeObject(data);
        out.flush();
        return bos.toByteArray();
    }
}
















