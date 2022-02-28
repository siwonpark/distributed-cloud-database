package ecs;

import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.data.Stat;
import shared.ZKData;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class ZKWatcher implements Watcher {
    private ZooKeeper zooKeeper;
    private Logger logger = Logger.getRootLogger();
    static String ROOT_PATH = "/ecs";
    static String ZK_HOST = "localhost";
    static int ZK_PORT = 2181;
    public CountDownLatch connectedSignal = new CountDownLatch(1);
    public CountDownLatch awaitSignal;

    public ZooKeeper connect() {
        try {
            zooKeeper = new ZooKeeper(ZK_HOST + ":" + ZK_PORT, 1000, this);
        } catch (IOException e) {
            logger.error("Failed to connect to ZooKeeper server at localhost:2181");
        }
        return zooKeeper;
    }

    public byte[] serializeData(ZKData data) throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutputStream out = new ObjectOutputStream(bos)) {
            out.writeObject(data);
            out.flush();
            return bos.toByteArray();
        }
    }

    public void create(String path) {
        try {
            Stat stat = zooKeeper.exists(path, false);

            if (stat == null) {
                zooKeeper.create(path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (Exception e) {
            logger.error("Failed to create z-node");
        }
    }

    @Override
    public void process(WatchedEvent event) {
        logger.info("Watcher triggered");
        if (event == null) {
            return;
        }

        // Get connection status
        Event.KeeperState keeperState = event.getState();
        // Event type
        EventType eventType = event.getType();
        // Affected path
        String path = event.getPath();

        logger.info("Connection status:" + keeperState.toString());
        logger.info("Event type:" + eventType.toString());

        if (KeeperState.SyncConnected == keeperState) {
            // Successfully connected to ZK server
            if (EventType.None == eventType) {
                connectedSignal.countDown();
                logger.info("Successfully connected to ZK server!");
            }
            // Create node
            else if (EventType.NodeCreated == eventType) {
                logger.info("Node creation at znode " + path);
                awaitSignal.countDown();
            }
            // Update node
            else if (EventType.NodeDataChanged == eventType) {
                logger.info("Received acknowledgement from znode " + path);
                awaitSignal.countDown();
            }
            // Update child nodes
            else if (EventType.NodeChildrenChanged == eventType) {
                awaitSignal.countDown();
                logger.info("Child node change");
            }
            // Delete node
            else if (EventType.NodeDeleted == eventType) {
                awaitSignal.countDown();
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

    public void watchNode(String path) {
        try {
            zooKeeper.exists(path, this);
        } catch (Exception e) {
            logger.error("Failed to set watcher for znode");
        }
    }

    public void setData(String nodeName, ZKData data) {
        try {
            String path = ROOT_PATH + "/" + nodeName;
            byte[] dataBytes = serializeData(data);
            watchNode(path);

            Stat stat = zooKeeper.exists(path + "-server", false);
            zooKeeper.setData(path + "-server", dataBytes, stat.getVersion());
        } catch (Exception e) {
            logger.error("Failed to set data for znode");
        }
    }

    public void deleteZnode(String nodeName) {
        try {
            String path = ROOT_PATH + "/" + nodeName;
            Stat stat = zooKeeper.exists(path, false);
            zooKeeper.delete(path, stat.getVersion());

            path = ROOT_PATH + "/" + nodeName + "-server";
            stat = zooKeeper.exists(path, false);
            zooKeeper.delete(path, stat.getVersion());
        } catch (Exception e) {
            logger.error("Failed to delete znode");
        }
    }

    public void close() throws InterruptedException {
        zooKeeper.close();
    }
}
