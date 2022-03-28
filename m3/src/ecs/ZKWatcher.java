package ecs;

import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.data.Stat;
import shared.KVAdminMessage;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.concurrent.CountDownLatch;

public class ZKWatcher implements Watcher {
    private ZooKeeper zooKeeper;
    private ECS ecs;
    private Logger logger = Logger.getRootLogger();
    static String ROOT_PATH = "/ecs";
    static String ACK_PATH = "/ecs/ack";
    static String COMMAND_PATH = "/ecs/command";
    static String ZK_HOST = "localhost";
    static int ZK_PORT = 2200;
    public CountDownLatch connectedSignal = new CountDownLatch(1);
    public CountDownLatch awaitSignal;

    public ZKWatcher(ECS ecs) {
        this.ecs = ecs;
    }

    public ZooKeeper connect() {
        try {
            zooKeeper = new ZooKeeper(ZK_HOST + ":" + ZK_PORT, 1000, this);
            connectedSignal.await();
        } catch (Exception e) {
            logger.error("Failed to connect to ZooKeeper server at " + ZK_HOST + ":" + ZK_PORT);
        }
        return zooKeeper;
    }

    public byte[] serializeData(KVAdminMessage data) throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutputStream out = new ObjectOutputStream(bos)) {
            out.writeObject(data);
            out.flush();
            return bos.toByteArray();
        }
    }

    public void watchNode(String nodeName) {
        try {
            zooKeeper.addWatch(ACK_PATH + "/" + nodeName, this, AddWatchMode.PERSISTENT);
        } catch (Exception e) {
            logger.error("Could not add watch to node " + nodeName);
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
            // Delete node
            else if (EventType.NodeDeleted == eventType) {
                String[] pathParts = path.split("/");
                String nodeName = pathParts[pathParts.length - 1];
                if (ecs.getECSNode(nodeName) != null) {
                    logger.info("Node deleted at znode " + path);
                    ecs.handleServerFailure(nodeName);
                }
            }
        } else if (KeeperState.Disconnected == keeperState) {
            logger.info("And ZK Server Disconnected");
        } else if (KeeperState.AuthFailed == keeperState) {
            logger.info("Permission check failed");
        } else if (KeeperState.Expired == keeperState) {
            logger.info("Session failure");
        }
    }

    public void setData(String nodeName, KVAdminMessage data) {
        try {
            byte[] dataBytes = serializeData(data);
            String path = COMMAND_PATH + "/" + nodeName;

            Stat stat = zooKeeper.exists(path, false);

            zooKeeper.setData(path, dataBytes, stat.getVersion());
        } catch (Exception e) {
            logger.error("Failed to set data for znode");
        }
    }

    public void deleteZnode(String nodeName) {
        try {
            String commandPath = COMMAND_PATH + "/" + nodeName;
            String ackPath = ACK_PATH + "/" + nodeName;
            Stat stat = zooKeeper.exists(commandPath, false);
            zooKeeper.delete(commandPath, stat.getVersion());

            zooKeeper.removeWatches(ackPath, this, WatcherType.Any, true);

//            path = ACK_PATH + "/" + nodeName;
//            stat = zooKeeper.exists(path, false);
//            zooKeeper.delete(path, stat.getVersion());
        } catch (Exception e) {
            logger.error("Failed to delete znode");
        }
    }

    public void close() throws InterruptedException {
        zooKeeper.close();
    }
}
