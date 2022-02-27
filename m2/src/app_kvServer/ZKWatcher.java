package app_kvServer;

import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.data.Stat;
import shared.ZKData;

import java.io.*;
import java.util.concurrent.CountDownLatch;

public class ZKWatcher implements Watcher {
    private ZooKeeper zooKeeper;
    private Logger logger = Logger.getRootLogger();
    private String nodeName;
    private String zkHost;
    private int zkPort;
    private ECSCommandHandler ecsCommandHandler;
    static String ROOT_PATH = "/ecs";
    public CountDownLatch connectedSignal = new CountDownLatch(1);

    public ZKWatcher(String nodeName, String zkHost, int zkPort, ECSCommandHandler ecsCommandHandler) {
        this.nodeName = nodeName;
        this.zkHost = zkHost;
        this.zkPort = zkPort;
        this.ecsCommandHandler = ecsCommandHandler;
    }

    public ZooKeeper connect() {
        try {
            zooKeeper = new ZooKeeper(zkHost + ":" + zkPort, 1000, this);
        } catch (IOException e) {
            logger.error("Failed to connect to ZooKeeper server at localhost:2181");
        }
        return zooKeeper;
    }

    public ZKData deserializeData(byte[] data) throws IOException, ClassNotFoundException {
        try( ByteArrayInputStream bis = new ByteArrayInputStream(data);
             ObjectInputStream in = new ObjectInputStream(bis)) {
            return (ZKData) in.readObject();
        }
    }

    public boolean create() {
        try {
            String path = ROOT_PATH + "/" + nodeName;
            Stat stat = zooKeeper.exists(path, this);

            if (stat == null) {
                zooKeeper.create(ROOT_PATH + "/" + nodeName, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }

            zooKeeper.exists(path, this);
            return true;
        } catch (Exception e) {
            logger.error("Failed to create z-node");
            logger.error(e.getMessage());
            return false;
        }
    }

    @Override
    public void process(WatchedEvent event) {
        logger.info("WATCHER NOTIFICATION!");
        if (event == null) {
            return;
        }

        // Get connection status
        KeeperState keeperState = event.getState();
        // Event type
        EventType eventType = event.getType();
        // Affected path
        logger.info("Connection status:\t" + keeperState.toString());
        logger.info("Event type:\t" + eventType.toString());

        if (KeeperState.SyncConnected == keeperState) {
            // Successfully connected to ZK server
            if (EventType.None == eventType) {
                connectedSignal.countDown();
                logger.info("Successfully connected to ZK server!");
            }
            // Update node
            else if (EventType.NodeDataChanged == eventType) {
                logger.info("Node data update");
                ecsCommandHandler.handleCommand(getData());
            }
        } else if (KeeperState.Disconnected == keeperState) {
            logger.info("And ZK Server Disconnected");
        }
    }

    public void setData() {
        try {
            zooKeeper.setData(ROOT_PATH + "/" + nodeName, new byte[0], -1);
        } catch (Exception e) {
            logger.error("Failed to set data for znode");
        }
    }

    public ZKData getData() {
        try {
            String path = ROOT_PATH + "/" + nodeName;
            Stat stat = zooKeeper.exists(path, false);
            byte[] data = zooKeeper.getData(path, this, stat);
            return deserializeData(data);
        } catch (Exception e) {
            logger.error("Failed to get data for znode");
            logger.error(e.getMessage());
            return null;
        }
    }

    public void close() throws InterruptedException {
        zooKeeper.close();
    }
}