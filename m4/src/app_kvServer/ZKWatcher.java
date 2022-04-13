package app_kvServer;

import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.data.Stat;
import shared.KVAdminMessage;
import shared.KVAdminMessage.OperationType;
import shared.messages.Message;

import java.io.*;
import java.util.concurrent.CountDownLatch;
import java.util.ArrayList;

import static java.lang.Thread.sleep;

public class ZKWatcher implements Watcher {
    private ZooKeeper zooKeeper;
    private Logger logger = Logger.getRootLogger();
    private String nodeName;
    private String zkHost;
    private int zkPort;
    private ECSCommandHandler ecsCommandHandler;
    static String ROOT_PATH = "/ecs";
    static String ACK_PATH = "/ecs/ack";
    static String COMMAND_PATH = "/ecs/command";
    static String OPERATIONS_PATH = "/ecs/operations";
    public CountDownLatch connectedSignal = new CountDownLatch(1);
    public CountDownLatch commitedSignal;
    public Message transactionReplys;

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

    public KVAdminMessage deserializeData(byte[] data) throws IOException, ClassNotFoundException {
        if (data.length == 0) {
            logger.error("Byte array received from get was empty");
            return null;
        }
        try( ByteArrayInputStream bis = new ByteArrayInputStream(data);
             ObjectInputStream in = new ObjectInputStream(bis)) {
            return (KVAdminMessage) in.readObject();
        }
    }

    public boolean create() {
        try {
            watchNode(nodeName);
            watchNode("metadata");
            zooKeeper.create(ACK_PATH + "/" + nodeName, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

            return true;
        } catch (Exception e) {
            logger.error("Failed to create z-node");
            logger.error(e.getMessage());
            return false;
        }
    }

    public void watchNode(String nodeName) {
        try {
            zooKeeper.exists(COMMAND_PATH + "/" + nodeName, this);
        } catch (Exception e) {
            logger.error("Failed to set watcher for znode");
        }
    }

    public void watchOperations() throws Exception{
            zooKeeper.exists(OPERATIONS_PATH + "/" + nodeName, this);
    }

    @Override
    public void process(WatchedEvent event) {
        logger.info("Watch triggered");
        if (event == null) {
            return;
        }

        // Get connection status
        KeeperState keeperState = event.getState();
        // Event type
        EventType eventType = event.getType();

        String path = event.getPath();
        logger.info("Connection status: " + keeperState.toString());
        logger.info("Event type: " + eventType.toString());

        if (KeeperState.SyncConnected == keeperState) {
            // Successfully connected to ZK server
            if (EventType.None == eventType) {
                connectedSignal.countDown();
                logger.info("Successfully connected to ZK server!");
            }
            // Update node
            else if (EventType.NodeDataChanged == eventType) {
                if(path.startsWith(OPERATIONS_PATH)){
                    if(path.equals(OPERATIONS_PATH + "/" + nodeName)){
                        transactionReplys = getReplys();
                        commitedSignal.countDown();
                    }
                }else{
                    KVAdminMessage data = getData(path);
                    logger.info("Received operation: " + data.getOperationType().toString());

                    ecsCommandHandler.handleCommand(data);
                }
            }
        } else if (KeeperState.Disconnected == keeperState) {
            logger.info("And ZK Server Disconnected");
        }
    }

    public void setData() {
        try {
            logger.info("SENDING ACK to ECS");
            String path = ACK_PATH + "/" + nodeName;
            Stat stat = zooKeeper.exists(path, false);
            zooKeeper.setData(path, new byte[stat.getVersion()], stat.getVersion());
        } catch (Exception e) {
            logger.error("Failed to set data for znode");
        }
    }

    public byte[] serializeData(KVAdminMessage data) throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutputStream out = new ObjectOutputStream(bos)) {
            out.writeObject(data);
            out.flush();
            return bos.toByteArray();
        }
    }

    public void setGetData(String value){
        try {
            KVAdminMessage data = new KVAdminMessage(null, value, OperationType.GET_SUCCESS);
            byte[] dataBytes = serializeData(data);
            String path = ACK_PATH + "/" + nodeName;

            Stat stat = zooKeeper.exists(path, false);
            if (stat == null) {
                stat = zooKeeper.exists(path, false);
            }
            watchNode(nodeName);
            
            zooKeeper.setData(path, dataBytes, stat.getVersion());
        } catch (Exception e) {
            logger.error("Failed to set data for znode");
        }
    }
    
    public Message getReplys() {
        try {
            String path = OPERATIONS_PATH + "/" + nodeName;
            Stat stat = zooKeeper.exists(path, false);
            byte[] data = zooKeeper.getData(path, this, stat);
            return deserializeReplys(data);
        } catch (Exception e) {
            logger.error("Failed to get operations");
            logger.error(e.getMessage());
            return null;
        }
    }

    public byte[] serializeOperations(ArrayList<Message> operations) throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutputStream out = new ObjectOutputStream(bos)) {
            out.writeObject(operations);
            out.flush();
            return bos.toByteArray();
        }
    }

    public Message deserializeReplys(byte[] data) throws IOException, ClassNotFoundException {
        if (data.length == 0) {
            logger.error("Byte array received from get was empty");
            return null;
        }
        try (ByteArrayInputStream bis = new ByteArrayInputStream(data);
                ObjectInputStream in = new ObjectInputStream(bis)) {
            return (Message) in.readObject();
        }
    }

    public boolean setOperations(ArrayList<Message> operations) {
        try {
            byte[] dataBytes = serializeOperations(operations);
            String path = OPERATIONS_PATH + "/" + nodeName;
            
            Stat stat = zooKeeper.exists(path, false);
            if (stat == null) {
                stat = zooKeeper.exists(path, false);
            }
            watchOperations();
            zooKeeper.setData(path, dataBytes, stat.getVersion());
            return true;
        } catch (Exception e) {
            logger.error("Failed to set operations for ecs");
            logger.error(e);
            return false;
        }
    }

    public KVAdminMessage getData(String path) {
        try {
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
