package ecs;

import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.data.Stat;
import shared.messages.Message;
import shared.KVAdminMessage;
import shared.KVAdminMessage.OperationType;

import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;

public class ZKWatcher implements Watcher {
    private ZooKeeper zooKeeper;
    private Logger logger = Logger.getRootLogger();
    static String ROOT_PATH = "/ecs";
    static String ACK_PATH = "/ecs/ack";
    static String COMMAND_PATH = "/ecs/command";
    static String OPERATIONS_PATH = "/ecs/operations";
    static String ZK_HOST = "localhost";
    static int ZK_PORT = 2200;
    public CountDownLatch connectedSignal = new CountDownLatch(1);
    public CountDownLatch awaitSignal;

    // use this to pass value from server when ecs calls get
    public String value;

    public ZooKeeper connect() {
        try {
            zooKeeper = new ZooKeeper(ZK_HOST + ":" + ZK_PORT, 1000, this);
            connectedSignal.await();
        } catch (Exception e) {
            logger.error("Failed to connect to ZooKeeper server at localhost:2181");
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

    public byte[] serializeReplys(Message data) throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutputStream out = new ObjectOutputStream(bos)) {
            out.writeObject(data);
            out.flush();
            return bos.toByteArray();
        }
    }

    public ArrayList<Message> deserializeOperations(byte[] data) throws IOException, ClassNotFoundException {
        if (data.length == 0) {
            logger.error("Byte array received from get was empty");
            return null;
        }
        try( ByteArrayInputStream bis = new ByteArrayInputStream(data);
             ObjectInputStream in = new ObjectInputStream(bis)) {
            return (ArrayList<Message>) in.readObject();
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

    public ArrayList<Message> getOperations(String path) {
        try {
            Stat stat = zooKeeper.exists(path, false);
            byte[] data = zooKeeper.getData(path, this, stat);
            return deserializeOperations(data);
        } catch (Exception e) {
            logger.error("Failed to get operations");
            logger.error(e.getMessage());
            return null;
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
                KVAdminMessage data = getData(path);
                if(data == null){
                    if(path.startsWith(OPERATIONS_PATH)){
                        ArrayList<Message> operations = getOperations(path);
                        String nodeName = path.substring(OPERATIONS_PATH.length() + 1);
                        // TODO handle operations

                        /**
                         * how to send replys back to server?
                         * 
                         * call setReplys(nodeName, replys) to send replys to server
                         * 
                         * replys is a Message new with an ArrayList of Message and a StatusType,
                         * if failed the StatusType should be StatusType.COMMIT_FAILURE 
                         *      and ArrayList could be empty
                         * if success the StatusType should be StatusType.COMMIT_SUCCESS 
                         *      each Message in a ArrayList should be new by "public Message(String key, String value, StatusType status)" and corresponds to the element in the opertaions arraylist
                        */
                    }else{
                        logger.info("Received acknowledgement from znode " + path);
                        awaitSignal.countDown();
                    }
                } else {
                    if(data.getOperationType() == OperationType.GET_SUCCESS){
                        logger.info("Received get success from znode " + path);
                        this.value = data.getValue();
                        awaitSignal.countDown();
                    }else{
                        logger.error("contained a status wrong to the ecs " + path);
                    }
                }
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

    public void watchNode(String nodeName) {
        try {
            zooKeeper.exists(ACK_PATH + "/" + nodeName, this);
        } catch (Exception e) {
            logger.error("Failed to set watcher for znode");
        }
    }

    public void watchOperations(String nodeName) {
        try {
            zooKeeper.exists(OPERATIONS_PATH + "/" + nodeName, this);
        } catch (Exception e) {
            logger.error("Failed to set watcher for znode");
        }
    }

    public void setData(String nodeName, KVAdminMessage data) {
        try {
            byte[] dataBytes = serializeData(data);
            String path = COMMAND_PATH + "/" + nodeName;

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
    
    public void setReplys(String Nodename, Message replys) {
        try {
            byte[] dataBytes = serializeReplys(replys);
            String path = OPERATIONS_PATH + "/" + Nodename;
            zooKeeper.setData(path, dataBytes, -1);
        } catch (Exception e) {
            logger.error("Failed to set operations for ecs");
        }
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

    public void deleteZnode(String nodeName) {
        try {
            String path = COMMAND_PATH + "/" + nodeName;
            Stat stat = zooKeeper.exists(path, false);
            zooKeeper.delete(path, stat.getVersion());

            path = ACK_PATH + "/" + nodeName;
            stat = zooKeeper.exists(path, false);
            zooKeeper.delete(path, stat.getVersion());

            path = OPERATIONS_PATH + "/" + nodeName;
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
