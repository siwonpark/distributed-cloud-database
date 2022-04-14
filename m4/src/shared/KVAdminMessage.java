package shared;

import ecs.ECSNode;
import shared.messages.KVMessage;
import shared.messages.Message;

import java.io.*;
import java.util.ArrayList;
import java.util.TreeMap;

public class KVAdminMessage implements Serializable {
    private TreeMap<String, ECSNode> metadata;
    private OperationType status;
    private String keyStart;
    private String keyEnd;
    private ECSNode targetNode;
    private String key;
    private String value;
    private ArrayList<Message> operations;

    public enum OperationType {
        INIT,
        START,
        STOP,
        SHUT_DOWN,
        METADATA,
        LOCK_WRITE,
        UNLOCK_WRITE,
        MOVE_DATA,
        COMMIT_SUCCESS,
        COMMIT_FAILED,
        PUT,
        PUT_UPDATE,
        PUT_FAILED,
        GET,
        PUT_SUCCESS,
        GET_FAILED,
        GET_SUCCESS,
        SEND_OPERATIONS,
        ACK
    }

    public KVAdminMessage(TreeMap<String, ECSNode> metadata, OperationType status) {
        this.metadata = metadata;
        this.status = status;
    }

    public KVAdminMessage(String key, String value, OperationType status) {
        this.key = key;
        this.value = value;
        this.status = status;
    }

    public KVAdminMessage(OperationType status) {
        this.status = status;
    }

    public void setOperations(ArrayList<Message> operations) {
        this.operations = operations;
    }

    public void setKeyStart(String keyStart) {
        this.keyStart = keyStart;
    }

    public void setKeyEnd(String keyEnd) {
        this.keyEnd = keyEnd;
    }

    public void setTargetNode(ECSNode targetNode) {
        this.targetNode = targetNode;
    }

    public ECSNode getTargetNode() {
        return targetNode;
    }

    public String[] getMoveRange() {
        return new String[]{this.keyStart, this.keyEnd};
    }

    public OperationType getOperationType() {
        return this.status;
    }

    public ArrayList<Message> getOperations() {
        return this.operations;
    }

    public String getKey() {
        return this.key;
    }

    public String getValue() {
        return this.value;
    }

    public TreeMap<String, ECSNode> getMetadata() {
        return this.metadata;
    }
}
