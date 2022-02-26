package shared;

import ecs.ECSNode;

import java.io.Serializable;
import java.util.TreeMap;

public class ZKData implements Serializable {
    private TreeMap<String, ECSNode> metadata;
    private OperationType status;
    private String keyStart;
    private String keyEnd;
    private ECSNode targetNode;

    public enum OperationType {
        INIT,
        START,
        STOP,
        SHUT_DOWN,
        METADATA,
        LOCK_WRITE,
        UNLOCK_WRITE,
        MOVE_DATA
    }

    public ZKData(TreeMap<String, ECSNode> metadata, OperationType status) {
        this.metadata = metadata;
        this.status = status;
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
        return new String[] {this.keyStart, this.keyEnd};
    }

    public OperationType getOperationType(){
        return this.status;
    }

    public TreeMap<String, ECSNode> getMetadata(){
        return this.metadata;
    }
}
