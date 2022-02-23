package shared;

import ecs.ECSNode;

import java.io.Serializable;
import java.util.TreeMap;

public class ZKData implements Serializable {
    private TreeMap<String, ECSNode> metadata;
    private OperationType status;

    public enum OperationType {
        START,
        STOP,
        CREATE
    }

    public ZKData(TreeMap<String, ECSNode> metadata, OperationType status) {
        this.metadata = metadata;
        this.status = status;
    }
}
