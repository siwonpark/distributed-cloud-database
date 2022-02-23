package ecs;

import java.io.Serializable;

public class ECSNode implements IECSNode, Serializable {
    private String nodeName;
    private String nodeHost;
    private int nodePort;
    private String startHash;
    private String endHash;

    public ECSNode(String nodeName, String nodeHost, int nodePort) {
        this.nodeName = nodeName;
        this.nodeHost = nodeHost;
        this.nodePort = nodePort;
    }

    @Override
    public String getNodeName() {
        return nodeName;
    }

    @Override
    public String getNodeHost() {
        return nodeHost;
    }

    @Override
    public int getNodePort() {
        return nodePort;
    }

    @Override
    public String[] getNodeHashRange() {
        // TODO: Check if hashes exist on object?
        return new String[] {startHash, endHash};
    }

    public void setStartHash(String startHash) {
        this.startHash = startHash;
    }

    public void setEndHash(String endHash) {
        this.endHash = endHash;
    }

    public boolean isResponsibleForKey(String key) {
        if (startHash.compareTo(endHash) < 0) {
            return key.compareTo(startHash) > 0 && key.compareTo(endHash) < 0;
        } else {
            // start is greater than end, the node is responsible for an area across the start of the ring
            return key.compareTo(startHash) > 0 || key.compareTo(endHash) < 0;
        }
    }
}