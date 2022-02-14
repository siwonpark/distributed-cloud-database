package ecs;

public class ECSNode implements IECSNode {
    private String nodeName;
    private String nodeHost;
    private int nodePort;
    private String[] nodeHashRange;

    public ECSNode(String nodeName, String nodeHost, int nodePort, String[] nodeHashRange) {
        this.nodeName = nodeName;
        this.nodeHost = nodeHost;
        this.nodePort = nodePort;
        this.nodeHashRange = nodeHashRange;
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
        return nodeHashRange;
    }
}