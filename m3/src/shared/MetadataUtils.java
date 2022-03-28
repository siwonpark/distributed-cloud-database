package shared;

import ecs.ECSNode;

import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

public class MetadataUtils {

    /**
     * Given a key and the state of the metadata, Return the ECSNode (representing a server) that is
     * responsible For the key
     *
     * @param key The key to query
     * @param metadata The metadata
     * @return The responsible ECSNode, null if not found.
     */
    public static ECSNode getResponsibleServerForKey(
            String key, TreeMap<String, ECSNode> metadata) {
        String keyHash = HashUtils.computeHash(key);
        for (Map.Entry<String, ECSNode> entry : metadata.entrySet()) {
            ECSNode ecsNode = entry.getValue();
            if (ecsNode.isResponsibleForKey(keyHash)) {
                return ecsNode;
            }
        }
        return null;
    }

    public static ECSNode getServerNodeWithName(String serverName, TreeMap<String, ECSNode> metadata){
        for(Map.Entry<String, ECSNode> metadataEntry : metadata.entrySet()){
            ECSNode currNode = metadataEntry.getValue();
            if(Objects.equals(currNode.getNodeName(), serverName)){
                return currNode;
            }
        }
        throw new RuntimeException(String.format("Server with name %s is not found in the metadata", serverName));
    }

    /**
     * Get Successor of given node in the HashRing, returns itself if the hashring is empty
     * @param metadata HashRing of servers
     * @param node The node we want the successor of
     * @return successor ECSNode
     */
    public static ECSNode getSuccessor(TreeMap<String, ECSNode> metadata, ECSNode node) {
        if (metadata.isEmpty()) {
            return node;
        }

        Map.Entry<String, ECSNode> successor =
                metadata.higherEntry(node.getHash()) != null
                        ? metadata.higherEntry(node.getHash())
                        : metadata.firstEntry();

        return successor.getValue();
    }

    /**
     * Get Predecessor of given node in the HashRing, returns itself if the hashring is empty
     * @param metadata HashRing of servers
     * @param node The node we want the predecessor of
     * @return predecessor ECSNode
     */
    public static ECSNode getPredecessor(TreeMap<String, ECSNode> metadata, ECSNode node) {
        if (metadata.isEmpty()) {
            return node;
        }

        Map.Entry<String, ECSNode> predecessor =
                metadata.lowerEntry(node.getHash()) != null
                        ? metadata.lowerEntry(node.getHash())
                        : metadata.lastEntry();

        return predecessor.getValue();
    }

    public static int getServersNum(TreeMap<String, ECSNode> metadata){
        return metadata.size();
    }
}
