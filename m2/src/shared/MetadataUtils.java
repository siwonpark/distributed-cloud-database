package shared;

import ecs.ECSNode;

import java.util.Map;
import java.util.TreeMap;

public class MetadataUtils {

    /**
     * Given a key and the state of the metadata,
     * Return the ECSNode (representing a server) that is responsible
     * For the key
     * @param key The key to query
     * @param metadata The metadata
     * @return The responsible ECSNode, null if not found.
     */
    public static ECSNode getResponsibleServerForKey(String key, TreeMap<String, ECSNode> metadata){
        String keyHash = HashUtils.computeHash(key);
        for(Map.Entry<String,ECSNode> entry : metadata.entrySet()){
            ECSNode ecsNode = entry.getValue();
            if (ecsNode.isResponsibleForKey(keyHash)){
                return ecsNode;
            }
        }
        return null;
    }

    /**
     * Parse the metadata to find the ECSNode corresponding to the server
     * @param server The server name (ip:port)
     * @param metadata Metadata
     * @return The ECSNode corresponding to server
     */
    public static ECSNode getServerNode(
            String server, TreeMap<String, ECSNode> metadata) throws RuntimeException {
        String serverHash = HashUtils.computeHash(server);
        if(metadata.containsKey(serverHash)){
            return metadata.get(serverHash);
        } else{
            throw new RuntimeException(String.format(
                    "Server Hash %s is not contained in the metadata", serverHash));
        }
    }


}
