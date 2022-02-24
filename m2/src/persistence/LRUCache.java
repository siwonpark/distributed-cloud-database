package persistence;

import java.util.LinkedHashMap;
import java.util.Map;

public class LRUCache extends LinkedHashMap<String, Node> {
    private static int size;

    private LRUCache() {
        super(DBConfig.getInstance().cacheSize, 0.75f, true);
        size = DBConfig.getInstance().cacheSize;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<String, Node> eldest) {
        boolean removeIt = size() > size;
        if(removeIt){
            FileOp.dumpFile(eldest.getValue(), false);
        }
        return removeIt;
    }

    private final static LRUCache cache = new LRUCache();

    public static LRUCache getInstance() {
        return cache;
    }

}
