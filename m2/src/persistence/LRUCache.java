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
            FileOp.dumpFile(eldest.getValue());
        }
        return removeIt;
    }

    private static LRUCache cache;

    public static LRUCache getInstance() {
        cache = new LRUCache();
        return cache;
    }

}
