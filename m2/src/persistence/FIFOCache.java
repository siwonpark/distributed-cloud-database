package persistence;

import java.util.LinkedHashMap;
import java.util.Map;

public class FIFOCache extends LinkedHashMap<String, Node> {
    private static int size;

    private FIFOCache() {
        super(DBConfig.getInstance().cacheSize, 0.75f, false);
        size = DBConfig.getInstance().cacheSize;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<String, Node> eldest) {
        boolean removeIt = size() > size;
        if (removeIt) {
            FileOp.dumpFile(eldest.getValue());
        }
        return removeIt;
    }

    private static FIFOCache cache;

    public static FIFOCache getInstance() {
        cache = new FIFOCache();
        return cache;
    }
}

