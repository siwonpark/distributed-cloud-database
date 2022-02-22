package persistence;

import java.util.LinkedHashMap;
import java.util.Map;

public class DataBase {
    private BTree b = null;
    private static DataBase instance = null;

    static DBConfig config;
    LinkedHashMap<String, Node> cache;

    private DataBase(boolean recoverFromDisk) {
        if (recoverFromDisk) {
            b = FileOp.loadTree();
        }
        if (b == null) {
            FileOp.deleteTree();
            b = FileOp.newTree();
        }

        if (config.cacheType == DBConfig.CacheType.LRU) {
            cache = LRUCache.getInstance();
        } else if (config.cacheType == DBConfig.CacheType.FIFO) {
            cache = FIFOCache.getInstance();
        } else if (config.cacheType == DBConfig.CacheType.LFU) {
            cache = LFUCache.getInstance();
        } else {
            cache = null;
        }
    }

    public static DataBase initInstance(int cacheSize, String strategy, boolean recoverFromDisk) {
        if (DataBase.instance == null) {
            DataBase.config = DBConfig.initInstance(cacheSize, strategy);
            DataBase.instance = new DataBase(recoverFromDisk);
        }
        return DataBase.instance;
    }

    public static DataBase getInstance() {
        return instance;
    }

    public void put(String key, String value) {
        b.put(key, value);
    }

    public String get(String key) {
        return b.get(key);
    }

    public void dumpCache() {
        if (config.cacheType == DBConfig.CacheType.None) {
            assert true;
        } else if (config.cacheType == DBConfig.CacheType.LFU) {
            ((LFUCache) cache).dumpCache();
        } else {
            for (Map.Entry<String, Node> entry : cache.entrySet()) {
                FileOp.dumpFile(entry.getValue(), false);
            }
        }
    }

    public void clearCache() {
        dumpCache();
        if (config.cacheType == DBConfig.CacheType.None) {
            assert true;
        } else if (config.cacheType == DBConfig.CacheType.LFU) {
            ((LFUCache) cache).myClear();
        } else {
            for (Map.Entry<String, Node> entry : cache.entrySet()) {
                cache.clear();
            }
        }
    }

    public void deleteHistory() {
        clearCache();
        FileOp.deleteTree();
    }

    public void batchDeleteNull() {

    }
}
