package persistence;


import java.util.*;

public class LFUCache extends LinkedHashMap<String, Node> {
    private static int maxSize;
    PriorityQueue<CacheUnit> queue;
    HashMap<String, CacheUnit> hashmap;
    int size = 0;
    int timestamp = 0;

    private LFUCache() {
        super(0, 0.75f, false);// LFUCache extends LinkedHashMap just because the cache at database is LinkedHashMap
        maxSize = DBConfig.getInstance().cacheSize;
        queue = new PriorityQueue<CacheUnit>(maxSize);
        hashmap = new HashMap<String, CacheUnit>(maxSize);
    }

    @Override
    public Node get(Object key) {
        CacheUnit cacheUnit = hashmap.get(key);
        if (cacheUnit == null)
            return null;
        cacheUnit.freq++;
        cacheUnit.timestamp = timestamp++;
        queue.remove(cacheUnit);
        queue.offer(cacheUnit);
        return cacheUnit.value;
    }

    @Override
    public Node put(String key, Node value) {
        if (maxSize == 0)
            return null;
        CacheUnit cacheUnit = hashmap.get(key);

        if (cacheUnit != null) {
            Node oldNode = cacheUnit.value;
            cacheUnit.value = value;
            cacheUnit.freq++;
            cacheUnit.timestamp = timestamp++;
            queue.remove(cacheUnit);
            queue.offer(cacheUnit);
            return oldNode;
        } else {
            if (size == maxSize) {
                hashmap.remove(Objects.requireNonNull(queue.poll()).key);
                size--;
            }
            CacheUnit newCacheUnit = new CacheUnit(key, value, timestamp++);
            queue.offer(newCacheUnit);
            hashmap.put(key, newCacheUnit);
            size++;
            return null;
        }
    }

    @Override
    public boolean containsKey(Object key) {
        return hashmap.containsKey(key);
    }

    public void dumpCache() {
        for (Map.Entry<String, CacheUnit> entry : hashmap.entrySet()) {
            FileOp.dumpFile(entry.getValue().value);
        }
    }

    public void myClear() {
        timestamp = 0;
        size = 0;
        hashmap.clear();
        queue.clear();
    }

    private static LFUCache cache;

    public static LFUCache getInstance() {
        cache = new LFUCache();
        return cache;
    }

    private class CacheUnit implements Comparable<CacheUnit> {
        String key;
        Node value;
        int freq = 1;
        int timestamp;

        public CacheUnit() {
        }

        public CacheUnit(String key, Node value, int index) {
            this.key = key;
            this.value = value;
            this.timestamp = index;
        }

        @Override
        public int compareTo(CacheUnit o) {
            int minus = this.freq - o.freq;
            return minus == 0 ? this.timestamp - o.timestamp : minus;
        }
    }
}
