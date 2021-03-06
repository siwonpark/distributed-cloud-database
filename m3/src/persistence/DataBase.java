package persistence;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import shared.HashUtils;
import static shared.PrintUtils.DELETE_STRING;

public class DataBase {
    private BTree b = null;
    private static DataBase instance = null;

    LinkedHashMap<String, Node> cache = null;

    private DataBase(boolean recoverFromDisk) {
        if (recoverFromDisk) {
            b = FileOp.loadTree();
            if (b == null){
                FileOp.deleteTree();
                b = FileOp.newTree();
            }
        }else{
            FileOp.deleteTree();
            b = FileOp.newTree();
        }


        if (DBConfig.getInstance().cacheType == DBConfig.CacheType.LRU) {
            cache = LRUCache.getInstance();
        } else if (DBConfig.getInstance().cacheType == DBConfig.CacheType.FIFO) {
            cache = FIFOCache.getInstance();
        } else if (DBConfig.getInstance().cacheType == DBConfig.CacheType.LFU) {
            cache = LFUCache.getInstance();
        } else {
            cache = null;
        }
    }


    public static DataBase initInstance(int cacheSize, String strategy, String dbName, boolean recoverFromDisk) {
        DBConfig.initInstance(cacheSize, strategy, dbName);
        DataBase.instance = new DataBase(recoverFromDisk);
        return DataBase.instance;
    }

    public static DataBase getInstance() {
        return instance;
    }

    public void put(String key, String value) {
        if(value == null){
            value = DELETE_STRING;
        }
        b.put(key, value);
    }

    public String get(String key) {
        return b.get(key);
    }


    /**
     * write the cache to disk.
     */
    public void dumpCache() {
        if (DBConfig.getInstance().cacheType == DBConfig.CacheType.None) {
            assert true;
        } else if (DBConfig.getInstance().cacheType == DBConfig.CacheType.LFU) {
            ((LFUCache) cache).dumpCache();
        } else {
            for (Map.Entry<String, Node> entry : cache.entrySet()) {
                FileOp.dumpFile(entry.getValue());
            }
        }
    }

    /**
     * clear all the data in memory after write them to disk.
     */
    public void clearCache() {
        dumpCache();
        if (DBConfig.getInstance().cacheType == DBConfig.CacheType.None) {
            assert true;
        } else if (DBConfig.getInstance().cacheType == DBConfig.CacheType.LFU) {
            ((LFUCache) cache).myClear();
        } else {
            cache.clear();
        }
    }

    /**
     * delete all the data in memory and disk, after deleteHistory the database is empty.
     */
    public void deleteHistory() {
        clearCache();
        FileOp.deleteTree();
    }

    /**
     * don't need to dumpCache before using getData.
     * can't import Pair, use ArrayList instead.
     *
     * @return we can use it like this:
     * for (ArrayList<String> i : data) {
     * System.out.printf("The key is %s, the value is %s\n", i.get(0), i.get(1));
     * }
     */
    public ArrayList<ArrayList<String>> getData(String start, String end) {
        boolean getAllData = start.equals(end);//Bondary situation
        ArrayList<ArrayList<String>> ans = new ArrayList<>();
        DataNode node = (DataNode) FileOp.loadFile(b.getLeft());
        while (node != null) {
            for (int i = 0; i < node.number; i++) {
                String hash = HashUtils.computeHash(node.keys[i]);
                assert hash != null;
                if ((getAllData || HashUtils.withinHashRange(hash, start, end)) && node.values[i] != null && !Objects.equals(node.values[i], DELETE_STRING)) {
                    ArrayList<String> tmp = new ArrayList<>();
                    tmp.add(node.keys[i]);
                    tmp.add(node.values[i]);
                    ans.add(tmp);
                }
            }
            node = (DataNode) FileOp.loadFile(node.right);
        }
        return ans;
    }

    /**
     * batch delete all the null values in the DataBase
     * */
    public void batchDeleteNull() {
        DataNode node = (DataNode) FileOp.loadFile(b.getLeft());
        ArrayList<ArrayList<String>> data = new ArrayList<>();
        int null_num = 0;
        while (node != null) {
            for (int i = 0; i < node.number; i++) {
                if (!Objects.equals(node.values[i], DELETE_STRING) && node.values[i] != null) {
                    ArrayList<String> tmp = new ArrayList<>();
                    tmp.add(node.keys[i]);
                    tmp.add(node.values[i]);
                    data.add(tmp);
                } else {
                    null_num += 1;
                }
            }
            node = (DataNode) FileOp.loadFile(node.right);
        }

        if ((float) null_num / (float) data.size() > 0.5) {//rebuild the tree if necessary
            this.deleteHistory();
			b = FileOp.newTree();
            for (ArrayList<String> i : data) {
                this.put(i.get(0), i.get(1));
            }
        }
    }
}
