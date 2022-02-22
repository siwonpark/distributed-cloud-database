package persistence;

//after any change is made in config, we should delete all the data on the disk and make a new tree.

public class DBConfig {
    private static DBConfig instance = null;

    enum CacheType {
        LRU, FIFO, LFU, None
    }

    CacheType cacheType;
    int cacheSize;
    int maxNumber = 100;

    String filePath;

    public DBConfig(int cacheSize, String strategy) {

        this.cacheSize = cacheSize;
        switch (strategy) {
            case "FIFO":
                cacheType = CacheType.FIFO;
                break;
            case "LRU":
                cacheType = CacheType.LRU;
                break;
            case "LFU":
                cacheType = CacheType.LFU;
                break;
            default:
                cacheType = CacheType.None;
                break;
        }
        String rootPath = System.getProperty("user.dir");
        this.filePath = rootPath + "/data/";
    }

    public static DBConfig initInstance(int cacheSize, String strategy) {
        if (DBConfig.instance == null) {
            DBConfig.instance = new DBConfig(cacheSize, strategy);
        }
        return DBConfig.instance;
    }

    public static DBConfig getInstance() {
        return instance;
    }
}
