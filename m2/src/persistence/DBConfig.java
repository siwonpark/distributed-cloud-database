package persistence;

//after any change is made in config, we should delete all the data on the disk and make a new tree.
public class DBConfig {
    private final static DBConfig instance = new DBConfig();
    enum CacheType{
        LRU, FIFO, LFU, None
    }
    CacheType cacheType = CacheType.LFU;
    int maxNumber = 100;
    int cacheSize = 100;

    String filePath;
    private DBConfig() {
        String rootPath = System.getProperty("user.dir");
        this.filePath = rootPath + "/data/";

    }
    public static DBConfig getInstance() {
        return instance;
    }
}
