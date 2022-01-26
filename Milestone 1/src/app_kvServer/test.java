package app_kvServer;

import java.util.Random;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.BasicConfigurator;

class Data {
    public final String key;
    public String value;

    public Data(String key, String value) {
        this.key = key;
        this.value = value;
    }
}

public class test {

    private static final Logger logger = Logger.getRootLogger();
    static final Random random = new Random();

    static String getRandomString(int length) {
        StringBuilder buffer = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            buffer.append((char) (97 + random.nextInt(122 - 97 + 1)));
        }
        return buffer.toString();
    }


    static void testChain(BTree b){
        String lef = b.getLeft();
        DataNode tmp_lef = (DataNode) b.f.loadFile(lef);
        while (tmp_lef.right != null) {
            for (int i = 0; i < tmp_lef.number - 1; i++) {
                if (tmp_lef.keys[i].compareTo(tmp_lef.keys[i + 1]) >= 0) {
                    logger.error("The sequences is disordered!");
                }
            }
            DataNode tmp = tmp_lef;
            tmp_lef = (DataNode) b.f.loadFile(tmp_lef.right);
            if (!tmp_lef.left.equals(tmp.name)) {
                logger.error("The left pointer is wrong!");
            }
            if (tmp.keys[tmp.number - 1].compareTo(tmp_lef.keys[0]) >= 0) {
                logger.error("The chain is disordered!");
            }
        }
    }

    static void putAndGetTest(BTree b){
        int test_length = 5000;
        Data[] lis = new Data[test_length];
        for (int i = 0; i < test_length; i++) {
            lis[i] = new Data(test.getRandomString(10), test.getRandomString(20));
        }
//        long time1 = System.nanoTime();
        for (int i = 0; i < test_length; i++) {
            logger.debug("test: put " + lis[i].key + " " + lis[i].value);
            b.put(lis[i].key, lis[i].value);
        }
        testChain(b);
        for (int i = 0; i < test_length; i++) {
            logger.debug("test: get " + lis[i].key);
            String s = b.get(lis[i].key);
            if (!s.equals(lis[i].value)) {
                logger.error("The value is not correct!");
            }
        }
        testChain(b);
        for (int i = 0; i < test_length; i++) {
            lis[i].value = test.getRandomString(20);
            logger.debug("test: put " + lis[i].key + " " + lis[i].value);
            b.put(lis[i].key, lis[i].value);
        }
        testChain(b);
//        long time2 = System.nanoTime();
        for (int i = 0; i < test_length; i++) {
            logger.debug("test: get " + lis[i].key);
            String s = b.get(lis[i].key);
            if (!s.equals(lis[i].value)) {
                logger.error("The value is not correct!");
            }
        }
        testChain(b);
//        long time3 = System.nanoTime();

//        logger.debug("time for put: " + (time2 - time1));
//        logger.debug("time for get: " + (time3 - time2));
    }

    public static void main(String[] args) throws Exception {
        logger.setLevel(Level.INFO);
        BTree b = new BTree(4, logger);
        putAndGetTest(b);
    }
}