package persistence;


import org.apache.log4j.Level;

import java.util.ArrayList;
import java.util.Random;

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

public class mytest {

    private static final Logger logger = Logger.getRootLogger();
    static final Random random = new Random();

    static String getRandomString(int length) {

        StringBuilder buffer = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            buffer.append((char) (97 + random.nextInt(122 - 97 + 1)));
        }
        return buffer.toString();
    }

    static boolean testChain(BTree b) {
        String lef = b.getLeft();
        DataNode tmp_lef = (DataNode) FileOp.loadFile(lef);
        while (tmp_lef != null && tmp_lef.right != null) {
            for (int i = 0; i < tmp_lef.number - 1; i++) {
                if (tmp_lef.keys[i].compareTo(tmp_lef.keys[i + 1]) >= 0) {
                    logger.error("The sequences is disordered!");
                    return false;
                }
            }
            DataNode tmp = tmp_lef;
            tmp_lef = (DataNode) FileOp.loadFile(tmp_lef.right);
            if (!tmp_lef.left.equals(tmp.name)) {
                logger.error("The left pointer is wrong!");
                return false;
            }
            if (tmp.keys[tmp.number - 1].compareTo(tmp_lef.keys[0]) >= 0) {
                logger.error("The chain is disordered!");
                return false;
            }
        }
        return true;
    }

    static boolean repeatPutAndGetTest(BTree b, int test_length) {

        Data[] lis = new Data[test_length];
        for (int i = 0; i < test_length; i++) {
            lis[i] = new Data(mytest.getRandomString(10), mytest.getRandomString(20));
        }
//        long time1 = System.nanoTime();
        for (int i = 0; i < test_length; i++) {
            logger.debug("test: put " + lis[i].key + " " + lis[i].value);
            b.put(lis[i].key, lis[i].value);
        }
        if (!testChain(b)) {
            return false;
        }
        for (int i = 0; i < test_length; i++) {
            logger.debug("test: get " + lis[i].key);
            String s = b.get(lis[i].key);
            if (!s.equals(lis[i].value)) {
                logger.error("The value is not correct!");
            }
        }
        if (!testChain(b)) {
            return false;
        }
        for (int i = 0; i < test_length; i++) {
            lis[i].value = mytest.getRandomString(20);
            logger.debug("test: put " + lis[i].key + " " + lis[i].value);
            b.put(lis[i].key, lis[i].value);
        }
        if (!testChain(b)) {
            return false;
        }
//        long time2 = System.nanoTime();
        for (int i = 0; i < test_length; i++) {
            logger.debug("test: get " + lis[i].key);
            String s = b.get(lis[i].key);
            if (!s.equals(lis[i].value)) {
                logger.error("The value is not correct!");
            }
        }
        if (!testChain(b)) {
            return false;
        } else {
            return true;
        }
    }

    static void simpleTest(DataBase db) {
        System.out.println(db.get("3"));
        db.put("1", "a");
        db.put("2", "b");
        db.put("1", null);
        String s = db.get("1");
        System.out.println(s == null);
        db.put("3", "c");
        System.out.println(db.get("3"));
        db.put("4", "abc");
        System.out.println(db.get("4"));
        db.put("4", "cba");
        System.out.println(db.get("4"));
    }

    static void simpleRecoverTest(BTree b) {
        System.out.println(b.get("3"));
        b.put("1", "a");
        b.put("2", "b");
        b.put("1", null);
        String s = b.get("1");
        System.out.println(s == null);
        b.put("3", "c");
        System.out.println(b.get("3"));
        b.put("4", "abc");
        System.out.println(b.get("4"));
        b.put("4", "cba");
        System.out.println(b.get("4"));
    }

    static void alwaysChooseATreeExample() {
        BTree b = FileOp.loadTree();
        if (b == null) {//A haven't been build
            b = FileOp.newTree();
        }
        //do something else with b
    }

    public static void main(String[] args) {
        logger.setLevel(Level.DEBUG);
        BasicConfigurator.configure();
        DataBase db = DataBase.initInstance(100, "LRU", false);
        simpleTest(db);
    }
}
