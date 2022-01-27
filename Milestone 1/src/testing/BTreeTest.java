package app_kvServer;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Ignore;
import org.junit.Test;
import junit.framework.TestCase;

import java.util.Random;

public class BTreeTest extends TestCase {
    static final Random random = new Random();

    static String getRandomString(int length) {
        StringBuilder buffer = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            buffer.append((char) (97 + random.nextInt(122 - 97 + 1)));
        }
        return buffer.toString();
    }

    static void SeqenceOrderAndChainTest(BTree b) {
        String lef = b.getLeft();
        DataNode tmp_lef = (DataNode) b.f.loadFile(lef);
        while (tmp_lef.right != null) {
            for (int i = 0; i < tmp_lef.number - 1; i++) {
                assertTrue(tmp_lef.keys[i].compareTo(tmp_lef.keys[i + 1]) < 0);
            }
            DataNode tmp = tmp_lef;
            tmp_lef = (DataNode) b.f.loadFile(tmp_lef.right);
            assertEquals(tmp_lef.left, tmp.name);
            assertTrue(tmp.keys[tmp.number - 1].compareTo(tmp_lef.keys[0]) < 0);
        }
    }

    public void testSimplePutGet() {
        Exception ex = null;
        try {
            FileOp f = new FileOp();
            BTree b = f.newTree(10, "BTree Test Simple Put Get");
            assertNull(b.get("3"));
            b.put("1", "a");
            b.put("2", "b");
            assertEquals("b", b.get("2"));
            b.put("1", null);
            assertNull(b.get("1"));
            b.put("3", "c");
            assertEquals("c", b.get("3"));
            b.put("3", "abc");
            assertEquals("abc", b.get("3"));
            b.put("3", "cba");
            assertEquals("cba", b.get("3"));
        } catch (Exception e) {
            ex = e;
        }
        assertNull(ex);
    }

    @Test
    public void testLoadAndDump() {
        FileOp f1 = new FileOp();
        BTree b1 = f1.newTree(4, "BTree Test Load And Dump");
        assertEquals("BTree Test Load And Dump", b1.treeName);
        FileOp f2 = new FileOp();
        BTree b2 = f2.loadTree("BTree Test Load And Dump");
        assertEquals("BTree Test Load And Dump", b2.treeName);
    }

    @Test
    public void testMassivePutGet() {
        Exception ex = null;
        try {
            org.apache.log4j.Logger logger = Logger.getRootLogger();
            logger.setLevel(Level.DEBUG);

            FileOp f = new FileOp();
            BTree b = f.newTree(10, "BTree Test Massive Put Get");
            class Data {
                public final String key;
                public String value;

                public Data(String key, String value) {
                    this.key = key;
                    this.value = value;
                }
            }
            int test_length = 500;
            Data[] lis = new Data[test_length];
            for (int i = 0; i < test_length; i++) {
                lis[i] = new Data(getRandomString(10), getRandomString(20));
            }
            //init all the value
            for (int i = 0; i < test_length; i++) {
                b.put(lis[i].key, lis[i].value);
            }
            SeqenceOrderAndChainTest(b);
            for (int i = 0; i < test_length; i++) {
                String s = b.get(lis[i].key);
                assertEquals(lis[i].value, s);
            }
            SeqenceOrderAndChainTest(b);
            //update all the values
            for (int i = 0; i < test_length; i++) {
                lis[i].value = getRandomString(20);
                b.put(lis[i].key, lis[i].value);
            }
            SeqenceOrderAndChainTest(b);
            for (int i = 0; i < test_length; i++) {
                String s = b.get(lis[i].key);
                assertEquals(lis[i].value, s);
            }
            SeqenceOrderAndChainTest(b);
        } catch (Exception e) {
            ex = e;
        }
        assertNull(ex);
    }
}