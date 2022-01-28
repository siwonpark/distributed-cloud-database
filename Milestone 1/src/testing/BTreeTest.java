package testing;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Test;
import junit.framework.TestCase;
import app_kvServer.BTree;
import app_kvServer.FileOp;

import java.util.Random;

class Data {
    public final String key;
    public String value;

    public Data(String key, String value) {
        this.key = key;
        this.value = value;
    }
}

public class BTreeTest extends TestCase {
    static final Random random = new Random();

    static String getRandomString(int length) {
        StringBuilder buffer = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            buffer.append((char) (97 + random.nextInt(122 - 97 + 1)));
        }
        return buffer.toString();
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
            assertTrue(FileOp.SeqenceOrderAndChainTest(b));
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
}