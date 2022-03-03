package testing;

import junit.framework.TestCase;
import org.junit.Test;
import persistence.*;
import java.util.Random;

class Data {
    public final String key;
    public String value;

    public Data(String key, String value) {
        this.key = key;
        this.value = value;
    }
}

public class DataBasePutGetTest extends TestCase {
    static final Random random = new Random();

    static String getRandomString(int length) {
        StringBuilder buffer = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            buffer.append((char) (97 + random.nextInt(122 - 97 + 1)));
        }
        return buffer.toString();
    }

    @Test
    public void testSimplePutGet() {
        Exception ex = null;
        try {
            DataBase b = DataBase.initInstance(1000, "FIFO", "DataBaseSimpleTest", false);
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
    public void testBatchPutGet() {
        Exception ex = null;
        try {
            DataBase b = DataBase.initInstance(3000, "LRU", "BatchPutGet", false);

            int test_length = 1500;
            Data[] lis = new Data[test_length];
            for (int i = 0; i < test_length; i++) {
                lis[i] = new Data(getRandomString(10), getRandomString(20));
            }
            for (int i = 0; i < test_length; i++) {
                b.put(lis[i].key, lis[i].value);
            }
            for (int i = 0; i < test_length; i++) {
                assertEquals(b.get(lis[i].key), lis[i].value);
            }
            for (int i = 0; i < test_length; i++) {
                lis[i].value = getRandomString(20);
                b.put(lis[i].key, lis[i].value);
            }
            for (int i = 0; i < test_length; i++) {
                assertEquals(b.get(lis[i].key), lis[i].value);
            }
        } catch (Exception e) {
            ex = e;
        }
        assertNull(ex);
    }
}
