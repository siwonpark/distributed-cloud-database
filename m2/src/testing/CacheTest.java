package testing;

import junit.framework.TestCase;
import org.junit.Test;
import persistence.DataBase;

public class CacheTest extends TestCase {
    @Test
    public void testLRUCache() {
        Exception ex = null;
        try {
            DataBase b = DataBase.initInstance(10, "LRU", "testLRUCache", false);
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
            b.deleteHistory();
        } catch (Exception e) {
            ex = e;
        }
        assertNull(ex);
    }
    @Test
    public void testLFUCache() {
        Exception ex = null;
        try {
            DataBase b = DataBase.initInstance(10, "LFU", "testLFUCache", false);
            assertTrue(b!=null);
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
            b.deleteHistory();
        } catch (Exception e) {
            ex = e;
        }
        assertNull(ex);
    }
    @Test
    public void testFIFOCache() {
        Exception ex = null;
        try {
            DataBase b = DataBase.initInstance(10, "FIFO", "testFIFOCache", false);
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
            b.deleteHistory();
        } catch (Exception e) {
            ex = e;
        }
        assertNull(ex);
    }

}
