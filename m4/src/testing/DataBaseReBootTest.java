package testing;

import junit.framework.TestCase;
import org.junit.Test;
import persistence.DataBase;

public class DataBaseReBootTest extends TestCase {
    @Test
    public void testReboot() {
        Exception ex = null;
        try {
            DataBase b = DataBase.initInstance(10, "LRU", "testReboot", false);
            b.put("1", "a");
            b.put("2", "b");
            b.put("3", "c");
            b = DataBase.initInstance(10,"LRU","testReboot",true);
            assertEquals(b.get("1"),"a");
            assertEquals(b.get("2"),"b");
            assertEquals(b.get("3"),"c");
        } catch (Exception e) {
            ex = e;
        }
        assertNull(ex);
    }
}
