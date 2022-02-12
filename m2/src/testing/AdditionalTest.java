package testing;

import org.junit.Test;
import shared.messages.Message;
import shared.messages.KVMessage.StatusType;
import java.util.Arrays;

import junit.framework.TestCase;

public class AdditionalTest extends TestCase {

	@Test
	/**
	 * Test our message can be encoded properly, and fetchers work properly
	 */
	public void testMessageGetters() {
		Message message = new Message("key", "value", StatusType.PUT);

		assertEquals("key", message.getKey());
		assertEquals("value", message.getValue());
		assertEquals(StatusType.PUT, message.getStatus());
	}

}
