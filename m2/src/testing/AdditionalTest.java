package testing;

import org.junit.Test;
import shared.messages.Message;
import shared.messages.KVMessage.StatusType;

import java.util.HashMap;

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
		assertEquals(null, message.getServerMetadata());
	}

	@Test
	/**
	 * Test our message can be encoded properly, and fetchers work properly
	 * For the server metadata case
	 * */
	public void testMessageGettersServerMetadata() {
		HashMap<String, String[]> metadata = new HashMap();
		metadata.put("qowiej", new String[]{"weqwe", "jqowiej"});
		metadata.put("qowiej", new String[]{"qwwj", "jqwriej"});

		Message message = new Message(metadata, StatusType.SERVER_NOT_RESPONSIBLE);

		assertEquals(null, message.getKey());
		assertEquals(null, message.getValue());
		assertEquals(metadata, message.getServerMetadata());
		assertEquals(StatusType.SERVER_NOT_RESPONSIBLE, message.getStatus());
	}

	@Test
	/**
	 * Test that instantiating a message with metadata while providing
	 * an incorrect status code will throw an exception
	 * */
	public void testMessageMetadataStatusCode() {
		HashMap<String, String[]> metadata = new HashMap();
		metadata.put("qowiej", new String[]{"weqwe", "jqowiej"});
		metadata.put("qowiej", new String[]{"qwwj", "jqwriej"});
		Error ex = null;

		try{

			Message message = new Message(metadata, StatusType.PUT);
		} catch(AssertionError e) {
			ex = e;
		}
		assertTrue(ex instanceof AssertionError);
	}

}
