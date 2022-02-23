package testing;

import ecs.ECSNode;
import junit.framework.TestCase;
import org.junit.Test;
import shared.messages.KVMessage.StatusType;
import shared.messages.Message;

import java.util.TreeMap;

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
		TreeMap<String, ECSNode> metadata = new TreeMap<>();
		metadata.put("qowiej", new ECSNode("1", "localhost", 5000));
		metadata.put("qowiej", new ECSNode("2", "localhost", 5001));

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
	 */
	public void testMessageMetadataStatusCode() {
		TreeMap<String, ECSNode> metadata = new TreeMap<>();
		metadata.put("qowiej", new ECSNode("1", "localhost", 5000));
		metadata.put("qowiej", new ECSNode("2", "localhost", 5001));
		Exception ex = null;
		try{
			Message msg = new Message(metadata, StatusType.PUT);
			System.out.println(msg.getMessageString());
		} catch(IllegalArgumentException e) {
			ex = e;
		}
		assertTrue(ex instanceof IllegalArgumentException);
	}
}
