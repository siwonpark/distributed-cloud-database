package testing;

import org.junit.Test;
import shared.messages.Message;
import shared.messages.KVMessage.StatusType;
import java.util.Arrays;

import junit.framework.TestCase;

public class AdditionalTest extends TestCase {
	
	// TODO add your test cases, at least 3
	
	@Test
	/**
	 * Test that a message can be encoded into a byte array
	 */
	public void testMessageEncode() {
		Message messageEncode = new Message("key", "value", StatusType.PUT);
		byte[] expectedByteArray = new byte[] {51, 2, 107, 101, 121, 3, 2, 118, 97, 108, 117, 101, 3};

		assertEquals("key", messageEncode.getKey());
		assertEquals("value", messageEncode.getValue());
		assertEquals(StatusType.PUT, messageEncode.getStatus());
		assertTrue(Arrays.equals(messageEncode.getMsgBytes(), expectedByteArray));
	}

	@Test
	/**
	 * Test that a given byte array can be successfully decoded into a message
	 */
	public void testMessageDecode() {
		String testKey = "TestKey1 key";
		String testValue = "TestValue value";
		StatusType testStatusType = StatusType.PUT;

		byte[] statusBytes = String.valueOf(testStatusType.ordinal()).getBytes();
		byte[] keyBytes = testKey.getBytes();
		byte[] valueBytes = testValue.getBytes();

		Message msg = new Message(keyBytes, valueBytes, statusBytes);

		assertEquals(testKey, msg.getKey());
		assertEquals(testValue, msg.getValue());
		assertEquals(testStatusType, msg.getStatus());
	}
}
