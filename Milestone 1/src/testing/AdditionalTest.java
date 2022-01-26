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
		byte[] expectedByteArray = new byte[] {(byte) StatusType.PUT.ordinal(), 2, 107, 101, 121, 3, 2, 118, 97, 108, 117, 101, 3};

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
		String testKey = "TestKey1key";
		String testValue = "TestValuevalue";
		StatusType testStatusType = StatusType.PUT;

		byte statusByte = (byte) testStatusType.ordinal();
		byte[] keyBytes = testKey.getBytes();
		byte[] valueBytes = testValue.getBytes();

		Message msg = new Message(keyBytes, valueBytes, statusByte);

		assertEquals(testKey, msg.getKey());
		assertEquals(testValue, msg.getValue());
		assertEquals(testStatusType, msg.getStatus());
	}

	/**
	 * Test that if an incorrect status bytes is supplied to message constructor
	 * Then a message is created with no status
	 */
	public void testMessageInvalidStatusBytes(){
		String key = "TestKey1key";
		byte[] keyBytes = key.getBytes();
		byte statusType = (byte) 127;

		Message msg = new Message(keyBytes, null, statusType);
		assertEquals(msg.getStatus(), StatusType.FAILED);
		assertEquals(msg.getKey(), key);
		assertEquals(msg.getValue(), null);
	}
}
