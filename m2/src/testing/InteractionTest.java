package testing;

import org.junit.Test;

import client.KVStore;
import junit.framework.TestCase;
import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;

import java.util.Arrays;

import static shared.PrintUtils.DELETE_STRING;
import static testing.AllTests.port;


public class InteractionTest extends TestCase {

	private KVStore kvClient;
	
	public void setUp() {
		kvClient = new KVStore("localhost", port);
		try {
			kvClient.connect();
		} catch (Exception e) {
		}
	}

	public void tearDown() {
		kvClient.disconnect();
	}
	
	
	@Test
	public void testPut() {
		String key = "foo2";
		String value = "bar2";
		KVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.PUT_SUCCESS);
	}
	
	@Test
	public void testPutDisconnected() {
		kvClient.disconnect();
		String key = "foo";
		String value = "bar";
		Exception ex = null;

		try {
			kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
		}

		assertNotNull(ex);
	}

	@Test
	public void testUpdate() {
		String key = "updateTestValue";
		String initialValue = "initial";
		String updatedValue = "updated";
		
		KVMessage response = null;
		Exception ex = null;

		try {
			kvClient.put(key, initialValue);
			response = kvClient.put(key, updatedValue);
			
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.PUT_UPDATE
				&& response.getValue().equals(updatedValue));
	}
	
	@Test
	public void testDelete() {
		String key = "deleteTestValue";
		String value = "toDelete";
		
		KVMessage response = null;
		Exception ex = null;

		try {
			kvClient.put(key, value);
			response = kvClient.put(key, "null");
			
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.DELETE_SUCCESS);
	}
	
	@Test
	public void testGet() {
		String key = "foo";
		String value = "bar";
		KVMessage response = null;
		Exception ex = null;

			try {
				kvClient.put(key, value);
				response = kvClient.get(key);
			} catch (Exception e) {
				ex = e;
			}
		
		assertTrue(ex == null && response.getValue().equals("bar"));
	}

	@Test
	public void testGetUnsetValue() {
		String key = "an unset value";
		KVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.get(key);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.GET_ERROR);
	}

	@Test
	/**
	 * Test the operation where we run a get on a value that was just deleted
	 */
	public void testGetDeletedValue() {
		String key = "key";
		String value = "value";
		KVMessage response = null;
		Exception ex = null;

		try {
			kvClient.put(key, value);
			kvClient.put(key, DELETE_STRING);
			response = kvClient.get(key);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.GET_ERROR);
	}

	@Test
	/**
	 * Test the operation where the key supplied is too long
	 */
	public void testKeyTooLong() {
		char[] k = getFilledCharArrayOfLength(25);
		String key = new String(k);
		String value = "value";
		KVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
		}
		String expectedErrorMsg = "Key of length 25 exceeds";
		assertTrue(ex == null &&
				response.getStatus() == StatusType.FAILED &&
				response.getKey().startsWith(expectedErrorMsg));
	}

	@Test
	/**
	 * Test the operation where the value supplied is too long
	 */
	public void testValueTooLong() {
		String key = "key";
		char[] v = getFilledCharArrayOfLength(121 * 1000);
		String value = "value";
		String longValue = new String(v);
		KVMessage putResponse = null;
		KVMessage getResponse = null;

		Exception ex = null;

		try {
			kvClient.put(key, value);
			putResponse = kvClient.put(key, longValue);
			getResponse = kvClient.get(key);
		} catch (Exception e) {
			ex = e;
		}
		String expectedErrorMsg = "Value of length 121000 exceeds";
		assertTrue(ex == null &&
				putResponse.getStatus() == StatusType.FAILED &&
				putResponse.getKey().startsWith(expectedErrorMsg));

		assertTrue(getResponse.getStatus() == StatusType.GET_SUCCESS &&
				getResponse.getValue().equals(value));

	}

	@Test
	public void testDeleteError() {
		String key = "deleteTestValue";
		String value = "toDelete";

		KVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.put(key, null);

		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.DELETE_ERROR);
	}

	private char[] getFilledCharArrayOfLength(int n){
		char[] a = new char[n];
		Arrays.fill(a, 'c');
		return a;
	}
	


}
