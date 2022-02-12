package testing;

import java.net.UnknownHostException;
import java.util.ArrayList;

import client.KVStore;

import junit.framework.TestCase;
import shared.messages.KVMessage;

import static shared.PrintUtils.DELETE_STRING;
import static testing.AllTests.PORT;


public class ConnectionTest extends TestCase {

	
	public void testConnectionSuccess() {
		
		Exception ex = null;
		
		KVStore kvClient = new KVStore("localhost", PORT);
		try {
			kvClient.connect();
		} catch (Exception e) {
			ex = e;
		}	
		
		assertNull(ex);
	}
	
	
	public void testUnknownHost() {
		Exception ex = null;
		KVStore kvClient = new KVStore("unknown", PORT);
		
		try {
			kvClient.connect();
		} catch (Exception e) {
			ex = e; 
		}
		
		assertTrue(ex instanceof UnknownHostException);
	}
	
	
	public void testIllegalPort() {
		Exception ex = null;
		KVStore kvClient = new KVStore("localhost", PORT+323);
		
		try {
			kvClient.connect();
		} catch (Exception e) {
			ex = e; 
		}
		
		assertTrue(ex instanceof IllegalArgumentException);
	}

	/**
	 * Test multiple client connections
	 */
	public void testMultipleClients(){

		ArrayList<KVStore> clients = new ArrayList<>();
		Exception ex = null;
		final int NUM_CLIENTS = 4;

		try{
			for(int i = 0; i < NUM_CLIENTS; i++){
				KVStore kvStore = new KVStore("localhost", PORT);
				kvStore.connect();
				clients.add(kvStore);
			}

			for(int i = 0; i < clients.size(); i++){
				KVStore client = clients.get(i);
				String key = "key";
				String value = String.valueOf(i);
				KVMessage response = client.put(key, value);
				KVMessage.StatusType expectedResponseType =
						i == 0 ? KVMessage.StatusType.PUT_SUCCESS : KVMessage.StatusType.PUT_UPDATE;
				assertTrue(response.getStatus() == expectedResponseType &&
						response.getValue().equals(value));
			}

			for (int i = 0; i < clients.size(); i++){
				String key = "key2";
				String value = "value2";
				KVStore client = clients.get(i);
				if (i % 2 == 0) {
					assertSame(client.put(key, value).getStatus(),
							KVMessage.StatusType.PUT_SUCCESS);
					assertTrue(client.get(key).getStatus() ==
							KVMessage.StatusType.GET_SUCCESS &&
							client.get(key).getValue().equals(value));
				} else{
					assertSame(client.put(key, DELETE_STRING).getStatus(),
							KVMessage.StatusType.DELETE_SUCCESS);
					assertSame(client.get(key).getStatus(),
							KVMessage.StatusType.GET_ERROR);
				}
			}

		} catch (Exception e) {
			ex = e;
		}
	}
}

