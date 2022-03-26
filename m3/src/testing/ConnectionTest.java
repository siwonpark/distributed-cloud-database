package testing;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.TreeMap;

import client.KVStore;

import ecs.ECSNode;
import ecs.IECSNode;
import junit.framework.TestCase;
import shared.MetadataUtils;
import shared.messages.KVMessage;

import static shared.PrintUtils.DELETE_STRING;
import static testing.AllTests.*;


public class ConnectionTest extends TestCase {

	@Override
	protected void setUp(){

	}

	@Override
	protected void tearDown(){
		ecs.shutdown();
		ecs.addNode(CACHE_STRATEGY, CACHE_SIZE);
		ecs.start();
	}
	
	public void testConnectionSuccess() {
		
		Exception ex = null;
		
		KVStore kvClient = new KVStore("localhost", port);
		try {
			kvClient.connect();
		} catch (Exception e) {
			ex = e;
		}	
		
		assertNull(ex);
	}
	
	
	public void testUnknownHost() {
		Exception ex = null;
		KVStore kvClient = new KVStore("unknown", port);
		
		try {
			kvClient.connect();
		} catch (Exception e) {
			ex = e; 
		}
		
		assertTrue(ex instanceof UnknownHostException);
	}
	
	
	public void testIllegalPort() {
		Exception ex = null;
		KVStore kvClient = new KVStore("localhost", 1238019283);
		
		try {
			kvClient.connect();
		} catch (Exception e) {
			ex = e; 
		}
		
		assertTrue(ex instanceof IllegalArgumentException);
	}

	/**
	 * Test the graceful client error handling mechanism
	 * For the case that there is only one server in the hash ring
	 */
	public void testServerDisconnectNoHandling(){
		Exception ex = null;
		ArrayList<String> addedKeys = new ArrayList<>();
		// start with no nodes
		ecs.shutdown();

		// add 3 nodes
		IECSNode[] addedNodes = ecs.addNodes(3, CACHE_STRATEGY, CACHE_SIZE).toArray(new IECSNode[0]);

		// start service
		ecs.start();
		try {
			// start kv client and connect to one node
			KVStore kvClient = new KVStore("localhost", addedNodes[0].getNodePort());
			kvClient.connect();

			// Remove the server that the client is connected to.
			// Since the client has not put any keys, the client has no access to metadata
			// This means that it should disconnect successfully without connecting to any other server.
			ArrayList<String> nodesToRemove = new ArrayList<>();
			nodesToRemove.add(addedNodes[0].getNodeName());
			ecs.removeNodes(nodesToRemove);
			assert(!kvClient.isRunning());
		} catch (Exception e)  {
			ex = e;
		}
		assertNull(ex);
	}


	/**
	 * Test the graceful client error handling mechanism
	 * For the case that there is only one server in the hash ring
	 */
	public void testServerDisconnectOneServerWithHandling(){
		Exception ex = null;
		ArrayList<String> addedKeys = new ArrayList<>();
		// start with no nodes
		ecs.shutdown();

		// add 3 nodes
		IECSNode[] addedNodes = ecs.addNodes(1, CACHE_STRATEGY, CACHE_SIZE).toArray(new IECSNode[0]);

		// start service
		ecs.start();
		try {
			// start kv client and connect to one node
			KVStore kvClient = new KVStore("localhost", addedNodes[0].getNodePort());
			kvClient.connect();
			// Put some keys
			kvClient.put("asdf", "asdf");
			kvClient.put("fdsa", "fdsa");

			// Remove the server that the client is connected to.
			// Since there's only one server in the storage service, the client
			// Should disconnect properly.
			ArrayList<String> nodesToRemove = new ArrayList<>();
			nodesToRemove.add(addedNodes[0].getNodeName());
			ecs.removeNodes(nodesToRemove);
			assert(!kvClient.isRunning());
		} catch (Exception e)  {
			ex = e;
		}
		assertNull(ex);
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
				KVStore kvStore = new KVStore("localhost", port);
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

