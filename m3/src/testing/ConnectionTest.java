package testing;

import java.net.UnknownHostException;
import java.util.*;

import app_kvClient.KVClient;
import client.KVStore;

import ecs.ECSNode;
import ecs.IECSNode;
import junit.framework.TestCase;
import shared.MetadataUtils;
import shared.messages.KVMessage;

import static java.lang.Thread.sleep;
import static shared.PrintUtils.DELETE_STRING;
import static testing.AllTests.*;


public class ConnectionTest extends TestCase {

	@Override
	protected void setUp(){

	}

	@Override
	protected void tearDown(){
		ecs.shutdown();
		ECSNode node = (ECSNode) ecs.addNode(CACHE_STRATEGY, CACHE_SIZE);
		port = node.getNodePort();
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
			KVClient kvClient = new KVClient();
			kvClient.newConnection("localhost", addedNodes[0].getNodePort());
			KVStore kvStore = (KVStore) kvClient.getStore();

			// Remove the server that the client is connected to.
			// Since the client has not put any keys, the client has no access to metadata
			// This means that it should disconnect successfully without connecting to any other server.
			ArrayList<String> nodesToRemove = new ArrayList<>();
			nodesToRemove.add(addedNodes[0].getNodeName());
			ecs.removeNodes(nodesToRemove);
			sleep(2000);

			assert(!kvStore.isRunning());
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
		// start with no nodes
		ecs.shutdown();

		// add 3 nodes
		IECSNode[] addedNodes = ecs.addNodes(1, CACHE_STRATEGY, CACHE_SIZE).toArray(new IECSNode[0]);

		// start service
		ecs.start();
		try {
			// start kv client and connect to one node
			KVClient kvClient = new KVClient();
			kvClient.newConnection("localhost", addedNodes[0].getNodePort());
			KVStore kvStore = (KVStore) kvClient.getStore();
			// Put some keys
			kvStore.put("asdf", "asdf");
			kvStore.put("fdsa", "fdsa");

			// Remove the server that the client is connected to.
			// Since there's only one server in the storage service, the client
			// Should disconnect properly.
			ArrayList<String> nodesToRemove = new ArrayList<>();
			nodesToRemove.add(addedNodes[0].getNodeName());
			ecs.removeNodes(nodesToRemove);
			sleep(2000);

			assert(!kvStore.isRunning());
		} catch (Exception e)  {
			ex = e;
		}
		assertNull(ex);
	}

	/**
	 * Test the graceful client error handling mechanism
	 * For the case that there are two servers in the hash ring
	 */
	public void testServerDisconnectTwoServersWithHandling(){
		Exception ex = null;
		// start with no nodes
		ecs.shutdown();

		// add 3 nodes
		IECSNode[] addedNodes = ecs.addNodes(2, CACHE_STRATEGY, CACHE_SIZE).toArray(new IECSNode[0]);

		// start service
		ecs.start();
		try {
			// start kv client and connect to one node
			KVClient kvClient = new KVClient();
			kvClient.newConnection("localhost", addedNodes[0].getNodePort());
			KVStore kvStore = (KVStore) kvClient.getStore();
			// Put some keys, until we get metadata in the client
			HashSet<String> needed =
					new HashSet<>(
							Arrays.asList(
									addedNodes[0].getNodeName(),
									addedNodes[1].getNodeName()));
			int num = 100;

			// populate datastore until all nodes responsible for at least one key
			while (!needed.isEmpty()) {
				ECSNode responsible = MetadataUtils.getResponsibleServerForKey(String.valueOf(num), (TreeMap<String, ECSNode>) ecs.getNodes());
				kvStore.put(String.valueOf(num), String.valueOf(num));
				needed.remove(responsible.getNodeName());
				num++;
			}

			// At this point, the client should have metadata of the hash ring
			// When we disconnect from one server, it should connect to the other
			ArrayList<String> nodesToRemove = new ArrayList<>();
			nodesToRemove.add(addedNodes[0].getNodeName());
			ecs.removeNodes(nodesToRemove);
			sleep(2000);
			assert(kvStore.isRunning());
			assert(kvStore.getPort() == addedNodes[1].getNodePort());
			assert(Objects.equals(kvStore.getHost(), addedNodes[1].getNodeHost()));
		} catch (Exception e)  {
			ex = e;
		}
		assertNull(ex);
	}

	/**
	 * Test the graceful client error handling mechanism
	 * For the case that there are three servers in the hash ring
	 */
	public void testServerDisconnectManyServersWithHandling(){
		Exception ex = null;
		// start with no nodes
		ecs.shutdown();

		// add 3 nodes
		IECSNode[] addedNodes = ecs.addNodes(3, CACHE_STRATEGY, CACHE_SIZE).toArray(new IECSNode[0]);
		ArrayList<Integer> addedPorts = new ArrayList<>();
		for(IECSNode node: addedNodes){
			addedPorts.add(node.getNodePort());
		}


		// start service
		ecs.start();
		try {
			// start kv client and connect to one node
			KVClient kvClient = new KVClient();
			kvClient.newConnection("localhost", addedNodes[0].getNodePort());
			KVStore kvStore = (KVStore) kvClient.getStore();

			// Put some keys, until we get metadata in the client
			int num = 100;

			// populate datastore until all nodes responsible for at least one key
			boolean metadataUpdated = false;
			while (!metadataUpdated) {
				ECSNode responsible = MetadataUtils.getResponsibleServerForKey(String.valueOf(num), (TreeMap<String, ECSNode>) ecs.getNodes());
				kvStore.put(String.valueOf(num), String.valueOf(num));
				if(!Objects.equals(responsible.getNodeName(), addedNodes[0].getNodeName())){
					metadataUpdated = true;
				}
				num++;
			}

			// At this point, the client should have metadata of the hash ring
			// When we disconnect from one server, it should connect to the other
			for(int i = 0; i < 2; i ++){
				int previousPort = kvStore.getPort();
				ArrayList<String> nodesToRemove = new ArrayList<>();
				nodesToRemove.add(getClientConnectedNodeName(kvStore, addedNodes));
				ecs.removeNodes(nodesToRemove);
				sleep(2000);
				assert(!kvStore.isRunning());
				assert(kvStore.getPort() != previousPort);
				assert(addedPorts.contains(kvStore.getPort()));
			}
			// Now, we remove the last server in the ring, so the client finally disconnect
			ArrayList<String> nodesToRemove = new ArrayList<>();
			nodesToRemove.add(getClientConnectedNodeName(kvStore, addedNodes));
			ecs.removeNodes(nodesToRemove);
			sleep(2000);
			assert(!kvStore.isRunning());
		} catch (Exception e)  {
			ex = e;
		}
		assertNull(ex);
	}

	private String getClientConnectedNodeName(KVStore client, IECSNode[] addedNodes){
		int clientConnectedPort = client.getPort();
		for(IECSNode node: addedNodes){
			if(node.getNodePort() == clientConnectedPort){
				return node.getNodeName();
			}
		}
		throw new RuntimeException("Could not find nodename of client connected server");
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

