package client;

import app_kvClient.ClientSocketListener;
import app_kvClient.ClientSocketListener.SocketStatus;
import ecs.ECSNode;
import org.apache.log4j.Logger;
import shared.messages.KVMessage;
import shared.messages.Message;
import testing.HashUtils;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static shared.messages.KVMessage.StatusType.SERVER_NOT_RESPONSIBLE;

public class KVStore implements KVCommInterface {
	private Logger logger = Logger.getRootLogger();
	private boolean running = false;
	private CommModule commModule;
	private Set<ClientSocketListener> listeners;
	private TreeMap<String, ECSNode> serverMetadata;


	/**
	 * Initialize KVStore with address and port of KVServer
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */
	public KVStore(String address, int port) {
		commModule = new CommModule(address, port);
		listeners = new HashSet<ClientSocketListener>();
	}

	public void setRunning(boolean run) {
		running = run;
	}

	@Override
	public void connect() throws Exception {
		commModule.connect(); // Throws error if it fails
		setRunning(true);
		logger.info("Connection established");
	}

	@Override
	public synchronized void disconnect() {
		// TODO Auto-generated method stub
		logger.info("Requesting communication module to close the connection..");
		if (commModule != null) {
			try {
				commModule.disconnect();
				setRunning(false);
				for(ClientSocketListener listener : listeners) {
					listener.handleStatus(SocketStatus.DISCONNECTED);
				}
			} catch (IOException e) {
				logger.error("Client Socket was unable to close the connection. Error: " + e);
			}
		}
	}

	@Override
	public KVMessage put(String key, String value) throws Exception {
		// Note, this PUT operation works as DELETE if value is null.
		logger.info(String.format("Putting value %s into key %s", key, value));
		Message msg = new Message(key, value, KVMessage.StatusType.PUT);
		try {
			return retryMessageUntilSuccess(msg);
		} catch (IOException e) {
			for(ClientSocketListener listener : listeners) {
				listener.handleStatus(SocketStatus.CONNECTION_LOST);
			}
			throw e;
		}
	}

	@Override
	public KVMessage get(String key) throws Exception {
		logger.info(String.format("Getting key %s", key));
		Message msg = new Message(key, null, KVMessage.StatusType.GET);
		try {
			return retryMessageUntilSuccess(msg); // Pass back to client to display on command line
		} catch (IOException e){
			// Inform listeners that connection was lost
			for(ClientSocketListener listener : listeners) {
				listener.handleStatus(SocketStatus.CONNECTION_LOST);
			}
			throw e;
		}
	}

	/**
	 * Try sending msg to a server
	 * If cached serverMetadata exists, this method will connect
	 * To the server responsible for the key in msg, based on the
	 * currently cached metadata
	 *
	 * If the server responds with SERVER_NOT_RESPONSIBLE,
	 * This method will update the metadata, and retry the message
	 * (based on the new metadata), until the server responds with something
	 * Other than SERVER_NOT_RESPONSIBLE
	 *
	 * @param msg The msg to send
	 * @return The successful server response
	 * @throws IOException
	 */
	private Message retryMessageUntilSuccess(Message msg) throws IOException {
		Message response = sendMessageToCorrectServer(msg);
		while (response.getStatus() == SERVER_NOT_RESPONSIBLE){
			// update metadata
			this.serverMetadata = response.getServerMetadata();
			response = sendMessageToCorrectServer(msg);
		}
		return response;
	}

	/**
	 * Send a message to a server
	 * If there is cached metadata, try to connect
	 * To the correct host first, before sending
	 * @param msg The message to send
	 * @return The server response
	 * @throws IOException
	 */
	private Message sendMessageToCorrectServer(Message msg) throws IOException {
		connectToCorrectServer(msg);
		commModule.sendMessage(msg);
		Message response = commModule.receiveMessage();
		return response;
	}

	/**
	 * Parse msg to find the specified key, then find the server
	 * responsible for that key. Connect the CommModule to this server.
	 */
	private void connectToCorrectServer(Message msg) throws IOException {
		// If there is no previous metadata stored,
		// We just naively send the message to the server that the
		// User initialized, so we can return early from here
		if(serverMetadata == null){
			return;
		}
		ECSNode responsibleServer = findCorrectServerForKey(msg.getKey());
		if(responsibleServer == null){
			logger.error("Could not find a responsible server in the server metadata");
			return;
		}
		String host = responsibleServer.getNodeHost();
		int port = responsibleServer.getNodePort();
		commModule.disconnect();
		commModule = new CommModule(host, port);
		commModule.connect();
	}

	private ECSNode findCorrectServerForKey(String key){
		String keyHash = HashUtils.computeHash(key);
		for(Map.Entry<String,ECSNode> entry : serverMetadata.entrySet()){
			String serverHash = entry.getKey();
			ECSNode ecsNode = entry.getValue();
			if (ecsNode.isReponsibleForKey(keyHash)){
				return ecsNode;
			}
		}
		return null;
	}

	/**
	 * Is KVStore running?
	 * @return True if KVstore is running, false otherwise
	 */
	public boolean isRunning() {
		return running;
	}

	/**
	 * Add a listener to KVStore
	 * @param listener The listener to add
	 */
	public void addListener(ClientSocketListener listener){
		listeners.add(listener);
	}

	/**
	 * Send a no-op to the server, used to check for liveness
	 * @return The returned message from the server
	 * @throws IOException If no connection can be made with the server
	 */
	public KVMessage heartbeat() throws IOException {
		logger.debug("Sending heartbeat");
		String key = "Heartbeat";
		Message msg = new Message(key, null, KVMessage.StatusType.HEARTBEAT);
		commModule.sendMessage(msg);
		Message response = commModule.receiveMessage();
		return response;
	}
}
