package client;

import app_kvClient.ClientSocketListener;
import app_kvClient.ClientSocketListener.SocketStatus;
import ecs.ECSNode;
import org.apache.log4j.Logger;
import shared.MetadataUtils;
import shared.communication.CommModule;
import shared.messages.KVMessage;
import shared.messages.Message;

import java.io.IOException;
import java.util.*;

import static shared.messages.KVMessage.StatusType.SERVER_NOT_RESPONSIBLE;

public class KVStore implements KVCommInterface {
	private Logger logger = Logger.getRootLogger();
	private boolean running = false;
	private CommModule commModule;
	private CommModule dynamicCommModule;
	private Set<ClientSocketListener> listeners;
	private TreeMap<String, ECSNode> serverMetadata;
	private String address;
	private int port;

	/**
	 * Initialize KVStore with address and port of KVServer
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */
	public KVStore(String address, int port) {
		this.address = address;
		this.port = port;
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
			// We need two try blocks here since the dynamic communication module may
			// Be connected to a server that is removed by ECS
			// So an IOException on disconnect may not be an actual error.
			try{
				if(dynamicCommModule != null) {
					dynamicCommModule.disconnect();
				}
			} catch (IOException e){
				logger.warn("The dynamic communication module was not able to be disconnected");
			}
		}
	}

	@Override
	public KVMessage put(String key, String value) throws Exception {
		if(!isRunning()){
			throw new RuntimeException("KVStore not connected!");
		}
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
		if(!isRunning()){
			throw new RuntimeException("KVStore not connected!");
		}
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
		// The null case should only happen if we send a retry to a server that
		// gets taken down
		while (response == null || response.getStatus() == SERVER_NOT_RESPONSIBLE){
			// update metadata
			if (response == null) {
				this.serverMetadata = null;
			} else {
				this.serverMetadata = response.getServerMetadata();
			}
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
	private Message sendMessageToCorrectServer(Message msg) {
		try {
			connectToCorrectServer(msg);
			dynamicCommModule.sendMessage(msg);
			Message response = dynamicCommModule.receiveMessage();
			return response;
		} catch (IOException e) {
			// When an IOException is thrown when sending the
			// Msg to the correct server (e.g. the server we routed to was taken down),
			// We want to set the serverMetadata to null
			// So that can get a metadata update and try again.
			return null;
		}
	}

	/**
	 * Parse msg to find the specified key, then find the server
	 * responsible for that key. Connect the CommModule to this server.
	 */
	private void connectToCorrectServer(Message msg) throws IOException {
		// If there is no previous metadata stored,
		// We just naively send the message to the server that the
		// User initialized, so we can return early from here
		String host;
		int port;
		if (serverMetadata == null) {
			host = this.address;
			port = this.port;
		} else {
			ECSNode responsibleServer = MetadataUtils.getResponsibleServerForKey(msg.getKey(), serverMetadata);
			if (responsibleServer == null) {
				logger.error("Could not find a responsible server in the server metadata");
				return;
			}
			host = responsibleServer.getNodeHost();
			port = responsibleServer.getNodePort();
		}
		if (dynamicCommModule != null) {
			if(dynamicCommModule.getPort() == port &&
					Objects.equals(dynamicCommModule.getHost(), host)){
				// We can return early if its already connected to the right host/port
				return;
			} else {
				try {
					dynamicCommModule.disconnect();
				} catch (IOException e) {
					logger.warn("The dynamicCommModule was unable to be closed.");
				}
			}
		}
		dynamicCommModule = new CommModule(host, port);
		dynamicCommModule.connect();
	}

	/**
	 *
	 * @return true if was able to connect to another server, false otherwise
	 */
	public boolean tryConnectingOtherServer(){
		System.out.println("Attempting to connect to other servers in the storage service");
		if(this.serverMetadata == null){
			return false;
		}
		for (ECSNode server : serverMetadata.values()){
			if(Objects.equals(server.getNodeHost(), getHost()) &&
					server.getNodePort() == getPort()){
				continue;
			}
			try {
				commModule = new CommModule(server.getNodeHost(), server.getNodePort());
				commModule.connect();
				this.port = server.getNodePort();
				this.address = server.getNodeHost();
				System.out.printf("Able to maintain connection to storage service via new server %s / %s",
						this.address, this.port);
			} catch (IOException e){
				logger.warn("Could not connect to a server in the cached metadata");
			}
		}
		return false;
	}

	public int getPort(){
		return this.port;
	}

	public String getHost(){
		return this.address;
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
