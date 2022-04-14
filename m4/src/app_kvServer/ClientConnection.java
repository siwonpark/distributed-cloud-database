package app_kvServer;

import org.apache.log4j.Logger;
import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;
import shared.messages.Message;

import java.io.*;
import java.net.Socket;

import java.util.ArrayList;
import static shared.PrintUtils.DELETE_STRING;


/**
 * Represents a connection end point for a particular client that is 
 * connected to the server. This class is responsible for message reception 
 * and sending. 
 * The class also implements the echo functionality. Thus whenever a message 
 * is received it is going to be echoed back to the client.
 */
public class ClientConnection implements Runnable {

	private static Logger logger = Logger.getRootLogger();
	
	private boolean isOpen;
	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 128 * BUFFER_SIZE;
	public static final int MAX_KEY_BYTES = 20; // 20 Bytes
	public static final int MAX_VALUE_BYTES = 1000 * 120; // 1kByte

	private Socket clientSocket;
	private ObjectInputStream input;
	private ObjectOutputStream output;
	private KVServer server;
	
	/**
	 * Constructs a new CientConnection object for a given TCP socket.
	 * @param clientSocket the Socket object for the client connection.
	 */
	public ClientConnection(Socket clientSocket, KVServer server) {
		this.clientSocket = clientSocket;
		this.isOpen = true;
		this.server = server;
	}
	
	/**
	 * Initializes and starts the client connection. 
	 * Loops until the connection is closed or aborted by the client.
	 */
	public void run() {
		try {
			output = new ObjectOutputStream(clientSocket.getOutputStream());
			input = new ObjectInputStream(clientSocket.getInputStream());

			while(isOpen) {
				try {
					Message latestMsg = receiveMessage();
					handleRequest(latestMsg);
					
				/* connection either terminated by the client or lost due to 
				 * network problems*/	
				} catch (IOException ioe) {
					logger.error("Error! Connection lost!");
					isOpen = false;
				}
			}
			
		} catch (IOException ioe) {
			logger.error("Error! Connection could not be established!", ioe);
			
		} finally {
			try {
				stop();
			} catch (IOException ioe) {
				logger.error("Error! Unable to tear down connection!", ioe);
			}
		}
	}

	public void stop() throws IOException{
		logger.info("Stopping clientConnection");
		isOpen = false;
		server.removeConnection(this);
		if (clientSocket != null) {
			input.close();
			output.close();
			clientSocket.close();
			clientSocket = null;
		}
	}

	/**
	 * Takes the client request as a message and actions on it depending on the StatusType of the request.
	 * @param message
	 * @throws IOException
	 */
	public void handleRequest(Message message) throws IOException {
		StatusType responseStatus;
		String key = message.getKey();
		String value = message.getValue();

		if(message.getStatus() == StatusType.DATA_MIGRATION){
			// If this is a data migration message
			// We always want to store the key
			// Regardless of whether the server is write-locked
			try {
				server.putKV(key, value);
				responseStatus = StatusType.PUT_SUCCESS;
			} catch (Exception e) {
				responseStatus = StatusType.PUT_ERROR;
				logger.error("Unable to add to store: Key " + message.getKey() + " Value " + message.getValue());
			}
			Message response = new Message(key, value, responseStatus);
			sendMessage(response);
			return;
		}else if (server.getServerState() == IKVServer.ServerState.STOPPED){
			// Server is not accepting requests from client
			responseStatus = StatusType.SERVER_STOPPED;
			Message response = new Message(key, value, responseStatus);
			sendMessage(response);
			return;
		}
		else if (key != null && key.length() > MAX_KEY_BYTES) {
			String errorMsg = String.format("Key of length %s exceeds max key length (%s Bytes)",
					key.length(), MAX_KEY_BYTES);
			sendFailure(errorMsg);
			return;
		} else if (value != null && value.length() > MAX_VALUE_BYTES){
			String errorMsg = String.format("Value of length %s exceeds max key length (%s Bytes)",
					value.length(), MAX_VALUE_BYTES);
			sendFailure(errorMsg);
			return;
		}

		if (message.getStatus() == StatusType.COMMIT_TRANSACTION) {
			ArrayList<Message> operations = message.getOperations();
			Message repliesToOperations = server.handleOperations(operations);
			logger.info("get the message from ecs and send it to client");
			sendMessage(repliesToOperations);
			return;
		}

		switch(message.getStatus()) {
			case GET:
				try {
					if (!server.isResponsibleForKey(message.getKey())){
						responseStatus = StatusType.SERVER_NOT_RESPONSIBLE;
						Message response = new Message(server.getMetadata(), responseStatus);
						sendMessage(response);
						return;
					}
					value = server.getKV(message.getKey());
					responseStatus = StatusType.GET_SUCCESS;
				} catch (Exception e) {
					logger.error("Unable to get value for key: " + message.getKey());
					responseStatus = StatusType.GET_ERROR;
				}
				break;
			case PUT:
				if (server.isWriteLocked()) {
					responseStatus = StatusType.SERVER_WRITE_LOCK;
					break;
				}
				if (!server.isResponsibleForKey(message.getKey())){
					responseStatus = StatusType.SERVER_NOT_RESPONSIBLE;
					Message response = new Message(server.getMetadata(), responseStatus);
					sendMessage(response);
					return;
				}
				boolean isInStorage = server.inStorage(key);

				if (value == null || value.equals(DELETE_STRING)) {
					if (!isInStorage) {
						logger.error("Trying to delete key that does not exist: Key " + message.getKey());
						responseStatus = StatusType.DELETE_ERROR;
						break;
					}

					try {
						server.putKV(key, null);
						responseStatus = StatusType.DELETE_SUCCESS;
					} catch (Exception e) {
						logger.error("Unable to delete from store: Key " + message.getKey());
						responseStatus = StatusType.DELETE_ERROR;
					}
				} else {
					try {
						server.putKV(key, value);
						responseStatus = isInStorage ? StatusType.PUT_UPDATE : StatusType.PUT_SUCCESS;
					} catch (Exception e) {
						logger.error("Unable to add to store: Key " + message.getKey() + " Value " + message.getValue());
						responseStatus = StatusType.PUT_ERROR;
					}
				}
				break;
			case HEARTBEAT:
				logger.debug("Received heartbeat request from client");
				responseStatus = StatusType.HEARTBEAT;
				break;
				
			default:
				String errorMsg = "Request contained a status unknown to the server: " + message.getStatus();
				logger.error(errorMsg);
				key = errorMsg;
				responseStatus = StatusType.FAILED;
				break;
		}

		Message response = new Message(key, value, responseStatus);

		sendMessage(response);
	}

	/**
	 * Send a failure message back to client
	 * Note: This sends a message with StatusType.Failure!
	 */
	private void sendFailure(String errorMsg) throws IOException {
		logger.error(errorMsg);
		Message failedResponse = new Message(errorMsg, null, StatusType.FAILED);
		sendMessage(failedResponse);
	}

	/**
	 * Method sends a TextMessage using this socket.
	 * @throws IOException some I/O error regarding the output stream
	 */
	private void sendMessage(Message message) throws IOException{
		output.writeObject(message);
		output.flush();
		logger.debug("Send message: " + message.getMessageString());
	}

	/**
	 * Read from the socket input stream to get the messages based on the following format.
	 * StatusCode - TXTSTART - Key - TXTEND - TXTSTART - Value (optional) - TXTEND
	 * @return
	 * @throws IOException
	 */
	private Message receiveMessage() throws IOException {
		Message msg;
		try {
			msg = (Message) input.readObject();
		} catch (ClassNotFoundException e){
			msg = new Message("Message was not able to be read properly", null,
					KVMessage.StatusType.FAILED);
		}
		logger.debug("Receive Message: " + msg.getMessageString());
		return msg;
	}

}
