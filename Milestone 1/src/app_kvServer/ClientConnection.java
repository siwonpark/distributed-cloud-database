package app_kvServer;

import org.apache.log4j.Logger;
import shared.messages.KVMessage.StatusType;
import shared.messages.Message;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;


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
	private static final int MAX_KEY_BYTES = 20; // 20 Bytes
	private static final int MAX_VALUE_BYTES = 1000 * 120; // 1kByte

	private Socket clientSocket;
	private InputStream input;
	private OutputStream output;
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
			output = clientSocket.getOutputStream();
			input = clientSocket.getInputStream();

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
				if (clientSocket != null) {
					input.close();
					output.close();
					clientSocket.close();
				}
			} catch (IOException ioe) {
				logger.error("Error! Unable to tear down connection!", ioe);
			}
		}
	}

	public void handleRequest(Message message) throws IOException {
		StatusType responseStatus;
		String key = message.getKey();
		String value = message.getValue();

		if (key.length() > MAX_KEY_BYTES) {
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

		switch(message.getStatus()) {
			case GET:
				try {
					value = server.getKV(message.getKey());
					responseStatus = StatusType.GET_SUCCESS;
				} catch (Exception e) {
					logger.error("Unable to get value for key: " + message.getKey());
					responseStatus = StatusType.GET_ERROR;
				}
				break;
			case PUT:
				boolean isInStorage = server.inStorage(key);

				if (value == null) {
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
	};

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
	public void sendMessage(Message message) throws IOException{
		byte[] msgBytes = message.getMsgBytes();
		output.write(msgBytes, 0, msgBytes.length);
		output.flush();
		logger.debug("Send message: " + message.getMessageString());
	};

	public Message receiveMessage() throws IOException {
		/* read the status byte, always the first byte in the message */
		byte statusByte = (byte) input.read();

		if (statusByte == -1) {
			throw new IOException("Connection Lost!");
		}

		byte[] keyBytes = readText();
		byte[] valueBytes = null;

		int numRemainingBytes = input.available();

		if (numRemainingBytes > 0) {
			valueBytes = readText();
		}

		/* build final String */
		Message msg = new Message(keyBytes, valueBytes, statusByte);
		logger.debug("Received message: " + msg.getMessageString());
		return msg;
	};

	public byte[] readText() throws IOException {
		int index = 0;
		byte[] textBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];
		boolean reading = true;

		/* load start of message */
		byte read = (byte) input.read();
		if (read != 2) {
			return null;
		}

		while (reading && read != 3 /* 3 is the end of text control character */) {
			/* if buffer filled, copy to msg array */
			if (index == BUFFER_SIZE) {
				if (textBytes == null) {
					tmp = new byte[BUFFER_SIZE];
					System.arraycopy(bufferBytes, 0, tmp, 0, BUFFER_SIZE);
				} else {
					tmp = new byte[textBytes.length + BUFFER_SIZE];
					System.arraycopy(textBytes, 0, tmp, 0, textBytes.length);
					System.arraycopy(bufferBytes, 0, tmp, textBytes.length, BUFFER_SIZE);
				}

				textBytes = tmp;

				/* reset buffer after msgBytes is populated */
				bufferBytes = new byte[BUFFER_SIZE];
				index = 0;
			}

			/* only read valid characters, i.e. letters and numbers */
			if((read > 31 && read < 127)) {
				bufferBytes[index] = read;
				index++;
			}

			/* stop reading is DROP_SIZE is reached */
			if (textBytes != null && textBytes.length + index >= DROP_SIZE) {
				reading = false;
			}

			/* read next char from stream */
			read = (byte) input.read();
		}

		/* commit what's left in the buffer */
		if (textBytes == null){
			tmp = new byte[index];
			System.arraycopy(bufferBytes, 0, tmp, 0, index);
		} else {
			tmp = new byte[textBytes.length + index];
			System.arraycopy(textBytes, 0, tmp, 0, textBytes.length);
			System.arraycopy(bufferBytes, 0, tmp, textBytes.length, index);
		}

		textBytes = tmp;

		return textBytes;
	};
}
