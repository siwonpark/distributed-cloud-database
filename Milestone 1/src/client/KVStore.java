package client;

import app_kvClient.ClientSocketListener;
import app_kvClient.ClientSocketListener.SocketStatus;
import shared.messages.KVMessage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;
import shared.messages.Message;

public class KVStore implements KVCommInterface {
	private Logger logger = Logger.getRootLogger();
	private boolean running = false;
	private Socket clientSocket;
	private CommModule commModule;
	private Set<ClientSocketListener> listeners;
	private OutputStream output;
	private InputStream input;


	/**
	 * Initialize KVStore with address and port of KVServer
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */
	public KVStore(String address, int port) {
		commModule = new CommModule(address, port);
		listeners = new HashSet<ClientSocketListener>();
		setRunning(true);
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
			commModule.sendMessage(msg);
			Message response = commModule.receiveMessage();
			return response;
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
			commModule.sendMessage(msg);
			Message response = commModule.receiveMessage();
			return response; // Pass back to client to display on command line
		} catch (IOException e){
			// Inform listeners that connection was lost
			for(ClientSocketListener listener : listeners) {
				listener.handleStatus(SocketStatus.CONNECTION_LOST);
			}
			throw e;
		}
	}

	public boolean isRunning() {
		return running;
	}
	public void addListener(ClientSocketListener listener){
		listeners.add(listener);
	}

	private Message receiveMessage() throws IOException {
//		int index = 0;
//		byte[] msgBytes = null, tmp = null;
//		byte[] bufferBytes = new byte[BUFFER_SIZE];
//
//		byte[] statusByte = new byte[1];
//
//		boolean reading = true;
//		/* read the status byte, always the first byte in the message */
//		int read = input.read(statusByte);
//		if (read != 1) {
//			logger.error("Did not receive correct status byte from server");
//		}
//
//		byte[]
//
//		while (reading) {
//			/* if buffer filled, copy to msg array */
//			if (index == BUFFER_SIZE) {
//				if (msgBytes == null) {
//					tmp = new byte[BUFFER_SIZE];
//					System.arraycopy(bufferBytes, 0, tmp, 0, BUFFER_SIZE);
//				} else {
//					tmp = new byte[msgBytes.length + BUFFER_SIZE];
//					System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
//					System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, BUFFER_SIZE);
//				}
//
//				msgBytes = tmp;
//
//				/* reset buffer after msgBytes is populated */
//				bufferBytes = new byte[BUFFER_SIZE];
//				index = 0;
//			}
//
//			/* only read valid characters, i.e. letters and numbers */
//			if((read > 31 && read < 127)) {
//				bufferBytes[index] = read;
//				index++;
//			}
//
//			/* stop reading is DROP_SIZE is reached */
//			if (msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
//				reading = false;
//			}
//
//			/* read next char from stream */
//			read = (byte) input.read();
//		}
//
//		/* build final String */
//		Message msg = new Message(msgBytes);
//		logger.info("Receive message:\t '" + msg.getMsg() + "'");
//		return msg;
		return null;
	}
}
