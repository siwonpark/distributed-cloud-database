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

	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 1024 * BUFFER_SIZE;

	/**
	 * Initialize KVStore with address and port of KVServer
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */
	public KVStore(String address, int port) throws UnknownHostException, IOException {
		CommModule commModule = new CommModule(address, port);
		listeners = new HashSet<ClientSocketListener>();

		setRunning(true);
	}

	public void setRunning(boolean run) {
		running = run;
	}

	@Override
	public void connect() throws Exception {
		commModule.connect();
		// receive successful connection response from comm module
		logger.info("Connection established");

		// TODO Auto-generated method stub
		setRunning(true);
		try {
			output = clientSocket.getOutputStream();
			input = clientSocket.getInputStream();

			while(isRunning()) {
				try {
					Message latestMsg = receiveMessage();
					for(ClientSocketListener listener : listeners) {
						listener.handleNewMessage(latestMsg);
					}
				} catch (IOException ioe) {
					if(isRunning()) {
						logger.error("Connection lost!");
						try {
							tearDownConnection();
							for(ClientSocketListener listener : listeners) {
								listener.handleStatus(
										SocketStatus.CONNECTION_LOST);
							}
						} catch (IOException e) {
							logger.error("Unable to close connection!");
						}
					}
				}
			}
		} catch (IOException ioe) {
			logger.error("Connection could not be established!");

		} finally {
			if(isRunning()) {
				closeConnection();
			}
		}
	}

	@Override
	public void disconnect() {
		// TODO Auto-generated method stub
		setRunning(false);
		logger.info("tearing down the connection ...");
		if (clientSocket != null) {

			clientSocket.close();
			clientSocket = null;
			logger.info("connection closed!");
		}
	}

	public void closeConnection() {
		logger.info("try to close connection ...");

		disconnect();
		for(ClientSocketListener listener : listeners) {
			listener.handleStatus(SocketStatus.DISCONNECTED);
		}
	}

	@Override
	public KVMessage put(String key, String value) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public KVMessage get(String key) throws Exception {
		// TODO Auto-generated method stub
		logger.info("Hello World");
		return null;
	}

	public boolean isRunning() {
		return running;
	}

	private Message receiveMessage() throws IOException {
		int index = 0;
		byte[] msgBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];

		byte[] statusByte = new byte[1];

		boolean reading = true;
		/* read the status byte, always the first byte in the message */
		int read = input.read(statusByte);
		if (read != 1) {
			logger.error("Did not receive correct status byte from server");
		}

		byte[]

		while (reading) {
			/* if buffer filled, copy to msg array */
			if (index == BUFFER_SIZE) {
				if (msgBytes == null) {
					tmp = new byte[BUFFER_SIZE];
					System.arraycopy(bufferBytes, 0, tmp, 0, BUFFER_SIZE);
				} else {
					tmp = new byte[msgBytes.length + BUFFER_SIZE];
					System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
					System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, BUFFER_SIZE);
				}

				msgBytes = tmp;

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
			if (msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
				reading = false;
			}

			/* read next char from stream */
			read = (byte) input.read();
		}

		/* build final String */
		Message msg = new Message(msgBytes);
		logger.info("Receive message:\t '" + msg.getMsg() + "'");
		return msg;
	}

}
