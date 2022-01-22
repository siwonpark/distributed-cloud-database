package client;

import shared.messages.KVMessage;

import org.apache.log4j.Logger;

public class KVStore implements KVCommInterface {
	private Logger logger = Logger.getRootLogger();
	private boolean running = false;

	/**
	 * Initialize KVStore with address and port of KVServer
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */
	public KVStore(String address, int port) {
		// TODO Auto-generated method stub
		logger.info("Connection Established");
	}

	@Override
	public void connect() throws Exception {
		// TODO Auto-generated method stub
		running = true;

	}

	@Override
	public void disconnect() {
		// TODO Auto-generated method stub
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

}
