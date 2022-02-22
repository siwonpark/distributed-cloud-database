package app_kvServer;

import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;

import static shared.PrintUtils.printError;
import static shared.PrintUtils.printPossibleLogLevels;
import static shared.LogUtils.setLevel;

import persistence.DataBase;

public class KVServer extends Thread implements IKVServer {

	private static Logger logger = Logger.getRootLogger();
	private ServerSocket serverSocket;
	private static int MAX_NUMBER = 2000;
	private int port;
	private int cacheSize;
	private String strategy;
	private boolean isRunning;
	private DataBase db;
	/**
	 * Start KV Server at given port
	 * @param port given port for storage server to operate
	 * @param cacheSize specifies how many key-value pairs the server is allowed
	 *           to keep in-memory
	 * @param strategy specifies the cache replacement strategy in case the cache
	 *           is full and there is a GET- or PUT-request on a key that is
	 *           currently not contained in the cache. Options are "FIFO", "LRU",
	 *           and "LFU".
	 */
	public KVServer(int port, int cacheSize, String strategy) {
		// TODO Auto-generated method stub
		this.port = port;
		this.cacheSize = cacheSize;
		this.strategy = strategy;
		this.db = DataBase.initInstance(this.cacheSize, this.strategy, false);
	}

	@Override
	public int getPort(){
		// TODO Auto-generated method stub
		return serverSocket.getLocalPort();
	}

	@Override
    public String getHostname(){
		// TODO Auto-generated method stub
		return serverSocket.getInetAddress().getHostName();
	}

	@Override
    public CacheStrategy getCacheStrategy(){
		// TODO Auto-generated method stub
		return IKVServer.CacheStrategy.None;
	}

	@Override
    public int getCacheSize(){
		// TODO Auto-generated method stub
		return -1;
	}

	@Override
    public boolean inStorage(String key){
		// TODO Auto-generated method stub
		try{
			String value = db.get(key);
			return value != null;
		} catch (Exception e){
			return false;
		}
	}

	@Override
    public boolean inCache(String key){
		// the unit of cache is node rather than key. and the cache visit is in the database level.
		return false;
	}

	@Override
    public String getKV(String key) throws Exception{
		String result = db.get(key);
		if (result == null){
			throw new RuntimeException(String.format("No such key %s exists", key));
		} else {
			return result;
		}
	}

	@Override
    public void putKV(String key, String value) throws Exception{
		db.put(key, value);
	}

	@Override
    public void clearCache(){
		db.clearCache();
	}

	@Override
    public void clearStorage(){
		db.deleteHistory();
	}

	@Override
    public void run(){
		isRunning = initializeServer();
		if (serverSocket != null) {
			while (isRunning) {
				try {
					Socket client = serverSocket.accept();
					ClientConnection connection =
							new ClientConnection(client, this);
					new Thread(connection).start();

					logger.info("Connected to "
							+ client.getInetAddress().getHostName()
							+  " on port " + client.getPort());
				} catch (IOException e) {
					logger.error("Error! " +
							"Unable to establish connection. \n", e);
				}
			}
		}
		logger.info("Server stopped.");
	}

	@Override
    public void kill(){
		// TODO Auto-generated method stub
	}

	@Override
    public void close(){
		// TODO Auto-generated method stub
	}

	/**
	 * Initialize the server by opening up a ServerSocket to listen to incoming connection requests.
	 * @return
	 */
	private boolean initializeServer() {
		logger.info("Initialize server ...");
		try {
			serverSocket = new ServerSocket(port);
			logger.info("Server listening on port: "
					+ serverSocket.getLocalPort());
			return true;

		} catch (IOException e) {
			logger.error("Error! Cannot open server socket:");
			if (e instanceof BindException) {
				logger.error("Port " + port + " is already bound!");
			}
			return false;
		}
	}

	/**
	 * Main entry point for the echo server application.
	 * @param args contains the port number at args[0].
	 */
	public static void main(String[] args) {
		try {
			if(args.length < 1 || args.length > 2) {
				System.out.println("Error! Invalid number of arguments!");
				System.out.println("Usage: Server <port> [<logLevel>]!");
			} else {
				new LogSetup("logs/server.log", Level.ALL);
				int port = Integer.parseInt(args[0]);

				if (args.length == 2) {
					String level = setLevel(args[1]);
					if (level.equals(LogSetup.UNKNOWN_LEVEL)) {
						printError("Not a valid log level!");
						printPossibleLogLevels();
						return;
					}
				}

				new KVServer(port, 0, null).start();
			}
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
		} catch (NumberFormatException nfe) {
			System.out.println("Error! Invalid argument <port>! Not a number!");
			System.out.println("Usage: Server <port>!");
			System.exit(1);
		}
	}
}
