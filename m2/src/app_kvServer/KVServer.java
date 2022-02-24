package app_kvServer;


import ecs.ECSNode;

import persistence.DataBase;

import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.ZooKeeper;
import shared.MetadataUtils;
import shared.communication.CommModule;

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Objects;
import java.util.TreeMap;

import static shared.LogUtils.setLevel;
import static shared.PrintUtils.printError;
import static shared.PrintUtils.printPossibleLogLevels;

public class KVServer extends Thread implements IKVServer {

	private static Logger logger = Logger.getRootLogger();
	private ServerSocket serverSocket;
	private static int MAX_NUMBER = 2000;
	private int port;
	private int cacheSize;
	private String strategy;
	private boolean isRunning;

	private String name;
	private String zkHost;
	private int zkPort;
	private ZooKeeper zookeeper;
	private boolean lockWrite;
	private ServerState state;

	// Used for server->server communication when moving data
	private CommModule commModule;
	private TreeMap<String, ECSNode> metadata;
	private String hostName;


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
	public KVServer(int port, String hostname, int cacheSize, String strategy) {
		this.port = port;
		this.cacheSize = cacheSize;
		this.strategy = strategy;

		// TODO: We need to change this once we have a way to
		// Change the server state from ECS in the tests
		this.state = ServerState.RUNNING;
		this.lockWrite = false;
		this.hostName = hostname;



		this.db = DataBase.initInstance(this.cacheSize, this.strategy, "a database some",false);//we should get a name from outside the KVSever.

	}
	
	@Override
	public int getPort(){
		return serverSocket.getLocalPort();
	}

	@Override
    public String getHostname(){
		return this.hostName == null ? serverSocket.getInetAddress().getHostName():
				this.hostName;
	}

	@Override
    public CacheStrategy getCacheStrategy(){
		return IKVServer.CacheStrategy.None;
	}

	@Override
    public int getCacheSize(){
		// TODO Auto-generated method stub
		return this.cacheSize;
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
		// TODO Auto-generated method stub
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
		// TODO Auto-generated method stub
		db.clearCache();
	}

	@Override
    public void clearStorage(){
		// TODO Auto-generated method stub
		db.deleteHistory();
	}

	/**
	 * Starts the KVServer, all client and ECS requests are processed
	 */
	public void startServer(){
		this.state = ServerState.RUNNING;
	}

	/**
	 * Stop the KVServer, all clients requests are rejected,
	 * Only ECS requests are processed
	 */
	public void stopServer(){
		this.state = ServerState.STOPPED;
	}

	/**
	 * Exit the KVServer application
	 */
	public void shutDown(){
		isRunning = false;
	}

	/**
	 * Lock this KVServer for write operations
	 */
	public void lockWrite(){
		this.lockWrite = true;
	}

	/**
	 * Unlock this KVServer for write operations
	 */
	public void unlockWrite(){
		this.lockWrite = false;
	}

	/**
	 * Transfer a subset (range) of the KVServer’s data to another KVServer
	 * (reallocation before removing this server or adding a new KVServer to the ring);
	 * send a notification to the ECS, if data transfer is completed.
	 * Internally, this function creates a new thread to send the migration
	 * data to the destination server. This is so that we don't block client reads
	 * On the KVServer.
	 *
	 * @param range The subset of this Server's data to transfer to the new server
	 * @param server The new server to move data to
	 */
	public void moveData(String[] range, String server) throws IOException, RuntimeException {
		DataMigrationManager migrationMgr = new DataMigrationManager(server, metadata, range, db);
		new Thread(migrationMgr).start();
	}

	/**
	 * Update the metadata repository of this KVServer
	 */
	public void update(TreeMap<String, ECSNode> metadata){
		this.metadata = metadata;
	}

	public TreeMap<String, ECSNode> getMetadata(){
		return this.metadata;
	}

	/**
	 * Is the server write locked?
	 * @return True if the server is write locked, false otherwise
	 */
	public boolean isWriteLocked(){
		return this.lockWrite;
	}

	/**
	 * Get the state of this server
	 * @return The state of the server
	 */
	public ServerState getServerState(){
		return this.state;
	}

	/**
	 * Is this server responsible for key
	 * @param key The key to query
	 * @return True if server is responsible, false otherwise
	 */
	public boolean isResponsibleForKey(String key){
		if (this.metadata == null){
			// TODO: Remove this once updates to metadata thru ECS is implemented
			return true;
		}
		ECSNode responsibleServer = MetadataUtils.getResponsibleServerForKey(key, metadata);
		assert responsibleServer != null;
		return responsibleServer.getNodePort() == port &&
				Objects.equals(responsibleServer.getNodeHost(), getHostname());
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
			if(args.length < 2 || args.length > 3) {
				System.out.println("Error! Invalid number of arguments!");
				System.out.println("Usage: Server <port> <host> [<logLevel>]!");
			} else {
				new LogSetup("logs/server.log", Level.ALL);
				int port = Integer.parseInt(args[0]);
				String host = args[1];

				if (args.length == 3) {
					String level = setLevel(args[2]);
					if (level.equals(LogSetup.UNKNOWN_LEVEL)) {
						printError("Not a valid log level!");
						printPossibleLogLevels();
						return;
					}
				}

				new KVServer(port, host, 0, null).start();
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
