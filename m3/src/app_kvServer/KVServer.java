package app_kvServer;


import ecs.ECSNode;

import persistence.DataBase;

import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import shared.MetadataUtils;
import shared.communication.CommModule;

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Objects;
import java.util.TreeMap;
import java.util.HashMap;

import static shared.LogUtils.setLevel;
import static shared.PrintUtils.printError;
import static shared.PrintUtils.printPossibleLogLevels;
import static shared.PrintUtils.DELETE_STRING;

public class KVServer extends Thread implements IKVServer {

	private static Logger logger = Logger.getRootLogger();
	private ServerSocket serverSocket;
	private static int MAX_NUMBER = 2000;
	private int port;
	private int cacheSize;
	private String strategy;
	private boolean isRunning;

	private ZKWatcher zkWatcher;
	private boolean lockWrite;
	private ServerState state;
	private ArrayList<ClientConnection> clientConnections;

	// Used for server->server communication when moving data
	private CommModule commModule;
	private TreeMap<String, ECSNode> metadata;
	private String serverName;


	private DataBase db;

	HashMap<Long, ReplicationMsg> coordinatorBuffer;
	HashMap<Long, ReplicationMsg> middleReplicaBuffer;
	private boolean consistent = true;
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
	public KVServer(int port, String serverName, String zkHost,
					int zkPort, String strategy, int cacheSize) throws InterruptedException {
		this.port = port;
		this.cacheSize = cacheSize;
		this.strategy = strategy;

		this.state = ServerState.STOPPED;
		this.lockWrite = false;
		this.serverName = serverName;
		this.clientConnections = new ArrayList<>();
		
		this.db = DataBase.initInstance(this.cacheSize, this.strategy, this.serverName, true);
		this.coordinatorBuffer = new HashMap<Long, ReplicationMsg>();
		this.middleReplicaBuffer = new HashMap<Long, ReplicationMsg>();
		ECSCommandHandler ecsCommandHandler = new ECSCommandHandler(this);
		this.zkWatcher = new ZKWatcher(serverName, zkHost, zkPort, ecsCommandHandler);
		initZkWatcher();
	}

	private void initZkWatcher() throws InterruptedException {
		this.zkWatcher.connect();
		this.zkWatcher.connectedSignal.await();
		this.zkWatcher.create();
	}

	@Override
	public int getPort(){
		return serverSocket.getLocalPort();
	}

	@Override
    public String getHostname(){
		return serverSocket.getInetAddress().getHostName();
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
			return value != null && value != DELETE_STRING;
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
		return db.get(key);
	}

	// TODO make the buffer work like sliding window to ignore old resend putKV request if the kv is already in window (not urgent)

	public void putKVinCoordinator(String key, String value){
		logger.info("putKV in Coordinator Server Num: " + MetadataUtils.getServersNum(metadata));
		if(MetadataUtils.getServersNum(metadata) <= 1){// won't be any replica if there is only one server
			if(MetadataUtils.getServersNum(metadata) == 1){
				this.db.put(key, value);
			}else{
				logger.error(String.format("there shouldn't be %d servers on the ring", MetadataUtils.getServersNum(metadata)));
			}
		}else{
			this.consistent = false;
			this.coordinatorBuffer.put(ReplicationMsg.getSeq(), new ReplicationMsg(key, value, ReplicationMsg.getSeq(), ReplicationMsg.ReplicationMsgType.REPLICATE_MIDDLE_REPLICA));
			
			ReplicationMsg.increSeq();
			for(ReplicationMsg u : this.coordinatorBuffer.values()){
				this.sendReplicationMsg(u);
			}
		}
	}

	public void putKVinMiddleReplica(String key, String value, long seq){
		logger.info("putKV in Middle Replica Server Num: " + MetadataUtils.getServersNum(metadata));
		if(MetadataUtils.getServersNum(metadata) <= 2){
			if(MetadataUtils.getServersNum(metadata) == 2){// won't be any tail replica if there is only two servers
				this.db.put(key, value);
				this.sendReplicationMsg(new ReplicationMsg(key, value, seq, ReplicationMsg.ReplicationMsgType.ACK_FROM_MIDDLE_REPLICA));
			}else{
				logger.error(String.format("there shouldn't be %d servers on the ring when we do putKV in middle replica", MetadataUtils.getServersNum(metadata)));
			}
		}else{
			this.middleReplicaBuffer.put(seq, new ReplicationMsg(key, value, seq, ReplicationMsg.ReplicationMsgType.REPLICATE_TAIL));
			for(ReplicationMsg u : this.middleReplicaBuffer.values()){
				this.sendReplicationMsg(u);
			}
		}
	}

	public void putKVinTail(String key, String value, long seq){
		logger.info("putKV in Tail Replica");
		this.db.put(key, value);
		this.sendReplicationMsg(new ReplicationMsg(key, value, seq, ReplicationMsg.ReplicationMsgType.ACK_FROM_TAIL));
	}

	public void getAckFromTail(String key, String value, long seq){
		logger.info("get ACK from Tail Replica");
		ReplicationMsg inBuffer = middleReplicaBuffer.get(seq);
		if(inBuffer.key.equals(key) && inBuffer.value.equals(value)){
			this.db.put(inBuffer.key, inBuffer.value);
			middleReplicaBuffer.remove(seq);
			this.sendReplicationMsg(new ReplicationMsg(key, value, seq, ReplicationMsg.ReplicationMsgType.ACK_FROM_MIDDLE_REPLICA));
		}else{
			logger.error("recieve wrong ack from tail");
		}
	}

	public void getAckFromMiddleReplica(String key, String value, long seq){
		logger.info("get ACK from Middle Replica");
		ReplicationMsg inBuffer = coordinatorBuffer.get(seq);
		if(inBuffer.key.equals(key) && inBuffer.value.equals(value)){
			this.db.put(inBuffer.key, inBuffer.value);
			coordinatorBuffer.remove(seq);
			if(coordinatorBuffer.size() == 0){
				this.consistent = true;
			}
		}else{
			logger.error("recieve wrong ack from middle replica");
		}
	}

	public void sendReplicationMsg(ReplicationMsg msg){
		ECSNode dest = null;
		if(msg.type == ReplicationMsg.ReplicationMsgType.REPLICATE_MIDDLE_REPLICA 
		|| msg.type == ReplicationMsg.ReplicationMsgType.REPLICATE_TAIL){
			dest = MetadataUtils.getSuccessor(metadata, MetadataUtils.getServerNodeWithName(serverName, metadata));
		}else if(msg.type == ReplicationMsg.ReplicationMsgType.ACK_FROM_MIDDLE_REPLICA 
		|| msg.type == ReplicationMsg.ReplicationMsgType.ACK_FROM_TAIL){
			dest = MetadataUtils.getPredecessor(metadata, MetadataUtils.getServerNodeWithName(serverName, metadata));
		}
		if (dest == null){
			logger.error("can't get the destination in sendReplicationMsg!");
			return;
		}else{
			ReplicationMsgSender sender = new ReplicationMsgSender(dest, msg);
			new Thread(sender).start();
			return;
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
		logger.info(String.format("Starting server %s", serverName));
		this.state = ServerState.RUNNING;
		zkWatcher.setData();
	}

	/**
	 * Stop the KVServer, all clients requests are rejected,
	 * Only ECS requests are processed
	 */
	public void stopServer(){
		logger.info(String.format("Stopping server %s", serverName));
		this.state = ServerState.STOPPED;
		zkWatcher.setData();
	}

	/**
	 * Exit the KVServer application
	 */
	public void shutDown(){
		logger.info(String.format("Shutting down server %s", serverName));
		zkWatcher.setData();
		try {
			serverSocket.close();
		} catch (Exception e) {
			logger.error("Could not close server socket");
		}
		while(clientConnections.size() > 0){
			ClientConnection connection = clientConnections.get(0);
			try{
				connection.stop();
			} catch (IOException e){
				logger.warn("A clientConnection threw an exception while trying to stop");
			}
		}
		isRunning = false;
	}

	/**
	 * Lock this KVServer for write operations
	 */
	public void lockWrite(){
		logger.info(String.format("Locking writes for server %s", serverName));
		this.lockWrite = true;
		zkWatcher.setData();
	}

	/**
	 * Unlock this KVServer for write operations
	 */
	public void unlockWrite(){
		logger.info(String.format("Unlocking writes for server %s", serverName));
		this.lockWrite = false;
		zkWatcher.setData();
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
	public void moveData(String[] range, ECSNode server) {
		logger.info(String.format("Moving data from server %s to server %s",
				serverName, server.getNodeName()));
		DataMigrationManager migrationMgr = new DataMigrationManager(server, range, db, zkWatcher);
		new Thread(migrationMgr).start();
	}


	/**
	 * if this coordinator is not consistent now, just don't reply. 
	 * can't do the ack waiting at this function because the ack handling is the same thread, 
	 * just send ReplicationMsg again and wait for someone check the consistent status later
	 */
	public void forceConsistency(){
		if(this.consistent){
			zkWatcher.setData();
			return;
		}else{
			for(ReplicationMsg u : this.coordinatorBuffer.values()){
				this.sendReplicationMsg(u);
			}
			return;
		}
	}

	/**
	 * Update the metadata repository of this KVServer
	 */
	public void update(TreeMap<String, ECSNode> metadata){
		logger.info(String.format("Updating metadata for server %s", serverName));
		logger.info(metadata.toString());
		this.metadata = metadata;
		zkWatcher.setData();
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
			logger.error(String.format("Server %s does not have any metadata", serverName));
			return true;
		}
		ECSNode responsibleServer = MetadataUtils.getResponsibleServerForKey(key, metadata);
		assert responsibleServer != null;
		return responsibleServer.getNodePort() == port &&
				Objects.equals(responsibleServer.getNodeName(), this.serverName);
	}

	private String getServerHashName(){
		return "127.0.0.1:" + Integer.toString(this.port);
	}

	/**
	 * Is this server responsible for key (including replicas)
	 * @param key The key to query
	 * @return True if server is responsible, false otherwise
	 */
	public boolean isResponsibleForKeywithReplicas(String key){
		if (this.metadata == null){
			logger.error(String.format("Server %s does not have any metadata", serverName));
			return true;
		}
		ECSNode responsibleServer = MetadataUtils.getResponsibleServerForKey(key, metadata);
		assert responsibleServer != null;
		
		if(responsibleServer.getNodePort() == port && Objects.equals(responsibleServer.getNodeName(), this.serverName)){
			return true;
		}
		if(MetadataUtils.getServersNum(metadata) <= 1){
			return false;
		}
		ECSNode pre1 = MetadataUtils.getPredecessor(metadata,  MetadataUtils.getServerNodeWithName(serverName, metadata));
		if(responsibleServer.getNodePort() == pre1.getNodePort() && Objects.equals(responsibleServer.getNodeName(), pre1.getNodeName())){
			return true;
		}
		if(MetadataUtils.getServersNum(metadata) <= 2){
			return false;
		}
		ECSNode pre2 = MetadataUtils.getPredecessor(metadata,  MetadataUtils.getServerNodeWithName(pre1.getNodeName(), metadata));
		if(responsibleServer.getNodePort() == pre2.getNodePort() && Objects.equals(responsibleServer.getNodeName(), pre2.getNodeName())){
			return true;
		}
		return false;
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
					clientConnections.add(connection);
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

	public void removeConnection(ClientConnection connectionToRemove){
		clientConnections.remove(connectionToRemove);
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
			if(args.length < 6 || args.length > 7) {
				System.out.println("Error! Invalid number of arguments!");
				System.out.println("Usage: Server <port> <serverName> <zkHost> <zkPort> <cacheStrategy> <cacheSize> [<logLevel>]!");
			} else {
				String serverName = args[1];
				new LogSetup("logs/" + serverName + ".log", Level.ALL);
				int port = Integer.parseInt(args[0]);
				String zkHost = args[2];
				int zkPort = Integer.parseInt(args[3]);
				String cacheStrategy = args[4];
				int cacheSize = Integer.parseInt(args[5]);

				if (args.length == 7) {
					String level = setLevel(args[6]);
					if (level.equals(LogSetup.UNKNOWN_LEVEL)) {
						printError("Not a valid log level!");
						printPossibleLogLevels();
						return;
					}
				}

				try {
					new KVServer(port, serverName, zkHost, zkPort, cacheStrategy, cacheSize).start();
				} catch (InterruptedException e) {
					System.out.println("Unable to connect to zookeeper");
				}
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
