package shared.messages;

import ecs.ECSNode;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.TreeMap;

/**
 * Represents a simple text message, which is intended to be received and sent 
 * by the server.
 */
public class Message implements Serializable, KVMessage {

	private static Logger logger = Logger.getRootLogger();
	private static final long serialVersionUID = 5549512212003782618L;
	private String key;
	private String value;
	private StatusType status;

	/**
	 * The server metadata.
	 * A map corresponding to Server_Hash -> [Keyhash_range_start, Keyhash_range_end]
	 */
	private TreeMap<String, ECSNode> serverMetadata;

	/**
	 * The operations associated with this transaction
	 */
	private ArrayList<Message> operations;

	/**
     * Constructs a Message object with a given String that
     * forms the message.
     *
     * @param key the String that forms the key
	 * @param value the String that forms the value
	 * @param status the StatusType of the message
     */
	public Message(String key, String value, StatusType status) {
		this.key = key;
		this.value = value;
		this.status = status;
	}

	/**
	 * Instantiate a message class with server metadata
	 * Note that this message instantiation should only be used
	 * When the server is not responsible. I.e. with statusType
	 * SERVER_NOT_RESPONSIBLE
	 * @param metadata The server metadata
	 * @param status == StatusType.SERVER_NOT_RESPONSIBLE
	 */
	public Message(TreeMap<String, ECSNode> metadata, StatusType status){
		if(status != StatusType.SERVER_NOT_RESPONSIBLE){
			throw new IllegalArgumentException("Status Type must be SERVER_NOT_RESPONSIBLE");
		}
		this.serverMetadata = metadata;
		this.status = status;
	}

	/**
	 * Instantiate a message class with a commit operation
	 * Note that this message instantiation should only be used
	 * For TRANSACTIONs. I.e. when we are committing transactions,
	 * Or returning the response code from a committed transaction
	 * @param operations The operations to commit
	 * @param status == COMMIT_TRANSACTION, COMMIT_SUCCESS, COMMIT_FAILURE
	 */
	public Message(ArrayList<Message> operations, StatusType status){
		if(status != StatusType.COMMIT_TRANSACTION &&
				status != StatusType.COMMIT_SUCCESS &&
				status != StatusType.COMMIT_FAILURE
		){
			throw new IllegalArgumentException("Status Type must be SERVER_NOT_RESPONSIBLE");
		}
		this.operations = operations;
		this.status = status;
	}



	/**
	 * Helper method to make it easier to print out messages for server logs
	 * @return String
	 */
	public String getMessageString() {
		StringBuilder outputString = new StringBuilder("");
		outputString.append("Status: " + status);
		outputString.append(" Key: " + key);
		if (value != null){
			outputString.append(" Value: " + value);
		}

		return outputString.toString();
	}

	@Override
	public String getKey() {
		return this.key;
	}

	@Override
	public String getValue() {
		return this.value;
	}

	@Override
	public StatusType getStatus() {
		return this.status;
	}

	@Override
	public TreeMap<String, ECSNode> getServerMetadata() {
		return this.serverMetadata;
	}

	@Override
	public ArrayList<Message> getOperations() {
		return this.operations;
	}
}
