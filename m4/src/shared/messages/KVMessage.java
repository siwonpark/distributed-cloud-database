package shared.messages;

import ecs.ECSNode;

import java.util.ArrayList;
import java.util.TreeMap;

public interface KVMessage {
	
	public enum StatusType {
		GET, 			/* Get - request */
		GET_ERROR, 		/* requested tuple (i.e. value) not found */
		GET_SUCCESS, 	/* requested tuple (i.e. value) found */
		PUT, 			/* Put - request */
		PUT_SUCCESS, 	/* Put - request successful, tuple inserted */
		PUT_UPDATE, 	/* Put - request successful, i.e. value updated */
		PUT_ERROR, 		/* Put - request not successful */
		DELETE_SUCCESS, /* Delete - request successful */
		DELETE_ERROR, 	/* Delete - request successful */
		FAILED, 		/* Request failed for some general reason e.g. improper message format */
		HEARTBEAT,		/* Heartbeat - for KVClient to make sure KVServer is alive */

		SERVER_STOPPED, /* Server is stopped, no requests are processed */
		SERVER_WRITE_LOCK, /* Server locked for write, only get possible */
		SERVER_NOT_RESPONSIBLE, /* Request not successful, server not responsible for key */
		DATA_MIGRATION, /* Server is sending other server data as part of data migration process */
		COMMIT_TRANSACTION, /* This message is part of a transaction command */
		COMMIT_SUCCESS, /* The transaction commit succeeded */
		COMMIT_FAILURE, /* The transaction commit failed */

	}

	/**
	 * @return the key that is associated with this message,
	 * 		null if not key is associated.
	 */
	public String getKey();

	/**
	 * @return the value that is associated with this message,
	 * 		null if not value is associated.
	 */
	public String getValue();

	/**
	 * @return a status string that is used to identify request types,
	 * response types and error types associated to the message.
	 */
	public StatusType getStatus();

	/**
	 * @return the server metadata that is associated with this message,
	 * Null if no server data is associated
	 */
	public TreeMap<String, ECSNode> getServerMetadata();

	/**
	 * @return The operations related to this commit related message
	 * Null if no operations / this is not a commit msg
	 */
	public ArrayList<Message> getOperations();

}


