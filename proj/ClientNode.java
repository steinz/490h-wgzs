import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

// Implicit transactions are handled by CC

public class ClientNode {

	/**
	 * Possible cache statuses
	 */
	public static enum CacheStatuses {
		ReadWrite, ReadOnly
	};
	
	/**
	 * Operation types the client can remember in a PendingClientOperation
	 */
	protected static enum ClientOperation {
		PUT, APPEND
	};

	/**
	 * Encapsulates a client command and argument. This includes operation and
	 * contents but not filename, which is the key used to look up this object.
	 */
	protected static class PendingClientOperation {
		/**
		 * What we intend to do later
		 */
		protected ClientOperation operation;

		/**
		 * The content to put or append
		 */
		protected String content;

		/**
		 * Create an intent for an op that has no content
		 */
		public PendingClientOperation(ClientOperation operation) {
			this.operation = operation;
			this.content = null;
		}

		/**
		 * Create an intent for an op that has content
		 */
		public PendingClientOperation(ClientOperation type, String content) {
			this.operation = type;
			this.content = content;
		}
	}

	/**
	 * Status of cached files on disk. Keys are filenames.
	 */
	protected Map<String, CacheStatuses> clientCacheStatus;

	/**
	 * Map from filenames to the operation we want to do on them later
	 */
	protected Map<String, PendingClientOperation> clientPendingOperations;

	/*
	 * TODO: Abstract into some kind of Locker class so you're forced to use
	 * helpers to access this that we can log
	 */

	/**
	 * List of files locked on the client's side
	 */
	protected Set<String> clientLockedFiles;

	/**
	 * Saves commands on client side locked files
	 */
	protected Map<String, Queue<String>> clientQueuedCommands;

	/**
	 * Whether or not the client is currently performing a transaction
	 */
	protected boolean clientTransacting;

	/**
	 * Whether or not the client is waiting for a response to it's txcommit
	 */
	protected boolean clientWaitingForCommitSuccess;

	/**
	 * Commands queued because the client is waiting for a commit result
	 */
	protected Queue<String> clientWaitingForCommitQueue;
	
	public ClientNode() {
		this.clientCacheStatus = new HashMap<String, CacheStatuses>();
		this.clientPendingOperations = new HashMap<String, PendingClientOperation>();
		this.clientLockedFiles = new HashSet<String>();
		this.clientQueuedCommands = new HashMap<String, Queue<String>>();
		this.clientTransacting = false;
		this.clientWaitingForCommitSuccess = false;
		this.clientWaitingForCommitQueue = new LinkedList<String>();
	}
}
