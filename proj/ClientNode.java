import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;
import java.util.StringTokenizer;

import edu.washington.cs.cse490h.lib.Utility;

/*
 * TODO: Purge uncached files from disk periodically (unless manager thinks I have RW still)
 */

/**
 * Implicit transactions do the right thing by CC
 * 
 * IMPORTANT: Methods names should not end in Handler unless they are meant to
 * handle commands passed in by onCommand - onCommand dynamically dispatches
 * commands to the method named <cmdName>Handler.
 */
public class ClientNode {

	/**
	 * Possible cache statuses
	 */
	private static enum CacheStatuses {
		ReadWrite, ReadOnly
	};

	/**
	 * Wrapper around a map from filenames to cache statuses that logs changes
	 * 
	 * Files not in the map should be considered invalid
	 */
	private static class Cache {
		private Map<String, CacheStatuses> cache;
		private RIONode n;

		public Cache(RIONode n) {
			this.cache = new HashMap<String, CacheStatuses>();
			this.n = n;
		}

		public void put(String filename, CacheStatuses newStatus) {
			n.printVerbose("changing cache status of " + filename + " to "
					+ newStatus);
			cache.put(filename, newStatus);
		}

		public void remove(String filename) {
			n.printVerbose("removing " + filename + " from cache");
			cache.remove(filename);
		}

		public boolean hasRW(String filename) {
			return cache.get(filename) == CacheStatuses.ReadWrite;
		}

		public boolean hasRO(String filename) {
			return cache.get(filename) == CacheStatuses.ReadOnly;
		}

		public boolean contains(String filename) {
			return cache.containsKey(filename);
		}
	}

	/**
	 * Operation types the client can remember in a PendingClientOperation
	 */
	private static enum ClientOperation {
		PUT, APPEND
	};

	/**
	 * Encapsulates a client command and argument. This includes operation and
	 * contents but not filename, which is the key used to look up this object.
	 */
	private static class PendingClientOperation {
		/**
		 * What we intend to do later
		 */
		private ClientOperation operation;

		/**
		 * The content to put or append
		 */
		private String content;

		/**
		 * Create an intent for an op that has content
		 */
		public PendingClientOperation(ClientOperation type, String content) {
			this.operation = type;
			this.content = content;
		}
	}

	/**
	 * Status of cached files on disk.
	 */
	private Cache cache;

	@Deprecated
	private Queue<String> getQueue = new LinkedList<String>();

	/*
	 * We have client side locking to handle the following type of cmd flows:
	 * 
	 * 1 put test hello
	 * 
	 * 1 delete test
	 * 
	 * I should wait to get ReadWrite on test from the manager before executing
	 * the second command - I don't send as many messages and the manager is
	 * less likely to get confused
	 */

	/**
	 * List of files locked on the client's side
	 */
	private Set<String> lockedFiles;

	/**
	 * The address of the manager node.
	 */
	protected int managerAddr;

	/**
	 * The maximum number of commands the client will queue before timing out
	 * and restarting the node
	 */
	private int maxWaitingForCommitQueueSize;

	/**
	 * The parent node associated with this client
	 */
	private Client parent;

	/**
	 * Map from filenames to the operation we want to do on them once we receive
	 * some response from the manager (a WD, RD, or Error)
	 */
	private Map<String, PendingClientOperation> pendingOperations;

	/**
	 * Saves commands on client side locked files
	 */
	private Map<String, Queue<String>> queuedCommands;

	/**
	 * Whether or not the client is currently performing a transaction
	 */
	private boolean transacting;

	/**
	 * Whether or not the client is waiting for a response to it's txcommit
	 */
	private boolean waitingForCommitSuccess;

	/**
	 * True iff the client is waiting to satisfy all PendingOperations to commit
	 */
	private boolean waitingToCommit;

	/**
	 * Commands queued because the client is waiting for a commit result
	 */
	private Queue<String> waitingForCommitQueue;

	public ClientNode(Client n, int maxWaitingForCommitQueueSize) {
		this.parent = n;
		this.maxWaitingForCommitQueueSize = maxWaitingForCommitQueueSize;
		
		this.cache = new Cache(parent);
		this.pendingOperations = new HashMap<String, PendingClientOperation>();
		this.lockedFiles = new HashSet<String>();
		this.queuedCommands = new HashMap<String, Queue<String>>();
		this.transacting = false;
		this.waitingForCommitSuccess = false;
		this.waitingForCommitQueue = new LinkedList<String>();
		this.waitingToCommit = false;
		this.managerAddr = 0; // TODO: learn this somehow
	}

	public void onCommand(String line) {
		// Create a tokenizer and get the first token (the actual cmd)
		StringTokenizer tokens = new StringTokenizer(line, " ");
		String cmd = "";
		try {
			cmd = tokens.nextToken().toLowerCase();
		} catch (NoSuchElementException e) {
			parent.printError("no command found in: " + line);
			return;
		}

		if (waitingForCommitSuccess
				&& waitingForCommitQueue.size() > maxWaitingForCommitQueueSize) {
			// TODO: just invoke receivedTxFailure here?
			parent.restartAsClient();
			return;
		} else if (waitingForCommitSuccess) {
			waitingForCommitQueue.add(line);
			return;
		}

		/*
		 * Dynamically call <cmd>Command, passing off the tokenizer and the full
		 * command string
		 */
		try {
			Class<?>[] paramTypes = { StringTokenizer.class, String.class };
			Method handler = this.getClass().getMethod(cmd + "Handler",
					paramTypes);
			Object[] args = { tokens, line };
			handler.invoke(this, args);
		} catch (NoSuchMethodException e) {
			parent.printError("invalid command:" + line);
		} catch (IllegalAccessException e) {
			parent.printError("invalid command:" + line);
		} catch (InvocationTargetException e) {
			parent.printError(e);
			if (transacting) {
				sendToManager(Protocol.TX_ABORT);
				abortCurrentTransaction();
			}
			/*
			 * Since locks are the last thing that happen in handlers, nothing
			 * was locked so there's nothing to unlock here
			 */
		}
	}

	/*******************************
	 * begin commandHandlers
	 *******************************/

	/**
	 * Get ownership of a file and append to it
	 * 
	 * @throws IOException
	 * 
	 * @throws TransactionException
	 */
	public void appendHandler(StringTokenizer tokens, String line)
			throws IOException, TransactionException {
		// TODO: I think I found a framework bug - "append 1 test  world" is
		// losing the extra space

		String filename = tokens.nextToken();
		String content = parseAddContent(line, "append", filename);

		if (queueLineIfLocked(filename, line)) {
			return;
		} else if (cache.hasRW(filename)) {
			// have ownership - writeFile verifies existence
			if (transacting) {
				parent.fs.writeFileTX(parent.addr, filename, content, true);
			} else {
				parent.fs.writeFile(filename, content, true);
			}
		} else {
			// lock and request ownership
			sendToManager(Protocol.WQ, Utility.stringToByteArray(filename));
			pendingOperations.put(filename, new PendingClientOperation(
					ClientOperation.APPEND, content));
			lockFile(filename);
		}
	}

	/**
	 * Get ownership of a file and create it
	 * 
	 * @throws IOException
	 * 
	 * @throws TransactionException
	 * @throws UnknownManagerException
	 */
	public void createHandler(StringTokenizer tokens, String line)
			throws IOException, TransactionException, UnknownManagerException {
		String filename = tokens.nextToken();

		if (queueLineIfLocked(filename, line)) {
			return;
		} else if (cache.hasRW(filename)) {
			// have permissions
			if (transacting) {
				parent.fs.createFileTX(parent.addr, filename);
			} else {
				parent.fs.createFile(filename);
			}
		} else {
			// lock and perform rpc
			if (managerAddr == -1) {
				throw new UnknownManagerException();
			} else {
				createRPC(managerAddr, filename);
				lockFile(filename);
			}
		}
	}

	/**
	 * For debugging purposes only. Prints expected numbers for in and out
	 * channels. Likely to change as new problems arise.
	 */
	public void debugHandler(StringTokenizer tokens, String line) {
		parent.RIOLayer.printSeqStateDebug();
	}

	/**
	 * Get ownership of a file and delete it
	 * 
	 * @throws IOException
	 * 
	 * @throws TransactionException
	 * @throws UnknownManagerException
	 */
	public void deleteHandler(StringTokenizer tokens, String line)
			throws IOException, TransactionException, UnknownManagerException {
		String filename = tokens.nextToken();

		if (queueLineIfLocked(filename, line)) {
			return;
		} else if (cache.hasRW(filename)) {
			// have permissions
			if (transacting) {
				parent.fs.deleteFileTX(parent.addr, filename);
			} else {
				parent.fs.deleteFile(filename);
			}
		} else {
			// lock and perform rpc
			if (managerAddr == -1) {
				throw new UnknownManagerException();
			} else {
				deleteRPC(managerAddr, filename);
				lockFile(filename);
			}
		}
	}

	/**
	 * Get read access for a file and then get its contents
	 * 
	 * @throws IOException
	 */
	public void getHandler(StringTokenizer tokens, String line)
			throws IOException {
		String filename = tokens.nextToken();

		if (queueLineIfLocked(filename, line)) {
			return;
		} else if (cache.contains(filename)) {
			// have permissions
			String content;
			if (transacting) {
				content = parent.fs.getFileTX(parent.addr, filename);
			} else {
				content = parent.fs.getFile(filename);
			}
			parent.printInfo("Got file, contents below:");
			parent.printInfo(content);
			
			getQueue.add("got " + filename + ":");
			getQueue.add(content);
		} else {
			// lock and get permissions
			parent.printVerbose("requesting read access for " + filename);
			sendToManager(Protocol.RQ, Utility.stringToByteArray(filename));
			lockFile(filename);
		}
	}

	/**
	 * Initiates a remote handshake
	 */
	public void handshakeHandler(StringTokenizer tokens, String line) {
		int server = Integer.parseInt(tokens.nextToken());
		String payload = parent.getID().toString();
		parent.printInfo("sending handshake to " + server);
		parent.RIOSend(server, Protocol.HANDSHAKE,
				Utility.stringToByteArray(payload));
	}

	/**
	 * Used for project2 to tell a node it is the manager.
	 */
	public void managerHandler(StringTokenizer tokens, String line) {
		parent.restartAsManager();
	}

	/**
	 * Used for project2 to tell a node the address of the manager.
	 */
	public void managerisHandler(StringTokenizer tokens, String line) {
		managerAddr = Integer.parseInt(tokens.nextToken());
		parent.printInfo("setting manager address to " + managerAddr);
	}

	/**
	 * Sends a noop
	 */
	public void noopHandler(StringTokenizer tokens, String line) {
		int server = Integer.parseInt(tokens.nextToken());
		parent.RIOSend(server, Protocol.NOOP, Client.emptyPayload);
	}

	/**
	 * Get ownership of a file and put to it
	 * 
	 * @throws IOException
	 * 
	 * @throws TransactionException
	 */
	public void putHandler(StringTokenizer tokens, String line)
			throws IOException, TransactionException {
		String filename = tokens.nextToken();
		String content = parseAddContent(line, "put", filename);

		if (queueLineIfLocked(filename, line)) {
			return;
		} else if (cache.hasRW(filename)) {
			// have ownership - writeFile verifies existence
			if (transacting) {
				parent.fs.writeFileTX(parent.addr, filename, content, false);
			} else {
				parent.fs.writeFile(filename, content, false);
			}
		} else {
			// lock and request ownership
			sendToManager(Protocol.WQ, Utility.stringToByteArray(filename));
			pendingOperations.put(filename, new PendingClientOperation(
					ClientOperation.PUT, content));
			lockFile(filename);
		}
	}

	/**
	 * Sends a TX_ABORT if performing a transaction
	 * 
	 * @throws TransactionException
	 * @throws IOException
	 */
	public void txabortHandler(StringTokenizer tokens, String line)
			throws TransactionException, IOException {
		if (!transacting) {
			throw new TransactionException(
					"client not performing a transaction");
		} else {
			abortCurrentTransaction();
			sendToManager(Protocol.TX_ABORT);
		}
	}

	/*
	 * TODO: HIGH: What happens if I have RW but I fail? I'm the only one with
	 * the newest version of the file, so the server has to block while it waits
	 * for me to come back up - the only way to make this non-blocking is to
	 * replicate the file to other clients/managers before responding, so that
	 * the manager can still find the newest version of the file if I'm down
	 * 
	 * The next node address will be my replica
	 * 
	 * Send changes to replica before committing anything to manager
	 */

	/**
	 * Sends a TX_COMMIT if performing a transaction
	 * 
	 * @throws TransactionException
	 * 
	 * @throws IOException
	 */
	public void txcommitHandler(StringTokenizer tokens, String line)
			throws TransactionException, IOException {
		if (!transacting) {
			throw new TransactionException(
					"client not performing a transaction");
		} else if (lockedFiles.size() > 0) {
			waitingToCommit = true;
			parent.printVerbose("queueing commit");
		} else {
			// transacting is updated when a response is received
			waitingForCommitSuccess = true;
			sendToManager(Protocol.TX_COMMIT);
		}
	}

	/**
	 * Sends a TX_START if not already performing a transaction
	 * 
	 * @throws TransactionException
	 * 
	 * @throws IOException
	 */
	public void txstartHandler(StringTokenizer tokens, String line)
			throws TransactionException, IOException {
		// this will be queued in onCommand if waitingForCommitSuccess
		if (transacting) {
			throw new TransactionException(
					"client already performing a transaction");
		} else {
			transacting = true;
			parent.fs.startTransaction(parent.addr);
			sendToManager(Protocol.TX_START);
		}
	}

	/*******************************
	 * end commandHandlers
	 *******************************/

	/*******************************
	 * begin helpers
	 *******************************/

	/**
	 * Tells the node's fs to abort the current transaction, updates internal
	 * transacting flag, and clears out all queued and pending commands for this
	 * tx / releases the associated locks
	 * 
	 * If this fails, the Client will restart, meaning all references to this
	 * will be lost, so this should be the last thing called
	 * 
	 * @throws TransactionException
	 * @throws IOException
	 */
	public void abortCurrentTransaction() {
		if (!transacting) {
			return;
		}

		try {
			getQueue.clear();
			
			parent.printVerbose("aborting transaction");
			transacting = false;

			// clear out all commands
			parent.printVerbose("unlocking " + lockedFiles.toString());
			lockedFiles.clear();
			queuedCommands.clear();
			pendingOperations.clear();
			waitingToCommit = false;
			
			parent.fs.abortTransaction(parent.addr);
		} catch (IOException e) {			
			/*
			 * Failed to write an abort to the log - if we keep going, we could
			 * corrupt the log (since txs don't have ids)
			 */
			parent.restartAsClient();
		} catch (NullPointerException e) {
			// NPE in abortTransaction on: start - crash - abort
			parent.printError(e);
		}
		
		processWaitingForCommitQueue();
	}

	/**
	 * Tells the node's fs to commit the current transaction and upates internal
	 * transacting flag
	 * 
	 * If this fails, the Client will restart, meaning all references to this
	 * will be lost, so this should probably be the last thing called (although
	 * unlocks still need to be after this, in which case the node might end up
	 * printing some garbage to its log)
	 */
	private void commitCurrentTransaction() {
		try {
			parent.printVerbose("committing transaction");
			transacting = false;
			parent.fs.commitTransaction(parent.addr);
		} catch (IOException e) {
			/*
			 * Failed to write the commit to the log or a change to disk - if we
			 * keep going, we could corrupt the log (since txs don't have ids)
			 */
			parent.restartAsClient();
		}
	}

	/**
	 * Perform a create RPC to the given address
	 */
	private void createRPC(int address, String filename) {
		parent.RIOSend(address, Protocol.CREATE,
				Utility.stringToByteArray(filename));
	}

	/**
	 * Perform a delete RPC to the given address
	 */
	private void deleteRPC(int address, String filename) {
		parent.RIOSend(address, Protocol.DELETE,
				Utility.stringToByteArray(filename));
	}

	/**
	 * Lock the provided filename and print a message
	 * 
	 * This should be the last thing done in a Handler so that we don't have to
	 * figure out what file to unlock if the handler throws an exception
	 */
	private void lockFile(String filename) {
		parent.printVerbose("client locking file: " + filename);
		parent.logSynopticEvent("CLIENT-LOCK");
		lockedFiles.add(filename);
	}

	/**
	 * Helper that just checks if managerAddr is still -1
	 */
	private boolean managerUnknown() {
		if (managerAddr == -1) {
			parent.printError(new UnknownManagerException());
			return true;
		} else {
			return false;
		}
	}

	/**
	 * Parse what content to add to a file for put and append (the rest of the
	 * line)
	 */
	private String parseAddContent(String line, String cmd, String filename) {
		int parsedLength = cmd.length() + filename.length() + 2;
		if (parsedLength >= line.length()) {
			throw new NoSuchElementException("command content empty");
		}
		return line.substring(parsedLength);
	}

	/**
	 * Updates internal waiting flag and trys to handle all queued commands
	 */
	private void processWaitingForCommitQueue() {
		waitingForCommitSuccess = false;
		while (waitingForCommitQueue.size() > 0) {
			parent.onCommand(waitingForCommitQueue.poll());
		}
	}

	/**
	 * Check if the client has locked the filename. Queue the passed in action
	 * if the file is locked and return true. Otherwise return false.
	 */
	private boolean queueLineIfLocked(String filename, String line) {
		if (lockedFiles.contains(filename)) {
			parent.printVerbose("queueing command on locked file: " + filename
					+ ", " + line);

			Queue<String> requests = queuedCommands.get(filename);
			if (requests == null) {
				requests = new LinkedList<String>();
				queuedCommands.put(filename, requests);
			}
			requests.add(line);

			return true;
		} else {
			return false;
		}
	}

	/**
	 * Convenience wrapper of RIOSend that sends a message with an empty payload
	 * to the manager and assumes that it is known. Use managerUnknown to check
	 * if the manager is really known at the beginning of your handler before
	 * calling this.
	 */
	private void sendToManager(int protocol) {
		sendToManager(protocol, Client.emptyPayload);
	}

	/**
	 * Convenience wrapper of RIOSend that sends a message to the manager and
	 * assumes that it is known. Use managerUnknown to check if the manager is
	 * really known at the beginning of your handler before calling this.
	 */
	private void sendToManager(int protocol, byte[] payload) {
		parent.RIOSend(managerAddr, protocol, payload);
	}

	/**
	 * Unlock the filename and service any queued requests on it - because this
	 * services the next requests in the queue immediately, calling it should be
	 * the last thing you do after mutating state for your current op
	 */
	private void unlockFile(String filename) {
		parent.printVerbose("client unlocking file: " + filename);
		parent.logSynopticEvent("CLIENT-UNLOCK");
		lockedFiles.remove(filename);

		// process queued commands on this file
		Queue<String> queuedRequests = queuedCommands.get(filename);
		if (queuedRequests != null) {
			while (queuedRequests.size() > 0) {
				onCommand(queuedRequests.poll());
			}
		}

		// send a commit if one is waiting
		if (lockedFiles.isEmpty() && waitingToCommit) {
			waitingToCommit = false;
			onCommand("txcommit");
		}
	}

	/*******************************
	 * end helpers
	 *******************************/

	/*************************************************
	 * begin receiveHandlers
	 ************************************************/

	// TODO: Convince myself all the receive logic is right

	/**
	 * RPC Error
	 */
	public void receiveError(int from, String msgString) {
		parent.printError(msgString);
		// tx failure cleans up data structures
	}

	// TODO: HIGH: Check cleaning up state after {R,W}{Q,D,F} etc fails

	/**
	 * Client receives {W,R}F as a request to propagate their changes
	 */
	private void receiveF(String msgString, String RForWF,
			int responseProtocol, boolean keepRO) {
		if (managerUnknown()) {
			return;
		}

		StringTokenizer tokens = new StringTokenizer(msgString);
		String filename = tokens.nextToken();

		/*
		 * No reason to check CacheStatus since anything on my disk was
		 * committed to the manager
		 */

		String payload = null;

		if (!Utility.fileExists(parent, msgString)) {
			// no file on disk, file was deleted
			responseProtocol = Protocol.WD_DELETE;
			payload = filename;
		} else {
			// read file contents
			// manager guarantees you're not currently transacting on this file
			try {
				payload = filename + Client.packetDelimiter
						+ parent.fs.getFile(filename);
			} catch (IOException e) {
				// FS failure - might as well be disconnected
				return;
				// manager will query replica instead
			}
		}

		// send update to manager
		parent.printVerbose("sending "
				+ Protocol.protocolToString(responseProtocol) + " to manager "
				+ filename);
		sendToManager(responseProtocol, Utility.stringToByteArray(payload));

		// update permissions
		if (keepRO) {
			cache.put(filename, CacheStatuses.ReadOnly);
		} else {
			cache.remove(filename);
		}
	}

	/**
	 * Client receives IV as a notification to mark a cached file invalid
	 */
	public void receiveIV(int from, String filename) {
		if (managerUnknown()) {
			return;
		}

		parent.printVerbose("marking invalid " + filename);
		cache.remove(filename);

		sendToManager(Protocol.IC, Utility.stringToByteArray(filename));
	}

	// TODO: Low: Send TFS.PendingOp objects here instead of strings

	/**
	 * @param msgString
	 *            <filename> <contents> ex) test hello world
	 */
	public void receiveRD(int from, String filename, String contents) {
		if (managerUnknown()) {
			return;
		}

		// has RO
		cache.put(filename, CacheStatuses.ReadOnly);

		try {
			// update in cache
			if (!Utility.fileExists(parent, filename)) {
				parent.fs.createFile(filename);
			}
			parent.fs.writeFile(filename, contents, false);
		} catch (IOException e) {
			if (transacting) {
				abortCurrentTransaction();
				sendToManager(Protocol.TX_ABORT);
			}
			return;
		}

		// print GET result
		parent.printInfo("Got file, contents below:");
		parent.printInfo(contents);
		
		getQueue.add("got " + filename + ":");
		getQueue.add(contents);

		// send rc
		parent.printVerbose("sending rc to manager for " + filename);
		sendToManager(Protocol.RC, Utility.stringToByteArray(filename));

		// unlock the file for local use
		unlockFile(filename);
	}

	public void receiveRF(int from, String filename) {
		receiveF(filename, "RF", Protocol.RD, true);
	}

	/**
	 * RPC Successful (only received after successful Create or Delete)
	 */
	public void receiveSuccess(int from, String msgString) {

		String[] split = msgString.split(Client.packetDelimiter);
		String cmd = split[0];

		if (split.length < 2) {
			parent.printError("received empty "
					+ Protocol.protocolToString(Protocol.SUCCESS) + " packet");
			return;
		}

		String filename = split[1];

		try {
			if (cmd.equals(Protocol.protocolToString(Protocol.CREATE))) {
				if (!Utility.fileExists(parent, filename)) {
					if (transacting) {
						parent.fs.createFileTX(parent.addr, filename);
					} else {
						parent.fs.createFile(filename);
					}
				} else {
					/*
					 * file could have been deleted by someone else, and now I'm
					 * creating, but I could still have an old copy on disk
					 */
					if (transacting) {
						parent.fs.writeFileTX(parent.addr, filename, "", false);
					} else {
						parent.fs.writeFile(filename, "", false);
					}
				}
				cache.put(filename, CacheStatuses.ReadWrite);
				unlockFile(filename);
			} else if (cmd.equals(Protocol.protocolToString(Protocol.DELETE))) {
				// might not exist here
				if (transacting) {
					if (parent.fs.fileExistsTX(parent.addr, filename)) {
						parent.fs.deleteFileTX(parent.addr, filename);
					}
				} else {
					if (Utility.fileExists(parent, filename)) {
						parent.fs.deleteFile(filename);
					}
				}
				cache.put(filename, CacheStatuses.ReadWrite);
				unlockFile(filename);
			} else {
				parent.printError("received invalid cmd " + cmd + " in "
						+ Protocol.protocolToString(Protocol.SUCCESS)
						+ " packet");
			}
		} catch (Exception e) {
			parent.printError(e);
			abortCurrentTransaction();
		}
	}

	/**
	 * Transaction failed
	 */
	public void receiveTXFailure(int from, String empty) {
		abortCurrentTransaction();
		processWaitingForCommitQueue(); // TODO: redundant
	}

	/**
	 * Transaction succeeded
	 */
	public void receiveTXSuccess(int from, String empty) {
		commitCurrentTransaction();
		
		while (getQueue.size() > 0) {
			parent.printInfo(getQueue.poll());
		}
		
		processWaitingForCommitQueue();
	}

	/**
	 * @param msgString
	 *            <filename> <contents> ex) test hello world
	 */
	public void receiveWD(int from, String filename, String contents) {
		if (managerUnknown()) {
			return;
		}

		// has RW!
		cache.put(filename, CacheStatuses.ReadWrite);

		PendingClientOperation intent = pendingOperations.get(filename);
		pendingOperations.remove(filename);

		if (intent == null) {
			parent.printError("missing intent on file: " + filename);
			return;
		}

		try {
			// Managers version is always correct, so write straight to RFS
			if (!Utility.fileExists(parent, filename)) {
				parent.fs.createFile(filename);
			}
			parent.fs.writeFile(filename, contents, false);
		} catch (IOException e) {
			if (transacting) {
				abortCurrentTransaction();
				sendToManager(Protocol.TX_ABORT);
			}
			return;
		}

		try {
			// do what you originally intended with the file
			switch (intent.operation) {
			case PUT:
				if (transacting) {
					parent.fs.writeFileTX(parent.addr, filename,
							intent.content, false);
				} else {
					parent.fs.writeFile(filename, intent.content, false);
				}
				break;
			case APPEND:
				if (transacting) {
					parent.fs.writeFileTX(parent.addr, filename,
							intent.content, true);
				} else {
					parent.fs.writeFile(filename, intent.content, true);
				}
				break;
			default:
				parent.printError("unhandled operation recalled on file: "
						+ filename);
			}
		} catch (Exception e) {
			parent.printError(e);
			if (transacting) {
				abortCurrentTransaction();
				sendToManager(Protocol.TX_ABORT);
			}
			return;
		}

		// send wc and unlock locally
		sendToManager(Protocol.WC, Utility.stringToByteArray(filename));
		unlockFile(filename);
	}

	public void receiveWF(int from, String filename) {
		receiveF(filename, "WF", Protocol.WD, false);
	}

	/*************************************************
	 * end receiveHandlers
	 ************************************************/
}
