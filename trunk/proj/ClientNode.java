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
		 * Create an intent for an op that has content
		 */
		public PendingClientOperation(ClientOperation type, String content) {
			this.operation = type;
			this.content = content;
		}
	}

	/**
	 * Status of cached files on disk. Keys are filenames. Files not in here
	 * should be considered Invalid.
	 */
	protected Map<String, CacheStatuses> cacheStatus;

	/*
	 * TODO: Verify we need client side locking. I think the below case will be
	 * handled better if we have it:
	 * 
	 * 1 put test hello
	 * 
	 * 1 delete test
	 * 
	 * I won't have RW yet on the delete, so I'll ask the manager for it, which
	 * the manager will currently send an error on. Alternatively, the manager
	 * could send me a RW, I send a RD, he sends an RD, I do the delete - but
	 * this is a lot of messages compared to the zero sent if I just wait.
	 */

	/**
	 * List of files locked on the client's side
	 */
	protected Set<String> lockedFiles;

	/**
	 * The address of the manager node.
	 */
	protected int managerAddr;

	/**
	 * The parent node associated with this client
	 */
	protected Client node;

	/**
	 * Map from filenames to the operation we want to do on them later
	 */
	protected Map<String, PendingClientOperation> pendingOperations;

	/**
	 * Saves commands on client side locked files
	 */
	protected Map<String, Queue<String>> queuedCommands;

	/**
	 * Whether or not the client is currently performing a transaction
	 */
	protected boolean transacting;

	/**
	 * Whether or not the client is waiting for a response to it's txcommit
	 */
	protected boolean waitingForCommitSuccess;

	/**
	 * Commands queued because the client is waiting for a commit result
	 */
	protected Queue<String> waitingForCommitQueue;

	/**
	 * The maximum number of commands the client will queue before timing out
	 * and restarting the node
	 */
	protected int maxWaitingForCommitQueueSize;

	public ClientNode(Client n, int maxWaitingForCommitQueueSize) {
		this.node = n;
		this.maxWaitingForCommitQueueSize = maxWaitingForCommitQueueSize;

		this.cacheStatus = new HashMap<String, CacheStatuses>();
		this.pendingOperations = new HashMap<String, PendingClientOperation>();
		this.lockedFiles = new HashSet<String>();
		this.queuedCommands = new HashMap<String, Queue<String>>();
		this.transacting = false;
		this.waitingForCommitSuccess = false;
		this.waitingForCommitQueue = new LinkedList<String>();
		this.managerAddr = -1;
	}

	public void onCommand(String line) {
		// Create a tokenizer and get the first token (the actual cmd)
		StringTokenizer tokens = new StringTokenizer(line, " ");
		String cmd = "";
		try {
			cmd = tokens.nextToken().toLowerCase();
		} catch (NoSuchElementException e) {
			node.printError("no command found in: " + line);
			return;
		}

		if (waitingForCommitSuccess
				&& waitingForCommitQueue.size() > maxWaitingForCommitQueueSize) {
			/*
			 * TODO: figure out what we need to do to kill this object after the
			 * node restarts, or if we should even restart the node at all - at
			 * this point we don't know whether or not or TX succeeded
			 */
			node.restart();
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
			node.printError("invalid command:" + line);
		} catch (IllegalAccessException e) {
			node.printError("invalid command:" + line);
		} catch (InvocationTargetException e) {
			node.printError(e);
			if (transacting) {
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
	 * @throws UnknownManagerException
	 * @throws TransactionException
	 */
	public void appendHandler(StringTokenizer tokens, String line)
			throws IOException, UnknownManagerException, TransactionException {
		// TODO: I think I found a framework bug - "append 1 test  world" is
		// losing the extra space

		String filename = tokens.nextToken();
		String content = parseAddContent(line, "append", filename);

		if (queueLineIfLocked(filename, line)) {
			return;
		} else if (cacheStatus.containsKey(filename)
				&& cacheStatus.get(filename) == CacheStatuses.ReadWrite) {
			// have ownership - writeFile verifies existence
			if (transacting) {
				node.fs.writeFileTX(node.addr, filename, content, true);
			} else {
				node.fs.writeFile(filename, content, true);
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
	 * @throws UnknownManagerException
	 * @throws TransactionException
	 */
	public void createHandler(StringTokenizer tokens, String line)
			throws IOException, UnknownManagerException, TransactionException {
		String filename = tokens.nextToken();

		if (queueLineIfLocked(filename, line)) {
			return;
		} else if (cacheStatus.containsKey(filename)
				&& (cacheStatus.get(filename) == CacheStatuses.ReadWrite)) {
			// have permissions
			if (transacting) {
				node.fs.createFileTX(node.addr, filename);
			} else {
				node.fs.createFile(filename);
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
		node.RIOLayer.printSeqStateDebug();
	}

	/**
	 * Get ownership of a file and delete it
	 * 
	 * @throws IOException
	 * @throws UnknownManagerException
	 * @throws TransactionException
	 */
	public void deleteHandler(StringTokenizer tokens, String line)
			throws IOException, UnknownManagerException, TransactionException {
		String filename = tokens.nextToken();

		if (queueLineIfLocked(filename, line)) {
			return;
		} else if (cacheStatus.containsKey(filename)
				&& cacheStatus.get(filename) == CacheStatuses.ReadWrite) {
			// have permissions
			if (transacting) {
				node.fs.deleteFileTX(node.addr, filename);
			} else {
				node.fs.deleteFile(filename);
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
	 * @throws UnknownManagerException
	 */
	public void getHandler(StringTokenizer tokens, String line)
			throws IOException, UnknownManagerException {
		String filename = tokens.nextToken();

		if (queueLineIfLocked(filename, line)) {
			return;
		} else if (cacheStatus.containsKey(filename)) {
			// have permissions
			String content;
			if (transacting) {
				content = node.fs.getFileTX(node.addr, filename);
			} else {
				content = node.fs.getFile(filename);
			}
			node.printInfo("Got file, contents below:");
			node.printInfo(content);
		} else {
			// lock and get permissions
			node.printVerbose("requesting read access for " + filename);
			sendToManager(Protocol.RQ, Utility.stringToByteArray(filename));
			lockFile(filename);
		}
	}

	/**
	 * Initiates a remote handshake
	 */
	public void handshakeHandler(StringTokenizer tokens, String line) {
		int server = Integer.parseInt(tokens.nextToken());
		String payload = node.getID().toString();
		node.printInfo("sending handshake to " + server);
		node.RIOSend(server, Protocol.HANDSHAKE,
				Utility.stringToByteArray(payload));
	}

	/**
	 * Used for project2 to tell a node it is the manager.
	 */
	public void managerHandler(StringTokenizer tokens, String line) {
		if (!node.isManager) {
			node.printInfo("promoted to manager");
			node.isManager = true;
			node.managerFunctions = new ManagerNode(node);
		} else {
			node.printInfo("already manager");
		}
	}

	/**
	 * Used for project2 to tell a node the address of the manager.
	 */
	public void managerisHandler(StringTokenizer tokens, String line) {
		managerAddr = Integer.parseInt(tokens.nextToken());
		node.printInfo("setting manager address to " + managerAddr);
	}

	/**
	 * Sends a noop
	 */
	public void noopHandler(StringTokenizer tokens, String line) {
		int server = Integer.parseInt(tokens.nextToken());
		node.RIOSend(server, Protocol.NOOP, Client.emptyPayload);
	}

	/**
	 * Get ownership of a file and put to it
	 * 
	 * @throws IOException
	 * @throws UnknownManagerException
	 * @throws TransactionException
	 */
	public void putHandler(StringTokenizer tokens, String line)
			throws IOException, UnknownManagerException, TransactionException {
		String filename = tokens.nextToken();
		String content = parseAddContent(line, "put", filename);

		if (queueLineIfLocked(filename, line)) {
			return;
		} else if (cacheStatus.containsKey(filename)
				&& cacheStatus.get(filename) == CacheStatuses.ReadWrite) {
			// have ownership - writeFile verifies existence
			if (transacting) {
				node.fs.writeFileTX(node.addr, filename, content, false);
			} else {
				node.fs.writeFile(filename, content, false);
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
	 * @throws UnknownManagerException
	 * @throws TransactionException
	 * @throws IOException
	 */
	public void txabortHandler(StringTokenizer tokens, String line)
			throws UnknownManagerException, TransactionException, IOException {
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
	 * Is it safe for my transaction to just be lost? Aka, if I fail, the
	 * manager just uses whatever it has, which will be everything before my RWs
	 * in my latest transaction. I'm worried that someone could get ownership of
	 * one but not all of the files I mutated, leaving the system in an
	 * inconsistent state
	 * 
	 * Send changes to replica before committing anything to manager
	 */

	/**
	 * Sends a TX_COMMIT if performing a transaction
	 * 
	 * @throws TransactionException
	 * @throws UnknownManagerException
	 * @throws IOException
	 */
	public void txcommitHandler(StringTokenizer tokens, String line)
			throws TransactionException, UnknownManagerException, IOException {
		if (!transacting) {
			throw new TransactionException(
					"client not performing a transaction");
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
	 * @throws UnknownManagerException
	 * @throws IOException
	 */
	public void txstartHandler(StringTokenizer tokens, String line)
			throws TransactionException, UnknownManagerException, IOException {
		if (transacting) {
			throw new TransactionException(
					"client already performing a transaction");
		} else {
			transacting = true;
			node.fs.startTransaction(node.addr);
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
	 * Tells the node's fs to abort the current transaction and updates internal
	 * transacting flag
	 * 
	 * If this fails, the Client will restart, meaning all references to this
	 * will be lost, so this should probably be the last thing called (although
	 * unlocks still need to be after this, in which case the node might end up
	 * printing some garbage to its log)
	 * 
	 * @throws TransactionException
	 * @throws IOException
	 */
	protected void abortCurrentTransaction() {
		if (!transacting) {
			return;
		}

		try {
			node.printVerbose("aborting transaction");
			node.fs.abortTransaction(node.addr);
			transacting = false;
		} catch (IOException e) {
			/*
			 * Failed to write an abort to the log - if we keep going, we could
			 * corrupt the log (since txs don't have ids)
			 */
			node.restart();
		}
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
	protected void commitCurrentTransaction() {
		try {
			node.printVerbose("committing transaction");
			node.fs.commitTransaction(node.addr);
			transacting = false;
		} catch (IOException e) {
			/*
			 * Failed to write the commit to the log or a change to disk - if we
			 * keep going, we could corrupt the log (since txs don't have ids)
			 */
			node.restart();
		}
	}

	/**
	 * Perform a create RPC to the given address
	 */
	public void createRPC(int address, String filename) {
		node.RIOSend(address, Protocol.CREATE,
				Utility.stringToByteArray(filename));
	}

	/**
	 * Perform a delete RPC to the given address
	 */
	public void deleteRPC(int address, String filename) {
		node.RIOSend(address, Protocol.DELETE,
				Utility.stringToByteArray(filename));
	}

	/**
	 * Lock the provided filename and print a message
	 * 
	 * This should be the last thing done in a Handler so that we don't have to
	 * figure out what file to unlock if the handler throws an exception
	 */
	protected void lockFile(String filename) {
		node.printVerbose("client locking file: " + filename);
		node.logSynopticEvent("CLIENT-LOCK");
		lockedFiles.add(filename);
	}

	/**
	 * Helper that just checks if managerAddr is still -1
	 */
	protected boolean managerUnknown() {
		if (managerAddr == -1) {
			node.printError(new UnknownManagerException());
			return true;
		} else {
			return false;
		}
	}

	/**
	 * Parse what content to add to a file for put and append (the rest of the
	 * line)
	 */
	protected String parseAddContent(String line, String cmd, String filename) {
		int parsedLength = cmd.length() + filename.length() + 2;
		if (parsedLength >= line.length()) {
			throw new NoSuchElementException("command content empty");
		}
		return line.substring(parsedLength);
	}

	/**
	 * Updates internal waiting flag and trys to handle all queued commands
	 */
	protected void processWaitingForCommitQueue() {
		waitingForCommitSuccess = false;
		for (String line : waitingForCommitQueue) {
			node.onCommand(line);
		}
	}

	/**
	 * Check if the client has locked the filename. Queue the passed in action
	 * if the file is locked and return true. Otherwise return false.
	 */
	protected boolean queueLineIfLocked(String filename, String line) {
		if (lockedFiles.contains(filename)) {
			node.printVerbose("queueing command on locked file: " + filename
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
	protected void sendToManager(int protocol) {
		sendToManager(protocol, Client.emptyPayload);
	}

	/**
	 * Convenience wrapper of RIOSend that sends a message to the manager and
	 * assumes that it is known. Use managerUnknown to check if the manager is
	 * really known at the beginning of your handler before calling this.
	 */
	protected void sendToManager(int protocol, byte[] payload) {
		node.RIOSend(managerAddr, protocol, payload);
	}

	/**
	 * RPC Error
	 */
	protected void receiveError(Integer from, String msgString) {
		/*
		 * TODO: HIGH: Not sure what do we do here - have i been aborted? I
		 * think the manager should have aborted me, so I'll assume that.
		 */

		node.printError(msgString);

		abortCurrentTransaction();

		String filename = msgString.split("")[0];
		unlockFile(filename);
	}

	/**
	 * Unlocks all files locally
	 */
	public void unlockAll() {
		lockedFiles.clear();
	}

	/**
	 * Unlock the filename and service and queued requests on it - because this
	 * services the next requests in the queue immediately, calling it should be
	 * the last thing you do after mutating state for your current op
	 */
	protected void unlockFile(String filename) {
		node.printVerbose("client unlocking file: " + filename);
		node.logSynopticEvent("CLIENT-UNLOCK");
		lockedFiles.remove(filename);

		Queue<String> queuedRequests = queuedCommands.get(filename);
		if (queuedRequests != null) {
			while (queuedRequests.size() > 0) {
				String request = queuedRequests.poll();
				onCommand(request);
			}
		}
	}

	/*******************************
	 * end helpers
	 *******************************/

	/*************************************************
	 * begin receiveHandlers
	 ************************************************/

	// TODO: HIGH: Cleanup state after {R,W}{Q,D,F} etc fails

	/**
	 * Client receives {W,R}F as a request to propagate their changes
	 */
	protected void receiveF(String msgString, String RForWF,
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

		if (!Utility.fileExists(node, msgString)) {
			// no file on disk, file was deleted
			responseProtocol = Protocol.WD_DELETE;
			payload = filename;
		} else {
			// read file contents
			// manager guarantees you're not currently transacting on this file
			try {
				payload = filename + Client.delimiter
						+ node.fs.getFile(filename);
			} catch (IOException e) {
				// FS failure - might as well be disconnected
				return;
				/*
				 * TODO: HIGH: How does the manager deal w/ this? Needs to give
				 * requester access at some point, so I guess it just uses the
				 * most recent version of the file it has and revokes RW to the
				 * client who failed to respond to the F request?
				 */
			}
		}

		// send update to manager
		node.printVerbose("sending "
				+ Protocol.protocolToString(responseProtocol) + " to manager "
				+ filename);
		sendToManager(responseProtocol, Utility.stringToByteArray(payload));

		// update permissions
		if (keepRO) {
			cacheStatus.put(filename, CacheStatuses.ReadOnly);
			node.printVerbose("changed permission level to ReadOnly on file: "
					+ filename);
		} else {
			cacheStatus.remove(filename);
			node.printVerbose("losing permissions on file: " + filename);
		}
	}

	/**
	 * Client receives IV as a notification to mark a cached file invalid
	 * 
	 * @throws NotClientException
	 * @throws UnknownManagerException
	 */
	protected void receiveIV(String msgString) {
		if (managerUnknown()) {
			return;
		}

		node.printVerbose("marking invalid " + msgString);
		cacheStatus.remove(msgString);

		sendToManager(Protocol.IC, Utility.stringToByteArray(msgString));
	}

	/**
	 * @param msgString
	 *            <filename> <contents> ex) test hello world
	 */
	protected void receiveRD(int from, String filename, String contents) {
		if (managerUnknown()) {
			return;
		}

		// has RO
		cacheStatus.put(filename, CacheStatuses.ReadOnly);
		node.printVerbose("got ReadOnly on " + filename);

		try {
			// update in cache
			if (!Utility.fileExists(node, filename)) {
				node.fs.createFile(filename);
			}
			node.fs.writeFile(filename, contents, false);
		} catch (IOException e) {
			// TODO: double check doing everything needed
			if (transacting) {
				abortCurrentTransaction();
				sendToManager(Protocol.TX_ABORT);
				unlockFile(filename);
			}
			return;
		}

		// print GET result
		node.printInfo("Got file, contents below:");
		node.printInfo(contents);

		// send rc
		node.printVerbose("sending rc to manager for " + filename);
		sendToManager(Protocol.RC, Utility.stringToByteArray(filename));

		// unlock the file for local use
		unlockFile(filename);
	}

	/**
	 * RPC Successful (only received after successful Create or Delete)
	 */
	protected void receiveSuccessful(int from, String msgString) {

		String[] split = msgString.split(Client.delimiter);
		String cmd = split[0];

		if (split.length < 2) {
			node.printError("received empty "
					+ Protocol.protocolToString(Protocol.SUCCESS) + " packet");
			return;
		}

		String filename = split[1];

		try {
			if (cmd.equals(Protocol.protocolToString(Protocol.CREATE))) {
				if (!Utility.fileExists(node, filename)) {
					node.fs.createFile(filename);
				} else {
					/*
					 * file could have been deleted by someone else, and now I'm
					 * creating, but I could still have an old copy on disk
					 */
					node.fs.writeFile(filename, "", false);
				}
				cacheStatus.put(filename, CacheStatuses.ReadWrite);
				unlockFile(filename);
			} else if (cmd.equals(Protocol.protocolToString(Protocol.DELETE))) {
				if (Utility.fileExists(node, filename)) {
					// migh not exist here
					node.fs.deleteFile(filename);
				}
				cacheStatus.put(filename, CacheStatuses.ReadWrite);
				unlockFile(filename);

			} else {
				node.printError("received invalid cmd " + cmd + " in "
						+ Protocol.protocolToString(Protocol.SUCCESS)
						+ " packet");
			}
		} catch (IOException e) {
			node.printError(e);
			abortCurrentTransaction();
			unlockFile(filename);
		}
	}

	/**
	 * Transaction failed
	 */
	protected void receiveTX_FAILURE() {
		abortCurrentTransaction();
		processWaitingForCommitQueue();
	}

	/**
	 * Transaction succeeded
	 */
	protected void receiveTX_SUCCESS() {
		commitCurrentTransaction();
		processWaitingForCommitQueue();
	}

	/**
	 * @param msgString
	 *            <filename> <contents> ex) test hello world
	 */
	protected void receiveWD(int from, String filename, String contents) {
		if (managerUnknown()) {
			return;
		}

		// has RW!
		// TODO: Make/use a helper for this that takes care of the logging
		node.printVerbose("got ReadWrite on " + filename);
		cacheStatus.put(filename, CacheStatuses.ReadWrite);

		PendingClientOperation intent = pendingOperations.get(filename);

		if (intent == null) {
			node.printError(new MissingPendingRequestException(
					"missing intent on file: " + filename));
			return;
		}

		if (intent.operation == ClientOperation.APPEND) {
			try {
				// Managers version is always correct, so write straight to RFS
				if (!Utility.fileExists(node, filename)) {
					node.fs.createFile(filename);
				}
				node.fs.writeFile(filename, contents, false);
			} catch (IOException e) {
				// TODO: double check doing everything needed
				if (transacting) {
					abortCurrentTransaction();
					sendToManager(Protocol.TX_ABORT);
					unlockFile(filename);
				}
				return;
			}
		}

		try {
			// do what you originally intended with the file
			switch (intent.operation) {
			case PUT:
				if (transacting) {
					node.fs.writeFileTX(node.addr, filename, intent.content,
							false);
				} else {
					node.fs.writeFile(filename, intent.content, false);
				}
				break;
			case APPEND:
				if (transacting) {
					node.fs.writeFileTX(node.addr, filename, intent.content,
							true);
				} else {
					node.fs.writeFile(filename, intent.content, true);
				}
				break;
			default:
				throw new MissingPendingRequestException(
						"unhandled intent operation recalled on file: "
								+ filename);
			}
		} catch (Exception e) {
			node.printError(e);
			// TODO: double check doing the right thing
			if (transacting) {
				abortCurrentTransaction();
				sendToManager(Protocol.TX_ABORT);
				unlockFile(filename);
			}
			return;
		}

		// send wc and unlock locally
		sendToManager(Protocol.WC, Utility.stringToByteArray(filename));
		unlockFile(filename);
	}

	/*************************************************
	 * end receiveHandlers
	 ************************************************/
}
