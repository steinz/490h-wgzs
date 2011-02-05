import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;
import java.util.StringTokenizer;

import edu.washington.cs.cse490h.lib.Utility;

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

	protected Client node;

	public ClientNode(Client n) {
		this.node = n;

		this.clientCacheStatus = new HashMap<String, CacheStatuses>();
		this.clientPendingOperations = new HashMap<String, PendingClientOperation>();
		this.clientLockedFiles = new HashSet<String>();
		this.clientQueuedCommands = new HashMap<String, Queue<String>>();
		this.clientTransacting = false;
		this.clientWaitingForCommitSuccess = false;
		this.clientWaitingForCommitQueue = new LinkedList<String>();
	}

	/**
	 * For debugging purposes only. Prints expected numbers for in and out
	 * channels. Likely to change as new problems arise.
	 */
	public void debugHandler(StringTokenizer tokens, String line) {
		node.RIOLayer.printSeqStateDebug();
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
		node.managerAddr = Integer.parseInt(tokens.nextToken());
		node.printInfo("setting manager address to " + node.managerAddr);
	}

	/**
	 * Check if the client has locked the filename. Queue the passed in action
	 * if the file is locked and return true. Otherwise return false.
	 */
	protected boolean clientQueueLineIfLocked(String filename, String line) {
		if (clientLockedFiles.contains(filename)) {
			node.printVerbose("queueing command on locked file: " + filename
					+ ", " + line);

			Queue<String> requests = clientQueuedCommands.get(filename);
			if (requests == null) {
				requests = new LinkedList<String>();
				clientQueuedCommands.put(filename, requests);
			}
			requests.add(line);

			return true;
		} else {
			return false;
		}
	}

	/**
	 * Lock the provided filename and print a message
	 */
	protected void clientLockFile(String filename) {
		node.printVerbose("client locking file: " + filename);
		node.logSynopticEvent("CLIENT-LOCK");
		clientLockedFiles.add(filename);
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

		if (clientQueueLineIfLocked(filename, line)) {
			return;
		} else if (clientCacheStatus.containsKey(filename)
				&& (clientCacheStatus.get(filename) == CacheStatuses.ReadWrite)) {
			// have permissions
			if (clientTransacting) {
				node.fs.createFileTX(node.addr, filename);
			} else {
				node.fs.createFile(filename);
			}
		} else {
			// lock and perform rpc
			if (node.managerAddr == -1) {
				throw new UnknownManagerException();
			} else {
				clientLockFile(filename);
				createRPC(node.managerAddr, filename);
			}
		}
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

		if (clientQueueLineIfLocked(filename, line)) {
			return;
		} else if (clientCacheStatus.containsKey(filename)
				&& clientCacheStatus.get(filename) == CacheStatuses.ReadWrite) {
			// have permissions
			if (clientTransacting) {
				node.fs.deleteFileTX(node.addr, filename);
			} else {
				node.fs.deleteFile(filename);
			}
		} else {
			// lock and perform rpc
			if (node.managerAddr == -1) {
				throw new UnknownManagerException();
			} else {
				clientLockFile(filename);
				deleteRPC(node.managerAddr, filename);
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

		if (clientQueueLineIfLocked(filename, line)) {
			return;
		} else if (clientCacheStatus.containsKey(filename)) {
			// have permissions
			String content;
			if (clientTransacting) {
				content = node.fs.getFileTX(node.addr, filename);
			} else {
				content = node.fs.getFile(filename);
			}
			node.printInfo("Got file, contents below:");
			node.printInfo(content);
		} else {
			// lock and get permissions
			clientLockFile(filename);
			node.printVerbose("requesting read access for " + filename);
			sendToManager(Protocol.RQ, Utility.stringToByteArray(filename));
		}
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

		if (clientQueueLineIfLocked(filename, line)) {
			return;
		} else if (clientCacheStatus.containsKey(filename)
				&& clientCacheStatus.get(filename) == CacheStatuses.ReadWrite) {
			// have ownership - writeFile verifies existence
			if (clientTransacting) {
				node.fs.writeFileTX(node.addr, filename, content, false);
			} else {
				node.fs.writeFile(filename, content, false);
			}
		} else {
			// lock and request ownership
			clientLockFile(filename);
			node.printVerbose("requesting ownership of " + filename);
			sendToManager(Protocol.WQ, Utility.stringToByteArray(filename));
			clientPendingOperations.put(filename, new PendingClientOperation(
					ClientOperation.PUT, content));

		}
	}

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

		if (clientQueueLineIfLocked(filename, line)) {
			return;
		} else if (clientCacheStatus.containsKey(filename)
				&& clientCacheStatus.get(filename) == CacheStatuses.ReadWrite) {
			// have ownership - writeFile verifies existence
			if (clientTransacting) {
				node.fs.writeFileTX(node.addr, filename, content, true);
			} else {
				node.fs.writeFile(filename, content, true);
			}
		} else {
			// lock and request ownership
			clientLockFile(filename);
			node.printVerbose("requesting ownership of " + filename);
			sendToManager(Protocol.WQ, Utility.stringToByteArray(filename));
			clientPendingOperations.put(filename, new PendingClientOperation(
					ClientOperation.APPEND, content));

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
	 * Sends a noop
	 */
	public void noopHandler(StringTokenizer tokens, String line) {
		int server = Integer.parseInt(tokens.nextToken());
		node.RIOSend(server, Protocol.NOOP, Client.emptyPayload);
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
		if (clientTransacting) {
			throw new TransactionException(
					"client already performing a transaction");
		} else {
			clientTransacting = true;
			node.fs.startTransaction(node.addr);
			sendToManager(Protocol.TX_START);
		}
	}

	/**
	 * Sends a TX_COMMIT if performing a transaction
	 * 
	 * @throws TransactionException
	 * @throws UnknownManagerException
	 * @throws IOException
	 */
	public void txcommitHandler(StringTokenizer tokens, String line)
			throws TransactionException, UnknownManagerException, IOException {
		if (!clientTransacting) {
			throw new TransactionException(
					"client not performing a transaction");
		} else {
			// clientTransacting is updated when a response is received
			clientWaitingForCommitSuccess = true;
			sendToManager(Protocol.TX_COMMIT);
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
		if (!clientTransacting) {
			throw new TransactionException(
					"client not performing a transaction");
		} else {
			node.fs.abortTransaction(node.addr);
			clientTransacting = false;
			sendToManager(Protocol.TX_ABORT);
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

	/*************************************************
	 * begin client-only cache coherency functions
	 ************************************************/

	// TODO: HIGH: Cleanup state after {R,W}{Q,D,F} etc fails

	// TODO: HIGH: Probably should check isManager in Client instead of here

	/**
	 * Client receives IV as a notification to mark a cached file invalid
	 * 
	 * @throws NotClientException
	 * @throws UnknownManagerException
	 */
	protected void receiveIV(String msgString) throws NotClientException,
			UnknownManagerException {
		// If we're the manager and we received and IV, something bad happened
		if (node.isManager) {
			throw new NotClientException();
		}

		clientCacheStatus.remove(msgString);
		node.printVerbose("marking invalid " + msgString);

		node.printVerbose("sending ic to manager for file: " + msgString);
		sendToManager(Protocol.IC, Utility.stringToByteArray(msgString));
	}

	/**
	 * Client receives {W,R}F as a request to propagate their changes
	 * 
	 * @throws UnknownManagerException
	 * @throws IOException
	 * @throws PrivilegeLevelDisagreementException
	 */
	protected void receiveF(String msgString, String RForWF,
			int responseProtocol, boolean keepRO)
			throws UnknownManagerException, IOException,
			PrivilegeLevelDisagreementException {
		StringTokenizer tokens = new StringTokenizer(msgString);
		String filename = tokens.nextToken();

		String payload = null;

		if (!Utility.fileExists(node, msgString)) {
			// no file on disk, file was deleted
			responseProtocol = Protocol.WD_DELETE;
			payload = filename;
		} else {
			// read file contents
			payload = filename + Client.delimiter + node.fs.getFile(filename);
		}

		// send update to manager
		node.printVerbose("sending "
				+ Protocol.protocolToString(responseProtocol) + " to manager "
				+ filename);
		sendToManager(responseProtocol, Utility.stringToByteArray(payload));

		// update permissions
		if (keepRO) {
			clientCacheStatus.put(filename, CacheStatuses.ReadOnly);
			node.printVerbose("changed permission level to ReadOnly on file: "
					+ filename);
		} else {
			clientCacheStatus.remove(filename);
			node.printVerbose("losing permissions on file: " + filename);
		}
	}

	/**
	 * Convenience wrapper of RIOSend that sends a message to the manager if
	 * their address is known and throws an UnknownManagerException if not
	 * 
	 * @throws UnknownManagerException
	 */
	protected void sendToManager(int protocol) throws UnknownManagerException {
		sendToManager(protocol, Client.emptyPayload);
	}

	/**
	 * Convenience wrapper of RIOSend that sends a message to the manager if
	 * their address is known and throws an UnknownManagerException if not
	 * 
	 * @throws UnknownManagerException
	 */
	protected void sendToManager(int protocol, byte[] payload)
			throws UnknownManagerException {
		if (node.managerAddr == -1) {
			throw new UnknownManagerException();
		} else {
			node.RIOSend(node.managerAddr, protocol, payload);
		}
	}

	/*************************************************
	 * end client-only cache coherency functions
	 ************************************************/

	/*************************************************
	 * begin client and manager cache coherency functions
	 ************************************************/

	/**
	 * @param msgString
	 *            <filename> <contents> ex) test hello world
	 * @throws IOException
	 * @throws UnknownManagerException
	 * @throws IllegalConcurrentRequestException
	 * @throws MissingPendingRequestException
	 */
	protected void receiveWD(int from, String filename, String contents)
			throws IOException, UnknownManagerException,
			IllegalConcurrentRequestException, MissingPendingRequestException {

		// has RW!
		// TODO: Make/use a helper for this that takes care of the logging
		node.printVerbose("got ReadWrite on " + filename);
		clientCacheStatus.put(filename, CacheStatuses.ReadWrite);

		// TODO: HIGH: THIS SHOULD USE THE RFS - MANAGER IS GOD

		/*
		 * update in cache (not strictly necessary w/o txs on PUTs)
		 */
		if (!Utility.fileExists(node, filename)) {
			node.fs.createFile(filename);
		}
		node.fs.writeFile(filename, contents, false);

		// do what you originally intended with the file
		// TODO: HIGH: THIS SHOULD USE THE TFS
		if (clientPendingOperations.containsKey(filename)) {
			PendingClientOperation intent = clientPendingOperations
					.get(filename);
			switch (intent.operation) {
			case PUT:
				node.fs.writeFile(filename, intent.content, false);
				break;
			case APPEND:
				node.fs.writeFile(filename, intent.content, true);
				break;
			default:
				throw new MissingPendingRequestException(
						"unhandled intent operation recalled on file: "
								+ filename);
			}
		} else {
			throw new MissingPendingRequestException("missing intent on file: "
					+ filename);
		}

		// unlock for local use
		clientUnlockFile(filename);

		// send wc
		node.printVerbose("sending wc to manager for " + filename);
		sendToManager(Protocol.WC, Utility.stringToByteArray(filename));
	}

	/**
	 * @param msgString
	 * @throws IOException
	 * @throws UnknownManagerException
	 */
	protected void receiveRD(int from, String filename, String contents)
			throws IOException, UnknownManagerException {
		// has RO
		clientCacheStatus.put(filename, CacheStatuses.ReadOnly);
		node.printVerbose("got ReadOnly on " + filename);

		// update in cache
		if (!Utility.fileExists(node, filename)) {
			node.fs.createFile(filename);
		}
		node.fs.writeFile(filename, contents, false);

		// print GET result
		node.printInfo("Got file, contents below:");
		node.printInfo(contents);

		// unlock the file for local use
		clientUnlockFile(filename);

		// send rc
		node.printVerbose("sending rc to manager for " + filename);
		sendToManager(Protocol.RC, Utility.stringToByteArray(filename));
	}

	/*************************************************
	 * end client and manager cache coherency functions
	 ************************************************/

	/**
	 * RPC Error
	 * 
	 * @throws NotClientException
	 */
	protected void receiveError(Integer from, String msgString)
			throws NotClientException {

		// TODO: HIGH: Not sure what do we do here

		node.printError(msgString);

		String filename = msgString.split("")[0];
		clientUnlockFile(filename);

	}

	/**
	 * RPC Successful (only received after successful Create or Delete)
	 * 
	 * @throws Exception
	 */
	protected void receiveSuccessful(int from, String msgString)
			throws Exception {

		String[] split = msgString.split(Client.delimiter);
		String cmd = split[0];
		if (split.length > 1) {
			String filename = split[1];

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
				clientUnlockFile(filename);
				clientCacheStatus.put(filename, CacheStatuses.ReadWrite);
			} else if (cmd.equals(Protocol.protocolToString(Protocol.DELETE))) {
				if (Utility.fileExists(node, filename)) {
					// migh not exist here
					node.fs.deleteFile(filename);
				}
				clientUnlockFile(filename);
				clientCacheStatus.put(filename, CacheStatuses.ReadWrite);
			} else {
				// TODO: figure out what this exception really should be
				throw new Exception("receiveSuccessful got unknown packet cmd");
			}
		}
	}

	/**
	 * Transaction succeeded
	 * 
	 * @throws NotClientException
	 * @throws TransactionException
	 * @throws IOException
	 */
	protected void receiveTX_SUCCESS() throws NotClientException, IOException,
			TransactionException {

		if (!clientTransacting || !clientWaitingForCommitSuccess) {
			throw new TransactionException("unexpected "
					+ Protocol.protocolToString(Protocol.TX_SUCCESS)
					+ " received");
		}

		// commit tx locally
		node.fs.commitTransaction(node.addr);
		clientTransacting = false;

		// process pending commands
		clientWaitingForCommitSuccess = false;
		for (String line : clientWaitingForCommitQueue) {
			node.onCommand(line);
		}
	}

	/**
	 * Transaction failed
	 * 
	 * @throws NotClientException
	 * @throws TransactionException
	 * @throws IOException
	 */
	protected void receiveTX_FAILURE() throws NotClientException,
			TransactionException, IOException {
		node.fs.abortTransaction(node.addr);
		clientTransacting = false;
	}

	/**
	 * Unlock the filename and service and queued requests on it.
	 */
	protected void clientUnlockFile(String filename) {
		node.printVerbose("client unlocking file: " + filename);
		node.logSynopticEvent("CLIENT-UNLOCK");
		clientLockedFiles.remove(filename);

		// TODO: LOW: I think this needs a monad
		Queue<String> queuedRequests = clientQueuedCommands.get(filename);
		if (queuedRequests != null) {
			String request = queuedRequests.poll();
			if (request != null) {
				node.onCommand(request);
			}
		}
	}
}
