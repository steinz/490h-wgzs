/**
 * CSE 490h
 * @author wayger, steinz
 */

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Map.Entry;

import edu.washington.cs.cse490h.lib.Utility;

/**
 * Extension to the RIONode class that adds support basic file system operations
 * 
 * Nodes that extend this class for the support should use RIOSend and
 * onRIOReceive to send/receive the packets from the RIO layer. The underlying
 * layer can also be used by sending using the regular send() method and
 * overriding the onReceive() method to include a call to super.onReceive()
 * 
 * IMPORTANT: Methods names should not end in Handler unless they are meant to
 * handle commands passed in by onCommand - onCommand dynamically dispatches
 * commands to the method named <cmdName>Handler.
 * 
 * TODO: LOW: Managers and Clients are distinct in our implementation. That is,
 * a manager is not also a client. We should change receive methods in the so
 * that the manager acts as a client when it should and as manager otherwise.
 */
public class Client extends RIONode {

	/**
	 * TODO: HIGH: We require locking of files in order by name, document this
	 */

	/*
	 * TODO: LOW: Separate the Client and Manager code into two node types -
	 * this is impossible w/ framework since we can't start two node types
	 */

	/*
	 * TODO: LOW/EC: Let clients exchange files w/o sending them to the manager
	 */

	/*
	 * TODO: EC: Batch requests
	 */

	/*
	 * TODO: EC: Only send file diffs for big files
	 */

	/*
	 * TODO: EC: Don't send ACKs for messages that always get responses - for
	 * ex, let WD be WQ's ACK
	 */

	/*
	 * TODO: EC: Multiple TX for clients at the same time
	 */

	/**
	 * Possible cache statuses
	 */
	public static enum CacheStatuses {
		ReadWrite, ReadOnly
	};

	/**
	 * Operation types the client can remember in a PendingClientOperation
	 */
	public static enum ClientOperation {
		PUT, APPEND
	};

	/**
	 * Encapsulates a client command and argument
	 * 
	 * This includes operation and contents but not filename
	 */
	protected class PendingClientOperation {
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
	 * Delimiter used in protocol payloads. Should be a single character.
	 */
	protected static final String delimiter = " ";

	/**
	 * Name of the temp file used by write when append is false
	 */
	protected static final String tempFilename = ".temp";

	/**
	 * Name of the log file used by FS operations
	 */
	protected static final String logFilename = ".log";

	/**
	 * Whether or not this node is the manager for project 2.
	 */
	protected boolean isManager;

	/**
	 * The address of the manager node.
	 */
	protected int managerAddr;

	/*************************************************
	 * begin client data structures
	 ************************************************/

	/**
	 * Status of cached files on disk. Keys are filenames.
	 */
	protected Map<String, CacheStatuses> clientCacheStatus;

	/**
	 * Map from filenames to the operation we want to do on them later
	 */
	protected Map<String, PendingClientOperation> clientPendingOperations;

	/**
	 * List of files locked on the client's side
	 */
	protected Set<String> clientLockedFiles;

	/**
	 * Saves commands on client side locked files
	 */
	protected Map<String, Queue<String>> clientQueuedCommands;

	/*************************************************
	 * end client data structures
	 ************************************************/

	/*************************************************
	 * begin manager only data structures
	 ************************************************/

	/**
	 * List of files whose requests are currently being worked out.
	 */
	protected Set<String> managerLockedFiles;

	/*
	 * TODO: This might be cleaner if it was two Maps:
	 * 
	 * Map<String, Integer> managerCacheRW;
	 * 
	 * Map<String, List<Integer>> managerCacheRO;
	 */

	/**
	 * Status of cached files for all clients.
	 */
	protected Map<String, Map<Integer, CacheStatuses>> managerCacheStatuses;

	/**
	 * List of nodes the manager is waiting for ICs from.
	 */
	protected Map<String, List<Integer>> managerPendingICs;

	/**
	 * Save requests on locked files
	 */
	private Map<String, Queue<QueuedFileRequest>> managerQueuedFileRequests;

	/**
	 * Status of who is waiting for permission for this file
	 */
	private Map<String, Integer> managerPendingCCPermissionRequests;

	/**
	 * Status of who is waiting to delete this file via RPC
	 */
	private Map<String, Integer> managerPendingRPCDeleteRequests;

	/**
	 * Status of who is waiting to create this file via RPC
	 */
	private Map<String, Integer> managerPendingRPCCreateRequests;

	/*************************************************
	 * end manager only data structures
	 ************************************************/

	/*
	 * TODO: HIGH: Implement TransactionalFileSystem
	 * 
	 * All FS operations write to log and queue op in memory.
	 * 
	 * Queued ops are written to disk on commit, then log is cleared.
	 * 
	 * Queue is thrown out on abort.
	 */

	/**
	 * FS for this node
	 */
	protected ReliableFileSystem fs;

	/**
	 * Cleans up failed puts if necessary
	 */
	public void start() {
		// NOTE: Initialize manager state in the managerHandler

		// Initialization
		this.clientCacheStatus = new HashMap<String, CacheStatuses>();
		this.clientPendingOperations = new HashMap<String, PendingClientOperation>();
		this.clientLockedFiles = new HashSet<String>();
		this.clientQueuedCommands = new HashMap<String, Queue<String>>();
		this.isManager = false;
		this.managerAddr = -1;

		// Wipe the server log
		Logger.eraseLog();

		fs = new ReliableFileSystem(this);

		// Look for a temp file to recover
		try {
			fs.recoverTempFile();
		} catch (FileNotFoundException e) {
			printError(e);
		} catch (IOException e) {
			printError(e);
		}
	}

	/**************************************************************************
	 * begin onCommand Handler methods
	 * 
	 * these methods should pass all exceptions up to their caller
	 **************************************************************************/

	/*
	 * TODO: Verify that we log all sends
	 * 
	 * /** Prints expected numbers for in and out channels. Likely to change as
	 * new problems arise.
	 */
	public void debugHandler(StringTokenizer tokens, String line) {
		RIOLayer.printSeqStateDebug();
	}

	/**
	 * Used for project2 to tell a node it is the manager.
	 */
	public void managerHandler(StringTokenizer tokens, String line) {
		if (!isManager) {
			printInfo("promoted to manager");

			this.isManager = true;
			this.managerLockedFiles = new HashSet<String>();
			this.managerCacheStatuses = new HashMap<String, Map<Integer, CacheStatuses>>();
			this.managerPendingICs = new HashMap<String, List<Integer>>();
			this.managerQueuedFileRequests = new HashMap<String, Queue<QueuedFileRequest>>();
			this.managerPendingCCPermissionRequests = new HashMap<String, Integer>();
			this.managerPendingRPCDeleteRequests = new HashMap<String, Integer>();
			this.managerPendingRPCCreateRequests = new HashMap<String, Integer>();
		} else {
			printInfo("already manager");
		}
	}

	/**
	 * Used for project2 to tell a node the address of the manager.
	 */
	public void managerisHandler(StringTokenizer tokens, String line) {
		this.managerAddr = Integer.parseInt(tokens.nextToken());
		printInfo("manager is " + this.managerAddr);
	}

	/**
	 * Check if the client has locked the filename. Queue the passed in action
	 * if the file is locked and return true. Otherwise return false.
	 */
	protected boolean clientQueueLineIfLocked(String filename, String line) {
		if (clientLockedFiles.contains(filename)) {
			printVerbose("Queueing command on locked file: " + line);

			Queue<String> requests = clientQueuedCommands.get(filename);
			if (requests == null) {
				requests = new LinkedList<String>();
			}
			requests.add(line);

			clientQueuedCommands.put(filename, requests);
			return true;
		} else {
			return false;
		}
	}

	/**
	 * Lock the provided filename and print a message
	 */
	protected void clientLockFile(String filename) {
		printVerbose("client locking file: " + filename);
		logSynopticEvent("CLIENT-LOCK");
		clientLockedFiles.add(filename);
	}

	/*
	 * TODO: Throw Exceptions when ops called on manager (this isn't currently
	 * supported by the CC protocol)
	 */

	/**
	 * Get ownership of a file and create it
	 * 
	 * @throws IOException
	 * @throws UnknownManagerException
	 */
	public void createHandler(StringTokenizer tokens, String line)
			throws IOException, UnknownManagerException {
		String filename = tokens.nextToken();

		if (clientQueueLineIfLocked(filename, line)) {
			return;
		} else if (clientCacheStatus.containsKey(filename)
				&& (clientCacheStatus.get(filename) == CacheStatuses.ReadWrite)) {
			// have permissions
			fs.createFile(filename);
		} else {
			// lock and perform rpc
			if (this.managerAddr == -1) {
				throw new UnknownManagerException();
			} else {
				clientLockFile(filename);
				createRPC(this.managerAddr, filename);
			}
		}
	}

	/**
	 * Get ownership of a file and delete it
	 * 
	 * @throws IOException
	 * @throws UnknownManagerException
	 */
	public void deleteHandler(StringTokenizer tokens, String line)
			throws IOException, UnknownManagerException {
		String filename = tokens.nextToken();

		if (clientQueueLineIfLocked(filename, line)) {
			return;
		} else if (clientCacheStatus.containsKey(filename)
				&& clientCacheStatus.get(filename) == CacheStatuses.ReadWrite) {
			// have permissions
			fs.deleteFile(filename);
		} else {
			// lock and perform rpc
			if (this.managerAddr == -1) {
				throw new UnknownManagerException();
			} else {
				clientLockFile(filename);
				deleteRPC(this.managerAddr, filename);
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
			String content = fs.getFile(filename);
			printInfo("Got file, contents below:");
			printInfo(content);
		} else {
			// lock and get permissions
			clientLockFile(filename);
			printVerbose("requesting read access for " + filename);
			SendToManager(Protocol.RQ, Utility.stringToByteArray(filename));
		}
	}

	/*
	 * TODO: Investigate the cause of the PUT<->DELETE loop near the bottom of
	 * the PUT command chain in CCTester2ClientsCorrectness
	 */

	/**
	 * Get ownership of a file and put to it
	 * 
	 * @throws IOException
	 * @throws UnknownManagerException
	 */
	public void putHandler(StringTokenizer tokens, String line)
			throws IOException, UnknownManagerException {
		String filename = tokens.nextToken();
		String content = parseAddContent(line, "put", filename);

		if (clientQueueLineIfLocked(filename, line)) {
			return;
		} else if (clientCacheStatus.containsKey(filename)
				&& clientCacheStatus.get(filename) == CacheStatuses.ReadWrite) {
			// have ownership - writeFile verifies existence on disk
			fs.writeFile(filename, content, false);
		} else {
			// lock and request ownership
			clientLockFile(filename);
			printVerbose("requesting ownership of " + filename);
			SendToManager(Protocol.WQ, Utility.stringToByteArray(filename));
			clientPendingOperations.put(filename, new PendingClientOperation(
					ClientOperation.PUT, content));

		}
	}

	/**
	 * Get ownership of a file and append to it
	 * 
	 * @param tokens
	 * @param line
	 * @throws IOException
	 * @throws UnknownManagerException
	 */
	public void appendHandler(StringTokenizer tokens, String line)
			throws IOException, UnknownManagerException {
		// TODO: I think I found a framework bug - "append 1 test  world" is
		// losing the extra space

		String filename = tokens.nextToken();
		String content = parseAddContent(line, "append", filename);

		if (clientQueueLineIfLocked(filename, line)) {
			return;
		} else if (clientCacheStatus.containsKey(filename)
				&& clientCacheStatus.get(filename) == CacheStatuses.ReadWrite) {
			// have ownership
			fs.writeFile(filename, content, true);
		} else {
			// lock and request ownership
			clientLockFile(filename);
			printVerbose("requesting ownership of " + filename);
			SendToManager(Protocol.WQ, Utility.stringToByteArray(filename));
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
		String payload = getID().toString();
		printInfo("sending handshake to " + server);
		RIOSend(server, Protocol.HANDSHAKE, Utility.stringToByteArray(payload));
	}

	/**
	 * Sends a noop
	 */
	public void noopHandler(StringTokenizer tokens, String line) {
		int server = Integer.parseInt(tokens.nextToken());
		printVerbose("sending noop to " + server);
		RIOSend(server, Protocol.NOOP, Utility.stringToByteArray(""));
	}

	/*************************************************
	 * end onCommand Handler methods
	 ************************************************/

	/*************************************************
	 * begin RPC methods
	 ************************************************/

	/**
	 * Perform a create RPC to the given address
	 */
	public void createRPC(int address, String filename) {
		printVerbose("sending create rpc to " + address + " for file: "
				+ filename);
		RIOSend(address, Protocol.CREATE, Utility.stringToByteArray(filename));
	}

	/**
	 * Perform a delete RPC to the given address
	 */
	public void deleteRPC(int address, String filename) {
		printVerbose("sending delete rpc to " + address + " for file: "
				+ filename);
		RIOSend(address, Protocol.DELETE, Utility.stringToByteArray(filename));
	}

	/*************************************************
	 * end RPC methods
	 ************************************************/

	/*
	 * Propagate errors back to clients, send aborts on failures, etc.
	 */

	/**
	 * Process a command from user or file. Lowercases the command for further
	 * internal use.
	 */
	public void onCommand(String line) {
		// Create a tokenizer and get the first token (the actual cmd)
		StringTokenizer tokens = new StringTokenizer(line, " ");
		String cmd = "";
		try {
			cmd = tokens.nextToken().toLowerCase();
		} catch (NoSuchElementException e) {
			printError("no command found");
			return;
		}

		// Dynamically call <cmd>Command, passing off the tokenizer and the full
		// command string
		try {
			Class<?>[] paramTypes = { StringTokenizer.class, String.class };
			Method handler = this.getClass().getMethod(cmd + "Handler",
					paramTypes);
			Object[] args = { tokens, line };
			handler.invoke(this, args);
		} catch (NoSuchMethodException e) {
			printError("invalid command");
		} catch (IllegalArgumentException e) {
			printError("invalid command");
		} catch (IllegalAccessException e) {
			printError("invalid command");
		} catch (InvocationTargetException e) {
			/*
			 * TODO: HIGH: Command failed, abort tx
			 */

			printError(e);
		}
	}

	/*************************************************
	 * begin logger wrappers
	 ************************************************/

	/**
	 * Prepend the node address and then call Logger.verbose.
	 */
	public void printVerbose(String msg, boolean frame) {
		StringBuilder sb = appendNodeAddress();
		sb.append(msg);
		Logger.verbose(sb.toString(), frame);
	}

	/**
	 * Stub for printVerbose that doesn't print a frame.
	 */
	public void printVerbose(String msg) {
		printVerbose(msg, false);
	}

	/**
	 * Prepend the node address and then call Logger.info
	 */
	public void printInfo(String msg) {
		StringBuilder sb = appendNodeAddress();
		sb.append(msg);
		Logger.info(sb.toString());
	}

	/**
	 * Prints node name and then exception via logger
	 */
	public void printError(Exception e) {
		StringBuilder sb = appendNodeAddress();
		sb.append("caught exception (see below)");
		Logger.error(e);
	}

	/**
	 * Print node name, error, then msg
	 */
	public void printError(String msg) {
		StringBuilder sb = appendNodeAddress();
		sb.append("Error: ");
		sb.append(msg);
		Logger.error(sb.toString());
	}

	/*************************************************
	 * end logger wrappers
	 ************************************************/

	/**
	 * Method that is called by the RIO layer when a message is to be delivered.
	 * 
	 * @param from
	 *            The address from which the message was received
	 * @param protocol
	 *            The protocol identifier of the message
	 * @param msg
	 *            The message that was received
	 */
	public void onRIOReceive(Integer from, int protocol, byte[] msg) {
		printVerbose("receiving packet from RIOLayer");

		String msgString = Utility.byteArrayToString(msg);

		// TODO: Replace massive switch w/ dynamic dispatch

		try {
			switch (protocol) {
			case Protocol.CREATE:
				receiveCreate(from, msgString);
				break;
			case Protocol.DELETE:
				receiveDelete(from, msgString);
				break;
			case Protocol.GET:
				printError("received deprecated "
						+ Protocol.protocolToString(Protocol.GET) + " packet");
				break;
			case Protocol.PUT:
				printError("received deprecated "
						+ Protocol.protocolToString(Protocol.PUT) + " packet");
				break;
			case Protocol.APPEND:
				printError("received deprecated "
						+ Protocol.protocolToString(Protocol.APPEND)
						+ " packet");
				break;
			case Protocol.DATA:
				printError("received deprecated "
						+ Protocol.protocolToString(Protocol.DATA) + " packet");
				break;
			case Protocol.NOOP:
				printInfo("received noop from " + from);
				break;
			case Protocol.IC:
				receiveIC(from, msgString);
				break;
			case Protocol.IV:
				receiveIV(msgString);
				break;
			case Protocol.WC:
				receiveWC(from, msgString);
				break;
			case Protocol.RC:
				receiveRC(from, msgString);
				break;
			case Protocol.WD:
				receiveWD(from, msgString);
				break;
			case Protocol.RD:
				receiveRD(from, msgString);
				break;
			case Protocol.RQ:
				receiveRQ(from, msgString);
				break;
			case Protocol.WQ:
				receiveWQ(from, msgString);
				break;
			case Protocol.WF:
				receiveWF(msgString);
				break;
			case Protocol.RF:
				receiveRF(msgString);
				break;
			case Protocol.WD_DELETE:
				receiveWD_DELETE(from, msgString);
				break;
			case Protocol.ERROR:
				receiveError(from, msgString);
				break;
			case Protocol.SUCCESS:
				receiveSuccessful(from, msgString);
				break;
			default:
				printError("received invalid packet");
			}
		} catch (Exception e) {

			/*
			 * TODO: HIGH: All errors should be caught and dealt w/ here
			 * 
			 * Manager side: respond by sendError to from or failing tx
			 * 
			 * Client side: printError, abort tx?
			 */

			printError(e);
		}
	}

	/*************************************************
	 * begin manager-only cache coherency functions
	 ************************************************/

	/**
	 * TODO: HIGH: Manager: send heartbeat pings to client in middle of
	 * transactions, fail their transactions if they are too slow in responding
	 */

	/**
	 * Create RPC
	 * 
	 * @throws NotManagerException
	 * @throws IOException
	 */
	protected void receiveCreate(int client, String filename)
			throws NotManagerException, IOException {
		if (!isManager) {
			throw new NotManagerException();
		}

		if (managerQueueRequestIfLocked(client, Protocol.CREATE, filename)) {
			return;
		}

		if (managerCacheStatuses.containsKey(filename)
				&& managerCacheStatuses.get(filename).size() > 0) {

			// Find out if anyone has RW
			Integer rw = null;
			Map<Integer, CacheStatuses> cacheStatuses = managerCacheStatuses
					.get(filename);
			for (Entry<Integer, CacheStatuses> entry : cacheStatuses.entrySet()) {
				if (entry.getValue() == CacheStatuses.ReadWrite) {
					rw = entry.getKey();
				}
			}

			if (rw == null) {
				// everyone has RO, so file must exist
				sendError(client, Protocol.ERROR, filename,
						ErrorCode.FileAlreadyExists);
			} else {
				// owner could have deleted the file, WF them
				sendRequest(rw, filename, Protocol.WF);
				managerPendingRPCCreateRequests.put(filename, client);
				managerLockFile(filename);
			}

		} else { // file not in system
			createNewFile(filename, client);
		}
	}

	/**
	 * receiveCreate helper called when filename is not in the system
	 * 
	 * @throws IOException
	 * @throws NotManagerException
	 */
	protected void createNewFile(String filename, int client)
			throws IOException, NotManagerException {
		// local create
		fs.createFile(filename);

		// give RW to the requester for filename
		managerUpdateCacheStatus(CacheStatuses.ReadWrite, client, filename);

		// send success to requester
		printVerbose("sending " + Protocol.protocolToString(Protocol.SUCCESS)
				+ " to " + client);
		sendSuccess(client, Protocol.CREATE, filename);
	}

	/**
	 * Delete RPC
	 * 
	 * @throws NotManagerException
	 * @throws IOException
	 * @throws PrivilegeLevelDisagreementException
	 * @throws InconsistentPrivelageLevelsDetectedException
	 */
	protected void receiveDelete(int from, String filename)
			throws NotManagerException, IOException,
			PrivilegeLevelDisagreementException,
			InconsistentPrivelageLevelsDetectedException {
		if (!isManager) {
			throw new NotManagerException();
		}

		if (managerQueueRequestIfLocked(from, Protocol.DELETE, filename)) {
			return;
		}

		// Check if anyone has RW or RO status on this file
		Map<Integer, CacheStatuses> clientStatuses = managerCacheStatuses
				.get(filename);

		if (clientStatuses == null || clientStatuses.size() < 1) {
			// File doesn't exist, send an error to the requester
			sendError(from, Protocol.DELETE, filename,
					ErrorCode.FileDoesNotExist);
			return;
		}

		Integer rw = null;
		ArrayList<Integer> ro = new ArrayList<Integer>();

		/*
		 * TODO: clean this up, comment what's going on - could model after
		 * receiveQ's loop / factor that loop out into a helper
		 */

		// check for nodes with permissions on this file currently
		for (Entry<Integer, CacheStatuses> entry : clientStatuses.entrySet()) {
			if (entry.getValue().equals(CacheStatuses.ReadWrite)) {
				if (rw != null) {
					throw new InconsistentPrivelageLevelsDetectedException(
							"Detected multiple owners on file: " + filename);
				}
				rw = entry.getKey();
			}
			if (entry.getValue().equals(CacheStatuses.ReadOnly)) {
				if (rw != null) {
					throw new InconsistentPrivelageLevelsDetectedException(
							"Detected clients with simultaneous RW and RO on file: "
									+ filename);
				}
				if (entry.getKey() != from) {
					ro.add(entry.getKey());
				}
			}
		}

		boolean waitingForResponses = false;

		// add to pending ICs
		if (rw != null && rw == from) {
			// Requester should have RW
			throw new PrivilegeLevelDisagreementException(
					"Got delete request from client with RW");
		} else if (rw != null && rw != from) {
			// Someone other than the requester has RW status, get updates
			sendRequest(rw, filename, Protocol.WF);
			waitingForResponses = true;
		} else if (ro.size() != 0) {
			managerPendingICs.put(filename, ro);
			for (Integer i : ro) {
				/*
				 * Send invalidate requests to everyone with RO (doesn't include
				 * the requester)
				 */
				sendRequest(i, filename, Protocol.IV);
			}
			waitingForResponses = true;
		}

		if (waitingForResponses) {
			// track pending request
			managerPendingRPCDeleteRequests.put(filename, from);
			managerLockFile(filename);
		} else {
			deleteExistingFile(filename, from);
		}
	}

	/**
	 * receiveDelete helper called when the file is in the system.
	 * 
	 * @throws IOException
	 */
	public void deleteExistingFile(String filename, int from)
			throws IOException {
		// delete the file locally
		fs.deleteFile(filename);

		// update permissions
		printVerbose("marking file " + filename + " as unowned");
		managerCacheStatuses.get(filename).clear();

		// no one had permissions, so send success
		sendSuccess(from, Protocol.DELETE, filename);
	}

	/**
	 * Queues the given request if the file is locked and returns true. Returns
	 * false if the file isn't locked.
	 */
	protected boolean managerQueueRequestIfLocked(int client, int protocol,
			String filename) {
		if (managerLockedFiles.contains(filename)) {
			Queue<QueuedFileRequest> requests = managerQueuedFileRequests
					.get(filename);
			if (requests == null) {
				requests = new LinkedList<QueuedFileRequest>();
				managerQueuedFileRequests.put(filename, requests);
			}
			requests.add(new QueuedFileRequest(client, protocol, Utility
					.stringToByteArray(filename)));
			return true;
		} else {
			return false;
		}
	}

	protected void managerLockFile(String filename) {
		printVerbose("manager locking file: " + filename);
		logSynopticEvent("MANAGER-LOCK");
		managerLockedFiles.add(filename);
	}

	protected void receiveQ(int client, String filename, int receivedProtocol,
			int responseProtocol, int forwardingProtocol, boolean preserveROs)
			throws IOException, NotManagerException,
			InconsistentPrivelageLevelsDetectedException {
		// check if locked
		if (managerQueueRequestIfLocked(client, receivedProtocol, filename)) {
			return;
		}

		// lock
		managerLockFile(filename);

		Map<Integer, CacheStatuses> clientStatuses = managerCacheStatuses
				.get(filename);

		// check if the file exists in the system
		if (clientStatuses == null || clientStatuses.size() == 0) {
			sendError(client, Protocol.ERROR, filename,
					ErrorCode.FileDoesNotExist);
			return;
		}

		// address of node w/ rw or null
		Integer rw = null;
		// list of nodes w/ ro
		List<Integer> ros = new ArrayList<Integer>();

		// populate rw and ros
		for (Entry<Integer, CacheStatuses> entry : clientStatuses.entrySet()) {
			if (entry.getValue().equals(CacheStatuses.ReadWrite)) {
				if (rw != null) {
					throw new InconsistentPrivelageLevelsDetectedException(
							"multiple owners " + "(" + rw + " and "
									+ entry.getKey() + ") detected on file: "
									+ filename);
				}
				rw = entry.getKey();
			} else if (entry.getValue().equals(CacheStatuses.ReadOnly)
					&& entry.getKey() != client) {
				// ros will not containg the requester (if has RO wants RW)
				ros.add(entry.getKey());
			}
		}

		if (rw != null && ros.size() > 0) {
			throw new InconsistentPrivelageLevelsDetectedException(
					"simultaneous RW (" + rw + ") and ROs (" + ros.toString()
							+ ") detected on file: " + filename);
		}

		if (rw != null) { // someone has RW
			// Get updates
			sendRequest(rw, filename, forwardingProtocol);
			managerPendingCCPermissionRequests.put(filename, client);
		} else if (!preserveROs && ros.size() > 0) { // someone(s) have RO
			managerPendingICs.put(filename, ros);
			for (int i : ros) {
				// Invalidate all ROs
				sendRequest(i, filename, Protocol.IV);
			}
			managerPendingCCPermissionRequests.put(filename, client);
		} else { // no one has RW or RO
			/*
			 * TODO: Should this be an error - I think it currently sends
			 * whatever is on disk?
			 */

			// send file to requester
			managerSendFile(client, filename, responseProtocol);
			// unlock and privelages updated by C message handlers
		}
	}

	/**
	 * Helper that sends the contents of filename to to with protocol protocol.
	 * Should only be used by the manager.
	 * 
	 * @throws IOException
	 */
	protected void managerSendFile(int to, String filename, int protocol)
			throws IOException {
		StringBuilder sendMsg = new StringBuilder();

		if (!Utility.fileExists(this, filename)) {
			// Manager doesn't have the file
			sendError(to, Protocol.ERROR, filename, ErrorCode.FileDoesNotExist);
		} else {
			sendMsg.append(filename);
			sendMsg.append(delimiter);
			sendMsg.append(fs.getFile(filename));
		}

		byte[] payload = Utility.stringToByteArray(sendMsg.toString());
		printVerbose("sending " + Protocol.protocolToString(protocol) + " to "
				+ to);
		RIOSend(to, protocol, payload);
	}

	/**
	 * Helper that sends a request for the provided filename to the provided
	 * client using the provided protocol
	 */

	protected void sendRequest(int client, String filename, int protocol) {
		byte[] payload = Utility.stringToByteArray(filename);
		printVerbose("sending " + Protocol.protocolToString(protocol) + " to "
				+ client);
		RIOSend(client, protocol, payload);
	}

	/**
	 * TODO: HIGH: Delay all manager side unlocks until transaction finishes
	 */

	/**
	 * Unlocks filename and checks if there is another request to service
	 */
	protected void managerUnlockFile(String filename) {
		printVerbose("manager unlocking file: " + filename);
		logSynopticEvent("MANAGER-UNLOCK");
		managerLockedFiles.remove(filename);

		Queue<QueuedFileRequest> outstandingRequests = managerQueuedFileRequests
				.get(filename);
		if (outstandingRequests != null) {
			QueuedFileRequest nextRequest = outstandingRequests.poll();
			if (nextRequest != null) {
				onRIOReceive(nextRequest.from, nextRequest.protocol,
						nextRequest.msg);
			}
		}
	}

	protected void receiveRQ(int client, String filename)
			throws NotManagerException, IOException,
			InconsistentPrivelageLevelsDetectedException {
		if (!isManager) {
			throw new NotManagerException();
		}

		receiveQ(client, filename, Protocol.RQ, Protocol.RD, Protocol.RF, true);
	}

	protected void receiveWQ(int client, String filename)
			throws NotManagerException, IOException,
			InconsistentPrivelageLevelsDetectedException {
		if (!isManager) {
			throw new NotManagerException();
		}

		receiveQ(client, filename, Protocol.WQ, Protocol.WD, Protocol.WF, false);
	}

	/**
	 * Changes the status of this client from IV or RW
	 * 
	 * @param client
	 *            The client to change
	 * @param filename
	 *            The filename
	 * @throws NotManagerException
	 */
	protected void receiveWC(int client, String filename)
			throws NotManagerException {
		if (!isManager) {
			throw new NotManagerException();
		}

		managerUpdateCacheStatus(CacheStatuses.ReadWrite, client, filename);
		managerUnlockFile(filename);
	}

	/**
	 * Receives an RC and changes this client's status from IV or RW to RO.
	 * 
	 * @param client
	 *            The client to change
	 * @param filename
	 *            The filename
	 * @throws NotManagerException
	 */
	protected void receiveRC(int client, String filename)
			throws NotManagerException {
		if (!isManager) {
			throw new NotManagerException();
		}

		printVerbose("Changing client: " + client + " to RO");
		managerUpdateCacheStatus(CacheStatuses.ReadOnly, client, filename);
		managerUnlockFile(filename);
	}

	protected void managerUpdateCacheStatus(CacheStatuses val, int client,
			String filename) throws NotManagerException {
		Map<Integer, CacheStatuses> clientStatuses = managerCacheStatuses
				.get(filename);
		if (clientStatuses == null) {
			clientStatuses = new HashMap<Integer, CacheStatuses>();
			managerCacheStatuses.put(filename, clientStatuses);
		}

		// TODO: verify val prints string not int
		printVerbose("changing client: " + client + " to " + val);
		clientStatuses.put(client, val);
	}

	/**
	 * 
	 * @param from
	 *            The node this IC was received from.
	 * @param filename
	 *            Should be the file name. Throws an error if we were not
	 *            waiting for an IC from this node for this file
	 * @throws NotManagerException
	 * @throws IOException
	 */
	protected void receiveIC(Integer from, String filename)
			throws NotManagerException, IOException {
		if (!isManager) {
			throw new NotManagerException();
		}

		/*
		 * TODO: Maybe different messages for the first two vs. the last
		 * scenario (node is manager but not expecting IC from this node for
		 * this file)?
		 */

		int destAddr;
		if (!managerPendingICs.containsKey(filename) || !isManager
				|| !managerPendingICs.get(filename).contains(from)) {
			sendError(from, Protocol.ERROR, filename, ErrorCode.UnknownError);
			Logger.error(ErrorCode.NotManager, "IC: " + filename);
		} else {

			// update the status of the client who sent the IC
			Map<Integer, CacheStatuses> m = managerCacheStatuses.get(filename);
			m.remove(from);
			printVerbose("Changing client: " + from + " to IV");
			managerCacheStatuses.put(filename, m);

			List<Integer> waitingForICsFrom = managerPendingICs.get(filename);

			waitingForICsFrom.remove(from);
			if (waitingForICsFrom.isEmpty()) {
				/*
				 * If the pending ICs are now empty, someone's waiting for a WD,
				 * so check for that and send
				 */

				/*
				 * TODO: this should just clear to prevent reinitialization
				 * maybe, although this way could save some memory... Anyway,
				 * check that whatever assumption is made holds
				 */
				managerPendingICs.remove(filename);

				if (managerPendingCCPermissionRequests.containsKey(filename)) {
					destAddr = managerPendingCCPermissionRequests
							.remove(filename);
					managerSendFile(destAddr, filename, Protocol.WD);
				} else {
					destAddr = managerPendingRPCDeleteRequests.remove(filename);
					sendSuccess(destAddr, Protocol.DELETE, filename);
				}
			} else {
				// still waiting for more ICs
				List<Integer> waitingFor = managerPendingICs.get(filename);
				StringBuilder waiting = new StringBuilder();
				waiting.append("Received IC but waiting for IC from clients : ");
				for (int i : waitingFor) {
					waiting.append(i + " ");
				}
				printVerbose(waiting.toString());
			}
		}
	}

	/*************************************************
	 * end manager-only cache coherency functions
	 ************************************************/

	/*************************************************
	 * begin client-only cache coherency functions
	 ************************************************/

	// TODO: Cleanup state after {R,W}{Q,D,F} etc fails

	/**
	 * Client receives IV as a notification to mark a cached file invalid
	 * 
	 * @throws NotClientException
	 * @throws UnknownManagerException
	 */
	protected void receiveIV(String msgString) throws NotClientException,
			UnknownManagerException {
		// If we're the manager and we received and IV, something bad happened
		if (isManager) {
			throw new NotClientException();
		}

		clientCacheStatus.remove(msgString);
		printVerbose("marking invalid " + msgString);

		printVerbose("sending ic to manager for file: " + msgString);
		SendToManager(Protocol.IC, Utility.stringToByteArray(msgString));
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

		if (!Utility.fileExists(this, msgString)) {
			if (clientCacheStatus.get(filename) == CacheStatuses.ReadWrite) {
				// Client has RW but no file on disk, file was deleted
				responseProtocol = Protocol.WD_DELETE;
				payload = filename;
			} else {
				// Privilege level disagreement w/ manager
				throw new PrivilegeLevelDisagreementException(
						"Manager asked for " + RForWF + " on " + filename
								+ " but I don't have RW or the file on disk");
			}
		} else {
			// read file contents
			payload = filename + delimiter + fs.getFile(filename);
		}

		// send update to manager
		printVerbose("sending " + Protocol.protocolToString(responseProtocol)
				+ " to manager " + filename);
		SendToManager(responseProtocol, Utility.stringToByteArray(payload));

		// update permissions
		if (keepRO) {
			clientCacheStatus.put(filename, CacheStatuses.ReadOnly);
			printVerbose("changed permission level to ReadOnly on file: "
					+ filename);
		} else {
			clientCacheStatus.remove(filename);
			printVerbose("losing permissions on file: " + filename);
		}
	}

	/**
	 * Client receives RF as a request from the server to propagate their
	 * changes.
	 * 
	 * @throws NotClientException
	 * @throws IOException
	 * @throws UnknownManagerException
	 * @throws PrivilegeLevelDisagreementException
	 */
	protected void receiveRF(String msgString) throws NotClientException,
			UnknownManagerException, IOException,
			PrivilegeLevelDisagreementException {
		if (isManager) {
			throw new NotClientException();
		}

		receiveF(msgString, "RF", Protocol.RD, true);
	}

	/**
	 * Client receives WF as a request from the server to propagate their
	 * changes.
	 * 
	 * @throws NotClientException
	 * @throws IOException
	 * @throws UnknownManagerException
	 * @throws PrivilegeLevelDisagreementException
	 */
	protected void receiveWF(String msgString) throws NotClientException,
			UnknownManagerException, IOException,
			PrivilegeLevelDisagreementException {
		if (isManager) {
			throw new NotClientException();
		}

		receiveF(msgString, "WF", Protocol.WD, false);
	}

	/**
	 * Convenience wrapper of RIOSend that sends a message to the manager if
	 * their address is known and throws an UnknownManagerException if not
	 * 
	 * @throws UnknownManagerException
	 */
	protected void SendToManager(int protocol, byte[] payload)
			throws UnknownManagerException {
		if (this.managerAddr == -1) {
			throw new UnknownManagerException();
		} else {
			RIOSend(managerAddr, protocol, payload);
		}
	}

	/*************************************************
	 * end client-only cache coherency functions
	 ************************************************/

	/*************************************************
	 * begin client and manager cache coherency functions
	 ************************************************/

	protected void receiveWD_DELETE(int from, String filename)
			throws NotManagerException, IOException,
			MissingPendingRequestException {
		if (!isManager) {
			throw new NotManagerException(
					"WD_DELETE should only be received by the manager");
		}

		// delete locally
		fs.deleteFile(filename);

		// remove permissions
		managerCacheStatuses.get(filename).clear();

		// look for pending requests

		// check for a create
		Integer requester = managerPendingRPCCreateRequests.remove(filename);
		if (requester != null) {
			// create the file which was deleted by the owner
			createNewFile(filename, requester);
			managerUnlockFile(filename);
			return;
		}

		requester = managerPendingRPCDeleteRequests.remove(filename);
		if (requester != null) {
			// file was previously deleted by owner
			sendError(requester, Protocol.DELETE, filename,
					ErrorCode.FileDoesNotExist);
			return;
		}

		requester = managerPendingCCPermissionRequests.remove(filename);
		if (requester != null) {
			// file was deleted by owner
			sendError(requester, filename, ErrorCode.FileDoesNotExist);
			return;
		}

		throw new MissingPendingRequestException("file: " + filename);
	}

	/**
	 * @param msgString
	 *            <filename> <contents> for ex) test hello world
	 * @throws IOException
	 * @throws UnknownManagerException
	 * @throws IllegalConcurrentRequestException
	 * @throws MissingPendingRequestException
	 */
	protected void receiveWD(int from, String msgString) throws IOException,
			UnknownManagerException, IllegalConcurrentRequestException,
			MissingPendingRequestException {

		// parse packet
		StringTokenizer tokens = new StringTokenizer(msgString);
		String filename = tokens.nextToken();
		String contents = "";
		if (tokens.hasMoreTokens()) {
			contents = msgString.substring(filename.length() + 1);
		}

		if (!isManager) {
			// has RW!
			// TODO: Make/use a helper for this that takes care of the logging
			printVerbose("got ReadWrite on " + filename);
			clientCacheStatus.put(filename, CacheStatuses.ReadWrite);

			// update in cache
			if (!Utility.fileExists(this, filename)) {
				fs.createFile(filename);
			}
			fs.writeFile(filename, contents, false);

			// do what you originally intended with the file
			if (clientPendingOperations.containsKey(filename)) {
				PendingClientOperation intent = clientPendingOperations
						.get(filename);
				switch (intent.operation) {
				case PUT:
					fs.writeFile(filename, intent.content, false);
					break;
				case APPEND:
					fs.writeFile(filename, intent.content, true);
					break;
				default:
					throw new MissingPendingRequestException(
							"unhandled intent operation recalled on file: "
									+ filename);
				}
			} else {
				throw new MissingPendingRequestException(
						"missing intent on file: " + filename);
			}

			// unlock for local use
			clientUnlockFile(filename);

			// send wc
			printVerbose("sending wc to manager for " + filename);
			SendToManager(Protocol.WC, Utility.stringToByteArray(filename));

		} else { // Manager receives WD

			/*
			 * TODO: LOW: No need to do this for deletes or creates (although
			 * for creates it might give us a newer file version, which might be
			 * nice)
			 */
			// first write the file to save a local copy
			fs.writeFile(filename, contents, false);

			// look for pending request
			boolean foundPendingRequest = false;

			// check creates
			Integer destAddr = managerPendingRPCCreateRequests.remove(filename);
			if (destAddr != null) {
				foundPendingRequest = true;
				sendError(destAddr, Protocol.CREATE, filename,
						ErrorCode.FileAlreadyExists);
			}

			// check deletes
			destAddr = managerPendingRPCDeleteRequests.remove(filename);
			if (destAddr != null) {
				if (foundPendingRequest) {
					throw new IllegalConcurrentRequestException();
				}
				foundPendingRequest = true;
				deleteExistingFile(filename, destAddr);
			}

			// check CC
			destAddr = managerPendingCCPermissionRequests.remove(filename);
			if (destAddr != null) {
				if (foundPendingRequest) {
					throw new IllegalConcurrentRequestException();
				}
				foundPendingRequest = true;
				managerSendFile(destAddr, filename, Protocol.WD);
			}

			if (!foundPendingRequest) {
				throw new MissingPendingRequestException("file: " + filename);
			}

			// update the status of the client who sent the WD
			Map<Integer, CacheStatuses> m = managerCacheStatuses.get(filename);
			m.remove(from);
		}
	}

	/**
	 * @param msgString
	 * @throws IOException
	 * @throws UnknownManagerException
	 */
	protected void receiveRD(int from, String msgString) throws IOException,
			UnknownManagerException {
		// parse packet
		StringTokenizer tokens = new StringTokenizer(msgString);
		String filename = tokens.nextToken();
		String contents = "";
		if (tokens.hasMoreTokens()) {
			contents = msgString.substring(filename.length() + 1);
		}

		if (!isManager) {
			// has RO
			clientCacheStatus.put(filename, CacheStatuses.ReadOnly);
			printVerbose("got ReadOnly on " + filename);

			// update in cache
			if (!Utility.fileExists(this, filename)) {
				fs.createFile(filename);
			}
			fs.writeFile(filename, contents, false);

			// print GET result
			printInfo("Got file, contents below:");
			printInfo(contents);

			// unlock the file for local use
			clientUnlockFile(filename);

			// send rc
			printVerbose("sending rc to manager for " + filename);
			SendToManager(Protocol.RC, Utility.stringToByteArray(filename));
		} else {

			// first write the file to save a local copy
			fs.writeFile(filename, contents, false);

			/*
			 * send out a RD to anyone requesting this - unlike for WD, this
			 * shouldn't be a create or delete (which require RW, so send W{F,D}
			 * messages instead)
			 */
			int destAddr = managerPendingCCPermissionRequests.remove(filename);
			managerSendFile(destAddr, filename, Protocol.RD);

			// update the status of the client who sent the WD
			Map<Integer, CacheStatuses> m = managerCacheStatuses.get(filename);
			m.put(from, CacheStatuses.ReadOnly);
		}
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
		if (isManager) {
			throw new NotClientException();
		}

		printError(msgString);

		String filename = msgString.split("")[0];
		clientUnlockFile(filename);

		// TODO: Figure out if this gets called any other times
	}

	/**
	 * RPC Successful (only received after successful Create or Delete)
	 * 
	 * @throws Exception
	 */
	protected void receiveSuccessful(int from, String msgString)
			throws Exception {
		if (isManager) {
			throw new NotClientException();
		}

		String[] split = msgString.split(delimiter);
		String cmd = split[0];
		if (split.length > 1) {
			String filename = split[1];

			if (cmd.equals(Protocol.protocolToString(Protocol.CREATE))) {
				if (!Utility.fileExists(this, filename)) {
					fs.createFile(filename);
				} else {
					/*
					 * file could have been deleted by someone else, and now I'm
					 * creating, but I could still have an old copy on disk
					 */
					fs.writeFile(filename, "", false);
				}
				clientUnlockFile(filename);
				clientCacheStatus.put(filename, CacheStatuses.ReadWrite);
			} else if (cmd.equals(Protocol.protocolToString(Protocol.DELETE))) {
				if (Utility.fileExists(this, filename)) {
					// migh not exist here
					fs.deleteFile(filename);
				}
				clientUnlockFile(filename);
				clientCacheStatus.put(filename, CacheStatuses.ReadWrite);
			} else {
				// TODO: figure out what this exception really should be
				throw new Exception("receiveSuccessful got unknown packet cmd");
			}
		}

		// TODO: Figure out if this gets called any other times
	}

	/**
	 * Unlock the filename and service and queued requests on it.
	 */
	protected void clientUnlockFile(String filename) {
		printVerbose("client unlocking file: " + filename);
		logSynopticEvent("CLIENT-UNLOCK");
		clientLockedFiles.remove(filename);

		// TODO: LOW: I think this needs a monad
		Queue<String> queuedRequests = clientQueuedCommands.get(filename);
		if (queuedRequests != null) {
			String request = queuedRequests.poll();
			if (request != null) {
				onCommand(request);
			}
		}
	}

	/*
	 * TODO: Wayne: Comment all send{Success, Error} methods and think about
	 * wheter or not we really need all of them
	 */

	protected void sendSuccess(int destAddr, int protocol, String message) {
		String msg = Protocol.protocolToString(protocol) + delimiter + message;
		byte[] payload = Utility.stringToByteArray(msg);
		RIOLayer.RIOSend(destAddr, Protocol.SUCCESS, payload);
	}

	protected void sendError(int destAddr, String filename, int errorcode) {
		String msg = filename + delimiter + ErrorCode.lookup(errorcode);
		byte[] payload = Utility.stringToByteArray(msg);
		RIOLayer.RIOSend(destAddr, Protocol.ERROR, payload);
	}

	/**
	 * Send Error method
	 * 
	 * @param destAddr
	 *            Who to send the error code to
	 * @param protocol
	 *            The protocol that failed
	 * @param filename
	 *            The filename for the protocol that failed
	 * @param errorcode
	 *            The error code
	 */
	protected void sendError(int destAddr, int protocol, String filename,
			int errorcode) {
		String msg = filename + delimiter + Protocol.protocolToString(protocol)
				+ delimiter + ErrorCode.lookup(errorcode);
		byte[] payload = Utility.stringToByteArray(msg);
		RIOLayer.RIOSend(destAddr, Protocol.ERROR, payload);
	}

	/*************************************************
	 * begin invariant checkers
	 ************************************************/

	/*
	 * TODO: LOW: Write invariant checkers to verify cache / file state /
	 * locking invariants
	 */

	/*************************************************
	 * end invariant checkers
	 ************************************************/
}
