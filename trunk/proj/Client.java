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

import edu.washington.cs.cse490h.lib.PersistentStorageReader;
import edu.washington.cs.cse490h.lib.PersistentStorageWriter;
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

	/*
	 * TODO: LOW: Separate the Client and Manager code into two node types -
	 * this is impossible w/ framework since we can't start two node types
	 */

	/*
	 * TODO: EC: Batch requests
	 */

	/*
	 * TODO: EC: Only send file diffs for big files
	 */

	/**
	 * Possible cache statuses
	 */
	public static enum CacheStatuses {
		ReadWrite, ReadOnly
	};

	/**
	 * Commands the client can perform
	 */
	public static enum ClientOperation {
		CREATE, DELETE, PUT, APPEND
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
	 * Save requests on client side locked files
	 */
	protected Map<String, Queue<QueuedFileRequest>> clientQueuedFileRequests;

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

	/*************************************************
	 * end manager only data structures
	 ************************************************/

	public Client() {
		// Initialize manager state in the managerHandler
		super();
		// TODO: This should be in start, but I don't think it matters
		this.clientCacheStatus = new HashMap<String, CacheStatuses>();
		this.clientPendingOperations = new HashMap<String, PendingClientOperation>();
		this.clientLockedFiles = new HashSet<String>();
		this.clientQueuedFileRequests = new HashMap<String, Queue<QueuedFileRequest>>();
		this.isManager = false;
		this.managerAddr = -1;
		Logger.eraseLog(); // Wipe the server log
	}

	/**
	 * Cleans up failed puts if necessary
	 */
	public void start() {
		try {
			recoverTempFile();
		} catch (FileNotFoundException e) {
			printError(e);
		} catch (IOException e) {
			printError(e);
		}
	}

	/**
	 * Replaces the file on disk with the temp file to recover from a crash
	 * 
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	protected void recoverTempFile() throws FileNotFoundException, IOException {
		if (!Utility.fileExists(this, tempFilename)) {
			// Do nothing if we don't have a temp file
			return;
		}

		String tempFile = getFile(tempFilename);
		int newline = tempFile.indexOf(System.getProperty("line.separator"));
		String filename = tempFile.substring(0, newline);
		String content = tempFile.substring(newline + 1);

		writeFile(filename, content, false);
		deleteFile(tempFilename);
	}

	/**************************************************************************
	 * begin onCommand Handler methods
	 * 
	 * these methods should pass all exceptions up to their caller
	 **************************************************************************/

	/*
	 * TODO: Log sends. We kind of entirely re-wrote logging at this point...
	 * but I didn't like theirs enough to not do it... /*
	 */

	/**
	 * Prints expected numbers for in and out channels. Likely to change as new
	 * problems arise.
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

	// TODO: log local file unlocks

	/**
	 * Check if the client has locked the filename. Queue the passed in action
	 * if the file is locked and return true. Otherwise return false.
	 */
	protected boolean clientQueueLineIfLocked(String filename, String line) {
		if (clientLockedFiles.contains(filename)) {
			printVerbose("Queueing command on locked file: " + line);

			Queue<QueuedFileRequest> requests = clientQueuedFileRequests
					.get(filename);
			if (requests == null) {
				requests = new LinkedList<QueuedFileRequest>();
			}
			requests.add(new QueuedFileRequest(line));

			clientQueuedFileRequests.put(filename, requests);
			return true;
		} else {
			return false;
		}
	}

	/**
	 * Lock the provided filename and print a message
	 */
	protected void clientLockFile(String filename) {
		printVerbose("locking file: " + filename);
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
			createFile(filename);
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
			deleteFile(filename);
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
			String content = getFile(filename);
			printInfo("Got file, content is: " + content);
		} else {
			// lock and get permissions
			clientLockFile(filename);
			printVerbose("requesting read access for " + filename);
			SendToManager(Protocol.RQ, Utility.stringToByteArray(filename));
		}
	}

	/**
	 * Get ownership of a file and put to it
	 * 
	 * @param tokens
	 * @param line
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
			// have ownership
			writeFile(filename, content, false);
		} else {
			// lock and request ownership
			clientLockFile(filename);
			printInfo("requesting ownership of " + filename);
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
			writeFile(filename, content, true);
		} else {
			// lock and request ownership
			clientLockFile(filename);
			printInfo("requesting ownership of " + filename);
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
		printInfo("sending noop to " + server);
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
		printInfo("sending create rpc to " + address + " for file: " + filename);
		RIOSend(address, Protocol.CREATE, Utility.stringToByteArray(filename));
	}

	/**
	 * Perform a delete RPC to the given address
	 */
	public void deleteRPC(int address, String filename) {
		printInfo("sending delete rpc to " + address + " for file: " + filename);
		RIOSend(address, Protocol.DELETE, Utility.stringToByteArray(filename));
	}

	/*************************************************
	 * end RPC methods
	 ************************************************/

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
			printError(ErrorCode.InvalidCommand, "");
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
			printError(ErrorCode.InvalidCommand, cmd);
		} catch (IllegalArgumentException e) {
			printError(ErrorCode.DynamicCommandError, cmd);
		} catch (IllegalAccessException e) {
			printError(ErrorCode.DynamicCommandError, cmd);
		} catch (InvocationTargetException e) {
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

	// TODO: cleanup and comment printError

	public void printError(Exception e) {
		StringBuilder sb = appendNodeAddress();
		sb.append("caught exception (see below)");
		Logger.error(sb.toString());
		Logger.error(e);
	}

	public void printError(String msg) {
		StringBuilder sb = appendNodeAddress();
		sb.append("Error: ");
		sb.append(msg);
		Logger.error(sb.toString());
	}

	// TODO: reevaluate the existance of these helpers - above seem eaiser

	/**
	 * Convenience method for printing errors
	 */
	public void printError(int error, String command, int server,
			String filename) {
		StringBuilder sb = appendNodeAddress();
		sb.append(" ");
		appendError(sb, command);
		sb.append(" on server ");
		sb.append(server);
		sb.append(", file ");
		sb.append(filename);
		Logger.error(error, sb.toString());
	}

	/**
	 * Stub for printError for when less information is available
	 */
	public void printError(int error, String command, String filename) {
		StringBuilder sb = appendNodeAddress();
		sb.append(" ");
		appendError(sb, command);
		sb.append(" on file ");
		sb.append(filename);
		Logger.error(error, sb.toString());
	}

	/**
	 * Stub for printError for when even less information is available
	 */
	public void printError(int error, String command) {
		StringBuilder sb = appendNodeAddress();
		sb.append(" ");
		appendError(sb, command);
		Logger.error(error, sb.toString());
	}

	/**
	 * Helper that appends Error: label
	 */
	protected StringBuilder appendError(StringBuilder sb, String command) {
		sb.append("Error: ");
		sb.append(command);
		sb.append(" returned error code ");
		return sb;
	}

	/*************************************************
	 * end logger wrappers
	 ************************************************/

	/*************************************************
	 * begin FS methods
	 ************************************************/

	/**
	 * Creates a file on the local filesystem
	 * 
	 * @param filename
	 *            the file to create
	 * @throws IOException
	 */
	public void createFile(String filename) throws IOException {

		printVerbose("creating file: " + filename);
		logSynopticEvent("CREATING-FILE");

		if (Utility.fileExists(this, filename)) {
			throw new FileAlreadyExistsException();
		} else {
			PersistentStorageWriter writer = getWriter(filename, false);
			writer.close();
		}
	}

	/**
	 * Deletes a file from the local file system
	 * 
	 * @param filename
	 *            the file to delete
	 * @throws IOException
	 */
	public void deleteFile(String filename) throws IOException {

		printVerbose("deleting file: " + filename);
		logSynopticEvent("DELETING-FILE");

		if (!Utility.fileExists(this, filename)) {
			throw new FileNotFoundException();
		} else {
			PersistentStorageWriter writer = getWriter(filename, false);
			if (!writer.delete())
				throw new IOException("delete failed");
			writer.close();
		}

	}

	/**
	 * Local get file
	 * 
	 * @param filename
	 * @throws IOException
	 */
	public String getFile(String filename) throws IOException {

		printVerbose("getting file: " + filename);
		logSynopticEvent("GETTING-FILE");

		// check if the file exists
		if (!Utility.fileExists(this, filename)) {
			throw new FileNotFoundException();
		} else {
			// read and return the file if it does
			StringBuilder contents = new StringBuilder();
			String inLine = "";
			PersistentStorageReader reader = getReader(filename);

			// TODO: Make sure this works w/ empty files
			contents.append(reader.readLine());
			while ((inLine = reader.readLine()) != null) {
				// TODO: Test this on file ending w/ and w/o newline
				contents.append(System.getProperty("line.separator"));
				contents.append(inLine);
			}

			reader.close();
			return contents.toString();
		}
	}

	/**
	 * Writes a file to the local filesystem. Fails if the file does not exist
	 * already
	 * 
	 * @param filename
	 *            the file name to write to
	 * @param contents
	 *            the contents to write
	 * @throws IOException
	 */
	public void writeFile(String filename, String contents, boolean append)
			throws IOException {

		if (append) {
			printVerbose("appending to file: " + filename + ", contents: "
					+ contents);
			logSynopticEvent("APPENDING-FILE");
		} else {
			printVerbose("putting to file: " + filename + ", contents: "
					+ contents);
			logSynopticEvent("PUTTING-FILE");
		}

		if (!Utility.fileExists(this, filename)) {
			throw new FileAlreadyExistsException();
		} else {
			if (!append) {
				// save current contents in temp file
				writeTempFile(filename);
			}

			PersistentStorageWriter writer = getWriter(filename, append);
			writer.write(contents);
			writer.close();

			if (!append) {
				deleteFile(tempFilename);
			}
		}
	}

	/**
	 * Used to temporarily save a file that could be lost in a crash since
	 * getWriter deletes a file it doesn't open for appending.
	 * 
	 * @param filename
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	protected void writeTempFile(String filename) throws IOException {
		StringBuilder oldContent = new StringBuilder();
		oldContent.append(filename);
		oldContent.append(System.getProperty("line.separator"));

		PersistentStorageReader oldFileReader = getReader(filename);

		// TODO: Make sure this works w/ empty files
		oldContent.append(oldFileReader.readLine());

		String inLine;
		while ((inLine = oldFileReader.readLine()) != null) {
			oldContent.append(System.getProperty("line.separator"));
			oldContent.append(inLine);
		}

		oldFileReader.close();

		PersistentStorageWriter temp = getWriter(tempFilename, false);
		temp.write(oldContent.toString());
	}

	/****************************************************
	 * end FS methods
	 ***************************************************/

	// TODO: Organize everything below here

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
			printError(e);
		}
	}

	/*************************************************
	 * begin manager-only cache coherency functions
	 ************************************************/

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

		// TODO: HIGH: Check if locked?

		if (managerCacheStatuses.containsKey(filename)
				&& managerCacheStatuses.get(filename).size() > 0) {
			/*
			 * TODO: HIGH: This assumption is bad:
			 * 
			 * 1 create test
			 * 
			 * 1 delete test
			 * 
			 * 0 create test
			 * 
			 * is a counterexample - 0 gets FAE from here. 1 gets RW via RPC
			 * then deletes it just locally, so the manager still thinks 0 has
			 * RW, so the manager needs to poll the owner to get the file's real
			 * status.
			 * 
			 * There is proably a similar bug w/ receiveDelete
			 */

			// send error, file already exists
			sendError(client, Protocol.ERROR, filename,
					ErrorCode.FileAlreadyExists);
		} else {
			// local create
			createFile(filename);

			// give RW to the requester for filename
			// TODO: HIGH: I think there is / should be a helper for this
			Map<Integer, CacheStatuses> cache = managerCacheStatuses
					.get(filename);
			if (cache == null) {
				cache = new HashMap<Integer, CacheStatuses>();
				managerCacheStatuses.put(filename, cache);
			}
			cache.put(client, CacheStatuses.ReadWrite);
			printVerbose("marking file " + filename + " RW for node " + client);

			// send success to requester
			sendSuccess(client, Protocol.CREATE, filename);
		}
	}

	/**
	 * Delete RPC
	 * 
	 * @throws NotManagerException
	 * @throws IOException
	 * 
	 *             TODO: Zach, code review this, keep in mind receiveClient bug
	 */
	public void receiveDelete(int from, String filename)
			throws NotManagerException, IOException {
		if (!isManager) {
			throw new NotManagerException();
		}

		// TODO: Check if locked?

		// Check if anyone has RW or RO status on this file
		Map<Integer, CacheStatuses> clientStatuses = managerCacheStatuses
				.get(filename);

		if (clientStatuses == null) {
			// File doesn't exist, send an error to the requester
			sendError(from, Protocol.DELETE, filename,
					ErrorCode.FileDoesNotExist);
			return;
		}

		// delete the file locally
		deleteFile(filename);

		Integer rw = null;
		ArrayList<Integer> ro = new ArrayList<Integer>();

		// send out IVs for this file as well
		// check for nodes with permissions on this file currently
		for (Entry<Integer, CacheStatuses> entry : clientStatuses.entrySet()) {
			if (entry.getValue().equals(CacheStatuses.ReadWrite)) {
				if (rw != null) {
					printError("Detected multiple owners on file: " + filename);
				}
				rw = entry.getKey();
			}
			if (entry.getValue().equals(CacheStatuses.ReadOnly)) {
				if (rw != null) {
					printError("Detected clients with simultaneous RW and RO on file: "
							+ filename);
				}
				if (entry.getKey() != from) {
					ro.add(entry.getKey());
				}
			}
		}

		// update permissions
		clientStatuses.clear();
		printVerbose("marking file " + filename + " as unowned");

		boolean ivSent = false;

		// add to pending ICs
		if (rw != null && rw != from) {
			// Some other than the requester has RW status
			sendRequest(rw, filename, Protocol.IV);
			ivSent = true;
		}
		if (ro.size() != 0) {
			managerPendingICs.put(filename, ro);
			for (Integer i : ro) {
				/*
				 * Send invalidate requests to everyone with RO not including
				 * the requester
				 */
				sendRequest(i, filename, Protocol.IV);
			}
			ivSent = true;
		}

		if (ivSent) {
			// track pending request
			managerPendingRPCDeleteRequests.put(filename, from);
			return;
		} else {
			// no one has permissions, so send success
			sendSuccess(from, Protocol.DELETE, filename);

			// TODO: give RW to from
		}
	}

	// TODO: Zach: Code review of Manager only functions from here down

	protected void receiveRQ(int client, String filename)
			throws NotManagerException, IOException {
		if (!isManager) {
			throw new NotManagerException();
		}

		// Deal with locked files, and lock the file if it's not currently
		if (managerLockedFiles.contains(filename)) {
			Queue<QueuedFileRequest> e = managerQueuedFileRequests
					.get(filename);
			if (e == null) {
				e = new LinkedList<QueuedFileRequest>();
				managerQueuedFileRequests.put(filename, e);
			}
			e.add(new QueuedFileRequest(client, Protocol.RQ, Utility
					.stringToByteArray(filename)));
			return;
		}

		printVerbose("Locking file: " + filename);
		managerLockedFiles.add(filename);

		Map<Integer, CacheStatuses> clientStatuses = managerCacheStatuses
				.get(filename);

		// Check if the file exists in the system
		if (clientStatuses == null || clientStatuses.size() == 0) {
			sendError(client, Protocol.ERROR, filename,
					ErrorCode.FileDoesNotExist);
			return;
		}

		// Check if anyone has RW status on this file
		Integer key = null;

		for (Entry<Integer, CacheStatuses> entry : clientStatuses.entrySet()) {
			if (entry.getValue().equals(CacheStatuses.ReadWrite)) {
				if (key != null)
					Logger.error(ErrorCode.MultipleOwners,
							"Multiple owners on file: " + filename);
				key = entry.getKey();
			}
		}

		if (key == null) { // no one has RW
			// TODO: Convince ourselve manager has most recent version
			sendFile(client, filename, Protocol.RD);
			// unlock and RO will be given by RC
		} else {
			sendRequest(key, filename, Protocol.RF);
			managerPendingCCPermissionRequests.put(filename, client);
		}

	}

	protected void sendFile(int client, String filename, int protocol)
			throws IOException {
		String sendMsg = "";

		/*
		 * TODO: Is int client always a client in this context, or can it be the
		 * manager (when called in response to a {R,W}F)?
		 */

		if (!Utility.fileExists(this, filename)) {
			/*
			 * TODO: HIGH: Bad assumption - breaks when called from WD after
			 * delete (I think?) - if I'm right, this should check if it has RW
			 * - if it does, it should send something on to let the requester
			 * know it deleted the file (maybe a Protocol.DELETE)
			 */
			sendError(client, Protocol.ERROR, filename,
					ErrorCode.FileDoesNotExist);
		} else {
			sendMsg = filename + delimiter + getFile(filename);
		}

		byte[] payload = Utility.stringToByteArray(sendMsg);
		RIOSend(client, protocol, payload);
		printVerbose("sending " + Protocol.protocolToString(protocol) + " to "
				+ client);

		// TODO: HIGH: Update permissions on filename for client
	}

	protected void sendRequest(int client, String filename, int protocol) {
		String sendMsg = filename;
		byte[] payload = Utility.stringToByteArray(sendMsg);
		RIOSend(client, protocol, payload);
		printVerbose("sending " + protocol + " to " + client);
	}

	protected void removeLock(String filename) {
		managerLockedFiles.remove(filename);
		if (!managerQueuedFileRequests.containsKey(filename))
			managerQueuedFileRequests.put(filename,
					new LinkedList<QueuedFileRequest>());
		Queue<QueuedFileRequest> outstandingRequests = managerQueuedFileRequests
				.get(filename);
		QueuedFileRequest nextRequest = outstandingRequests.poll();
		if (nextRequest != null) {
			onRIOReceive(nextRequest.from, nextRequest.protocol,
					nextRequest.msg);
		}
	}

	protected void receiveWQ(int client, String filename)
			throws NotManagerException, IOException {
		if (!isManager) {
			throw new NotManagerException();
		}

		// Deal with locked files, and lock the file if it's not currently
		if (managerLockedFiles.contains(filename)) {
			Queue<QueuedFileRequest> e = managerQueuedFileRequests
					.get(filename);
			if (e == null) {
				e = new LinkedList<QueuedFileRequest>();
				managerQueuedFileRequests.put(filename, e);
			}
			e.add(new QueuedFileRequest(client, Protocol.WQ, Utility
					.stringToByteArray(filename)));
			return;
		}
		managerLockedFiles.add(filename);
		printVerbose("Locking file: " + filename);

		// Check if anyone has RW or RO status on this file
		Map<Integer, CacheStatuses> clientStatuses = managerCacheStatuses
				.get(filename);

		if (clientStatuses == null) {
			sendError(client, Protocol.ERROR, filename,
					ErrorCode.FileDoesNotExist);
			return;
		}

		Integer rw = null;
		ArrayList<Integer> ro = new ArrayList<Integer>();

		// check for nodes with permissions on this file currently
		for (Entry<Integer, CacheStatuses> entry : clientStatuses.entrySet()) {
			if (entry.getValue().equals(CacheStatuses.ReadWrite)) {
				if (rw != null)
					Logger.error(ErrorCode.MultipleOwners,
							"Multiple owners on file: " + filename);
				rw = entry.getKey();
			}
			if (entry.getValue().equals(CacheStatuses.ReadOnly)) {
				if (rw != null)
					Logger.error(ErrorCode.ReadWriteAndReadOnly,
							"ReadOnly copies while other client has ownership: "
									+ filename);
				if (entry.getKey() != client)
					ro.add(entry.getKey());
			}
		}

		if (rw != null) { // If someone has RW status:
			// Request the changes from the owner
			sendRequest(rw, filename, Protocol.WF);
			// Remember to send the changes to the requester
			managerPendingCCPermissionRequests.put(filename, client);
		} else if (ro.size() != 0) {

			managerPendingICs.put(filename, ro);
			for (Integer i : ro) {
				/*
				 * Send invalidate requests to everyone with RO status unless
				 * that person is the requesting client
				 */
				sendRequest(i, filename, Protocol.IV);
			}
			managerPendingCCPermissionRequests.put(filename, client);
		} else {
			// else no one has any access on this file, so send it to requester
			sendFile(client, filename, Protocol.WD);
			// unlock and RW will be given by WC
		}
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

		printVerbose("Changing client: " + client + " to RW");
		updateClientCacheStatus(CacheStatuses.ReadWrite, client, filename);
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
		updateClientCacheStatus(CacheStatuses.ReadOnly, client, filename);
	}

	protected void updateClientCacheStatus(CacheStatuses val, int client,
			String filename) throws NotManagerException {
		Map<Integer, CacheStatuses> clientStatuses = managerCacheStatuses
				.get(filename);
		if (clientStatuses == null) {
			clientStatuses = new HashMap<Integer, CacheStatuses>();
			managerCacheStatuses.put(filename, clientStatuses);
		}

		clientStatuses.put(client, val);

		// TODO: this is a weird place to do this
		printVerbose("Removing lock on file: " + filename);
		removeLock(filename);
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
					destAddr = managerPendingCCPermissionRequests.get(filename);
					sendFile(destAddr, filename, Protocol.WD);
				} else {
					destAddr = managerPendingRPCDeleteRequests.get(filename);
					sendSuccess(destAddr, Protocol.DELETE, filename);
				}
			} else {
				// still waiting for more ICs
				List<Integer> waitingFor = managerPendingICs.get(filename);
				StringBuilder waiting = new StringBuilder();
				waiting
						.append("Received IC but waiting for IC from clients : ");
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

		SendToManager(Protocol.IC, Utility.stringToByteArray(msgString));
	}

	/**
	 * Client receives {W,R}F as a request to propagate their changes
	 * 
	 * @throws UnknownManagerException
	 * @throws IOException
	 */
	protected void receiveF(String msgString, String RForWF,
			int responseProtocol, boolean keepRO)
			throws UnknownManagerException, IOException {
		StringTokenizer tokens = new StringTokenizer(msgString);
		String filename = tokens.nextToken();

		String payload = null;

		if (!Utility.fileExists(this, msgString)) {
			// TODO: Client could have crashed here, recover

			// Privilege level disagreement w/ manager
			printError(ErrorCode.PrivilegeDisagreement, RForWF + delimiter
					+ msgString);
			return;
		} else {
			// read file contents
			payload = filename + delimiter + getFile(filename);
		}

		// send contents back
		SendToManager(responseProtocol, Utility.stringToByteArray(payload));
		printVerbose("sending " + Protocol.protocolToString(responseProtocol)
				+ " to manager " + filename);

		// update permissions
		if (keepRO) {
			clientCacheStatus.put(filename, CacheStatuses.ReadOnly);
			printVerbose("changed permission level to ReadOnly on file: "
					+ filename);
		} else {
			clientCacheStatus.remove(filename);
			printVerbose("changed permission level to Invalid on file: "
					+ filename);
		}
	}

	/**
	 * Client receives RF as a request from the server to propagate their
	 * changes.
	 * 
	 * @throws NotClientException
	 * @throws IOException
	 * @throws UnknownManagerException
	 */
	protected void receiveRF(String msgString) throws NotClientException,
			UnknownManagerException, IOException {
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
	 */
	protected void receiveWF(String msgString) throws NotClientException,
			UnknownManagerException, IOException {
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
	public void SendToManager(int protocol, byte[] payload)
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

	// TODO: Zach: Code review receive{W,R}D

	/**
	 * @param msgString
	 *            <filename> <contents> for ex) test hello world
	 * @throws IOException
	 * @throws UnknownManagerException
	 */
	protected void receiveWD(int from, String msgString) throws IOException,
			UnknownManagerException {

		// parse packet
		StringTokenizer tokens = new StringTokenizer(msgString);
		String filename = tokens.nextToken();
		String contents = "";
		if (tokens.hasMoreTokens()) {
			contents = msgString.substring(filename.length() + 1);
		}

		if (!isManager) {
			/*
			 * TODO: HIGH: Can't currently distinguish between append on file
			 * that doesn't exist and file that is empty - this should be fixed
			 * now, TEST IT
			 */

			// has RW!
			clientCacheStatus.put(filename, CacheStatuses.ReadWrite);
			printVerbose("got ReadWrite on " + filename);

			// update in cache
			if (!Utility.fileExists(this, filename)) {
				createFile(filename);
			}
			writeFile(filename, contents, false);

			// do what you originally intended with the file
			if (clientPendingOperations.containsKey(filename)) {
				PendingClientOperation intent = clientPendingOperations
						.get(filename);
				switch (intent.operation) {
				case CREATE:
					// TODO: this doesn't throw an error if the file already
					// exists right now - it should
					break;
				case DELETE:
					deleteFile(filename);
					break;
				case PUT:
					writeFile(filename, intent.content, false);
					break;
				case APPEND:
					writeFile(filename, intent.content, true);
					break;
				}
			} else {
				printError(ErrorCode.MissingIntent, "wd");
			}

			// send wc
			printVerbose("sending wc to manager for " + filename);
			SendToManager(Protocol.WC, Utility.stringToByteArray(filename));

		} else { // Manager receives WD

			// check if the payload is blank. If so, this is an indication that
			// the file was deleted
			if (contents == "" || contents == null) {
				deleteFile(filename);

				// send out a WD to anyone requesting this
				int destAddr = managerPendingCCPermissionRequests.get(filename);
				sendError(destAddr, filename, ErrorCode.FileDoesNotExist);

			} else {
				// first write the file to save a local copy
				writeFile(filename, contents, false);

				// send out a WD to anyone requesting this
				Integer destAddr = managerPendingCCPermissionRequests
						.get(filename);
				if (destAddr != null) {
					// TODO: when would this ever be null?
					sendFile(destAddr, filename, Protocol.WD);
				}

				// update the status of the client who sent the WD
				Map<Integer, CacheStatuses> m = managerCacheStatuses
						.get(filename);
				m.remove(from);
				managerCacheStatuses.put(filename, m);
			}
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
				createFile(filename);
			}
			writeFile(filename, contents, false);

			// print GET result
			printInfo(contents);

			// send rc
			printVerbose("sending rc to manager for " + filename);
			SendToManager(Protocol.RC, Utility.stringToByteArray(filename));
		} else {
			// check if the payload is blank. If so, this is an indication that
			// the file was deleted
			if (contents == "" || contents == null) {
				/*
				 * TODO: This shouldn't be needed anymore - this packet should
				 * also probably have a filename or something in it in all cases
				 */

				deleteFile(filename);

				// Send out an invalid file request
				int destAddr = managerPendingCCPermissionRequests.get(filename);
				sendError(destAddr, filename, ErrorCode.FileDoesNotExist);

			} else {
				// first write the file to save a local copy
				writeFile(filename, contents, false);

				// send out a RD to anyone requesting this
				int destAddr = managerPendingCCPermissionRequests.get(filename);
				sendFile(destAddr, filename, Protocol.RD);

			}
			// update the status of the client who sent the WD
			Map<Integer, CacheStatuses> m = managerCacheStatuses.get(filename);
			m.put(from, CacheStatuses.ReadOnly);
			managerCacheStatuses.put(filename, m);
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

		// TODO: Unlock file if RPC failed
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
				createFile(filename);
				clientUnlockFile(filename);
				clientCacheStatus.put(filename, CacheStatuses.ReadWrite);
			} else if (cmd.equals(Protocol.protocolToString(Protocol.DELETE))) {
				deleteFile(filename);
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
	 * Unlock the indicated file and service and queued requests on it.
	 */
	protected void clientUnlockFile(String filename) {
		clientLockedFiles.remove(filename);
		if (clientQueuedFileRequests.containsKey(filename)) {
			Queue<QueuedFileRequest> queuedRequests = clientQueuedFileRequests
					.get(filename);
			if (queuedRequests.size() > 0) {
				QueuedFileRequest request = queuedRequests.poll();
				onCommand(request.command);
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
