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
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.NoSuchElementException;
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
 */
public class Client extends RIONode {

	/*
	 * TODO: LOW: Separate the Client and Manager code into two node types -
	 * this is impossible w/ framework since we can't start two node types
	 */

	/**
	 * Possible cache statuses
	 * 
	 * TODO: Is Invalid needed?
	 */
	public static enum CacheStatuses {
		Invalid, ReadWrite, ReadOnly
	};

	public static enum intentType {
		CREATE, DELETE, PUT, APPEND
	};

	protected class Intent {
		/**
		 * What we intend to do later
		 */
		protected intentType type;

		/**
		 * The content to put or append
		 */
		protected String content;

		/**
		 * Create an intent for an op that has no content
		 * 
		 * @param type
		 */
		public Intent(intentType type) {
			this.type = type;
			this.content = null;
		}

		/**
		 * Create an intent for an op that has content
		 * 
		 * @param type
		 * @param content
		 */
		public Intent(intentType type, String content) {
			this.type = type;
			this.content = content;
		}
	}

	/**
	 * Delimiter used in protocol payloads. Should be a single character.
	 */
	private static final String delimiter = " ";

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
	protected Map<String, Intent> clientPendingOperations;

	/**
	 * List of files locked on the client's side
	 * 
	 * TODO: might be redundant with clientPendingOperations
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
		super();
		// TODO: Maybe this should be in start, but I don't think it matters
		this.clientCacheStatus = new HashMap<String, CacheStatuses>();
		this.clientPendingOperations = new HashMap<String, Intent>();
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
		// Replace .temp to its old file, if a crash occurred
		if (Utility.fileExists(this, ".temp")) {
			try {
				restoreFromTempFile();
			} catch (FileNotFoundException e) {
				Logger.error(e);
			} catch (IOException e) {
				Logger.error(e);
			}
		}
	}

	/**
	 * Attempts to delete the temporary file used in case of a crash. Replaces
	 * the old file with the temporary file
	 * 
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	protected void restoreFromTempFile() throws FileNotFoundException,
			IOException {
		PersistentStorageReader reader = getReader(".temp");

		if (!reader.ready())
			deleteFile(this.addr, ".temp");
		else {
			String oldString = "";
			String inLine = "";
			String fileName = reader.readLine();
			while ((inLine = reader.readLine()) != null)
				oldString = oldString + inLine
						+ System.getProperty("line.separator");
			PersistentStorageWriter writer = getWriter(fileName, false);
			writer.write(oldString);
			writer.flush();
			writer.close();
			deleteFile(this.addr, ".temp");
		}
	}

	/*************************************************
	 * begin onCommand Handler methods
	 ************************************************/

	/*
	 * TODO: Log sends. We kind of entirely re-wrote logging at this point...
	 * but I didn't like theirs enough to not do it... /*
	 */

	/**
	 * Prints expected numbers for in and out channels. Likely to change as new
	 * problems arise.
	 * 
	 * @param tokens
	 * 
	 * @param command
	 */
	public void debugHandler(StringTokenizer tokens, String line) {
		RIOLayer.printSeqStateDebug();
	}

	/**
	 * Used for project2 to tell a node it is the manager.
	 * 
	 * @param tokens
	 * @param command
	 */
	public void managerHandler(StringTokenizer tokens, String line) {
		if (!isManager) {
			this.isManager = true;
			this.managerLockedFiles = new HashSet<String>();
			this.managerCacheStatuses = new HashMap<String, Map<Integer, CacheStatuses>>();
			this.managerPendingICs = new HashMap<String, List<Integer>>();
			this.managerQueuedFileRequests = new HashMap<String, Queue<QueuedFileRequest>>();
			this.managerPendingCCPermissionRequests = new HashMap<String, Integer>();
			this.managerPendingRPCDeleteRequests = new HashMap<String, Integer>();
		}

		printInfo("promoted to manager");
	}

	/**
	 * Used for project2 to tell a node the address of the manager.
	 * 
	 * @param tokens
	 * @param line
	 */
	public void managerisHandler(StringTokenizer tokens, String line) {
		try {
			this.managerAddr = Integer.parseInt(tokens.nextToken());
			printInfo("manager is " + this.managerAddr);
		} catch (NumberFormatException e) {
			printError(ErrorCode.InvalidCommand, "manageris");
		} catch (NoSuchElementException e) {
			printError(ErrorCode.IncompleteCommand, "manageris");
		}
	}

	/**
	 * Check if the client has locked the filename. Queue the passed in action
	 * if the file is locked and return true. Otherwise return false.
	 */
	protected boolean clientQueueLineIfLocked(String filename, String line) {
		if (clientLockedFiles.contains(filename)) {
			Queue<QueuedFileRequest> e = clientQueuedFileRequests.get(filename);
			if (e == null) {
				e = new LinkedList<QueuedFileRequest>();
			}
			e.add(new QueuedFileRequest(line));
			clientQueuedFileRequests.put(filename, e);
			return true;
		} else {
			return false;
		}
	}

	// TODO: log local file locks and unlocks

	/**
	 * Get ownership of a file and create it
	 */
	public void createHandler(StringTokenizer tokens, String line) {
		String filename = parseFilename(tokens, "create");

		if (clientQueueLineIfLocked(filename, line)) {
			return;
		} else if (clientCacheStatus.containsKey(filename)
				&& (clientCacheStatus.get(filename) != CacheStatuses.Invalid)) {
			// have permissions
			createFile(filename);
		} else {
			// lock and perform rpc
			createRPC(this.managerAddr, filename);
			clientLockedFiles.add(filename);
		}
	}

	/**
	 * Get ownership of a file and delete it
	 */
	public void deleteHandler(StringTokenizer tokens, String line) {
		String filename = parseFilename(tokens, "delete");

		if (clientQueueLineIfLocked(filename, line)) {
			return;
		} else if (clientCacheStatus.containsKey(filename)
				&& clientCacheStatus.get(filename) == CacheStatuses.ReadWrite) {
			// have permissions
			deleteFile(filename);
		} else {
			// lock and perform rpc
			deleteRPC(this.managerAddr, filename);
			clientLockedFiles.add(filename);
		}
	}

	/**
	 * Get read access for a file and then get its contents
	 */
	public void getHandler(StringTokenizer tokens, String line) {
		String filename = parseFilename(tokens, "get");

		if (clientQueueLineIfLocked(filename, line)) {
			return;
		} else if (clientCacheStatus.containsKey(filename)
				&& (clientCacheStatus.get(filename) != CacheStatuses.Invalid)) {
			// have permissions
			try {
				getFile(filename);
			} catch (IOException e) {
				Logger.error(e);
			}
		} else {
			// lock and get permissions
			try {
				SendToManager(Protocol.RQ, Utility.stringToByteArray(filename));
				clientLockedFiles.add(filename);
				printInfo("requesting read access for " + filename);
			} catch (UnknownManagerException e) {
				printError(ErrorCode.UnknownManager, "get", filename);
			}
		}
	}

	/**
	 * Get ownership of a file and put to it
	 * 
	 * @param tokens
	 * @param line
	 */
	public void putHandler(StringTokenizer tokens, String line) {
		String filename = parseFilename(tokens, "put");
		String content = parseAddContent(line, "put", filename);

		if (clientQueueLineIfLocked(filename, line)) {
			return;
		} else if (clientCacheStatus.containsKey(filename)
				&& clientCacheStatus.get(filename) == CacheStatuses.ReadWrite) {
			// have ownership
			writeFile(filename, content, Protocol.PUT);
		} else {
			// lock and request ownership
			try {
				SendToManager(Protocol.WQ, Utility.stringToByteArray(filename));
				clientPendingOperations.put(filename, new Intent(
						intentType.PUT, content));
				clientLockedFiles.add(filename);
				printInfo("requesting ownership of " + filename);
			} catch (UnknownManagerException e) {
				printError(ErrorCode.UnknownManager, "put", filename);
				return;
			}
		}
	}

	/**
	 * Get ownership of a file and append to it
	 * 
	 * @param tokens
	 * @param line
	 */
	public void appendHandler(StringTokenizer tokens, String line) {
		// TODO: I think I found a framework bug - "append 1 test  world" is
		// losing the extra space

		String filename = parseFilename(tokens, "append");
		String content = parseAddContent(line, "append", filename);

		if (clientQueueLineIfLocked(filename, line)) {
			return;
		} else if (clientCacheStatus.containsKey(filename)
				&& clientCacheStatus.get(filename) == CacheStatuses.ReadWrite) {
			// have ownership
			writeFile(filename, content, Protocol.APPEND);
		} else {
			// lock and request ownership
			try {
				SendToManager(Protocol.WQ, Utility.stringToByteArray(filename));
				clientPendingOperations.put(filename, new Intent(
						intentType.APPEND, content));
				clientLockedFiles.add(filename);
				printInfo("requesting ownership of " + filename);
			} catch (UnknownManagerException e) {
				printError(ErrorCode.UnknownManager, "append", filename);
				return;
			}
		}
	}

	/**
	 * Initiates a remote handshake
	 * 
	 * @param tokens
	 * @param line
	 */
	public void handshakeHandler(StringTokenizer tokens, String line) {
		int server = parseServer(tokens, "handshake");
		String payload = getID().toString();
		RIOSend(server, Protocol.HANDSHAKE, Utility.stringToByteArray(payload));
		printInfo("sending handshake to " + server);
	}

	/**
	 * Sends a noop
	 */
	public void noopHandler(StringTokenizer tokens, String line) {
		int server = parseServer(tokens, "noop");
		RIOSend(addr, Protocol.NOOP, Utility.stringToByteArray(""));
		printInfo("sending noop rpc to " + server);
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
		RIOSend(address, Protocol.CREATE, Utility.stringToByteArray(filename));
		printInfo("sending create rpc to " + address);
	}

	/**
	 * Perform a delete RPC to the given address
	 */
	public void deleteRPC(int address, String filename) {
		RIOSend(address, Protocol.DELETE, Utility.stringToByteArray(filename));
		printInfo("sending delete rpc to " + address);
	}

	/*************************************************
	 * end RPC methods
	 ************************************************/

	/*************************************************
	 * begin onCommand parse helpers
	 ************************************************/

	/**
	 * Parses a server address from tokens
	 */
	protected int parseServer(StringTokenizer tokens, String cmd) {
		int server;
		try {
			server = Integer.parseInt(tokens.nextToken());
		} catch (NumberFormatException e) {
			printError(ErrorCode.InvalidServerAddress, cmd);
			throw e;
		} catch (NoSuchElementException e) {
			printError(ErrorCode.IncompleteCommand, cmd);
			throw e;
		}
		return server;
	}

	/**
	 * Parses a filename from tokens
	 * 
	 * @param tokens
	 * @param cmd
	 * @return
	 */
	protected String parseFilename(StringTokenizer tokens, String cmd) {
		String filename;
		try {
			filename = tokens.nextToken();
		} catch (NoSuchElementException e) {
			printError(ErrorCode.IncompleteCommand, cmd);
			throw e;
		}
		return filename;
	}

	/**
	 * Parse what content to add to a file for put and append (the rest of the
	 * line)
	 * 
	 * @param tokens
	 * @param cmd
	 * @return
	 */
	protected String parseAddContent(String line, String cmd, String filename) {

		int parsedLength = cmd.length() + filename.length() + 2;
		if (parsedLength >= line.length()) {
			// no contents
			printError(ErrorCode.IncompleteCommand, cmd, -1, filename);
			// TODO: throw an exception
		}

		return line.substring(parsedLength);
	}

	/*************************************************
	 * end onCommand parse helpers
	 ************************************************/

	/**
	 * Process a command from user or file. Lowercases the command for further
	 * internal use.
	 * 
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
			printError(ErrorCode.DynamicCommandError, cmd);
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
	private StringBuilder appendError(StringBuilder sb, String command) {
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
	 * Local create file
	 */
	public void createFile(String fileName) {
		createFile(this.addr, fileName);
	}

	/**
	 * Creates a file on the local filesystem
	 * 
	 * @param fileName
	 *            the file to create
	 */
	public void createFile(int from, String fileName) {

		printVerbose("attempting to CREATE file: " + fileName);
		logSynopticEvent("CREATING-FILE");

		// check if the file exists
		if (Utility.fileExists(this, fileName)) {
			printError(ErrorCode.FileAlreadyExists, "create", addr, fileName);
			return;
			// TODO: throw an exception instead
		}

		// create the file
		else {
			try {
				PersistentStorageWriter writer = getWriter(fileName, false);
				writer.close();
			} catch (IOException e) {
				Logger.error(e);
				// TODO: don't catch
			}
		}

	}

	/**
	 * Local delete file.
	 * 
	 * @param fileName
	 */
	public void deleteFile(String fileName) {
		try {
			deleteFile(this.addr, fileName);
		} catch (FileNotFoundException e) {
			printError(ErrorCode.FileDoesNotExist, "delete", this.addr,
					fileName);
		}
	}

	/**
	 * Deletes a file from the local file system. Fails and prints an error if
	 * the file does not exist
	 * 
	 * @param fileName
	 *            the file name to delete
	 */
	public void deleteFile(int from, String fileName)
			throws FileNotFoundException {

		printVerbose("attempting to DELETE file: " + fileName);
		logSynopticEvent("DELETING-FILE");

		// check if the file even exists
		if (!Utility.fileExists(this, fileName)) {
			printError(ErrorCode.FileDoesNotExist, "delete", addr, fileName);
			throw new FileNotFoundException();
		} else {
			// delete file
			try {
				PersistentStorageWriter writer = getWriter(fileName, false);
				if (!writer.delete())
					printError(ErrorCode.UnknownError, "Delete failed!");
				writer.close();
				if (Utility.fileExists(this, fileName))
					printError(ErrorCode.UnknownError, "Delete failed!");
			} catch (IOException e) {
				printError(ErrorCode.UnknownError, e.getMessage());
				// TODO: throw e
			}
		}

	}

	/**
	 * Local get file
	 * 
	 * @param fileName
	 * @return
	 * @throws IOException
	 */
	public String getFile(String fileName) throws IOException {

		printVerbose("attempting to READ file: " + fileName);
		logSynopticEvent("GETTING-FILE");

		// check if the file exists
		if (!Utility.fileExists(this, fileName)) {
			printError(ErrorCode.FileDoesNotExist, "get", addr, fileName);
			throw new FileNotFoundException();
		} else {
			// read and return the file if it does
			StringBuilder contents = new StringBuilder();
			String inLine = "";
			PersistentStorageReader reader = getReader(fileName);
			contents.append(reader.readLine());
			while ((inLine = reader.readLine()) != null) {
				contents.append(System.getProperty("line.separator"));
				contents.append(inLine);
			}

			reader.close();
			printVerbose("reading contents of file: " + fileName);
			return contents.toString();

		}
	}

	/**
	 * Sends a file to the client with the given filename
	 * 
	 * @param fileName
	 *            the filename to send
	 */
	@Deprecated
	public void getFile(String fileName, int from) {

		Logger.error("Node " + this.addr + ": called deprecated getFile method");

		printVerbose("attempting to READ/GET file: " + fileName + " for Node: "
				+ from);
		logSynopticEvent("GETTING-FILE");

		// check if the file exists
		if (!Utility.fileExists(this, fileName)) {
			printError(ErrorCode.FileDoesNotExist, "get", addr, fileName);
			sendError(from, Protocol.GET, fileName, ErrorCode.FileDoesNotExist);
			return;
		}
		// send the file if it does
		else {
			// load the file into a reader
			StringBuilder contents = new StringBuilder();
			contents.append(fileName + delimiter);
			String inLine = "";
			try {
				PersistentStorageReader reader = getReader(fileName);
				contents.append(reader.readLine());
				while ((inLine = reader.readLine()) != null) {
					contents.append(System.getProperty("line.separator"));
					contents.append(inLine);
				}
				reader.close();
			} catch (FileNotFoundException e) {
				Logger.error(e);
			} catch (IOException e) {
				Logger.error(e);
			}

			// send the payload
			byte[] payload = Utility.stringToByteArray(contents.toString());
			RIOLayer.RIOSend(from, Protocol.DATA, payload);
			printVerbose("sending contents of file: " + fileName + " to Node: "
					+ from);

		}
	}

	/**
	 * Wrapper for writeFile, doesn't send a response
	 * 
	 * @param fileName
	 * @param contents
	 */
	public void writeFile(String fileName, String contents, int protocol) {
		writeFile(this.addr, fileName, contents, protocol);
	}

	/**
	 * Writes a file to the local filesystem. Fails if the file does not exist
	 * already
	 * 
	 * @param filename
	 *            the file name to write to
	 * @param contents
	 *            the contents to write
	 */
	public void writeFile(int from, String fileName, String contents,
			int protocol) {

		printVerbose("attempting to PUT/APPEND File: " + fileName
				+ " with Contents: " + contents);
		logSynopticEvent("WRITING-FILE");

		// check if the file exists
		if (!Utility.fileExists(this, fileName)) {
			if (protocol == Protocol.PUT)
				printError(ErrorCode.FileDoesNotExist, "put", addr, fileName);
			else
				printError(ErrorCode.FileDoesNotExist, "append", addr, fileName);
			return;
			// TODO: throw an exception
		} else {
			try {
				PersistentStorageWriter writer = null;
				// create a new file writer w/ appropriate append setting
				if (protocol == Protocol.APPEND) {
					writer = getWriter(fileName, true);
				} else {
					writeTempFile(fileName);

					writer = getWriter(fileName, false);
				}
				writer.write(contents);
				writer.flush();
				writer.close();
				// Delete the temporary file if it exists
				if (protocol == Protocol.PUT)
					deleteFile(this.addr, ".temp");
			} catch (IOException e) {
				sendError(from, protocol, fileName, ErrorCode.UnknownError);
				Logger.error(e);
				// TODO: throw e
			}
		}
	}

	/**
	 * Used to temporarily save a file that could be lost in a crash since
	 * getWriter deletes a file it doesn't open for appending.
	 * 
	 * @param fileName
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	private void writeTempFile(String fileName) throws FileNotFoundException,
			IOException {
		// Temporary storage in case of a crash
		String oldString = "";
		String inLine;
		PersistentStorageReader oldFileReader = getReader(fileName);
		while ((inLine = oldFileReader.readLine()) != null)
			oldString = oldString + inLine
					+ System.getProperty("line.separator");
		PersistentStorageWriter temp = getWriter(".temp", false);
		temp.write(fileName + "\n" + oldString);
	}

	/****************************************************
	 * end FS methods
	 ***************************************************/

	// TODO: All FS methods should throw exceptions on failures
	/*
	 * TODO: Non-local methods are no longer used, responses shouldn't be sent
	 * from here anymore
	 */

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

		switch (protocol) {
		case Protocol.CREATE:
			receiveCreate(from, msgString);
			break;
		case Protocol.DELETE:
			receiveDelete(from, msgString);
			break;
		case Protocol.GET:
			// TODO: Deprecated by CC
			getFile(msgString, from);
			break;
		case Protocol.PUT:
		case Protocol.APPEND:
		case Protocol.DATA:
			// TODO: Deprecated by CC
			decideParseOrAppend(from, protocol, msgString);
			break;
		case Protocol.NOOP:
			printInfo("noop");
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
			printError(ErrorCode.InvalidCommand, "receive");
		}
	}

	/**
	 * Prints the file received from the get command. Also used to print
	 * success/failure responses returned from the server.
	 * 
	 * TODO: Get is handled by IVY CC now, not RPC - this shouldn't be called
	 * anymore
	 */
	@Deprecated
	public void receiveData(String cmdOrFileName, String contents) {
		Logger.error("Node " + this.addr
				+ " called deprecated method receiveData");

		String output = cmdOrFileName + " received with contents: " + contents;
		printInfo(output);
	}

	/*************************************************
	 * begin manager-only cache coherency functions
	 ************************************************/

	/**
	 * Create RPC
	 */
	protected void receiveCreate(int client, String filename) {
		if (!isManager) {
			printError(ErrorCode.NotManager, "create");
			return;
		}

		// TODO: HIGH: Check if locked?

		if (managerCacheStatuses.containsKey(filename)
				&& managerCacheStatuses.get(filename).size() > 0) {
			// send error, file already exists
			sendError(client, Protocol.ERROR, filename,
					ErrorCode.FileAlreadyExists);
		} else {
			// local create
			createFile(filename);

			// give RW to the requester for filename
			Map<Integer, CacheStatuses> cache = managerCacheStatuses
					.get(filename);
			if (cache == null) {
				cache = new HashMap<Integer, CacheStatuses>();
			}
			cache.put(client, CacheStatuses.ReadWrite);
			managerCacheStatuses.put(filename, cache);

			// send success to requester
			sendSuccess(client, Protocol.CREATE, filename);
		}
	}

	/**
	 * Delete RPC
	 */
	public void receiveDelete(int from, String filename) {
		if (!isManager) {
			printError(ErrorCode.NotManager,
					ErrorCode.lookup(ErrorCode.NotManager));
			return;
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
				if (rw != null)
					Logger.error(ErrorCode.MultipleOwners,
							"Multiple owners on file: " + filename);
				rw = entry.getKey();
			}
			if (entry.getValue().equals(CacheStatuses.ReadOnly)) {
				if (rw != null)
					Logger.error(ErrorCode.ReadWriteAndReadOnly,
							"ReadOnly copies exist while other client has ownership on file: "
									+ filename);
				if (entry.getKey() != from)
					ro.add(entry.getKey());
			}
		}

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
		}
	}

	// TODO: Zach: Code review of Manager only functions from here down

	protected void receiveRQ(int client, String filename) {
		// Deal with locked files, and lock the file if it's not currently
		if (managerLockedFiles.contains(filename)) {
			Queue<QueuedFileRequest> e = managerQueuedFileRequests
					.get(filename);
			if (e == null)
				e = new LinkedList<QueuedFileRequest>();
			e.add(new QueuedFileRequest(client, Protocol.RQ, Utility
					.stringToByteArray(filename)));
			managerQueuedFileRequests.put(filename, e);
			return;
		}
		printVerbose("Locking file: " + filename);
		managerLockedFiles.add(filename);

		// Check if anyone has RW status on this file
		Map<Integer, CacheStatuses> clientStatuses = managerCacheStatuses
				.get(filename);

		Integer key = null;
		if (clientStatuses == null) {
			clientStatuses = new HashMap<Integer, CacheStatuses>();
			managerCacheStatuses.put(filename, clientStatuses);

		}
		for (Entry<Integer, CacheStatuses> entry : clientStatuses.entrySet()) {
			if (entry.getValue().equals(CacheStatuses.ReadWrite)) {
				if (key != null)
					Logger.error(ErrorCode.MultipleOwners,
							"Multiple owners on file: " + filename);
				key = entry.getKey();
			}
		}

		// If no one owns a copy of this file, send them a copy and remove the
		// lock
		if (key == null) {
			sendFile(client, filename, Protocol.RD);
		} else {
			sendRequest(key, filename, Protocol.RF);
			managerPendingCCPermissionRequests.put(filename, client);
		}

	}

	private void sendFile(int client, String fileName, int protocol) {
		String sendMsg = "";

		if (!Utility.fileExists(this, fileName)) {
			sendError(client, Protocol.ERROR, fileName,
					ErrorCode.FileDoesNotExist);
		} else {
			try {
				sendMsg = fileName + delimiter + getFile(fileName);
			} catch (IOException e) {
				Logger.error(e);
			}
		}

		byte[] payload = Utility.stringToByteArray(sendMsg);
		RIOSend(client, protocol, payload);
		printVerbose("sending " + Protocol.protocolToString(protocol) + " to "
				+ client);
	}

	private void sendRequest(int client, String fileName, int protocol) {
		String sendMsg = fileName;
		byte[] payload = Utility.stringToByteArray(sendMsg);
		RIOSend(client, protocol, payload);
		printVerbose("sending " + protocol + " to " + client);
	}

	private void removeLock(String filename) {
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

	protected void receiveWQ(int client, String filename) {
		// Deal with locked files, and lock the file if it's not currently
		if (managerLockedFiles.contains(filename)) {
			Queue<QueuedFileRequest> e = managerQueuedFileRequests
					.get(filename);
			if (e == null)
				e = new LinkedList<QueuedFileRequest>();
			e.add(new QueuedFileRequest(client, Protocol.WQ, Utility
					.stringToByteArray(filename)));
			managerQueuedFileRequests.put(filename, e);
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
			sendRequest(rw, filename, Protocol.WF);
			managerPendingCCPermissionRequests.put(filename, client);
			return;
			// If so, send the data back to the client waiting
		}
		if (ro.size() != 0) {

			managerPendingICs.put(filename, ro);
			for (Integer i : ro) { // Send invalidate requests to everyone with
									// RO
									// status unless that person is the
									// requesting client
				sendRequest(i, filename, Protocol.IV);
			}
			managerPendingCCPermissionRequests.put(filename, client);
			return;
		}
		// else no one has any kind of access on this file, so send it to them
		sendFile(client, filename, Protocol.WD);

	}

	/**
	 * Changes the status of this client from IV or RW
	 * 
	 * @param client
	 *            The client to change
	 * @param fileName
	 *            The filename
	 */
	protected void receiveWC(int client, String fileName) {
		printVerbose("Changing client: " + client + " to RW");
		updateClientCacheStatus(CacheStatuses.ReadWrite, client, fileName);
	}

	/**
	 * Receives an RC and changes this client's status from IV or RW to RO.
	 * 
	 * @param client
	 *            The client to change
	 * @param fileName
	 *            The filename
	 */
	protected void receiveRC(int client, String fileName) {
		printVerbose("Changing client: " + client + " to RO");
		updateClientCacheStatus(CacheStatuses.ReadOnly, client, fileName);
	}

	private void updateClientCacheStatus(CacheStatuses val, int client,
			String fileName) {
		if (!isManager) {
			Logger.error(ErrorCode.NotManager,
					"Receieved confirm but not manager");
			return;
		}

		if (!managerCacheStatuses.containsKey(fileName))
			managerCacheStatuses.put(fileName,
					new HashMap<Integer, CacheStatuses>());

		// Update the client status and put it back in the cache status map
		HashMap<Integer, CacheStatuses> clientMap = (HashMap<Integer, CacheStatuses>) managerCacheStatuses
				.get(fileName);
		clientMap.put(client, val);
		managerCacheStatuses.put(fileName, clientMap);

		printVerbose("Removing lock on file: " + fileName);
		removeLock(fileName);
	}

	/**
	 * 
	 * @param from
	 *            The node this IC was received from.
	 * @param filename
	 *            Should be the file name. Throws an error if we were not
	 *            waiting for an IC from this node for this file
	 */
	protected void receiveIC(Integer from, String filename) {
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
			m.put(from, CacheStatuses.Invalid);
			printVerbose("Changing client: " + from + " to IV");
			managerCacheStatuses.put(filename, m);

			managerPendingICs.get(filename).remove(from);
			if (managerPendingICs.get(filename).isEmpty()) { // If the pending
																// ICs are
				// now empty, someone's
				// waiting for a WD, so
				// check for that and
				// send
				if (managerPendingCCPermissionRequests.containsKey(filename)) {
					destAddr = managerPendingCCPermissionRequests.get(filename);
					sendFile(destAddr, filename, Protocol.WD);
				} else {
					destAddr = managerPendingRPCDeleteRequests.get(filename);
					sendSuccess(destAddr, Protocol.DELETE, filename);
				}
			} else {
				printVerbose("Received IC but waiting for IC from at client (only first shown): "
						+ managerPendingICs.get(filename).get(0));
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
	 */
	protected void receiveIV(String msgString) {
		// If we're the manager and we received and IV, something bad happened
		if (isManager) {
			printError(ErrorCode.InvalidCommand, "iv " + msgString);
		} else {
			// TODO: put INVALID or delete entirely???
			clientCacheStatus.put(msgString, CacheStatuses.Invalid);
			printVerbose("marking invalid " + msgString);
			try {
				SendToManager(Protocol.IC, Utility.stringToByteArray(msgString));
			} catch (UnknownManagerException e) {
				Logger.error(e);
			}
		}
	}

	/**
	 * Client receives {W,R}F as a request to propagate their changes
	 */
	protected void receiveF(String msgString, String RForWF,
			int responseProtocol, CacheStatuses newCacheStatus) {
		if (isManager) {
			// TODO: Standardize this error
			printError(ErrorCode.InvalidCommand, RForWF + delimiter + msgString);
		}

		StringTokenizer tokens = new StringTokenizer(msgString);
		String filename = tokens.nextToken();

		try {
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
			printVerbose("sending "
					+ Protocol.protocolToString(responseProtocol)
					+ " to manager " + filename);

			// update permissions
			clientCacheStatus.put(filename, newCacheStatus);
			printVerbose("changed permission level to "
					+ newCacheStatus.toString() + " for file: " + filename);

		} catch (UnknownManagerException e) {
			printError(ErrorCode.UnknownManager, RForWF);
		} catch (IOException e) {
			printError(ErrorCode.UnknownError, RForWF);
			// TODO: better error code
		}
	}

	/**
	 * Client receives RF as a request from the server to propagate their
	 * changes.
	 */
	protected void receiveRF(String msgString) {
		receiveF(msgString, "RF", Protocol.RD, CacheStatuses.ReadOnly);
	}

	/**
	 * Client receives WF as a request from the server to propagate their
	 * changes.
	 */
	protected void receiveWF(String msgString) {
		receiveF(msgString, "WF", Protocol.WD, CacheStatuses.Invalid);
	}

	/**
	 * Convenience wrapper of RIOSend that sends a message to the manager if
	 * their address is known and throws an UnknownManagerException if not
	 * 
	 * @param protocol
	 * @param payload
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
	 */
	protected void receiveWD(int from, String msgString) {

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
			 * that doesn't exist and file that is empty
			 */

			// has RW!
			clientCacheStatus.put(filename, CacheStatuses.ReadWrite);
			printVerbose("got ReadWrite on " + filename);

			// update in cache
			if (!Utility.fileExists(this, filename)) {
				createFile(filename);
			}
			writeFile(filename, contents, Protocol.PUT);

			// do what you originally intended with the file
			if (clientPendingOperations.containsKey(filename)) {
				Intent intent = clientPendingOperations.get(filename);
				switch (intent.type) {
				case CREATE:
					// TODO: this doesn't throw an error if the file already
					// exists right now - it should
					break;
				case DELETE:
					deleteFile(filename);
					break;
				case PUT:
					writeFile(filename, intent.content, Protocol.PUT);
					break;
				case APPEND:
					writeFile(filename, intent.content, Protocol.APPEND);
					break;
				}
			} else {
				printError(ErrorCode.MissingIntent, "wd");
			}

			// send wc
			try {
				SendToManager(Protocol.WC, Utility.stringToByteArray(filename));
				printVerbose("sending wc to manager for " + filename);
			} catch (UnknownManagerException e) {
				printError(ErrorCode.UnknownManager, "wd");
			}

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
				writeFile(filename, contents, Protocol.PUT);

				// send out a WD to anyone requesting this
				Integer destAddr = managerPendingCCPermissionRequests
						.get(filename);
				if (destAddr != null)
					sendFile(destAddr, filename, Protocol.WD);

				// update the status of the client who sent the WD
				Map<Integer, CacheStatuses> m = managerCacheStatuses
						.get(filename);
				m.put(from, CacheStatuses.Invalid);
				managerCacheStatuses.put(filename, m);
			}
		}
	}

	/**
	 * @param msgString
	 */
	protected void receiveRD(int from, String msgString) {
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
			writeFile(filename, contents, Protocol.PUT);

			// print GET result
			printInfo(contents);

			// send rc
			try {
				SendToManager(Protocol.RC, Utility.stringToByteArray(filename));
				printVerbose("sending rc to manager for " + filename);
			} catch (UnknownManagerException e) {
				printError(ErrorCode.UnknownManager, "rd");
			}
		} else {
			// check if the payload is blank. If so, this is an indication that
			// the file was deleted
			if (contents == "" || contents == null) {
				deleteFile(filename);

				// Send out an invalid file request
				int destAddr = managerPendingCCPermissionRequests.get(filename);
				sendError(destAddr, filename, ErrorCode.FileDoesNotExist);

			} else {
				// first write the file to save a local copy
				writeFile(filename, contents, Protocol.PUT);

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
	 */
	protected void receiveError(Integer from, String msgString) {
		if (isManager) {
			// TODO: make error code
			printError(ErrorCode.UnknownError, "error");
			return;
		}

		// TODO: LOW: Use printError
		Logger.error("Node " + this.addr + ": Error from " + from + ": "
				+ msgString);

		// TODO: Unlock file if RPC failed
		String filename = msgString.split("")[0];
		clientUnlockFile(filename);

		// TODO: Figure out if this gets called any other times
	}

	/**
	 * RPC Successful (only received after successful Create or Delete)
	 */
	protected void receiveSuccessful(int from, String msgString) {
		if (isManager) {
			// TODO: make error code
			printError(ErrorCode.UnknownError, "successful");
			return;
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

	/**
	 * Parses a received command to decide whether to put or append to file
	 */
	@Deprecated
	private void decideParseOrAppend(Integer from, int protocol,
			String msgString) {
		Logger.error("Node " + this.addr
				+ " called deprecated method decideParseOrAppend");

		// tokenize the string and parse out the contents and filename
		StringTokenizer tokenizer = new StringTokenizer(msgString);
		String fileName = tokenizer.nextToken();
		int length = fileName.length();

		String contents = msgString.substring(length + 1, msgString.length());

		if (protocol == Protocol.DATA) {
			receiveData(fileName, contents);
		} else {
			writeFile(from, fileName, contents, protocol);
		}
	}

	// TODO: Wayne: Comment send{Success, Error} methods

	private void sendSuccess(int destAddr, int protocol) {
		String msg = Protocol.protocolToString(protocol);
		sendSuccess(destAddr, msg);
	}

	private void sendSuccess(int destAddr, int protocol, String message) {
		String msg = Protocol.protocolToString(protocol) + delimiter + message;
		sendSuccess(destAddr, msg);
	}

	private void sendSuccess(int destAddr, String message) {
		byte[] payload = Utility.stringToByteArray(message);
		RIOLayer.RIOSend(destAddr, Protocol.SUCCESS, payload);
	}

	private void sendError(int destAddr, String filename, int errorcode) {
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
	private void sendError(int destAddr, int protocol, String filename,
			int errorcode) {
		String msg = filename + delimiter + Protocol.protocolToString(protocol)
				+ delimiter + ErrorCode.lookup(errorcode);
		byte[] payload = Utility.stringToByteArray(msg);
		RIOLayer.RIOSend(destAddr, Protocol.ERROR, payload);

	}
}
