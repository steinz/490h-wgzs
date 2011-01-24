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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
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

	/**
	 * Possible cache statuses
	 */
	public static enum CacheStatuses {
		Invalid, ReadWrite, ReadOnly
	};

	/**
	 * Delimeter used in protocol payloads. Should be a single character.
	 */
	private static final String delimiter = " ";

	/**
	 * Status of cached files on disk. Keys are filenames.
	 */
	private Map<String, CacheStatuses> cacheStatus;

	/**
	 * 
	 */
	private Map<String, Intent> pendingOperations;

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
	 * Whether or not this node is the manager for project 2.
	 */
	private boolean isManager;

	/**
	 * The address of the manager node.
	 */
	private int managerAddr;

	/*************************************************
	 * begin manager only data structures
	 ************************************************/

	/**
	 * List of files whose requests are currently being worked out.
	 */
	private Set<String> lockedFiles;

	/**
	 * Status of cached files for all clients.
	 */
	private Map<String, Map<Integer, CacheStatuses>> clientCacheStatus;

	/**
	 * List of nodes the manager is waiting for ICs from.
	 */
	private Map<String, List<Integer>> pendingICs;

	private Map<String, Queue<QueuedFileRequest>> queuedFileRequests;

	/**
	 * Status of who is waiting for permission for this file
	 */
	private Map<String, Integer> pendingPermissionRequests;

	/*************************************************
	 * end manager only data structures
	 ************************************************/

	public Client() {
		super();
		this.cacheStatus = new HashMap<String, CacheStatuses>();
		this.pendingOperations = new HashMap<String, Intent>();
		this.queuedFileRequests = new HashMap<String, Queue<QueuedFileRequest>>();
		this.pendingPermissionRequests = new HashMap<String, Integer>();
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
	 * begin onCommand Handler methods / parse helpers
	 ************************************************/

	// TODO: Log sends. We kind of entirely re-wrote logging at this point...
	// but I didn't like theirs enough to not do it...

	/**
	 * Prints expected numbers for in and out channels. Likely to change as new
	 * problems arise.
	 * 
	 * @param tokens
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
			this.lockedFiles = new HashSet<String>();
			this.clientCacheStatus = new HashMap<String, Map<Integer, CacheStatuses>>();
			this.pendingICs = new HashMap<String, List<Integer>>();
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
	 * Get ownership of a file and create it
	 * 
	 * @param tokens
	 * @param line
	 */
	public void createHandler(StringTokenizer tokens, String line) {
		String filename = parseFilename(tokens, "create");

		if (cacheStatus.containsKey(filename)
				&& cacheStatus.get(filename) != CacheStatuses.Invalid) {
			// The file is in my cache as RW or RO so it already exists, throw
			// an error
			printError(ErrorCode.FileAlreadyExists, "create", filename);
		} else {
			// Request ownership
			try {
				SendToManager(Protocol.WQ, Utility.stringToByteArray(filename));
				printInfo("requesting ownership of " + filename);
			} catch (UnknownManagerException e) {
				printError(ErrorCode.UnknownManager, "create", filename);
				return;
			}
			// Remember that I want to create this file
			pendingOperations.put(filename, new Intent(intentType.CREATE));
		}
	}

	/**
	 * Get ownership of a file and delete it
	 * 
	 * @param tokens
	 * @param line
	 */
	public void deleteHandler(StringTokenizer tokens, String line) {
		String filename = parseFilename(tokens, "delete");

		if (cacheStatus.containsKey(filename)
				&& cacheStatus.get(filename) == CacheStatuses.ReadWrite) {
			// I have ownership
			deleteFile(filename);
		} else {
			try {
				SendToManager(Protocol.WQ, Utility.stringToByteArray(filename));
				printInfo("requesting ownership of " + filename);
			} catch (UnknownManagerException e) {
				printError(ErrorCode.UnknownManager, "delete", filename);
				return;
			}
			pendingOperations.put(filename, new Intent(intentType.DELETE));
		}
	}

	/**
	 * Get read access for a file and then get its contents
	 * 
	 * @param tokens
	 * @param line
	 */
	public void getHandler(StringTokenizer tokens, String line) {
		String filename = parseFilename(tokens, "get");

		if (cacheStatus.containsKey(filename)
				&& cacheStatus.get(filename) != CacheStatuses.Invalid) {
			try {
				getFile(filename);
			} catch (IOException e) {
				Logger.error(e);
			}
		} else {
			try {
				SendToManager(Protocol.RQ, Utility.stringToByteArray(filename));
				printInfo("requesting read access for " + filename);
			} catch (UnknownManagerException e) {
				printError(ErrorCode.UnknownManager, "get", filename);
				return;
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

		if (cacheStatus.containsKey(filename)
				&& cacheStatus.get(filename) == CacheStatuses.ReadWrite) {
			// I have ownership
			writeFile(filename, content, Protocol.PUT);
		} else {
			// Request ownership
			try {
				SendToManager(Protocol.WQ, Utility.stringToByteArray(filename));
				printInfo("requesting ownership of " + filename);
			} catch (UnknownManagerException e) {
				printError(ErrorCode.UnknownManager, "put", filename);
				return;
			}
			pendingOperations
					.put(filename, new Intent(intentType.PUT, content));
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
		if (cacheStatus.containsKey(filename)
				&& cacheStatus.get(filename) == CacheStatuses.ReadWrite) {
			// Have ownership
			writeFile(filename, content, Protocol.APPEND);
		} else {
			// Request ownership
			try {
				SendToManager(Protocol.WQ, Utility.stringToByteArray(filename));
				printInfo("requesting ownership of " + filename);
			} catch (UnknownManagerException e) {
				printError(ErrorCode.UnknownManager, "append", filename);
				return;
			}
			pendingOperations.put(filename, new Intent(intentType.APPEND,
					content));
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
	 * Noop RPC
	 * 
	 * @param tokens
	 * @param line
	 */
	public void noopHandler(StringTokenizer tokens, String line) {
		int server = parseServer(tokens, "noop");
		String payload = "";
		RIOSend(server, Protocol.NOOP, Utility.stringToByteArray(payload));

		printInfo("sending noop to " + server);
	}

	/**
	 * Parses a server address from tokens
	 * 
	 * @param tokens
	 * @param cmd
	 * @return
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
	 * end onCommand Handler methods / parse helpers
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

	public void printError(int error, String command, String filename) {
		StringBuilder sb = appendNodeAddress();
		sb.append(" ");
		appendError(sb, command);
		sb.append(" on file ");
		sb.append(filename);
		Logger.error(error, sb.toString());
	}

	/**
	 * Stub for printError for when less information is available
	 * 
	 * @param error
	 * @param command
	 */
	public void printError(int error, String command) {
		StringBuilder sb = appendNodeAddress();
		sb.append(" ");
		appendError(sb, command);
		Logger.error(error, sb.toString());
	}

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

		// check if the file exists
		if (Utility.fileExists(this, fileName)) {
			printError(ErrorCode.FileAlreadyExists, "create", addr, fileName);
			sendResponse(from, "create", false);
			return;
		}

		// create the file
		else {
			try {
				PersistentStorageWriter writer = getWriter(fileName, false);
				writer.close();
			} catch (IOException e) {
				Logger.error(e);
			}
		}

		sendResponse(from, "create", true);
	}

	/**
	 * Local delete file.
	 * 
	 * @param fileName
	 */
	public void deleteFile(String fileName) {
		deleteFile(this.addr, fileName);
	}

	/**
	 * Deletes a file from the local file system. Fails and prints an error if
	 * the file does not exist
	 * 
	 * @param fileName
	 *            the file name to delete
	 */
	public void deleteFile(int from, String fileName) {

		printVerbose("attempting to DELETE file: " + fileName);

		// check if the file even exists
		if (!Utility.fileExists(this, fileName)) {
			printError(ErrorCode.FileDoesNotExist, "delete", addr, fileName);
			if (from != this.addr)
				sendResponse(from, "delete", false);
			return;
		} else {
			// delete file
			try {
				PersistentStorageWriter writer = getWriter(fileName, false);
				writer.delete();
				writer.close();
			} catch (IOException e) {
				Logger.error(e);
			}
		}

		sendResponse(from, "delete", true);
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

		// check if the file exists
		if (!Utility.fileExists(this, fileName)) {
			printError(ErrorCode.FileDoesNotExist, "get", addr, fileName);
			throw new FileNotFoundException();
		} else {
			// read and return the file if it does
			StringBuilder contents = new StringBuilder();
			String inLine = "";
			PersistentStorageReader reader = getReader(fileName);
			// TODO: adding newlines when it shouldn't
			while ((inLine = reader.readLine()) != null) {
				contents.append(inLine);
				contents.append(System.getProperty("line.separator"));
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
	public void getFile(String fileName, int from) {

		printVerbose("attempting to READ/GET file: " + fileName + " for Node: "
				+ from);

		// check if the file exists
		if (!Utility.fileExists(this, fileName)) {
			printError(ErrorCode.FileDoesNotExist, "get", addr, fileName);
			sendResponse(from, "get", false);
			return;
		}
		// send the file if it does
		else {
			// load the file into a reader
			String sendMsg = fileName + delimiter;
			String inLine = "";
			try {
				PersistentStorageReader reader = getReader(fileName);
				// TODO: adding newlines when it shouldn't
				while (!((inLine = reader.readLine()) == null))
					sendMsg = sendMsg + inLine
							+ System.getProperty("line.separator");
				reader.close();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}

			// send the payload
			byte[] payload = Utility.stringToByteArray(sendMsg);
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

		// check if the file exists
		if (!Utility.fileExists(this, fileName)) {
			if (protocol == Protocol.PUT)
				printError(ErrorCode.FileDoesNotExist, "put", addr, fileName);
			else
				printError(ErrorCode.FileDoesNotExist, "append", addr, fileName);
			sendResponse(from, Protocol.protocolToString(protocol), false);
			return;
		} else {
			try {
				PersistentStorageWriter writer = null;
				// create a new file writer, setting the append option
				// appropriately
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
				sendResponse(from, Protocol.protocolToString(protocol), false);
				// use printError?
				System.err.println(e.getMessage());
				e.printStackTrace();
			}
		}

		sendResponse(from, Protocol.protocolToString(protocol), true);
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

	/*************************************************
	 * end FS methods
	 ************************************************/

	/**
	 * Prints the file received from the get command. Also used to print
	 * success/failure responses returned from the server.
	 */
	public void receiveData(String cmdOrFileName, String contents) {
		// TODO: eventually we'll probably want to stop using this for
		// success/failure responses, moving each to their own protocol types
		String output = cmdOrFileName + " received with contents: " + contents;
		printInfo(output);
	}

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
		printVerbose("reading packet");

		String msgString = Utility.byteArrayToString(msg);

		switch (protocol) {

		case Protocol.CREATE:
			createFile(from, msgString);
			break;
		case Protocol.DELETE:
			deleteFile(from, msgString);
			break;
		case Protocol.GET:
			getFile(msgString, from);
			break;
		case Protocol.PUT:
		case Protocol.APPEND:
		case Protocol.DATA:
			decideParseOrAppend(from, protocol, msgString);
			break;
		case Protocol.NOOP:
			printVerbose("noop");
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
		default:
			printError(ErrorCode.InvalidCommand, "receive");
		}

		// TODO: implement {W,R}{D,Q,F}
		// Only server receives: {R,W}{Q,C}, IC
		// Only client receives: {R,W}F, IV
		// Both ways: {W,R}D

	}

	/*************************************************
	 * begin manager-only cache coherency functions
	 ************************************************/

	private void receiveRQ(int client, String fileName) {
		// Deal with locked files, and lock the file if it's not currently
		if (lockedFiles.contains(fileName)) {
			Queue<QueuedFileRequest> e = queuedFileRequests.get(fileName);
			e.add(new QueuedFileRequest(client, Protocol.RQ, Utility
					.stringToByteArray(fileName)));
			queuedFileRequests.put(fileName, e);
			return;
		}
		lockedFiles.add(fileName);

		// Check if anyone has RW status on this file
		Map<Integer, CacheStatuses> clientStatuses = clientCacheStatus
				.get(fileName);

		Integer key = null;
		for (Entry<Integer, CacheStatuses> entry : clientStatuses.entrySet()) {
			if (entry.getValue().equals(CacheStatuses.ReadWrite)) {
				if (key != null)
					Logger.error(ErrorCode.MultipleOwners,
							"Multiple owners on file: " + fileName);
				key = entry.getKey();
			}
		}

		// If no one owns a copy of this file, send them a copy and remove the
		// lock
		if (key == null) {
			sendFile(client, fileName, Protocol.RD);
			removeLock(fileName);
		} else {
			sendRequest(key, fileName, Protocol.RF);
			pendingPermissionRequests.put(fileName, client);
		}

	}

	private void sendFile(int client, String fileName, int protocol) {
		String sendMsg = "";

		if (!Utility.fileExists(this, fileName)) {
			createFile(fileName);
			sendMsg = fileName;
		} else {
			try {
				sendMsg = fileName + delimiter + getFile(fileName);
			} catch (IOException e) {
				Logger.error(e);
			}
		}

		byte[] payload = Utility.stringToByteArray(sendMsg);
		RIOSend(client, protocol, payload);
		printVerbose("sending " + protocol + " to " + client);
	}

	private void sendRequest(int client, String fileName, int protocol) {
		String sendMsg = fileName;
		byte[] payload = Utility.stringToByteArray(sendMsg);
		RIOSend(client, protocol, payload);
		printVerbose("sending " + protocol + " to " + client);
	}

	private void removeLock(String filename) {
		lockedFiles.remove(filename);
		if (!queuedFileRequests.containsKey(filename))
			queuedFileRequests.put(filename,
					new PriorityQueue<QueuedFileRequest>());
		Queue<QueuedFileRequest> outstandingRequests = queuedFileRequests
				.get(filename);
		QueuedFileRequest nextRequest = outstandingRequests.poll();
		if (nextRequest != null) {
			onRIOReceive(nextRequest.from, nextRequest.protocol,
					nextRequest.msg);
		}
	}

	private void receiveWQ(int client, String filename) {
		// TODO: log locks and unlocks (WQ, RQ, WC, RC)
		// Deal with locked files, and lock the file if it's not currently
		if (lockedFiles.contains(filename)) {
			Queue<QueuedFileRequest> e = queuedFileRequests.get(filename);
			e.add(new QueuedFileRequest(client, Protocol.WQ, Utility
					.stringToByteArray(filename)));
			queuedFileRequests.put(filename, e);
			return;
		}
		lockedFiles.add(filename);

		// Check if anyone has RW or RO status on this file
		Map<Integer, CacheStatuses> clientStatuses = clientCacheStatus
				.get(filename);

		if (clientStatuses == null)
			clientStatuses = new HashMap<Integer, CacheStatuses>();

		Integer rw = null;
		ArrayList<Integer> ro = new ArrayList<Integer>();

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
				ro.add(entry.getKey());
			}
		}
		if (rw != null) { // If someone has RW status:
			sendRequest(rw, filename, Protocol.WF);
			// If so, send the data back to the client waiting
		}
		pendingICs.put(filename, ro);
		for (Integer i : ro) { // Send invalidate requests to everyone with RO
								// status
			sendRequest(i, filename, Protocol.IV);
		}
		// Else if no one has permissions on this file, send them a WD
		sendFile(client, filename, Protocol.WD);
	}

	/**
	 * Manager Only Changes the status of this client from IV or RW
	 * 
	 * @param client
	 *            The client to change
	 * @param fileName
	 *            The filename
	 */
	private void receiveWC(int client, String fileName) {
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
	private void receiveRC(int client, String fileName) {
		updateClientCacheStatus(CacheStatuses.ReadOnly, client, fileName);
	}

	private void updateClientCacheStatus(CacheStatuses val, int client,
			String fileName) {
		if (!isManager) {
			Logger.error(ErrorCode.InvalidCommand,
					"Receieved confirm but not manager");
			return;
		}

		if (!clientCacheStatus.containsKey(fileName))
			clientCacheStatus.put(fileName,
					new HashMap<Integer, CacheStatuses>());

		// Update the client status and put it back in the cache status map
		HashMap<Integer, CacheStatuses> clientMap = (HashMap<Integer, CacheStatuses>) clientCacheStatus
				.get(fileName);
		clientMap.put(client, val);
		clientCacheStatus.put(fileName, clientMap);

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
	private void receiveIC(Integer from, String filename) {
		// TODO: Maybe different messages for the first two vs. the last
		// scenario
		// (node is manager but not expecting IC from this node for this file)?
		if (!pendingICs.containsKey(filename) || !isManager
				|| !pendingICs.get(filename).contains(from)) {
			sendResponse(from, Protocol.protocolToString(Protocol.ERROR), false);
			Logger.error(ErrorCode.NotManager, "IC: " + filename);
		} else {

			// update the status of the client who sent the IC
			Map<Integer, CacheStatuses> m = clientCacheStatus.get(filename);
			m.put(from, CacheStatuses.Invalid);
			clientCacheStatus.put(filename, m);

			pendingICs.get(filename).remove(from);
			if (pendingICs.get(filename).isEmpty()) { // If the pending ICs are
														// now empty, someone's
														// waiting for a WD, so
														// check for that and
														// send
				int destAddr = pendingPermissionRequests.get(filename);
				sendFile(destAddr, filename, Protocol.WD);
				removeLock(filename);
				// TODO: Deal with queued file requests
			}
		}
	}

	/*************************************************
	 * end manager-only cache coherency functions
	 ************************************************/

	/*************************************************
	 * begin client-only cache coherency functions
	 ************************************************/

	/**
	 * Client receives IV as a notification to mark a cached file invalid
	 */
	private void receiveIV(String msgString) {
		// If we're the manager and we received and IV, something bad happened
		if (isManager) {
			printError(ErrorCode.InvalidCommand, "iv " + msgString);
		} else {
			cacheStatus.put(msgString, CacheStatuses.Invalid);
		}
	}

	// TODO: Cleanup state after {R,W}{D,F} etc fails

	/**
	 * Only client receives RF as a request from the server to propagate their
	 * changes.
	 * 
	 * @param msgString
	 */
	private void receiveRF(String msgString) {
		if (isManager) {
			printError(ErrorCode.InvalidCommand, "rf " + msgString);
		}

		StringTokenizer tokens = new StringTokenizer(msgString);
		String filename = tokens.nextToken();

		// RW -> RO
		cacheStatus.put(filename, CacheStatuses.ReadOnly);
		printVerbose("RW -> RO for " + filename);

		try {
			String payload = filename + delimiter + getFile(filename);
			SendToManager(Protocol.RD, Utility.stringToByteArray(payload));
		} catch (UnknownManagerException e) {
			printError(ErrorCode.UnknownManager, "rf");
		} catch (IOException e) {
			printError(ErrorCode.UnknownError, "rf");
			// TODO: better error code
		}
	}

	/**
	 * Only client receives WF as a request from the server to propagate their
	 * changes.
	 * 
	 * @param msgString
	 */
	private void receiveWF(String msgString) {
		if (isManager) {
			printError(ErrorCode.InvalidCommand, "wf " + msgString);
		}

		StringTokenizer tokens = new StringTokenizer(msgString);
		String filename = tokens.nextToken();

		// lose ownership
		cacheStatus.put(filename, CacheStatuses.Invalid);
		printVerbose("lost ownership of " + filename);

		try {
			String payload = filename + delimiter + getFile(filename);
			SendToManager(Protocol.WD, Utility.stringToByteArray(payload));
		} catch (UnknownManagerException e) {
			printError(ErrorCode.UnknownManager, "wf");
		} catch (IOException e) {
			printError(ErrorCode.UnknownError, "wf");
			// TODO: better error code
		}
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

	/**
	 * @param msgString
	 *            ex) test hello world <filename> <contents>
	 */
	private void receiveWD(int from, String msgString) {

		// parse packet
		StringTokenizer tokens = new StringTokenizer(msgString);
		String filename = tokens.nextToken();
		String contents = "";
		if (tokens.hasMoreTokens()) {
			contents = msgString.substring(filename.length() + 1);
		}

		if (!isManager) {
			// TODO: this breaks for creates (empty contents)

			// has RW!
			cacheStatus.put(filename, CacheStatuses.ReadWrite);
			printVerbose("got ReadWrite on " + filename);

			// update in cache
			if (!Utility.fileExists(this, filename)) {
				createFile(filename);
			}
			writeFile(filename, contents, Protocol.PUT);

			// do what you originally intended with the file
			if (pendingOperations.containsKey(filename)) {
				Intent intent = pendingOperations.get(filename);
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
			} catch (UnknownManagerException e) {
				printError(ErrorCode.UnknownManager, "wd");
			}

		} else {
			// first write the file to save a local copy
			writeFile(filename, contents, Protocol.PUT);

			// send out a WD to anyone requesting this
			int destAddr = pendingPermissionRequests.get(filename);
			sendFile(destAddr, filename, Protocol.WD);

			// update the status of the client who sent the WD
			Map<Integer, CacheStatuses> m = clientCacheStatus.get(filename);
			m.put(from, CacheStatuses.Invalid);
			clientCacheStatus.put(filename, m);

		}
	}

	/**
	 * @param msgString
	 */
	private void receiveRD(int from, String msgString) {
		// parse packet
		StringTokenizer tokens = new StringTokenizer(msgString);
		String filename = tokens.nextToken();
		String contents = "";
		if (tokens.hasMoreTokens()) {
			contents = msgString.substring(filename.length() + 1);
		}

		if (!isManager) {
			// has RO
			cacheStatus.put(filename, CacheStatuses.ReadOnly);
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
			} catch (UnknownManagerException e) {
				printError(ErrorCode.UnknownManager, "rd");
			}
		} else {
			// first write the file to save a local copy
			writeFile(filename, contents, Protocol.PUT);

			// send out a RD to anyone requesting this
			int destAddr = pendingPermissionRequests.get(filename);
			sendFile(destAddr, filename, Protocol.RD);

			// update the status of the client who sent the WD
			Map<Integer, CacheStatuses> m = clientCacheStatus.get(filename);
			m.put(from, CacheStatuses.ReadOnly);
			clientCacheStatus.put(filename, m);
		}
	}

	/*************************************************
	 * end client and manager cache coherency functions
	 ************************************************/

	/**
	 * Parses a received command to decide whether to put or append to file
	 * 
	 * @param from
	 * @param protocol
	 * @param msgString
	 */
	private void decideParseOrAppend(Integer from, int protocol,
			String msgString) {
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

	/**
	 * Sends a response if destAddr >= 0 (otherwise assumes that a response is
	 * not meant to be sent)
	 * 
	 * @param destAddr
	 *            Who to send the response to
	 * @param protocol
	 *            The protocol
	 * @param successful
	 *            Whether the operation was successful
	 */
	private void sendResponse(Integer destAddr, String protocol,
			boolean successful) {
		if (destAddr != this.addr) {
			String sendMsg = protocol + delimiter
					+ (successful ? "successful" : "not successful");

			byte[] payload = Utility.stringToByteArray(sendMsg);
			RIOLayer.RIOSend(destAddr, Protocol.DATA, payload);
			printVerbose("sending response: " + protocol + " status: "
					+ (successful ? "successful" : "not successful"));
		}
	}
}
