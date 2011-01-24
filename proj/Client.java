/**
 * CSE 490h
 * @author wayger, steinz
 */

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
	 * Whether or not this node is the manager for project 2.
	 */
	private boolean isManager;

	/**
	 * The address of the manager node.
	 */
	private int managerIs;
	
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

	// TODO: Later - add queue for pending requests of locked files. I think we
	// need to add a queuedFileRequest class to keep in here or something if we
	// want to queue requests we can't satisfy right away.
	// private Queue<E> queuedFileRequests;

	/*************************************************
	 * end manager only data structures
	 ************************************************/

	public Client() {
		super();
		this.cacheStatus = new HashMap<String, CacheStatuses>();
		this.isManager = false;
		this.managerIs = -1;
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
	
	public void managerisHandler(StringTokenizer tokens, String line) {
		try {
			this.managerIs = Integer.parseInt(tokens.nextToken());
			printInfo("manager is " + this.managerIs);
		} catch (NumberFormatException e) {
			printError(ErrorCode.InvalidCommand, "manageris");
		} catch (NoSuchElementException e) {
			printError(ErrorCode.IncompleteCommand, "manageris");
		}
	}

	/**
	 * Create RPC
	 * 
	 * @param tokens
	 * @param line
	 */
	public void createHandler(StringTokenizer tokens, String line) {
		int server = parseServer(tokens, "create");
		String filename = parseFilename(tokens, "create");
		RIOSend(server, Protocol.CREATE, Utility.stringToByteArray(filename));
	}

	/**
	 * Delete RPC
	 * 
	 * @param tokens
	 * @param line
	 */
	public void deleteHandler(StringTokenizer tokens, String line) {
		int server = parseServer(tokens, "delete");
		String filename = parseFilename(tokens, "delete");
		RIOSend(server, Protocol.DELETE, Utility.stringToByteArray(filename));
	}

	/**
	 * Get RPC
	 * 
	 * @param tokens
	 * @param line
	 */
	public void getHandler(StringTokenizer tokens, String line) {
		int server = parseServer(tokens, "get");
		String filename = parseFilename(tokens, "get");
		RIOSend(server, Protocol.GET, Utility.stringToByteArray(filename));
	}

	/**
	 * Put RPC
	 * 
	 * @param tokens
	 * @param line
	 */
	public void putHandler(StringTokenizer tokens, String line) {
		int server = parseServer(tokens, "put");
		String filename = parseFilename(tokens, "put");
		String content = parseAddContent(line, "put", server, filename);
		String payload = filename + delimiter + content;
		RIOSend(server, Protocol.PUT, Utility.stringToByteArray(payload));
	}

	/**
	 * Append RPC
	 * 
	 * @param tokens
	 * @param line
	 */
	public void appendHandler(StringTokenizer tokens, String line) {
		// TODO: I think I found a framework bug - "append 1 test  world" is
		// losing the extra space
		int server = parseServer(tokens, "append");
		String filename = parseFilename(tokens, "append");
		String content = parseAddContent(line, "append", server, filename);
		String payload = filename + delimiter + content;
		RIOSend(server, Protocol.APPEND, Utility.stringToByteArray(payload));
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
	protected String parseAddContent(String line, String cmd, int server,
			String filename) {

		int parsedLength = cmd.length() + Integer.toString(server).length()
				+ filename.length() + 3;
		if (parsedLength >= line.length()) {
			// no contents
			printError(ErrorCode.IncompleteCommand, cmd, server, filename);
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
		if (from != this.addr)
			sendResponse(from, "delete", true);
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
	 * @param fileName
	 * @param contents
	 */
	public void writeFile(String fileName, String contents, int protocol)
	{
		writeFile(-1, fileName, contents, protocol);
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
		printVerbose(output);
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
		default:
			Logger.error("Error: " + ErrorCode.InvalidCommand);

		}

		// TODO: implement {W,R}{D,Q,F,C}
		// Only server receives: {R,W}{Q,C}, IC
		// Only client receives: {R,W}F, IV
		// Both ways: {W,R}D

	}

	
	/*************************************************
	 * begin manager-only cache coherency functions
	 ************************************************/
	/**
	 * Manager only function
	 * Changes the status of this client from IV or RW
	 * @param client The client to change
	 * @param fileName The filename
	 */
	private void receiveWC(int client, String fileName)
	{
		updateClientCacheStatus(CacheStatuses.ReadWrite, client, fileName);
	}
	
	/**
	 * Receives an RC and changes this client's status from IV or RW to RO.
	 * @param client The client to change
	 * @param fileName The filename
	 */
	private void receiveRC(int client, String fileName)
	{
		updateClientCacheStatus(CacheStatuses.ReadOnly, client, fileName);
	}
	
	private void updateClientCacheStatus(CacheStatuses val, int client, String fileName)
	{
		if (!isManager) {
			Logger.error(ErrorCode.InvalidCommand, "Receieved confirm but not manager");
			return;
		}
		
		if (!clientCacheStatus.containsKey(fileName))
			clientCacheStatus.put(fileName, new HashMap<Integer, CacheStatuses>());
		
		// Update the client status and put it back in the cache status map
		HashMap<Integer, CacheStatuses> clientMap = (HashMap<Integer, CacheStatuses>) clientCacheStatus.get(fileName);
		clientMap.put(client, val);
		clientCacheStatus.put(fileName, clientMap);
	}
	
	/**
	 * 
	 * @param from
	 *            The node this IC was received from.
	 * @param msgString
	 *            Should be the file name. Throws an error if we were not
	 *            waiting for an IC from this node for this file
	 */
	private void receiveIC(Integer from, String msgString) {
		// TODO: Maybe different messages for the first two vs. the last
		// scenario
		// (node is manager but not expecting IC from this node for this file)?
		if (!pendingICs.containsKey(msgString) || !isManager
				|| !pendingICs.get(msgString).contains(from)) {
			sendResponse(from, Protocol.protocolToString(Protocol.ERROR), false);
			Logger.error(ErrorCode.InvalidCommand, "IC: " + msgString);
		} else {
			pendingICs.get(msgString).remove(from);
			// TODO: Check if pendingICs is empty now, and decide what to do?
		}
	}

	/*************************************************
	 * end manager-only cache coherency functions
	 ************************************************/
	
	/*************************************************
	 * begin client-only cache coherency functions
	 ************************************************/
	private void receiveIV(String msgString) {
		// If we're the manager and we received and IV, something bad happened
		if (isManager) {
			Logger.error(ErrorCode.InvalidCommand, "IV: " + msgString);
		} else {
			cacheStatus.put(msgString, CacheStatuses.Invalid);
		}
	}

	
	/*************************************************
	 * end client-only cache coherency functions
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
	 * Sends a response if destAddr >= 0 (otherwise assumes that a response is not meant to be sent)
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
		if (destAddr >= 0) {
			String sendMsg = protocol + delimiter
					+ (successful ? "successful" : "not successful");
	
			byte[] payload = Utility.stringToByteArray(sendMsg);
			RIOLayer.RIOSend(destAddr, Protocol.DATA, payload);
			printVerbose("sending response: " + protocol + " status: "
					+ (successful ? "successful" : "not successful"));
		}
	}
}
