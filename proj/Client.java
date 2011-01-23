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
	private final String delimiter = " ";

	/**
	 * Status of cached files on disk. Keys are filenames.
	 */
	private Map<String, CacheStatuses> cacheStatus;

	/**
	 * Whether or not this node is the manager for project 2.
	 */
	private boolean isManager;

	/*************************************************
	 * BEGIN MANAGER ONLY DATA STRUCTURES
	 ************************************************/

	/**
	 * List of files whose requests are currently being worked out.
	 */
	private Set<String> lockedFiles;

	/**
	 * Status of cached files for all clients.
	 */
	private Map<String, CacheStatuses> clientCacheStatus;

	/**
	 * List of nodes the manager is waiting for ICs from.
	 */
	private Map<String, List<Integer>> pendingICs;

	// TODO: Add queue for pending requests of locked files. I think we need to
	// add a queuedFileRequest class to keep in here or something if we want to
	// queue requests we can't satisfy right away.
	// private Queue<E> queuedFileRequests;

	/*************************************************
	 * END MANAGER ONLY DATA STRUCTURES
	 ************************************************/

	public Client() {
		super();
		this.cacheStatus = new HashMap<String, CacheStatuses>();
		this.isManager = false;
		Logger.eraseLog(); // Wipe the server log
	}

	/**
	 * Mandatory start method Cleans up failed puts if necessary
	 */
	public void start() {
		// Replace .temp to its old file, if a crash occurred
		if (Utility.fileExists(this, ".temp")) {
			try {
				restoreFromTempFile();
			} catch (FileNotFoundException e) {
				Logger.printError(addr, ErrorCode.FileDoesNotExist, ".temp");
				System.err.println(e.getMessage());
				e.printStackTrace();
			} catch (IOException e) {
				System.err.println(e.getMessage());
				e.printStackTrace();
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
	private void restoreFromTempFile() throws FileNotFoundException,
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
			this.clientCacheStatus = new HashMap<String, CacheStatuses>();
			this.pendingICs = new HashMap<String, List<Integer>>();
		}
		Logger.write(addr, " received command to become manager");

	}

	/**
	 * Create RPC
	 * 
	 * @param tokens
	 * @param line
	 */
	public void createHandler(StringTokenizer tokens, String line) {
		int server = parseServer(tokens, "append");
		String filename = parseFilename(tokens, "append");
		RIOSend(server, Protocol.CREATE, Utility.stringToByteArray(filename));
	}

	/**
	 * Delete RPC
	 * 
	 * @param tokens
	 * @param line
	 */
	public void deleteHandler(StringTokenizer tokens, String line) {
		int server = parseServer(tokens, "append");
		String filename = parseFilename(tokens, "append");
		RIOSend(server, Protocol.DELETE, Utility.stringToByteArray(filename));
	}

	/**
	 * Get RPC
	 * 
	 * @param tokens
	 * @param line
	 */
	public void getHandler(StringTokenizer tokens, String line) {
		int server = parseServer(tokens, "append");
		String filename = parseFilename(tokens, "append");
		RIOSend(server, Protocol.GET, Utility.stringToByteArray(filename));
	}

	/**
	 * Put RPC
	 * 
	 * @param tokens
	 * @param line
	 */
	public void putHandler(StringTokenizer tokens, String line) {
		int server = parseServer(tokens, "append");
		String filename = parseFilename(tokens, "append");
		String content = parseAddContent(line, "append", server, filename);
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
		int server = parseServer(tokens, "append");
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
			Logger.printError(addr, ErrorCode.InvalidServerAddress, cmd);
			throw e;
		} catch (NoSuchElementException e) {
			Logger.printError(addr, ErrorCode.IncompleteCommand, cmd);
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
			Logger.printError(addr, ErrorCode.IncompleteCommand, cmd);
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
			Logger.printError(addr, ErrorCode.IncompleteCommand, cmd, server, filename);
			// TODO: throw an exception
		}

		return line.substring(parsedLength);
	}

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
			Logger.printError(addr, ErrorCode.InvalidCommand, "");
			return;
		}

		// Dynamically call <cmd>Command, passing off the tokenizer and the full
		// command string
		try {
			Class[] paramTypes = { StringTokenizer.class, String.class };
			Method handler = this.getClass().getMethod(cmd + "Handler",
					paramTypes);
			Object[] args = { tokens, line };
			handler.invoke(this, args);
		} catch (NoSuchMethodException e) {
			Logger.printError(addr, ErrorCode.InvalidCommand, cmd);
		} catch (IllegalArgumentException e) {
			Logger.printError(addr, ErrorCode.DynamicCommandError, cmd);
		} catch (IllegalAccessException e) {
			Logger.printError(addr, ErrorCode.DynamicCommandError, cmd);
		} catch (InvocationTargetException e) {
			Logger.printError(addr, ErrorCode.DynamicCommandError, cmd);
		}
	}

	/**
	 * Extends onReceive for extra logging. Currently turned off.
	 */
	@Override
	public void onReceive(Integer from, int protocol, byte[] msg) {
		/*
		 * if (verbose) { // feedback for the console String msgString =
		 * Utility.byteArrayToString(msg); printVerbose("RECEIVED Protocol: " +
		 * Protocol.protocolToString(protocol) + " With Arguments: " + msgString
		 * + " From Node: " + from); }//
		 */

		super.onReceive(from, protocol, msg);
	}


	/**
	 * Creates a file on the local filesystem
	 * 
	 * @param fileName
	 *            the file to create
	 */
	public void createFile(int from, String fileName) {
		Logger.write(addr, "attempting to CREATE file: ");
		// check if the file exists
		if (Utility.fileExists(this, fileName)) {
			Logger.printError(addr, ErrorCode.FileAlreadyExists, "create", addr, fileName);
			sendResponse(from, "create", false);
			return;
		}

		// create the file
		else {
			try {
				PersistentStorageWriter writer = getWriter(fileName, false);
				writer.close();
			} catch (IOException e) {
				// TODO: use printError?
				System.err.println(e.getMessage());
				System.err.println(e.getStackTrace());
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
		
		Logger.write(addr, "attempting to DELETE file: " + fileName);
		// check if the file even exists
		if (!Utility.fileExists(this, fileName)) {
			Logger.printError(addr, ErrorCode.FileDoesNotExist, "delete", addr, fileName);
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
				// TODO: use printError?
				System.err.println(e.getMessage());
				e.printStackTrace();
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
		
		Logger.write(addr, "attempting to READ/GET file: " + fileName
					+ " for Node: " + from);
		// check if the file exists
		if (!Utility.fileExists(this, fileName)) {
			Logger.printError(addr, ErrorCode.FileDoesNotExist, "get", addr, fileName);
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
			Logger.write(addr, "sending contents of file: " + fileName + " to Node: "
					+ from);

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
	 */
	public void writeFile(int from, String fileName, String contents,
			int protocol) {
		
		Logger.write(addr, "attempting to PUT/APPEND File: " + fileName
					+ " with Contents: " + contents);
		// check if the file exists
		if (!Utility.fileExists(this, fileName)) {
			if (protocol == Protocol.PUT)
				Logger.printError(addr, ErrorCode.FileDoesNotExist, "put", addr, fileName);
			else
				Logger.printError(addr, ErrorCode.FileDoesNotExist, "append", addr, fileName);
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

	/**
	 * Prints the file received from the get command. Also used to print
	 * success/failure responses returned from the server.
	 */
	public void receiveData(String cmdOrFileName, String contents) {
		String output = cmdOrFileName + " received with contents: " + contents;
		Logger.write(addr, output);
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
		Logger.write(addr, "reading packet");

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
			Logger.write(addr, "noop");
			break;
		}

		// TODO: implement {W,R}{D,Q,F,C} and I{C,V} handling
		// Only server receives: {R,W}{Q,C}, IC
		// Only client receives: {R,W}F, IV
		// Both ways: {W,R}D

	}

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
	 * Sends
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
		String sendMsg = protocol + delimiter
				+ (successful ? "successful" : "not successful");

		byte[] payload = Utility.stringToByteArray(sendMsg);
		RIOLayer.RIOSend(destAddr, Protocol.DATA, payload);
		Logger.write(addr, "sending response: " + protocol + " status: "
				+ (successful ? "successful" : "not successful"));
	}

	@Override
	public String toString() {
		return RIOLayer.toString();
	}
}
