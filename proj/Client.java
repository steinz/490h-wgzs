import java.io.FileNotFoundException;
import java.io.IOException;
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
 */
public class Client extends RIONode {

	/**
	 * Delimeter used in protocol payloads
	 */
	private final String delimeter = " ";

	/**
	 * Verbose flag for debugging
	 */
	private static final boolean verbose = true;

	public Client() {
		super();
	}

	public void start() {
		// Replace .temp to its old file, if a crash occurred
		if (Utility.fileExists(this, ".temp"))
		{
			try{
				PersistentStorageReader reader = getReader(".temp");
				
				if (!reader.ready())
					deleteFile(".temp");
				else
				{
					String oldString = "";
					String inLine = "";
					String fileName = reader.readLine();
					while ((inLine = reader.readLine()) != null)
						oldString += inLine; 
					PersistentStorageWriter writer = getWriter(fileName, false);
					writer.write(oldString);
					deleteFile(".temp");
				}
			} catch (FileNotFoundException e)
			{
				System.err.println(e.getMessage());
				e.printStackTrace();
			} catch (IOException e)
			{
				System.err.println(e.getMessage());
				e.printStackTrace();
			}
		}
	}

	/**
	 * Process a command from user or file
	 * 
	 * @param command
	 *            The command for this node
	 */
	public void onCommand(String command) {
		StringTokenizer tokens = new StringTokenizer(command, " ");
		String cmd = "", filename = "";
		int server = -1;
		try {
			cmd = tokens.nextToken();
			server = Integer.parseInt(tokens.nextToken());
			filename = tokens.nextToken();
		} catch (NumberFormatException e) {
			printError(ErrorCode.InvalidServerAddress, cmd, server, filename);
			return;
		} catch (NoSuchElementException e) {
			// incomplete command
			printError(ErrorCode.IncompleteCommand, cmd, server, filename);
			return;
		}

		// parse contents for put and append
		String contents = "";
		if (cmd.equals("put") || cmd.equals("append")) {
			int parsedLength = cmd.length() + Integer.toString(server).length()
					+ filename.length() + 3;
			if (parsedLength >= command.length()) {
				// no contents
				printError(ErrorCode.IncompleteCommand, cmd, server, filename);
				return;
			} else {
				contents = command.substring(parsedLength);
			}
		}

		// build and send message
		String payload = filename;
		if (cmd.equals("put") || cmd.equals("append")) {
			payload += delimeter + contents;
		}
		int protocol = Protocol.stringToProtocol(cmd);
		if (protocol == -1) {
			printError(ErrorCode.InvalidCommand, cmd, server, filename);
			return;
		} else {
			RIOSend(server, protocol, Utility.stringToByteArray(payload));
		}
	}

	/**
	 * Prints msg if verbose is true Also prints a frame if frame is true
	 */
	public void printVerbose(String msg, boolean frame) {
		if (verbose) {
			if (frame) {
				System.out.println("\n===VERBOSE===");
			}
			System.out.println("Node " + addr + ": " + msg);
			if (frame) {
				System.out.println("===VERBOSE===\n");
			}
		}
	}

	/**
	 * Stub for printVerbose that doesn't print a frame
	 */
	public void printVerbose(String msg) {
		printVerbose(msg, false);
	}

	/**
	 * Prints an error message
	 */
	public void printError(int error, String command, int server,
			String filename) {
		String stringOut = "";
		stringOut += "Node " + addr + ": Error: " + command + " on server: "
				+ server + " and file: " + filename + " returned error code: "
				+ error;
		System.out.println(stringOut);
	}

	/**
	 * Extends onReceive for extra logging
	 */
	@Override
	public void onReceive(Integer from, int protocol, byte[] msg) {
		if (verbose) {
			// feedback for the console
			String msgString = Utility.byteArrayToString(msg);
			printVerbose("RECEIVED Protocol: "
					+ Protocol.protocolToString(protocol) + " With Arguments: "
					+ msgString + " From Node: " + from);
		}
		
		super.onReceive(from, protocol, msg);
	}

	/**
	 * Creates a file on the local filesystem
	 * 
	 * @param fileName
	 *            the file to create
	 */
	public void createFile(String fileName) {
		if (verbose) {
			printVerbose("attempting to CREATE file: " + fileName);
		}
		// check if the file exists
		if (Utility.fileExists(this, fileName)) {
			printError(ErrorCode.FileAlreadyExists, "create", addr, fileName);
		}

		// create the file
		else {
			try {
				PersistentStorageWriter writer = getWriter(fileName, false);
				writer.close();
			} catch (IOException e) {
				System.err.println(e.getMessage());
				System.err.println(e.getStackTrace());
			}
		}
	}

	/**
	 * Deletes a file from the local file system. Fails and prints an error if
	 * the file does not exist
	 * 
	 * @param fileName
	 *            the file name to delete
	 */
	public void deleteFile(String fileName) {
		if (verbose) {
			printVerbose("attempting to DELETE file: " + fileName);
		}
		// check if the file even exists
		if (!Utility.fileExists(this, fileName))
			printError(ErrorCode.FileDoesNotExist, "delete", addr, fileName);
		else {
			// delete file
			try {
				PersistentStorageWriter writer = getWriter(fileName, false);
				writer.delete();
				writer.close();
			} catch (IOException e) {
				System.err.println(e.getMessage());
				e.printStackTrace();
			}
		}
	}

	/**
	 * Sends a file to the client with the given filename
	 * 
	 * @param fileName
	 *            the filename to send
	 */
	public void getFile(String fileName, int from) {
		if (verbose) {
			printVerbose("attempting to READ/GET file: " + fileName
					+ " for Node: " + from);
		}
		// check if the file exists
		if (!Utility.fileExists(this, fileName))
			printError(ErrorCode.FileDoesNotExist, "get", addr, fileName);
		// send the file if it does
		else {
			// load the file into a reader
			String sendMsg = fileName + delimeter;
			String inLine = "";
			try {
				PersistentStorageReader reader = getReader(fileName);
				while (!((inLine = reader.readLine()) == null))
					sendMsg += inLine;
				reader.close();
			} catch (FileNotFoundException e) {
				// file not found, but should have been caught earlier...
			} catch (IOException e) {
				// ioexception
			}

			// send the payload
			byte[] payload = Utility.stringToByteArray(sendMsg);
			RIOLayer.RIOSend(from, Protocol.DATA, payload);
			printVerbose("sending contents of file: " + fileName + " to Node: "
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
	public void writeFile(String fileName, String contents, int protocol) {
		if (verbose) {
			printVerbose("attempting to PUT/APPEND File: " + fileName
					+ " with Contents: " + contents);
		}
		// check if the file exists
		if (!Utility.fileExists(this, fileName)) {
			if (protocol == Protocol.PUT)
				printError(ErrorCode.FileDoesNotExist, "put", addr, fileName);
			else
				printError(ErrorCode.FileDoesNotExist, "append", addr, fileName);
		} else {
			try {
				PersistentStorageWriter writer = null;
				// create a new file writer, setting the append option
				// appropriately
				if (protocol == Protocol.APPEND) {
					writer = getWriter(fileName, true);
				} else {
					// Temporary storage in case of a crash
					String oldString = "";
					String inLine;
					PersistentStorageReader oldFileReader = getReader(fileName);
					while ((inLine = oldFileReader.readLine()) != null)
					{
						oldString += inLine;
					}
					PersistentStorageWriter temp = getWriter(".temp", false);
					temp.write(fileName + "\n" + oldString);
					
					writer = getWriter(fileName, false);
				}
				writer.write(contents);
				writer.flush();
				writer.close();
				// Delete the temporary file if it exists
				if (protocol == Protocol.PUT)
					deleteFile(".temp");
			} catch (IOException e) {
				System.err.println(e.getMessage());
				e.printStackTrace();
			}
		}
	}

	/**
	 * Prints the file received from the get command
	 */
	public void receiveFile(String fileName, String contents) {
		if (verbose) {
			printVerbose("received the contents of file: " + fileName);
		}
		String output = fileName + " received with contents: " + contents;
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
		printVerbose("reading a message...");
		
		String msgString = Utility.byteArrayToString(msg);

		switch (protocol) {

		case Protocol.CREATE:
			createFile(msgString);
			break;
		case Protocol.DELETE:
			deleteFile(msgString);
			break;
		case Protocol.GET:
			getFile(msgString, from);
			break;
		case Protocol.PUT:
		case Protocol.APPEND:
		case Protocol.DATA:
			// tokenize the string and parse out the contents and filename
			StringTokenizer tokenizer = new StringTokenizer(msgString);
			String fileName = tokenizer.nextToken();
			int length = fileName.length();

			String contents = msgString.substring(length + 1,
					msgString.length());

			if (protocol == Protocol.DATA) {
				receiveFile(fileName, contents);
			} else {
				writeFile(fileName, contents, protocol);
			}
			break;
		case Protocol.NOOP:
			break;
		}
	}

	@Override
	public String toString() {
		return RIOLayer.toString();
	}
}
