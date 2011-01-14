/**
 * CSE 490h
 * @author wayneg, steinz
 */

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
	 * Delimeter used in protocol payloads Should be a single character
	 */
	private final String delimiter = " ";

	/**
	 * Verbose flag for debugging
	 */
	// TODO: Move to a more globaly accessible place - probably a logger class
	private static final boolean verbose = true;

	public Client() {
		super();
	}

	/**
	 * Mandatory start method Cleans up failed puts if necessary
	 */
	public void start() {
		// Replace .temp to its old file, if a crash occurred
		if (Utility.fileExists(this, ".temp")) {
			try {
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
			} catch (FileNotFoundException e) {
				// TODO: use printError (add an overload that takes an exception
				// maybe)?
				System.err.println(e.getMessage());
				e.printStackTrace();
			} catch (IOException e) {
				System.err.println(e.getMessage());
				e.printStackTrace();
			}
		}
	}

	/**
	 * Process a command from user or file Expects lowercase commands
	 * 
	 * @param command
	 *            The command for this node
	 */
	public void onCommand(String command) {
		if (command.toUpperCase().equals("DEBUG")) {
			RIOLayer.printSeqStateDebug();
			return;
		}

		StringTokenizer tokens = new StringTokenizer(command, " ");
		String cmd = "", filename = "";
		int server = -1;
		try {
			cmd = tokens.nextToken();
			server = Integer.parseInt(tokens.nextToken());
			if (!cmd.equals("handshake") && !cmd.equals("noop")) {
				filename = tokens.nextToken();
			}
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
		String payload = "";
		if (cmd.equals("handshake")) {
			payload = getID().toString();
		} else if (cmd.equals("noop")) {
			payload = " ";
		} else {
			payload = filename;
		}
		if (cmd.equals("put") || cmd.equals("append")) {
			payload += delimiter + contents;
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
		// TODO: Factor out to logger class
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
				+ ErrorCode.lookup(error);
		System.err.println(stringOut);
	}

	/**
	 * Extends onReceive for extra logging Currently turned off
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
		if (verbose) {
			printVerbose("attempting to CREATE file: " + fileName);
		}
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
		if (verbose) {
			printVerbose("attempting to DELETE file: " + fileName);
		}
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
		if (verbose) {
			printVerbose("attempting to READ/GET file: " + fileName
					+ " for Node: " + from);
		}
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
					// Temporary storage in case of a crash
					String oldString = "";
					String inLine;
					PersistentStorageReader oldFileReader = getReader(fileName);
					while ((inLine = oldFileReader.readLine()) != null)
						oldString = oldString + inLine
								+ System.getProperty("line.separator");
					PersistentStorageWriter temp = getWriter(".temp", false);
					temp.write(fileName + "\n" + oldString);

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
	 * Prints the file received from the get command. Also used to print
	 * success/failure responses returned from the server.
	 */
	public void receiveData(String cmdOrFileName, String contents) {
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
			// tokenize the string and parse out the contents and filename
			StringTokenizer tokenizer = new StringTokenizer(msgString);
			String fileName = tokenizer.nextToken();
			int length = fileName.length();

			String contents = msgString.substring(length + 1,
					msgString.length());

			if (protocol == Protocol.DATA) {
				receiveData(fileName, contents);
			} else {
				writeFile(from, fileName, contents, protocol);
			}
			break;
		case Protocol.NOOP:
			if (verbose) {
				printVerbose("noop");
			}
			break;
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
		printVerbose("sending response: " + protocol + " status: "
				+ (successful ? "successful" : "not successful"));
	}

	@Override
	public String toString() {
		return RIOLayer.toString();
	}
}
