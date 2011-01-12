import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.NoSuchElementException;

import edu.washington.cs.cse490h.lib.Node;
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
    //TODO: Ask if we're supposed to have client and server in same class

    /**
     * Verbose flag for debugging
     */
    private static final boolean verbose = true;
	
	public Client() {
		super();
	}

	public void start() {
	}

    /**
     * Process a command from user or file
     *
     * @param command
     *           The command for this node
     */
	public void onCommand(String command) {
		// TODO: For some reason, the protocol is always being sent as either 0 or 1
		// parse cmd, server, filename
		StringTokenizer tokens = new StringTokenizer(command, " ");
		String cmd = "", filename = "";
                int server = -1;
		try {
			cmd = tokens.nextToken();
			server = Integer.parseInt(tokens.nextToken());
			filename = tokens.nextToken();
                } catch (NumberFormatException e) {
                    //TODO: Refractor error codes to there own class for convenience
                    // bad server address
                    printError(910, cmd, server, filename);
                    return;
		} catch (NoSuchElementException e) {
                    // incomplete command
                    printError(900, cmd, server, filename);
                    return;
		}

		// parse contents for put and append
		String contents = "";
		if (cmd.equals("put") || cmd.equals("append")) {
                    int parsedLength = cmd.length() + Integer.toString(server).length() + filename.length() + 3;
			if (parsedLength >= command.length()) {
                            // no contents
                            printError(900, cmd, server, filename);
                            return;
			}
			else  {
				contents = command.substring(parsedLength);
			}
		}
		
		// build and send message
		String payload = filename;
		if (cmd.equals("put") || cmd.equals("append")) {
                    //TODO: Tabs look pretty bad in the simulator, switch to something else?
			payload += " " + contents;
		}
		int protocol = Protocol.stringToProtocol(cmd);
		if (protocol == -1) {
                    printError(901, cmd, server, filename);
                    return;
		} else {
                    RIOSend(server, protocol, Utility.stringToByteArray(payload));
		}
	}

    /**
     * Prints msg if verbose is true
     * Also prints a frame if frame is true
     */
    public void printVerbose(String msg, boolean frame) {
        if (verbose) {
            if (frame) {
                System.out.println("\n===VERBOSE===");
            }
            System.out.println(msg);
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
    public void printError(int error, String command, int server, String filename){
        //TODO: pass through command, server, filename for existing calls to printError
    	String stringOut = "";
    	stringOut += "Node " + addr + ": Error: " + command + 
    	" on server: " + server +  " and file: " + filename + 
    	" returned error code: " + error;
        System.out.println(stringOut);
    }

    /**
     * TODO: summarize
     */
	@Override
	public void onReceive(Integer from, int protocol, byte[] msg) {
            //TODO: Think about logging and debugging output...
            if (verbose) {
		// feedback for the console
		String msgString = Utility.byteArrayToString(msg);
		printVerbose("Node: " + addr + " RECEIVED Protocol: " + Protocol.protocolToString(protocol) + " With Arguments: " 
				+ msgString + " From Node: " +  from);
            }		

       switch(protocol) {
       // taking advantage of case-structure fall-through behavior
       case Protocol.DATA:
       case Protocol.CREATE:
       case Protocol.DELETE:
       case Protocol.GET:
       case Protocol.PUT:
       case Protocol.APPEND:
    	   RIOLayer.RIODataReceive(from, msg);
    	   break;
       case Protocol.ACK:
    	   RIOLayer.RIOAckReceive(from, msg);
    	   break;
            
       }

	}

	/**
	 * Creates a file on the local filesystem
	 * @param fileName the file to create
	 */
	public void createFile(String fileName){
		if (verbose){
			printVerbose("Node: " + addr + " attempting to CREATE file: " + fileName);
		}
		// check if the file exists
            if (Utility.fileExists(this, fileName)){
			printError(11, "create", addr, fileName);
		}
		
		// create the file
		else{
			try{
				PersistentStorageWriter writer = getWriter(fileName, false);
				writer.close();
			} catch (IOException e)
			{
				System.err.println(e.getMessage());
				System.err.println(e.getStackTrace());
			}
		}
	}
	
	/**
	 * Deletes a file from the local file system. Fails and prints an error if the file does not exist
	 * @param fileName the file name to delete
	 */
	public void deleteFile(String fileName){
		if (verbose){
			printVerbose("Node: " + addr + " attempting to DELETE file: " + fileName);
		}
		// check if the file even exists
            if (!Utility.fileExists(this, fileName))
			printError(10, "delete", addr, fileName);
		else{
			// delete file
			try{
				PersistentStorageWriter writer = getWriter(fileName, false);
				writer.delete();
				writer.close();
			} catch (IOException e) 
			{
				System.err.println(e.getMessage());
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Sends a file to the client with the given filename
	 * @param fileName the filename to send
	 */
	public void getFile(String fileName, int from){
		if (verbose){
			printVerbose("Node: " + addr + " attempting to READ/GET file: " + fileName + " for Node: " + from);
		}
		// check if the file exists
            if (!Utility.fileExists(this, fileName))
			printError(10, "get", addr, fileName);
		// send the file if it does
		else{
			// load the file into a reader
			String sendMsg = "";
			String inLine = "";
			try{
				PersistentStorageReader reader = getReader(fileName);
				while (!((inLine = reader.readLine()) == null))
					sendMsg += inLine;
				reader.close();
			} catch (FileNotFoundException e){
				// file not found, but should have been caught earlier...
			} catch (IOException e){
				// ioexception
			}
			
			// send the payload
			byte[] payload = Utility.stringToByteArray(sendMsg);
			RIOLayer.RIOSend(from, Protocol.DATA, payload);
		}
	}
	
	/**
	 * Writes a file to the local filesystem. Fails if the file does not exist already
	 * @param filename the file name to write to
	 * @param contents the contents to write
	 */
	public void writeFile(String fileName, String contents, int protocol)
	{
		if (verbose){
			printVerbose("Node: " + addr + " attempting to PUT/APPEND File: " + fileName + " with Contents: " + contents);
		}
		// check if the file exists
            if (!Utility.fileExists(this, fileName)){
            	if (protocol == Protocol.APPEND)
            		printError(10, "put", addr, fileName);
            	else
            		printError(10, "append", addr, fileName);
            }
		else{
			try{
				PersistentStorageWriter writer = null;
				// create a new file writer, setting the append option appropriately
				if (protocol == Protocol.APPEND){
					writer = getWriter(fileName, true);
				} else {
					writer = getWriter(fileName, false);
				}
				writer.write(contents);
                                writer.flush();
				writer.close();
			} catch (IOException e){
				// ioexception
			}
		}
	}
    /**
     * Prints the file received from the get command
     */	
    public void receiveFile(String msgString) {
        System.out.println(msgString);
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
	public void onRIOReceive(Integer from, int protocol, byte[] msg)
	{
		String msgString = Utility.byteArrayToString(msg);
		
		switch(protocol){
		
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
			// tokenize the string and parse out the contents and filename
			StringTokenizer tokenizer = new StringTokenizer(msgString);
			String fileName = tokenizer.nextToken();
			int length = fileName.length();

			String contents = msgString.substring(length+1, msgString.length());

			writeFile(fileName, contents, protocol);
			break;
		case Protocol.DATA:
            // get response
            receiveFile(msgString);
		}
	}

	@Override
	public String toString() {
            return RIOLayer.toString();
	}
}