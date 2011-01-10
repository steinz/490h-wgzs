package proj;

import java.io.File;
import java.util.StringTokenizer;

import edu.washington.cs.cse490h.lib.Node;
import edu.washington.cs.cse490h.lib.PersistentStorageReader;
import edu.washington.cs.cse490h.lib.PersistentStorageWriter;
import edu.washington.cs.cse490h.lib.Utility;

/**
 * Extension to the Node class that adds support for a reliable, in-order
 * messaging layer.
 * 
 * Nodes that extend this class for the support should use RIOSend and
 * onRIOReceive to send/receive the packets from the RIO layer. The underlying
 * layer can also be used by sending using the regular send() method and
 * overriding the onReceive() method to include a call to super.onReceive()
 */
public abstract class Server extends RIONode {
	
	private ReliableInOrderMsgLayer RIOLayer;
	private static final String dataDirectory = "Data";
	
	public static int NUM_NODES = 10;
	
	public Server() {
		super();
	}
	
	
	@Override
	public void onReceive(Integer from, int protocol, byte[] msg) {
		if(protocol == Protocol.DATA  || protocol == Protocol.CREATE ||
			protocol == Protocol.DELETE || protocol == Protocol.GET ||
			protocol = Protocol.PUT || protocol == Protocol.APPEND) {
			RIOLayer.RIODataReceive(from, msg);
		}else if(protocol == Protocol.ACK) {
			RIOLayer.RIOAckReceive(from, msg);
		}
	}

	/**
	 * Send a message using the reliable, in-order delivery layer
	 * 
	 * @param destAddr
	 *            The address to send to
	 * @param protocol
	 *            The protocol identifier of the message
	 * @param payload
	 *            The payload of the message
	 */
	public void RIOSend(int destAddr, int protocol, byte[] payload) {
		RIOLayer.RIOSend(destAddr, protocol, payload);
	}

	/**
	 * Prints an error message
	 * @param error The error code, as defined in the specifications
	 */
	public void printError(int error){
		
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
		msgString = Utility.byteArrayToString(msg);
		
		if (protocol == Protocol.CREATE) {
			File filePath = new File("./Data/") + msgString; 
			// check if the file exists
			if (filePath.Exists()){
				printError(11);
			}
			// create the file
			else{
				PersistentStorageWriter writer = getWriter(filePath, false);
				writer.close();
			}
		
		}else if (protocol == Protocol.DELETE){
			// check if the file even exists
			File filePath = new File("./Data/") + msgString;
			if (!filePath.Exists())
				printError(10);
			else{
				// delete file
				PersistentStorageWriter writer = getWriter(filePath, false);
				writer.delete();
				writer.close();
			}
			
		}else if (protocol == Protocol.GET){
			// check if the file exists
			File filePath = new File("./Data/") + msgString;
			if (!filePath.Exists())
				printError(10);
			// send the file if it does
			else{
				// load the file into a reader
				String sendMsg = "";
				PersistentStorageReader reader = getReader(filePath);
				while (!(inLine = reader.readLine() == null))
						sendMsg += inLine;
				reader.close();
				
				// send the payload
				byte[] payload = Utility.stringToByteArray(sendMsg);
				RIOLayer.RIOSend(from, Protocol.DATA, payload);
			}
			
		}else if (protocol == Protocol.PUT || protocol == Protocol.APPEND){

			// tokenize the string and parse out the contents and filename
			StringTokenizer tokenizer = new StringTokenizer(msgString);
			fileName = tokenizer.nextToken();
			int length = fileName.length();
			contents = msgString.substring(length+1, msgString.length());
			
			// check if the file exists
			File filePath = new File("./Data/") + fileName;
			if (!filePath.Exists())
				printError(10);
			else{
				// create a new file writer, setting the append option appropriately
				if (protocol == Protocol.APPEND){
					PersistentStorageWriter writer = new PersistentStorageWriter(filePath, true);
				} else {
					PersistentStorageWriter writer = new PersistentStorageWriter(filePath, false);
				}
				byte[] buf = Utility.stringToByteArray(contents);
				writer.write(buf);
				writer.close();
			}
		}
		
		
	}
	
	@Override
	public String toString() {
		return RIOLayer.toString();
	}
}
