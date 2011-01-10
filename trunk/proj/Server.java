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
public class Server extends RIONode {
	
	private static final String dataDirectory = "Data";
	
	public static int NUM_NODES = 10;
	
	public Server() {
		super();
	}
	
	
	@Override
	public void onReceive(Integer from, int protocol, byte[] msg) {
		if((protocol == Protocol.DATA)  || (protocol == Protocol.CREATE) || (protocol == Protocol.DELETE) 
				|| (protocol == Protocol.GET) || (protocol == Protocol.PUT) || (protocol == Protocol.APPEND)) {
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
	
	@Override
	public void onCommand(String command) {
		// method stub
		//TODO: Implement
	
	}
	
	@Override
	public void start(){
	
	}
	
	public void createFile(){
		
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
		//TODO: Break out into different methods
		if (protocol == Protocol.CREATE) {
			File filePath = new File("./Data/" + msgString);
			// check if the file exists
			if (filePath.exists()){
				printError(11);
			}
			// create the file
			else{
				PersistentStorageWriter writer = getWriter("./Data/" + msgString, false);
				writer.close();
			}
		
		}else if (protocol == Protocol.DELETE){
			// check if the file even exists
			File filePath = new File("./Data/" + msgString);
			if (!filePath.exists())
				printError(10);
			else{
				// delete file
				PersistentStorageWriter writer = getWriter("./Data/" + msgString, false);
				writer.delete();
				writer.close();
			}
			
		}else if (protocol == Protocol.GET){
			// check if the file exists
			File filePath = new File("./Data/" + msgString);
			if (!filePath.exists())
				printError(10);
			// send the file if it does
			else{
				// load the file into a reader
				String sendMsg = "";
				String inLine = "";
				PersistentStorageReader reader = getReader("./Data/" + msgString);
				while (!((inLine = reader.readLine()) == null))
						sendMsg += inLine;
				reader.close();
				
				// send the payload
				byte[] payload = Utility.stringToByteArray(sendMsg);
				RIOLayer.RIOSend(from, Protocol.DATA, payload);
			}
			
		}else if (protocol == Protocol.PUT || protocol == Protocol.APPEND){

			// tokenize the string and parse out the contents and filename
			StringTokenizer tokenizer = new StringTokenizer(msgString);
			String fileName = tokenizer.nextToken();
			int length = fileName.length();
			String contents = msgString.substring(length+1, msgString.length());
			
			// check if the file exists
			File filePath = new File("./Data/" + fileName);
			if (!filePath.exists())
				printError(10);
			else{
				PersistentStorageWriter writer = null;
				// create a new file writer, setting the append option appropriately
				if (protocol == Protocol.APPEND){
					writer = getWriter("./Data/" + msgString, true);
				} else {
					writer = getWriter("./Data/" + msgString, false);
				}
				writer.write(contents);
				writer.close();
			}
		}
		
		
	}
	
}
