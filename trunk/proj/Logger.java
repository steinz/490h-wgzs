import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

/**
 * A class just for logging data and printing errors
 * @author wayger, zstein
 *
 */
public class Logger {

	private static final String LOG_FILE = "server_log.txt";
    private static boolean append = false;
    private static boolean verbose = true; // whether or not to even do anything at all
    
	/**
	 * Prints an error message
	 */
	public static void printError(int addr, int error, String command, int server,
			String filename) {
		StringBuilder sb = new StringBuilder();
		sb.append("Node ");
		sb.append(addr);
		sb.append(": Error: ");
		sb.append(command);
		sb.append(" on server: ");
		sb.append(server);
		sb.append(" and file: ");
		sb.append(filename);
		sb.append(" returned error code: ");
		sb.append(ErrorCode.lookup(error));
		System.err.println(sb.toString());
	}

	/**
	 * Stub for printError for when less information is avaliable
	 * 
	 * @param error
	 * @param command
	 */
	public static void printError(int addr, int error, String command) {
		StringBuilder sb = new StringBuilder();
		sb.append("Node ");
		sb.append(addr);
		sb.append(": Error: ");
		sb.append(command);
		sb.append(" returned error code: ");
		sb.append(ErrorCode.lookup(error));
		System.err.println(sb.toString());
	}
	
	public static void printSeqStateDebug(int addr, HashMap<Integer, InChannel> inConnections, 
			HashMap<Integer, OutChannel> outConnections) {
		write(addr, " sequence state");
		Iterator<Entry<Integer, InChannel>> inIter = inConnections.entrySet()
				.iterator();
		while (inIter.hasNext()) {
			Entry<Integer, InChannel> entry = inIter.next();
			write("In connection to " + entry.getKey()
					+ " lastSeqNumDelivered = ");
			entry.getValue().printSeqNumDebug();
		}
		Iterator<Entry<Integer, OutChannel>> outIter = outConnections
				.entrySet().iterator();
		while (outIter.hasNext()) {
			Entry<Integer, OutChannel> entry = outIter.next();
			write("Out connection to " + entry.getKey()
					+ " lastSeqNumSent = ");
			entry.getValue().printSeqNumDebug();
		}
	}
    
	/**
	 * Write without a node address. Assumes no frame is wanted.
	 * @param message
	 */
	public static void write(String message)
	{
		writeToLog(message, false);
	}
	
    public static void write(int addr, String message)
    {
    	if (!verbose)
    		return;
    	writeToLog("Node " + addr + ": " + message, false);
    }
    
    public static void write(int addr, String message, boolean frame)
    {
    	if (!verbose)
    		return;
    	writeToLog("Node " + addr + ": " + message, frame);
    }
    
    /**
     * Writes the final message to the local file system
     * @param message The final message to write - assumes message is finalized
     */
    public static void writeToLog(String message, boolean frame){
       	
    	BufferedWriter r = null;
    	
    	try {
    		// Create the log file if it doesn't exist already
    		File outFile = new File(LOG_FILE);
    		if (!outFile.exists())
    			outFile.createNewFile();
    		
    	    r = new BufferedWriter(new FileWriter(LOG_FILE, append));
			
    	    // Append a frame if necessary
    	    if (frame) 
				r.write("\n===VERBOSE===");
    	    r.write(message + "\n");
			if (frame) 
				r.write("===VERBOSE===\n");
    	    
    	    r.write(System.getProperty("line.separator"));
    	    r.flush();
    	    r.close();
    	} catch (FileNotFoundException e) {
    	    System.err.println("Log file not found!");
    	} catch (IOException e) {
    	    System.err.println("IOException: " + e.getMessage());
    	    e.printStackTrace();
    	}
    }
}