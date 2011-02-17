package edu.washington.cs.cse490h.dfs;

import java.io.IOException;

public abstract class DFSThread extends Thread {

	// TODO: config options should be in DFSNode

	/**
	 * Name of the temp file used by write when append is false
	 */
	protected static final String tempFilename = ".temp";

	/**
	 * Name of the log file used by FS transactions
	 */
	protected static final String logFilename = ".log";

	/**
	 * Name of the temp file used when purging the log
	 */
	protected static final String logTempFilename = ".log.temp";

	/**
	 * Purge the log every fsPurgeFrequency commits/aborts
	 * 
	 * TODO: raise for prod
	 */
	protected static final int fsPurgeFrequency = 5;

	/**
	 * FS for this thread
	 */
	protected TransactionalFileSystem fs;

	/**
	 * Parent node for this thread
	 */
	protected DFSNode node;

	/**
	 * Sets up the FS
	 * 
	 * @param parent
	 * @throws IOException
	 *             Thrown if FS recovery fails
	 */
	public DFSThread(DFSNode parent) throws IOException {
		this.node = parent;
		setupFS();
	}

	/**
	 * Recovers and initializes the FS
	 * 
	 * @throws IOException
	 *             Thrown when recovery fails
	 */
	public void setupFS() throws IOException {
		fs = new TransactionalFileSystem(node, tempFilename, logFilename,
				logTempFilename, fsPurgeFrequency);
	}
}
