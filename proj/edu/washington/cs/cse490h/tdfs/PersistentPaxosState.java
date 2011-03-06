package edu.washington.cs.cse490h.tdfs;

import java.util.List;
import java.util.Map;

/**
 * CSE 490h
 * 
 * @author wayger, steinz
 */

/**
 * Logs operations to persistent storage for recovery
 */
public class PersistentPaxosState {
	private static final String lineSeparator = System
			.getProperty("line.separator");
	private static final String logFilename = "promises.log";

	/**
	 * The node this FS is associated with - used to access the underlying file
	 * system and for logging
	 */
	protected TDFSNode n;

	/**
	 * Last proposal number promised for a given file
	 */
	private Map<String, Integer> lastProposalNumberPromised;

	public PersistentPaxosState(TDFSNode n) {
		this.n = n;
		this.rebuild();
	}

	protected void promise(String filename, int proposalNumber) {
		PersistentStorageWriter writer = n.getWriter(filename, true);

		try {
			writer.write(contents);
		} finally {
			writer.close();
		}
	}

	protected void rebuild() {
		if (!Utility.fileExists(n, tempFilename)) {
			// Do nothing if we don't have a temp file
			return;
		}

		
	}
}
