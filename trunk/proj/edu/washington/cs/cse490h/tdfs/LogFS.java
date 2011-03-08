package edu.washington.cs.cse490h.tdfs;

public interface LogFS {

	/**
	 * null if unlocked, otherwise address of owner
	 */
	public Integer checkLocked(String filename) throws NotListeningException;

	public void createGroup(String filename)
			throws AlreadyParticipatingException;

	public boolean fileExists(String filename) throws NotListeningException;

	/**
	 * Returns null if the file doesn't exist
	 */
	public String getFile(String filename) throws NotListeningException;

	public LogEntry getLogEntry(String filename, int operationNumber)
			throws NotListeningException, NoSuchOperationNumberException;

	public boolean hasLogNumber(String filename, int operationNumber)
			throws NotListeningException;

	public boolean isListening(String filename);

	/**
	 * Returns the filename in the packedLog and unpacks the log into memory
	 * (overwriting any existing log)
	 */
	public String listen(byte[] packedLog);

	public int nextLogNumber(String filename) throws NotListeningException;

	public byte[] packLog(String filename) throws NotListeningException;

	public void writeLogEntry(String filename, int logEntryNumber, LogEntry op)
			throws NotListeningException;

}
