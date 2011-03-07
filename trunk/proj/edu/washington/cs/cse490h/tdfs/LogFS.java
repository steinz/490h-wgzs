package edu.washington.cs.cse490h.tdfs;

public interface LogFS {

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

	public void listen(String filename, byte[] packedLog)
			throws AlreadyParticipatingException;

	public int nextLogNumber(String filename) throws NotListeningException;

	public byte[] packLog(String filename) throws NotListeningException;

	public void writeLogEntry(String filename, int logEntryNumber, LogEntry op)
			throws NotListeningException;

}
