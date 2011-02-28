package edu.washington.cs.cse490h.tdfs;

import java.util.List;

public interface LogFS {

	public Integer checkLocked(String filename)
			throws NotParticipatingException;

	public void createFile(String filename, int address)
			throws FileAlreadyExistsException, NotParticipatingException,
			FileLockedByAnotherAddressException;

	public void createGroup(String filename)
			throws AlreadyParticipatingException;

	public void deleteFile(String filename, int address)
			throws FileDoesNotExistException, NotParticipatingException,
			FileLockedByAnotherAddressException;

	public boolean fileExists(String filename) throws NotParticipatingException;

	public String getFile(String filename) throws FileDoesNotExistException,
			NotParticipatingException;

	public int getNextOperationNumber(String filename)
			throws NotParticipatingException;

	public Operation getOperation(String filename, int operationNumber)
			throws NotParticipatingException, NoSuchOperationNumberException;

	public List<Integer> getParticipants(String filename)
			throws NotParticipatingException;

	public boolean isParticipating(String filename);

	public void join(String filename, int address)
			throws NotParticipatingException;

	public void joinGroup(String filename, byte[] packedLog)
			throws AlreadyParticipatingException;

	public void leave(String filename, int address)
			throws NotParticipatingException;

	public void lockFile(String filename, int address)
			throws NotParticipatingException, AlreadyLockedException;

	public byte[] packLog(String filename) throws NotParticipatingException;

	public void unlockFile(String filename, int address)
			throws NotParticipatingException, NotLockedException,
			FileLockedByAnotherAddressException;

	public void writeFile(String filename, String content, boolean append,
			int address) throws FileDoesNotExistException,
			NotParticipatingException, FileLockedByAnotherAddressException;
}
