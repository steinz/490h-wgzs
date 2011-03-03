package edu.washington.cs.cse490h.app;

import edu.washington.cs.cse490h.tdfs.FileAlreadyExistsException;
import edu.washington.cs.cse490h.tdfs.FileDoesNotExistException;

public interface TDFS {

	public void append(String filename, String contents) throws FileDoesNotExistException;
	
	public void create(String filename) throws FileAlreadyExistsException;
	
	public void delete(String filename) throws FileDoesNotExistException;
	
	public boolean exists(String filename);
	
	public String get(String filename) throws FileDoesNotExistException;

	public void put(String filename, String contents)  throws FileDoesNotExistException;
	
	public void txAbort() throws NotInTransactionException;
	
	public void txCommit() throws NotInTransactionException;
	
	public void txStart(String[] filenames) throws AlreadyInTransactionException;
}
