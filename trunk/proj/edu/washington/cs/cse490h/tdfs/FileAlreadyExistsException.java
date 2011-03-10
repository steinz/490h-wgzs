package edu.washington.cs.cse490h.tdfs;

public class FileAlreadyExistsException extends Exception {
	private static final long serialVersionUID = 6499862966247860636L;

	public FileAlreadyExistsException(String filename) {
		super(filename);
	}
}
