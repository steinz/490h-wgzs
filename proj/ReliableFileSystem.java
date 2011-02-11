/**
 * CSE 490h
 * 
 * @author wayger, steinz
 */

import java.io.FileNotFoundException;
import java.io.IOException;
import edu.washington.cs.cse490h.lib.PersistentStorageReader;
import edu.washington.cs.cse490h.lib.PersistentStorageWriter;
import edu.washington.cs.cse490h.lib.Utility;

public class ReliableFileSystem {

	/*
	 * TODO: Some of the FS logging is out of order
	 * 
	 * ex) Put: PUT -> GET -> DEL-TEMP -> GET
	 */

	protected static final String lineSeparator = System
			.getProperty("line.separator");

	/**
	 * Name of the temp file used by write when append is false
	 */
	protected String tempFilename;

	/**
	 * The client object this FS is associated with - used by the Persistent
	 * Reader/Writer and for logging
	 */
	protected Client n;

	public ReliableFileSystem(Client n, String tempFilename) throws IOException {
		this.n = n;
		this.tempFilename = tempFilename;

		this.recover();
	}

	/**
	 * Creates a file on the local filesystem
	 * 
	 * @param filename
	 *            the file to create
	 * @throws IOException
	 */
	public void createFile(String filename) throws IOException {
		logAccess(filename, "creating");

		if (Utility.fileExists(n, filename)) {
			throw new FileAlreadyExistsException();
		} else {
			// This implicitly creates the file on disk
			PersistentStorageWriter writer = n.getWriter(filename, false);
			writer.close();
		}
	}

	/**
	 * Deletes a file from the local file system
	 * 
	 * @param filename
	 *            the file to delete
	 * @throws IOException
	 */
	public void deleteFile(String filename) throws IOException {
		logAccess(filename, "deleting");

		if (!Utility.fileExists(n, filename)) {
			throw new FileNotFoundException();
		} else {
			PersistentStorageWriter writer = n.getWriter(filename, false);
			if (!writer.delete()) {
				throw new IOException("delete failed");
			}
			writer.close();
		}
	}

	/**
	 * Local get file
	 * 
	 * @param filename
	 * @throws IOException
	 */
	public String getFile(String filename) throws IOException {

		logAccess(filename, "getting");

		if (!Utility.fileExists(n, filename)) {
			throw new FileNotFoundException();
		} else {
			StringBuilder contents = new StringBuilder();
			PersistentStorageReader reader = n.getReader(filename);

			char[] buffer = new char[1024];
			int charsRead;
			while ((charsRead = reader.read(buffer)) != -1) {
				contents.append(buffer, 0, charsRead);
			}

			reader.close();
			return contents.toString();
		}
	}

	/**
	 * Writes a file to the local filesystem. Fails if the file does not exist
	 * already
	 * 
	 * @param filename
	 *            the file name to write to
	 * @param contents
	 *            the contents to write
	 * @throws IOException
	 */
	public void writeFile(String filename, String contents, boolean append)
			throws IOException {

		if (!Utility.fileExists(n, filename)) {
			throw new FileNotFoundException();
		} else {
			if (!append) {
				// save current contents in temp file
				writeTempFile(filename);
			}

			performWrite(filename, append, contents);

			if (!append) {
				deleteFile(tempFilename);
			}
		}
	}

	/**
	 * Logs access of type operation on filename via n.printVerbose and
	 * n.logSynopticEvent. Convenience version for operations w/o content.
	 * 
	 * @param filename
	 * @param operation
	 *            ex) "getting"
	 */
	protected void logAccess(String filename, String operation) {
		logAccess(filename, operation, null);
	}

	/**
	 * Logs access of type operation on filename via n.printVerbose and
	 * n.logSynopticEvent.
	 * 
	 * @param filename
	 * @param operation
	 *            ex) "getting"
	 */
	protected void logAccess(String filename, String operation, String content) {
		StringBuilder msg = new StringBuilder();
		msg.append(operation.toLowerCase());
		msg.append(" file: ");
		msg.append(filename);
		if (content != null) {
			msg.append(" content: ");
			msg.append(content);
		}
		n.printVerbose(msg.toString());
		// TODO: HIGH: Should filename ever be null? right now sometimes it is
		if (filename.equals(tempFilename)) {
			n.logSynopticEvent(operation.toUpperCase() + "-TEMP-FILE");
		} else {
			n.logSynopticEvent(operation.toUpperCase() + "-FILE");
		}
	}

	/**
	 * Used to temporarily save a file that could be lost in a crash since
	 * getWriter deletes a file it doesn't open for appending.
	 * 
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	protected void writeTempFile(String filename) throws IOException {
		String oldContent = getFile(filename);

		performWrite(tempFilename, false, oldContent.toString());
	}

	/**
	 * Actually performs a write of contents to filename
	 * 
	 * @throws IOException
	 */
	protected void performWrite(String filename, boolean append, String contents)
			throws IOException {
		if (append) {
			logAccess(filename, "appending", contents);
		} else {
			logAccess(filename, "putting", contents);
		}

		PersistentStorageWriter writer = n.getWriter(filename, append);
		writer.write(contents);
		writer.close();
	}

	/**
	 * Cleans up after a crash
	 * 
	 * Writes any temp file on disk to the proper file
	 * 
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	protected void recover() throws IOException {
		if (!Utility.fileExists(n, tempFilename)) {
			// Do nothing if we don't have a temp file
			return;
		}

		String tempFile = getFile(tempFilename);
		int newline = tempFile.indexOf(lineSeparator);
		String filename = tempFile.substring(0, newline);
		String content = tempFile.substring(newline + lineSeparator.length());

		performWrite(filename, false, content);
		deleteFile(tempFilename);
	}
}
