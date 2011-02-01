import java.io.FileNotFoundException;
import java.io.IOException;

import edu.washington.cs.cse490h.lib.PersistentStorageReader;
import edu.washington.cs.cse490h.lib.PersistentStorageWriter;
import edu.washington.cs.cse490h.lib.Utility;

public class ReliableFileSystem {

	/**
	 * Name of the temp file used by write when append is false
	 */
	protected static final String tempFilename = ".temp";

	/**
	 * Name of the log file used by FS operations
	 */
	protected static final String logFilename = ".log";

	protected Client n;

	public ReliableFileSystem(Client n) {
		this.n = n;
	}

	/*
	 * TODO: LOW: Log a different Synoptic event when temp files are written /
	 * deleted
	 */

	/**
	 * Creates a file on the local filesystem
	 * 
	 * @param filename
	 *            the file to create
	 * @throws IOException
	 */
	public void createFile(String filename) throws IOException {

		n.printVerbose("creating file: " + filename);
		n.logSynopticEvent("CREATING-FILE");

		if (Utility.fileExists(n, filename)) {
			throw new FileAlreadyExistsException();
		} else {
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

		n.printVerbose("deleting file: " + filename);
		n.logSynopticEvent("DELETING-FILE");

		if (!Utility.fileExists(n, filename)) {
			throw new FileNotFoundException();
		} else {
			PersistentStorageWriter writer = n.getWriter(filename, false);
			if (!writer.delete())
				throw new IOException("delete failed");
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

		n.printVerbose("getting file: " + filename);
		n.logSynopticEvent("GETTING-FILE");

		// check if the file exists
		if (!Utility.fileExists(n, filename)) {
			throw new FileNotFoundException();
		} else {
			// read and return the file if it does
			StringBuilder contents = new StringBuilder();
			PersistentStorageReader reader = n.getReader(filename);

			/*
			 * TODO: This is the same suck as in writeTempFile. See commet
			 * there, refactor writeTempFile to use getFile, and fix the suck
			 * (probably need to use read instead of readLine...).
			 */
			String inLine;
			while ((inLine = reader.readLine()) != null) {
				contents.append(inLine);
				contents.append(System.getProperty("line.separator"));
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

		if (append) {
			n.printVerbose("appending to file: " + filename + ", contents: "
					+ contents);
			n.logSynopticEvent("APPENDING-FILE");
		} else {
			n.printVerbose("putting to file: " + filename + ", contents: "
					+ contents);
			n.logSynopticEvent("PUTTING-FILE");
		}

		if (!Utility.fileExists(n, filename)) {
			throw new FileNotFoundException();
		} else {
			if (!append) {
				// save current contents in temp file
				writeTempFile(filename);
			}

			PersistentStorageWriter writer = n.getWriter(filename, append);
			writer.write(contents);
			writer.close();

			if (!append) {
				deleteFile(tempFilename);
			}
		}
	}

	/**
	 * Used to temporarily save a file that could be lost in a crash since
	 * getWriter deletes a file it doesn't open for appending.
	 * 
	 * @param filename
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	protected void writeTempFile(String filename) throws IOException {
		StringBuilder oldContent = new StringBuilder();
		oldContent.append(filename);
		oldContent.append(System.getProperty("line.separator"));

		PersistentStorageReader oldFileReader = n.getReader(filename);

		/*
		 * TODO: This sucks. I'm going to assume all files end w/ newlines for
		 * now. The readline(), while loop method was writing "null" to files.
		 */
		String inLine;
		while ((inLine = oldFileReader.readLine()) != null) {
			oldContent.append(inLine);
			oldContent.append(System.getProperty("line.separator"));
		}

		oldFileReader.close();

		PersistentStorageWriter temp = n.getWriter(tempFilename, false);
		temp.write(oldContent.toString());
		temp.close();
	}

	/**
	 * Replaces the file on disk with the temp file to recover from a crash
	 * 
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	protected void recoverTempFile() throws FileNotFoundException, IOException {
		if (!Utility.fileExists(n, tempFilename)) {
			// Do nothing if we don't have a temp file
			return;
		}

		String tempFile = getFile(tempFilename);
		int newline = tempFile.indexOf(System.getProperty("line.separator"));
		String filename = tempFile.substring(0, newline);
		String content = tempFile.substring(newline + 1);

		writeFile(filename, content, false);
		deleteFile(tempFilename);
	}

}
