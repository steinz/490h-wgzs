package edu.washington.cs.cse490h.tdfs;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import edu.washington.cs.cse490h.dfs.DFSNode;
import edu.washington.cs.cse490h.lib.Node;

public class LogFileSystem {

	// TODO: Contended op

	private class Operation {
	}

	private class FileOperation extends Operation {
		String filename;

		public FileOperation(String filename) {
			this.filename = filename;
		}
	}

	private class MemberOperation extends Operation {
		int address;

		public MemberOperation(int address) {
			this.address = address;
		}
	}

	private class Create extends FileOperation {
		public Create(String filename) {
			super(filename);
		}
	}

	private class Delete extends FileOperation {
		public Delete(String filename) {
			super(filename);
		}
	}

	/**
	 * no need to actually log these
	 */
	private class Get extends FileOperation {
		public Get(String filename) {
			super(filename);
		}
	}

	private class Join extends MemberOperation {
		public Join(int address) {
			super(address);
		}
	}

	private class Leave extends MemberOperation {
		public Leave(int address) {
			super(address);
		}
	}

	private class Lock extends MemberOperation {
		public Lock(int address) {
			super(address);
		}
	}

	private class TXAbort extends Operation {
	}

	private class TXCommit extends Operation {
	}

	private class TXStart extends Operation {
	}

	private class Write extends FileOperation {
		String content;
		boolean append;

		public Write(String filename, String content, boolean append) {
			super(filename);
			this.content = content;
			this.append = append;
		}
	}

	private class FileLog {
		// TODO: cache stuff

		/**
		 * newest first
		 */
		private LinkedList<Operation> operations;

		public FileLog(String log) {

		}

		public void addOperation(Operation op) {
			operations.addFirst(op);
		}

		public boolean checkExists() {
			for (Operation op : operations) {
				if (op instanceof Delete) {
					return false;
				} else if (op instanceof FileOperation) {
					return true;
				}
			}
			throw new RuntimeException("no FileOperation found in log");
		}

		public String getContent() throws FileDoesNotExistException {
			LinkedList<String> content = new LinkedList<String>();
			for (Operation op : operations) {
				if (op instanceof Delete) {
					throw new FileDoesNotExistException();
				} else if (op instanceof Create) {
					break;
				} else if (op instanceof Write) {
					Write w = (Write) op;
					content.addFirst(w.content);
					if (!w.append) {
						break;
					}
				}
			}
			StringBuilder str = new StringBuilder();
			for (String s : content) {
				str.append(s);
			}
			return str.toString();
		}
	}

	private Node parent;

	private Map<String, FileLog> logs;

	private Logger logger;

	public LogFileSystem(DFSNode n) throws IOException {
		this.parent = n;
		this.logs = new HashMap<String, FileLog>();
		this.logger = Logger
				.getLogger("edu.washington.cs.cse490h.dfs.LogFileSystem");
	}

	public void createFile(String filename) throws FileAlreadyExistsException {
		logAccess(filename, Create.class);
	}

	public void deleteFile(String filename) throws FileDoesNotExistException {
		logAccess(filename, Delete.class);
	}

	public boolean fileExists(String filename) {
		try {
			FileLog l = getLog(filename);
			return l.checkExists();
		} catch (FileDoesNotExistException e) {
			return false;
		}
	}

	public String getFile(String filename) throws FileDoesNotExistException {
		logAccess(filename, Get.class);
		FileLog l = getLog(filename);
		return l.getContent();
	}

	private FileLog getLog(String filename) throws FileDoesNotExistException {
		FileLog l = logs.get(filename);
		if (l == null) {
			throw new FileDoesNotExistException();
		}
		return l;
	}

	private FileLog getOrCreateLog(String filename) {
		FileLog l = logs.get(filename);
		if (l == null) {
			return new FileLog();
		} else {
			return l;
		}
	}

	public void logAccess(String filename, Class operation) {
		logAccess(filename, operation, null);
	}

	public void logAccess(String filename, Class operation, String content) {
		String msg = operation.getName().toLowerCase() + " file: " + filename
				+ (content == null ? "" : " content: " + content);
		logger.finer(msg);
	}

	public void participate(String filename, String log) {

	}

	public void writeFile(String filename, String content, boolean append) {
		logAccess(filename, Write.class, content);

	}
}
