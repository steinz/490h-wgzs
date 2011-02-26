package edu.washington.cs.cse490h.tdfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class LogFileSystem {

	private class TXBoolean {
		private Boolean exists = null;
		private Boolean txExists = null;
		private boolean inTx = false;

		public void txAbort() {
			txExists = null;
			inTx = false;
		}

		public void txCommit() {
			exists = txExists;
			inTx = false;
		}

		public void txStart() {
			txExists = exists;
			inTx = true;
		}

		public Boolean getExists() {
			return exists;
		}

		public void setExists(Boolean b) {
			if (inTx) {
				txExists = b;
			} else {
				exists = b;
			}
		}
	}

	private class TXString {
		private String content = null;
		private String txContent = null;
		private boolean inTx = false;

		public void txAbort() {
			txContent = null;
			inTx = false;
		}

		public void txCommit() {
			content = txContent;
			inTx = false;
		}

		public void txStart() {
			txContent = content;
			inTx = true;
		}

		public String getContent() {
			return content;
		}

		public void setContent(String s) {
			if (inTx) {
				txContent = s;
			} else {
				content = s;
			}
		}

		public void appendContent(String s) {
			if (inTx) {
				txContent += s;
			} else {
				content += s;
			}
		}
	}

	private class FileLog {
		// TODO: cache stuff

		private List<Operation> operations;

		public FileLog(List<Operation> operations) {
			this.operations = operations;
		}

		public void addOperation(Operation op) {
			operations.add(op);
		}

		public boolean checkExists() {
			TXBoolean exists = new TXBoolean();
			for (Operation op : operations) {
				if (op instanceof TXStart) {
					exists.txStart();
				} else if (op instanceof TXAbort) {
					exists.txAbort();
				} else if (op instanceof TXCommit) {
					exists.txCommit();
				} else if (op instanceof Delete) {
					exists.setExists(false);
				} else if (op instanceof FileOperation) {
					exists.setExists(true);
				}
			}
			Boolean result = exists.getExists();
			if (result == null) {
				throw new RuntimeException(
						"no committed FileOperation found in log");
			} else {
				return result;
			}
		}

		public String getContent() throws FileDoesNotExistException {
			TXString content = new TXString();
			for (Operation op : operations) {
				if (op instanceof TXStart) {
					content.txStart();
				} else if (op instanceof TXAbort) {
					content.txAbort();
				} else if (op instanceof TXCommit) {
					content.txCommit();
				} else if (op instanceof Delete) {
					content.setContent(null);
				} else if (op instanceof Create) {
					content.setContent("");
				} else if (op instanceof Write) {
					Write w = (Write) op;
					if (w.append) {
						content.appendContent(w.content);
					} else {
						content.setContent(w.content);
					}
				}
			}
			String result = content.getContent();
			if (result == null) {
				throw new FileDoesNotExistException();
			} else {
				return result;
			}
		}

		public List<Integer> getParticipants() {
			List<Integer> participants = new ArrayList<Integer>();
			for (Operation op : operations) {
				if (op instanceof Join) {
					Join j = (Join) op;
					participants.add(j.address);
				} else if (op instanceof Leave) {
					Leave l = (Leave) op;
					participants.remove(new Integer(l.address));
				}
			}
			return participants;
		}
	}

	private Map<String, FileLog> logs;

	private Logger logger;

	public LogFileSystem() throws IOException {
		this.logs = new HashMap<String, FileLog>();
		this.logger = Logger
				.getLogger("edu.washington.cs.cse490h.dfs.LogFileSystem");
	}

	public void createFile(String filename) throws FileAlreadyExistsException {
		logAccess(filename, "Create");
		FileLog l = getOrCreateLog(filename);
		if (l.checkExists()) {
			throw new FileAlreadyExistsException();
		} else {
			l.addOperation(new Create());
		}
	}

	public void deleteFile(String filename) throws FileDoesNotExistException {
		logAccess(filename, "Delete");
		FileLog l = getLog(filename);
		if (!l.checkExists()) {
			throw new FileDoesNotExistException();
		} else {
			l.addOperation(new Delete());
		}
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
		logAccess(filename, "Get");
		FileLog l = getLog(filename);
		return l.getContent();
	}

	private FileLog getLog(String filename) throws FileDoesNotExistException {
		FileLog l = logs.get(filename);
		if (l == null) {
			/*
			 * TODO: This might be a bit confusing - doesn't exist locally, but
			 * might globally
			 */
			throw new FileDoesNotExistException();
		}
		return l;
	}

	public List<Operation> getLogOpList(String filename)
			throws FileDoesNotExistException {
		FileLog l = getLog(filename);
		return l.operations;
	}

	private FileLog getOrCreateLog(String filename) {
		FileLog l = logs.get(filename);
		if (l == null) {
			return new FileLog(new ArrayList<Operation>());
		} else {
			return l;
		}
	}

	public List<Integer> getParticipants(String filename) {
		FileLog l = logs.get(filename);
		return l.getParticipants();
	}

	public void join(String filename, int address)
			throws FileDoesNotExistException {
		memberOperation(filename, "Join", new Join(address));
	}

	public void leave(String filename, int address)
			throws FileDoesNotExistException {
		memberOperation(filename, "Leave", new Leave(address));
	}

	public void lockFile(String filename, int address)
			throws FileDoesNotExistException {
		memberOperation(filename, "Lock", new Lock(address));
	}

	private void memberOperation(String filename, String opType,
			MemberOperation op) throws FileDoesNotExistException {
		logAccess(filename, opType);
		FileLog l = getLog(filename);
		l.addOperation(op);
	}

	public void logAccess(String filename, String operation) {
		logAccess(filename, operation, null);
	}

	public void logAccess(String filename, String operation, String content) {
		String msg = operation.toString().toLowerCase() + " file: " + filename
				+ (content == null ? "" : " content: " + content);
		logger.finer(msg);
	}

	public void participate(String filename, List<Operation> log)
			throws AlreadyParticipatingException {
		if (logs.containsKey(filename)) {
			throw new AlreadyParticipatingException();
		}
		FileLog l = new FileLog(log);
		logs.put(filename, l);
	}

	public void writeFile(String filename, String content, boolean append)
			throws FileDoesNotExistException {
		logAccess(filename, "Write", content);
		FileLog l = getLog(filename);
		if (l.checkExists()) {
			l.addOperation(new Write(content, append));
		} else {
			throw new FileDoesNotExistException();
		}
	}
}
