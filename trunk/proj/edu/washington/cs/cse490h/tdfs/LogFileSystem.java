package edu.washington.cs.cse490h.tdfs;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.logging.Logger;

public class LogFileSystem implements LogFS {

	private static class Transactional<T> {
		private T persistent;
		private T transactional;
		private boolean transacting;

		public void abort() {
			transacting = false;
		}

		public void commit() {
			persistent = transactional;
			transacting = false;
		}

		public T getPersistent() {
			return persistent;
		}

		public T getTransactional() {
			return transacting ? transactional : persistent;
		}

		public void set(T value) {
			if (transacting) {
				transactional = value;
			} else {
				persistent = value;
			}
		}

		public void start() {
			transactional = persistent;
			transacting = true;
		}
	}

	private static class FileLog {
		// TODO: cache

		private SortedMap<Integer, LogEntry> operations;

		private int nextOperationNumber;

		/**
		 * New file is implicitly: unlocked, deleted
		 */
		public FileLog() {
			this.operations = new TreeMap<Integer, LogEntry>();
			this.nextOperationNumber = 0;
		}

		public FileLog(SortedMap<Integer, LogEntry> operations,
				int nextOperationNumber) {
			this.operations = operations;
			this.nextOperationNumber = nextOperationNumber;
		}

		public void addOperation(int logLineNumber, LogEntry op) {
			operations.put(logLineNumber, op);
			if (logLineNumber + 1 > nextOperationNumber) {
				nextOperationNumber = logLineNumber + 1;
			}
		}

		public boolean checkExists() {
			Transactional<Boolean> exists = new Transactional<Boolean>();
			for (Entry<Integer, LogEntry> entry : operations.entrySet()) {
				LogEntry op = entry.getValue();
				if (op instanceof TXStartLogEntry) {
					exists.start();
				} else if (op instanceof TXAbortLogEntry) {
					exists.abort();
				} else if (op instanceof TXCommitLogEntry) {
					exists.commit();
				} else if (op instanceof DeleteLogEntry) {
					exists.set(false);
				} else if (op instanceof CreateLogEntry) {
					exists.set(true);
				}
			}
			return exists.getPersistent();
		}

		public Integer checkLocked() {
			Integer locked = null;
			for (Entry<Integer, LogEntry> entry : operations.entrySet()) {
				LogEntry op = entry.getValue();
				if (op instanceof LockLogEntry) {
					LockLogEntry l = (LockLogEntry) op;
					locked = l.address;
				} else if (op instanceof UnlockLogEntry) {
					locked = null;
				}
			}
			return locked;
		}

		public String getContent() {
			Transactional<String> content = new Transactional<String>();
			for (Entry<Integer, LogEntry> entry : operations.entrySet()) {
				LogEntry op = entry.getValue();
				if (op instanceof TXStartLogEntry) {
					content.start();
				} else if (op instanceof TXAbortLogEntry) {
					content.abort();
				} else if (op instanceof TXCommitLogEntry) {
					content.commit();
				} else if (op instanceof DeleteLogEntry) {
					content.set(null);
				} else if (op instanceof CreateLogEntry) {
					content.set("");
				} else if (op instanceof WriteLogEntry) {
					WriteLogEntry w = (WriteLogEntry) op;
					if (w.append) {
						content.set(content.getTransactional() + w.content);
					} else {
						content.set(w.content);
					}
				}
			}
			return content.getPersistent();
		}

		public byte[] pack() {
			ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
			DataOutputStream dataStream = new DataOutputStream(byteStream);

			try {
				dataStream.writeInt(nextOperationNumber);

				for (Entry<Integer, LogEntry> entry : operations.entrySet()) {
					int opNumber = entry.getKey();
					LogEntry op = entry.getValue();
					byte[] packedOp = op.pack();

					dataStream.writeInt(opNumber);
					dataStream.writeInt(packedOp.length);
					dataStream.write(packedOp);
				}
			} catch (IOException e) {
				throw new RuntimeException(e);
			}

			return byteStream.toByteArray();
		}

		public static FileLog unpack(byte[] packedLog) {
			DataInputStream stream = new DataInputStream(
					new ByteArrayInputStream(packedLog));

			SortedMap<Integer, LogEntry> operations = new TreeMap<Integer, LogEntry>();
			int nextOpNumber;

			try {
				nextOpNumber = stream.readInt();

				while (stream.available() > 0) {
					int opNumber = stream.readInt();
					int opLength = stream.readInt();
					byte[] packedOp = new byte[opLength];
					stream.read(packedOp, 0, opLength);
					operations.put(opNumber, LogEntry.unpack(packedOp));
				}
			} catch (IOException e) {
				throw new RuntimeException(e);
			}

			return new FileLog(operations, nextOpNumber);
		}
	}

	private Map<String, FileLog> logs;

	private Logger logger;

	public LogFileSystem() {
		this.logs = new HashMap<String, FileLog>();
		this.logger = Logger
				.getLogger("edu.washington.cs.cse490h.dfs.LogFileSystem");
		// TODO: HIGH: Logger config file, etc
	}

	/**
	 * null if unlocked, otherwise address of owner
	 */
	public Integer checkLocked(String filename) throws NotListeningException {
		FileLog l = getLog(filename);
		return l.checkLocked();
	}

	public void createGroup(String filename)
			throws AlreadyParticipatingException {
		participate(filename, new FileLog());
	}

	public boolean fileExists(String filename) throws NotListeningException {
		FileLog l = getLog(filename);
		return l.checkExists();
	}

	public String getFile(String filename) throws NotListeningException {
		logAccess(filename, "Get");
		FileLog l = getLog(filename);
		// TODO: HIGH: I think it's okay to let someone Get a locked file...
		return l.getContent();
	}

	private FileLog getLog(String filename) throws NotListeningException {
		FileLog l = logs.get(filename);
		if (l == null) {
			throw new NotListeningException();
		}
		return l;
	}

	public int nextLogNumber(String filename) throws NotListeningException {
		FileLog l = getLog(filename);
		return l.nextOperationNumber;
	}

	/**
	 * null if opearationNumber has been forgotten
	 * 
	 * exception if operationNumber isn't known to have ever existed
	 */
	public LogEntry getLogEntry(String filename, int operationNumber)
			throws NotListeningException, NoSuchOperationNumberException {
		FileLog l = getLog(filename);
		if (operationNumber >= l.nextOperationNumber) {
			throw new NoSuchOperationNumberException();
		}
		LogEntry op = l.operations.get(operationNumber);
		return op;
	}

	public boolean isListening(String filename) {
		return logs.containsKey(filename);
	}

	public void listen(String filename, byte[] packedLog)
			throws AlreadyParticipatingException {
		participate(filename, FileLog.unpack(packedLog));
	}

	private void logAccess(String filename, String operation) {
		logAccess(filename, operation, null);
	}

	private void logAccess(String filename, String operation, String content) {
		String msg = operation.toString().toLowerCase() + " file: " + filename
				+ (content == null ? "" : " content: " + content);
		logger.finer(msg);
	}

	public byte[] packLog(String filename) throws NotListeningException {
		FileLog l = getLog(filename);
		return l.pack();
	}

	private void participate(String filename, FileLog log)
			throws AlreadyParticipatingException {
		if (logs.containsKey(filename)) {
			throw new AlreadyParticipatingException();
		}
		logs.put(filename, log);
	}

	public void writeLogEntry(String filename, int logEntryNumber, LogEntry op)
			throws NotListeningException {
		logAccess(filename, "Add");
		FileLog l = getLog(filename);
		l.addOperation(logEntryNumber, op);
	}
}
