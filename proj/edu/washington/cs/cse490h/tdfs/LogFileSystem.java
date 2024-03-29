package edu.washington.cs.cse490h.tdfs;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.logging.Logger;

import edu.washington.cs.cse490h.lib.Utility;

public class LogFileSystem implements LogFS {

	private class FileLog {
		// TODO: cache

		public final String filename;

		private SortedMap<Integer, LogEntry> operations;

		private int nextOperationNumber;

		/**
		 * New file is implicitly: unlocked/not in a tx, deleted
		 */
		public FileLog(String filename) {
			this.filename = filename;
			this.operations = new TreeMap<Integer, LogEntry>();
			this.nextOperationNumber = 0;
		}

		public FileLog(String filename,
				SortedMap<Integer, LogEntry> operations, int nextOperationNumber) {
			this.filename = filename;
			this.operations = operations;
			this.nextOperationNumber = nextOperationNumber;
		}

		public void addOperation(int logLineNumber, LogEntry op) {
			operations.put(logLineNumber, op);
			if (logLineNumber + 1 > nextOperationNumber) {
				nextOperationNumber = logLineNumber + 1;
			}
		}

		public boolean checkFileExists() {
			Transactional<Boolean> exists = new Transactional<Boolean>(false);
			for (Entry<Integer, LogEntry> entry : operations.entrySet()) {
				LogEntry op = entry.getValue();
				if (op instanceof TXStartLogEntry) {
					TXStartLogEntry start = (TXStartLogEntry) op;
					if (start.address != node.addr) {
						exists.start();
					}
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
				if (op instanceof TXStartLogEntry) {
					TXStartLogEntry l = (TXStartLogEntry) op;
					locked = l.address;
				} else if (op instanceof TXCommitLogEntry
						|| op instanceof TXAbortLogEntry) {
					locked = null;
				}
			}
			return locked;
		}

		public String getContent() {
			Transactional<String> content = new Transactional<String>(null);
			for (Entry<Integer, LogEntry> entry : operations.entrySet()) {
				LogEntry op = entry.getValue();
				if (op instanceof TXStartLogEntry) {
					TXStartLogEntry start = (TXStartLogEntry) op;
					if (start.address != node.addr) {
						content.start();
					}
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
				byte[] filenameBytes = Utility.stringToByteArray(filename);
				dataStream.writeInt(filenameBytes.length);
				dataStream.write(filenameBytes);

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
	}

	public FileLog unpack(byte[] packedLog) {
		DataInputStream stream = new DataInputStream(new ByteArrayInputStream(
				packedLog));

		String filename;
		SortedMap<Integer, LogEntry> operations = new TreeMap<Integer, LogEntry>();
		int nextOpNumber = 0;

		try {
			int len = stream.readInt();
			byte[] filenameBytes = new byte[len];
			if (stream.read(filenameBytes) != len) {
				throw new EOFException("premature EOF detected in filename");
			}
			filename = Utility.byteArrayToString(filenameBytes);
			nextOpNumber = stream.readInt();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		try {
			while (true) {
				int opNumber = stream.readInt();
				int opLength = stream.readInt();
				byte[] packedOp = new byte[opLength];
				if (stream.read(packedOp) != opLength) {
					throw new EOFException("premature EOF detected in LogEntry");
				}
				operations.put(opNumber, LogEntry.unpack(packedOp));
			}
		} catch (EOFException e) {
			// done
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		return new FileLog(filename, operations, nextOpNumber);
	}

	private Map<String, FileLog> logs;

	private Logger logger;

	private TDFSNode node;

	public LogFileSystem(TDFSNode node) {
		this.logs = new HashMap<String, FileLog>();
		this.logger = Logger
				.getLogger("edu.washington.cs.cse490h.dfs.LogFileSystem");
		this.node = node;
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
		if (logs.containsKey(filename)) {
			throw new AlreadyParticipatingException();
		}
		logs.put(filename, new FileLog(filename));
	}

	public boolean fileExists(String filename) throws NotListeningException {
		FileLog l = getLog(filename);
		return l.checkFileExists();
	}

	/**
	 * Returns the most recent committed version of the file known
	 */
	public String getFile(String filename) throws NotListeningException {
		logAccess(filename, "get");
		FileLog l = getLog(filename);
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
	 * null if opearationNumber is missing (GC'd, never learned)
	 * 
	 * NoSuchOperationNumberException if operationNumber is >=
	 * nextOperationNumber (never learned)
	 */
	public LogEntry getLogEntry(String filename, int operationNumber)
			throws NotListeningException, NoSuchOperationNumberException {
		FileLog l = getLog(filename);
		if (operationNumber >= l.nextOperationNumber) {
			throw new NoSuchOperationNumberException("requested: "
					+ operationNumber + ", next: " + l.nextOperationNumber);
		}
		LogEntry op = l.operations.get(operationNumber);
		return op;
	}

	public boolean hasLogNumber(String filename, int operationNumber)
			throws NotListeningException {
		FileLog l = getLog(filename);
		return l.operations.containsKey(operationNumber);
	}

	public boolean isListening(String filename) {
		return logs.containsKey(filename);
	}

	public String listen(byte[] packedLog) {
		FileLog l = unpack(packedLog);
		logs.put(l.filename, l);
		return l.filename;
	}

	private void logAccess(String filename, String operation) {
		String msg = operation.toLowerCase() + filename;
		logger.finer(msg);
	}

	public byte[] packLog(String filename) throws NotListeningException {
		FileLog l = getLog(filename);
		return l.pack();
	}

	public void writeLogEntry(String filename, int logEntryNumber, LogEntry op)
			throws NotListeningException {
		logAccess(filename, op.toString());
		FileLog l = getLog(filename);
		l.addOperation(logEntryNumber, op);
	}
}
