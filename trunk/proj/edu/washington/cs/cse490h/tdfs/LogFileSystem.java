package edu.washington.cs.cse490h.tdfs;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.logging.Logger;

public class LogFileSystem implements LogFS {

	// TODO: HIGH: Use Transactional<T>
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
	
	private static class TXBoolean {
		private boolean exists = false;
		private Boolean txExists = null;
		private boolean inTx = false;

		public void txAbort() {
			txExists = null;
			inTx = false;
		}

		public void txCommit() {
			if (txExists != null) {
				exists = txExists;
			}
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

	private static class TXString {
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

	private static class FileLog {
		// TODO: cache

		private SortedMap<Integer, Operation> operations;

		private int nextOperationNumber;

		/**
		 * New file is implicitly: unlocked, deleted
		 */
		public FileLog() {
			this.operations = new TreeMap<Integer, Operation>();
			this.nextOperationNumber = 0;
		}

		public FileLog(SortedMap<Integer, Operation> operations,
				int nextOperationNumber) {
			this.operations = operations;
			this.nextOperationNumber = nextOperationNumber;
		}

		public void addOperation(Operation op) {
			operations.put(nextOperationNumber, op);
			nextOperationNumber++;
		}

		public boolean checkExists() {
			TXBoolean exists = new TXBoolean();
			for (Entry<Integer, Operation> entry : operations.entrySet()) {
				Operation op = entry.getValue();
				if (op instanceof TXStart) {
					exists.txStart();
				} else if (op instanceof TXAbort) {
					exists.txAbort();
				} else if (op instanceof TXCommit) {
					exists.txCommit();
				} else if (op instanceof Delete) {
					exists.setExists(false);
				} else if (op instanceof Create) {
					exists.setExists(true);
				}
			}
			return exists.getExists();
		}

		public Integer checkLocked() {
			Integer locked = null;
			for (Entry<Integer, Operation> entry : operations.entrySet()) {
				Operation op = entry.getValue();
				if (op instanceof Lock) {
					Lock l = (Lock) op;
					locked = l.address;
				} else if (op instanceof Unlock) {
					locked = null;
				}
			}
			return locked;
		}

		public String getContent() throws FileDoesNotExistException {
			TXString content = new TXString();
			for (Entry<Integer, Operation> entry : operations.entrySet()) {
				Operation op = entry.getValue();
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
			for (Entry<Integer, Operation> entry : operations.entrySet()) {
				Operation op = entry.getValue();
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

		public byte[] pack() {
			ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
			DataOutputStream dataStream = new DataOutputStream(byteStream);

			try {
				dataStream.writeInt(nextOperationNumber);

				for (Entry<Integer, Operation> entry : operations.entrySet()) {
					int opNumber = entry.getKey();
					Operation op = entry.getValue();
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

			SortedMap<Integer, Operation> operations = new TreeMap<Integer, Operation>();
			int nextOpNumber;

			try {
				nextOpNumber = stream.readInt();

				while (stream.available() > 0) {
					int opNumber = stream.readInt();
					int opLength = stream.readInt();
					byte[] packedOp = new byte[opLength];
					stream.read(packedOp, 0, opLength);
					operations.put(opNumber, Operation.unpack(packedOp));
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
	public Integer checkLocked(String filename)
			throws NotParticipatingException {
		FileLog l = getLog(filename);
		return l.checkLocked();
	}

	public void createFile(String filename, int address)
			throws FileAlreadyExistsException, NotParticipatingException,
			FileLockedByAnotherAddressException {
		logAccess(filename, "Create");
		FileLog l = getLog(filename);
		Integer locked = l.checkLocked();
		if (locked != null && locked != address) {
			throw new FileLockedByAnotherAddressException();
		}
		if (l.checkExists()) {
			throw new FileAlreadyExistsException();
		} else {
			l.addOperation(new Create());
		}
	}

	public void createGroup(String filename)
			throws AlreadyParticipatingException {
		participate(filename, new FileLog());
	}

	public void deleteFile(String filename, int address)
			throws FileDoesNotExistException, NotParticipatingException,
			FileLockedByAnotherAddressException {
		logAccess(filename, "Delete");
		FileLog l = getLog(filename);
		Integer locked = l.checkLocked();
		if (locked != null && locked != address) {
			throw new FileLockedByAnotherAddressException();
		}
		if (!l.checkExists()) {
			throw new FileDoesNotExistException();
		} else {
			l.addOperation(new Delete());
		}
	}

	public boolean fileExists(String filename) throws NotParticipatingException {
		FileLog l = getLog(filename);
		return l.checkExists();
	}

	public String getFile(String filename) throws FileDoesNotExistException,
			NotParticipatingException {
		logAccess(filename, "Get");
		FileLog l = getLog(filename);
		// TODO: HIGH: I think it's okay to let someone Get a locked file...
		return l.getContent();
	}

	private FileLog getLog(String filename) throws NotParticipatingException {
		FileLog l = logs.get(filename);
		if (l == null) {
			throw new NotParticipatingException();
		}
		return l;
	}

	public int getNextOperationNumber(String filename)
			throws NotParticipatingException {
		FileLog l = getLog(filename);
		return l.nextOperationNumber;
	}

	public Operation getOperation(String filename, int operationNumber)
			throws NotParticipatingException, NoSuchOperationNumberException {
		FileLog l = getLog(filename);
		if (operationNumber >= l.nextOperationNumber) {
			throw new NoSuchOperationNumberException();
		}
		Operation op = l.operations.get(operationNumber);
		if (op == null) {
			op = new Forgotten();
		}
		return op;
	}

	public List<Integer> getParticipants(String filename)
			throws NotParticipatingException {
		FileLog l = getLog(filename);
		return l.getParticipants();
	}

	public void join(String filename, int address)
			throws NotParticipatingException {
		memberOperation(filename, "Join", new Join(address));
	}

	public void joinGroup(String filename, byte[] packedLog)
			throws AlreadyParticipatingException {
		participate(filename, FileLog.unpack(packedLog));
	}

	public void leave(String filename, int address)
			throws NotParticipatingException {
		memberOperation(filename, "Leave", new Leave(address));
	}

	public void lockFile(String filename, int address)
			throws NotParticipatingException, AlreadyLockedException {
		logAccess(filename, "Lock");
		FileLog l = getLog(filename);
		if (l.checkLocked() != null) {
			throw new AlreadyLockedException();
		}
		l.addOperation(new Lock(address));
	}

	private void logAccess(String filename, String operation) {
		logAccess(filename, operation, null);
	}

	private void logAccess(String filename, String operation, String content) {
		String msg = operation.toString().toLowerCase() + " file: " + filename
				+ (content == null ? "" : " content: " + content);
		logger.finer(msg); 
	}

	private void memberOperation(String filename, String opType,
			MemberOperation op) throws NotParticipatingException {
		logAccess(filename, opType);
		FileLog l = getLog(filename);
		l.addOperation(op);
	}

	public byte[] packLog(String filename) throws NotParticipatingException {
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

	public void unlockFile(String filename, int address)
			throws NotParticipatingException, NotLockedException,
			FileLockedByAnotherAddressException {
		logAccess(filename, "Unlock");
		FileLog l = getLog(filename);
		Integer lockedBy = l.checkLocked();
		if (lockedBy == null) {
			throw new NotLockedException();
		} else if (lockedBy != address) {
			throw new FileLockedByAnotherAddressException();
		}
		l.addOperation(new Unlock(address));
	}

	public void writeFile(String filename, String content, boolean append,
			int address) throws FileDoesNotExistException,
			NotParticipatingException, FileLockedByAnotherAddressException {
		logAccess(filename, "Write", content);
		FileLog l = getLog(filename);
		Integer locked = l.checkLocked();
		if (locked != null && locked != address) {
			throw new FileLockedByAnotherAddressException();
		}
		if (l.checkExists()) {
			l.addOperation(new Write(content, append));
		} else {
			throw new FileDoesNotExistException();
		}
	}

	@Override
	public boolean isParticipating(String filename) {
		return logs.containsKey(filename);
	}
}
