/**
 * CSE 490h
 * 
 * @author wayger, steinz
 */

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.StringTokenizer;
import java.util.Map.Entry;

import edu.washington.cs.cse490h.lib.PersistentStorageReader;
import edu.washington.cs.cse490h.lib.Utility;

/*
 * TODO: HIGH: Implement TransactionalFileSystem
 * 
 * All FS operations write to log and queue op in memory.
 * 
 * Queued ops are written to disk on commit, then log is cleared.
 * 
 * Queue is thrown out on abort.
 */

/**
 * Extension of the ReliableFileSystem that adds support for transactions.
 */
public class TransactionalFileSystem extends ReliableFileSystem {

	/**
	 * Operations the FS can perform
	 */
	protected static enum Operation {
		CREATE, DELETE, PUT, APPEND, TXSTART, TXCOMMIT
	};

	/**
	 * Encapsulates an operation on a filename and arguments
	 * 
	 */
	protected static class PendingOperation {
		protected static final String lineSeparator = System
				.getProperty("line.separator");

		protected int client;

		protected Operation op;

		protected String filename;

		/**
		 * Used to store contents for put/append
		 */
		protected String contents;

		/**
		 * Construct an op w/o contents
		 */
		public PendingOperation(int client, Operation op, String filename) {
			this.client = client;
			this.op = op;
			this.filename = filename;
		}

		/**
		 * Construct an op w/ contents
		 */
		public PendingOperation(int client, Operation op, String filename,
				String contents) {
			this.client = client;
			this.op = op;
			this.filename = filename;
			this.contents = contents;
		}

		/*
		 * TODO: This should be done by the language or a library (serializable,
		 * JSON, ...)
		 */

		public String toLogString() {
			// TODO: HIGH: Document this format
			StringBuilder sb = new StringBuilder();
			sb.append(client);
			sb.append(lineSeparator);
			sb.append(op);
			sb.append(lineSeparator);
			sb.append(filename);
			sb.append(lineSeparator);
			if (contents != null) {
				sb.append(contents.split(lineSeparator).length); // line count
				sb.append(contents);
			} else {
				sb.append(-1);
			}
			sb.append(lineSeparator);
			return sb.toString();
		}

		/**
		 * Returns the next PendingOperation object read from reader or null
		 */
		public static PendingOperation fromLog(BufferedReader reader) {
			try {
				String sClient = reader.readLine();
				String sOp = reader.readLine();
				String filename = reader.readLine();
				String sLen = reader.readLine();

				int client = Integer.parseInt(sClient);
				Operation op = Operation.valueOf(sOp);
				int len = Integer.parseInt(sLen);

				if (len > -1) {
					StringBuilder contents = new StringBuilder();
					for (int i = 0; i < len; i++) {
						contents.append(reader.readLine());
						contents.append(lineSeparator);
					}
					return new PendingOperation(client, op, filename,
							contents.toString());
				} else {
					return new PendingOperation(client, op, filename);
				}
			} catch (IOException e) {
				return null;
			}
		}
	}

	/**
	 * Keeps a list of operations in memory and on disk in the log for each
	 * client
	 */
	protected static class TransactionLog {

		protected String logFilename;

		protected TransactionalFileSystem fs;

		/**
		 * Map from node addresses to queued operation lists waiting to be
		 * committed
		 */
		Map<Integer, Queue<PendingOperation>> queuedOperations;

		public TransactionLog(TransactionalFileSystem fs, String logFilename) {
			this.fs = fs;
			this.logFilename = logFilename;
			queuedOperations = new HashMap<Integer, Queue<PendingOperation>>();
		}

		/**
		 * Blindly creates a new queue for client
		 * 
		 * @param client
		 */
		public void createQueue(int client) {
			queuedOperations.put(client, new LinkedList<PendingOperation>());
		}

		/**
		 * Commits each operation in the queue for client and then clears the
		 * queue
		 * 
		 * @param client
		 * @throws IOException
		 */
		public void commitQueue(int client) throws IOException {
			Queue<PendingOperation> clientQueue = queuedOperations.get(client);
			for (PendingOperation op : clientQueue) {
				switch (op.op) {
				case CREATE:
					fs.createFile(op.filename);
					break;
				case DELETE:
					fs.deleteFile(op.filename);
					break;
				case PUT:
					fs.writeFile(op.filename, op.contents, false);
					break;
				case APPEND:
					fs.writeFile(op.filename, op.contents, true);
					break;
				}
			}
			clientQueue.clear();
		}

		/**
		 * Blindly clears the queue for client
		 * 
		 * @param client
		 */
		public void abortQueue(int client) {
			queuedOperations.get(client).clear();
		}

		/**
		 * @throws FileNotFoundException
		 * @throws TransactionLogException
		 * @throws TransactionException
		 */

		public void recover() throws FileNotFoundException,
				TransactionLogException {
			/*
			 * TODO: Decide if the client address should actually be part of the
			 * PendingOperation objects or just written and read externally.
			 */

			if (!Utility.fileExists(fs.n, logFilename)) {
				// no log, so nothing to do
				return;
			}

			Map<Integer, List<PendingOperation>> oldTxs = new HashMap<Integer, List<PendingOperation>>();

			// read log from disk, parsing lines into PendingOperation objects
			PersistentStorageReader reader = this.fs.n.getReader(logFilename);
			PendingOperation op = PendingOperation.fromLog(reader);
			while (op != null) {
				List<PendingOperation> clientQueue = oldTxs.get(op.client);
				if (clientQueue == null) {
					clientQueue = new ArrayList<PendingOperation>();
					oldTxs.put(op.client, clientQueue);
				}
				clientQueue.add(op);
				op = PendingOperation.fromLog(reader);
			}

			// reapply txs
			for (Entry<Integer, List<PendingOperation>> entry : oldTxs
					.entrySet()) {
				List<PendingOperation> ops = entry.getValue();

				List<PendingOperation> batch = new ArrayList<PendingOperation>();
				boolean seenStart = false;
				for (PendingOperation nextOp : ops) {
					if (seenStart && nextOp.op == Operation.TXCOMMIT) {
						// reapply batch
						for (PendingOperation reapply : batch) {
							
						}
					} else if (seenStart && nextOp.op == Operation.TXSTART) {
						throw new TransactionLogException(
								"multiple TXSTART without separating TXCOMMIT");
					} else if (seenStart) {
						batch.add(nextOp);
					}
				}
			}
		}

		/**
		 * Add a new operation to the given client's queue
		 * 
		 * @throws TransactionException
		 */
		public void enque(int client, PendingOperation op)
				throws TransactionException {
			Queue<PendingOperation> clientQueue = queuedOperations.get(client);
			if (clientQueue == null) {
				throw new TransactionException(
						"client transaction queue uninitialized");
			}
			clientQueue.add(op);
		}
	}

	/**
	 * TODO: LOW: Keep the log open between writes
	 */

	/**
	 * Name of the log file used by FS transactions
	 */
	protected String logFilename;

	/**
	 * Logged operations yet to be committed
	 */
	protected TransactionLog txLog;

	public TransactionalFileSystem(Client n, String tempFilename,
			String logFilename) throws IOException, TransactionLogException {
		// setup ReliableFileSystem
		super(n, tempFilename);

		// setup the log file
		this.logFilename = logFilename;

		// setup the log object
		this.txLog = new TransactionLog(this, this.logFilename);

		// recover from the log if necessary
		this.recover();

		// create or clear the log file on disk
		if (Utility.fileExists(n, logFilename)) {
			performWrite(logFilename, false, "");
		} else {
			createFile(logFilename);
		}
	}

	/**
	 * Recovers as ReliableFileSystem does but also recovers from the
	 * transaction log and then clears the log on disk.
	 * @throws IOException 
	 * @throws FileNotFoundException 
	 * @throws TransactionLogException 
	 */
	@Override
	protected void recover() throws FileNotFoundException, IOException, TransactionLogException{
		super.recover();
		txLog.recover();

	}

	public void createFileTX(int client, String filename)
			throws TransactionException, IOException {
		/**
		 * TODO: HIGH: Check that client is transacting here, or in Client?
		 */

		PendingOperation op = new PendingOperation(client, Operation.CREATE,
				filename);
		performWrite(logFilename, true, op.toLogString() + lineSeparator);
		txLog.enque(client, op);
	}

	public void deleteFileTX(int client, String filename) throws IOException,
			TransactionException {
		PendingOperation op = new PendingOperation(client, Operation.DELETE,
				filename);
		performWrite(logFilename, true, op.toLogString() + lineSeparator);
		txLog.enque(client, op);
	}

	public String getFileTX(int client, String filename) throws IOException {
		// Look through log for ops in this tx that alterd filename

		String putContents = null;
		Queue<String> appendsQueue = new LinkedList<String>();
		boolean deleted = false;
		for (PendingOperation op : txLog.queuedOperations.get(client)) {
			if (op.filename.equals(filename)) {
				switch (op.op) {
				case DELETE:
					deleted = true;
					break;
				case CREATE:
					deleted = false;
					break;
				case PUT:
					appendsQueue.clear();
					putContents = op.contents;
					break;
				case APPEND:
					appendsQueue.add(op.contents);
					break;
				}
			}
		}

		if (!deleted && putContents == null && appendsQueue.size() == 0) {
			// file hasn't been touched during this tx
			return getFile(filename);
		} else if (deleted) {
			// file deleted during this tx
			throw new FileNotFoundException("file deleted during this tx");
		} else {
			// file has been put or appeneded to
			StringBuilder result = new StringBuilder();
			result.append(putContents);
			for (String appendContents : appendsQueue) {
				result.append(appendContents);
			}
			return result.toString();
		}
	}

	public void writeFileTX(int client, String filename, String contents,
			boolean append) throws IOException, TransactionException {
		PendingOperation op;
		if (append) {
			op = new PendingOperation(client, Operation.APPEND, filename,
					contents + lineSeparator);
		} else {
			op = new PendingOperation(client, Operation.PUT, filename, contents
					+ lineSeparator);
		}
		performWrite(logFilename, true, op.toLogString());
		txLog.enque(client, op);
	}

	public void startTransaction(int client) throws IOException,
			TransactionException {
		performWrite(logFilename, true, "TX_START " + client + lineSeparator);
		txLog.createQueue(client);
	}

	public void commitTransaction(int client) throws IOException,
			TransactionException {
		performWrite(logFilename, true, "TX_COMMIT " + client + lineSeparator);
		txLog.commitQueue(client);
	}

	public void abortTransaction(int client) throws TransactionException {
		txLog.abortQueue(client);
	}
}
