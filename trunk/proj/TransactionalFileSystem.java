/**
 * CSE 490h
 * 
 * @author wayger, steinz
 */

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;

import edu.washington.cs.cse490h.lib.PersistentStorageReader;
import edu.washington.cs.cse490h.lib.Utility;

/*
 * TODO: EC: In memory file caching - DHT
 */

/*
 * TODO: EC: Implement some kind of snapshotting that could be used by long 
 * running, non-mutating transactions without locking files (for analytics, etc)
 */

/*
 * TODO: EC: Keep the log open between writes
 */

/**
 * Extension of the ReliableFileSystem that adds support for transactions via
 * -TX methods, which write to a redo log and are cached in memory.
 * 
 * The log is purged after initialization
 * 
 * TODO: also purge occasionally after transactions commit or abort (where
 * marked by todos)
 */
public class TransactionalFileSystem extends ReliableFileSystem {

	/**
	 * Operations the FS logs
	 */
	protected static enum Operation {
		CREATE, DELETE, PUT, APPEND, TXSTART, TXCOMMIT, TXABORT
	};

	/**
	 * Encapsulates an operation performed by a client, possibly with a filename
	 * and contents
	 */
	protected static class PendingOperation {
		protected static final String lineSeparator = System
				.getProperty("line.separator");

		protected int client;

		protected Operation op;

		protected String filename;

		/**
		 * Stores contents for put/append
		 */
		protected String contents;

		/**
		 * Construct a tx op
		 */
		public PendingOperation(int client, Operation op) {
			this.client = client;
			this.op = op;
		}

		/**
		 * Construct an op w/o contents
		 */
		public PendingOperation(int client, Operation op, String filename) {
			this(client, op);
			this.filename = filename;
		}

		/**
		 * Construct an op w/ contents
		 */
		public PendingOperation(int client, Operation op, String filename,
				String contents) {
			this(client, op, filename);
			this.contents = contents;
		}

		/**
		 * Turns a pending operation into a string to be written to the log in
		 * the format described in the third writeup.
		 */
		public String toLogString() {
			StringBuilder sb = new StringBuilder();
			sb.append(client);
			sb.append(lineSeparator);
			sb.append(op);
			sb.append(lineSeparator);

			if (op == Operation.TXSTART || op == Operation.TXCOMMIT
					|| op == Operation.TXABORT) {
				return sb.toString();
			}

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
		 * assuming the format described in the third writeup.
		 */
		public static PendingOperation fromLog(BufferedReader reader) {
			try {
				String sClient = reader.readLine();
				String sOp = reader.readLine();
				int client = Integer.parseInt(sClient);
				Operation op = Operation.valueOf(sOp);

				if (op == Operation.TXSTART || op == Operation.TXCOMMIT
						|| op == Operation.TXABORT) {
					return new PendingOperation(client, op);
				}

				String filename = reader.readLine();
				String sLen = reader.readLine();

				int len = Integer.parseInt(sLen);

				if (len > -1) {
					StringBuilder contents = new StringBuilder();
					for (int i = 0; i < len; i++) {
						contents.append(reader.readLine());
						contents.append(lineSeparator);
					}
					return new PendingOperation(client, op, filename, contents
							.toString());
				} else {
					return new PendingOperation(client, op, filename);
				}
			} catch (Exception e) {
				return null;
			}
		}
	}

	/**
	 * A map from client addresses to PendingOperation queues in memory that
	 * knows how to commit operations to disk and recover itself from the log on
	 * disk
	 */
	protected static class TransactionCache {

		protected String logFilename;

		protected TransactionalFileSystem fs;

		/**
		 * Map from node addresses to queued operation lists waiting to be
		 * committed
		 */
		Map<Integer, Queue<PendingOperation>> queuedOperations;

		public TransactionCache(TransactionalFileSystem fs, String logFilename) {
			this.fs = fs;
			this.logFilename = logFilename;
			queuedOperations = new HashMap<Integer, Queue<PendingOperation>>();
		}

		/**
		 * Blindly creates a new queue for client
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
		 * @throws TransactionException
		 */
		public void commitQueue(int client) throws IOException {
			Queue<PendingOperation> clientQueue = queuedOperations.get(client);
			for (PendingOperation op : clientQueue) {
				apply(op);
			}
			clientQueue.clear();
		}

		/**
		 * Blindly clears the queue for client
		 */
		public void abortQueue(int client) {
			queuedOperations.get(client).clear();
		}

		/**
		 * @throws IOException
		 */
		public void recover() throws IOException {
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
						// reapply this batch
						for (PendingOperation reapply : batch) {
							apply(reapply);
						}
						seenStart = false;
					} else if (seenStart && nextOp.op == Operation.TXABORT) {
						batch.clear();
						seenStart = false;
					} else if (seenStart && nextOp.op == Operation.TXSTART) {
						throw new TransactionLogException(
								"multiple TXSTART without separating TXCOMMIT");
					} else if (!seenStart && nextOp.op == Operation.TXSTART) {
						seenStart = true;
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
						"client hasn't started a transaction (transaction queue uninitialized)");
			}
			clientQueue.add(op);
		}

		protected void apply(PendingOperation op) throws IOException {
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
			default:
				throw new TransactionLogException("attemp to apply tx op");
			}

		}
	}

	/**
	 * Name of the log file used by FS transactions
	 */
	protected String logFilename;

	protected String logTempFilename;

	/**
	 * Logged operations yet to be committed
	 */
	protected TransactionCache txCache;

	public TransactionalFileSystem(Client n, String tempFilename,
			String logFilename) throws IOException, TransactionLogException {
		// setup ReliableFileSystem
		super(n, tempFilename);

		// setup log file state
		this.logFilename = logFilename;
		this.txCache = new TransactionCache(this, this.logFilename);

		// recover RFS and from the log if necessary
		this.recover();
		txCache.recover();

		// create or clear the log file on disk
		if (Utility.fileExists(n, logFilename)) {
			performWrite(logFilename, false, "");
		} else {
			createFile(logFilename);
		}
	}

	public String getFileTX(int client, String filename) throws IOException {
		// Look through op queue for ops in this tx that altered this file

		String putContents = null;
		Queue<String> appendsQueue = new LinkedList<String>();
		boolean deleted = false;
		for (PendingOperation op : txCache.queuedOperations.get(client)) {
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

		/*
		 * Shouldn't need to write these to log:
		 * 
		 * put test hello
		 * 
		 * txstart
		 * 
		 * get test
		 * 
		 * append test world
		 * 
		 * txcommit
		 * 
		 * The append should be recovered correctly since CC guarantees the
		 * newest version of test is already in persistent storage
		 */
	}

	/*
	 * The below methods all log what they are doing to disk and in the txCache
	 */

	public void createFileTX(int client, String filename)
			throws TransactionException, IOException {
		/**
		 * TODO: HIGH: Check that client is transacting here, or in Client?
		 */

		PendingOperation op = new PendingOperation(client, Operation.CREATE,
				filename);
		performWrite(logFilename, true, op.toLogString());
		txCache.enque(client, op);
	}

	public void deleteFileTX(int client, String filename) throws IOException,
			TransactionException {
		PendingOperation op = new PendingOperation(client, Operation.DELETE,
				filename);
		performWrite(logFilename, true, op.toLogString());
		txCache.enque(client, op);
	}

	public void writeFileTX(int client, String filename, String contents,
			boolean append) throws IOException, TransactionException {
		/*
		 * TODO: HIGH: throw a FNFException if the file isn't on disk / has been
		 * deleted during this tx
		 */

		PendingOperation op;
		if (append) {
			op = new PendingOperation(client, Operation.APPEND, filename,
					contents + lineSeparator);
		} else {
			op = new PendingOperation(client, Operation.PUT, filename, contents
					+ lineSeparator);
		}
		performWrite(logFilename, true, op.toLogString());
		txCache.enque(client, op);
	}

	public void startTransaction(int client) throws IOException {
		PendingOperation op = new PendingOperation(client, Operation.TXSTART);
		performWrite(logFilename, true, op.toLogString());
		txCache.createQueue(client);
	}

	public void commitTransaction(int client) throws IOException {
		PendingOperation op = new PendingOperation(client, Operation.TXCOMMIT);
		performWrite(logFilename, true, op.toLogString());
		txCache.commitQueue(client);

		// TODO: Cleanup log on disk
	}

	public void abortTransaction(int client) throws IOException {
		PendingOperation op = new PendingOperation(client, Operation.TXABORT);
		performWrite(logFilename, true, op.toLogString());
		txCache.abortQueue(client);

		// TODO: Cleanup log on disk
	}
}
