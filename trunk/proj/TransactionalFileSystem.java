/**
 * CSE 490h
 * 
 * @author wayger, steinz
 */

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.StringTokenizer;
import java.util.Map.Entry;

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
		 * Constcut ap op w/o contents
		 */
		public PendingOperation(int client, Operation op, String filename) {
			this.client = client;
			this.op = op;
			this.filename = filename;
		}

		/**
		 * Constuct an op w/ contents
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
				sb.append(contents.length());
				sb.append(lineSeparator);
				sb.append(contents);
			} else {
				sb.append(-1);
			}
			return sb.toString();
		}

		public static PendingOperation fromLog(String log, int offset) {
			String examine = log.substring(offset);
			StringTokenizer t = new StringTokenizer(examine, lineSeparator);
			String sClient = t.nextToken();
			String sOp = t.nextToken();
			String filename = t.nextToken();
			String sLen = t.nextToken();

			int client = Integer.parseInt(sClient);
			Operation op = Operation.valueOf(sOp);
			int len = Integer.parseInt(sLen);

			int next = offset + sClient.length() + sOp.length()
					+ filename.length() + sLen.length() + 4
					* lineSeparator.length();
			String contents;
			if (len > -1) {
				contents = log.substring(next, next + len);
				return new PendingOperation(client, op, filename, contents);
				// next entry is at log[next + delim.length() +
				// contents.length() ..
			} else {
				return new PendingOperation(client, op, filename);
				// next entry is at log[next ..
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
		 * 
		 */

		public void recover() {
			/*
			 * TODO: Decide if the client address should actually be part of the
			 * PendingOperation objects or just written and read externally.
			 */

			if (!Utility.fileExists(fs.n, logFilename)) {
				// no log, so nothing to do
				return;
			}
			
			// read log from disk, parsing lines into PendingOperation objects
			String log = "";

			Map<Integer, List<PendingOperation>> oldTxs = new HashMap<Integer, List<PendingOperation>>();
			int offset = 0;

			// loop
			do {
				PendingOperation op = PendingOperation.fromLog(log, offset);
				List<PendingOperation> clientQueue = oldTxs.get(op.client);
				if (clientQueue == null) {
					clientQueue = new ArrayList<PendingOperation>();
					oldTxs.put(op.client, clientQueue);
				}
				clientQueue.add(op);
			} while (false);
			// end loop

			for (Entry<Integer, List<PendingOperation>> entry : oldTxs
					.entrySet()) {
				List<PendingOperation> ops = entry.getValue();

				// TOOD: HIGH: Factor this loop out or something, it hurts my
				// face
				for (int i = 0; i < ops.size(); i++) {
					PendingOperation op = ops.get(ops.size() - 1 - i);
					if (op.op == Operation.TXCOMMIT) {
						int start = i + 1;

						// find TXSTART

						// redo between START and COMMIT
					}
				}
			}

			// update offset

			// reapply committed operations in log

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
			String logFilename) throws IOException {
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
	 */
	@Override
	public void recover() throws FileNotFoundException, IOException {
		super.recover();
		txLog.recover();
	}

	public void createFileTentative(int client, String filename)
			throws TransactionException, IOException {
		/**
		 * TODO: HIGH: Check that client is transacting here, or in Client?
		 */

		PendingOperation op = new PendingOperation(client, Operation.CREATE,
				filename);
		performWrite(logFilename, true, op.toLogString()
				+ PendingOperation.lineSeparator);
		txLog.enque(client, op);
	}

	public void deleteFileTentative(int client, String filename)
			throws IOException, TransactionException {
		PendingOperation op = new PendingOperation(client, Operation.DELETE,
				filename);
		performWrite(logFilename, true, op.toLogString());
		txLog.enque(client, op);
	}

	/*
	 * TODO: HIGH: Do we need a tentative get file? Depends on the semantics of
	 * transactions I think... see the disccusion at the top of Client about
	 * what happens for a get called during a transaction
	 * 
	 * public String getFileTentative(int client, String filename) { }
	 */

	// TODO: HIGH: Implement other Tentative TransactionalFS ops

	public void writeFileTentative(int client, String filename,
			String contents, boolean append) throws IOException,
			TransactionException {
		PendingOperation op;
		if (append) {
			op = new PendingOperation(client, Operation.APPEND, filename,
					contents);
		} else {
			op = new PendingOperation(client, Operation.PUT, filename, contents);
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
