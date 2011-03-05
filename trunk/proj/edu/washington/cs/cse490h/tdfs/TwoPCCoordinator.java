package edu.washington.cs.cse490h.tdfs;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.washington.cs.cse490h.lib.Callback;

public class TwoPCCoordinator extends RIONode {

	private final int TXTIMEOUT = 20;
	private LogFS logFs;
	private Map<String, String[]> fileTransactionMap;
	private Map<String, Integer> lastProposalNumbersSent;

	@Override
	public void start() {
		this.logFs = new LogFileSystem();
		fileTransactionMap = new HashMap<String, String[]>();
	}

	@Override
	public void onRIOReceive(Integer from, MessageType protocol, byte[] msg) {
		if (!protocol.equals(MessageType.Learned)) {
			Logger.error(this, "TwoPCCoordinator received non-learn message: "
					+ protocol.toString());
		}
		learn(from, msg);
	}

	/**
	 * The 2PC coordinator learns about a operation. It logs it to its own logFS, but also
	 * tries to discern whether the given command is something it should pay attention to - txstarts,
	 * commits, and aborts.
	 * @param from The coordinator who sent this message
	 * @param msg The proposal, packed
	 */
	public void learn(int from, byte[] msg) {
		Proposal p = new Proposal(msg);
		logFs.writeLogEntry(p.filename, p.operationNumber, p.operation);
		String[] txFiles = fileTransactionMap.get(p.filename);

		// check for transactions

		if (p.operation instanceof TXStartLogEntry) {
			// start callback
			addTxTimeout(from);
			// add files
			String[] files = ((TXStartLogEntry) p.operation).filenames;
			if (txFiles != null) { // client didn't abort or commit last tx
				Logger.error(this, "Client: " + from
						+ " did not commit or abort last tx!");
			}
			for (String file : files) {
				fileTransactionMap.put(file, files);
			}

		}

		else if (p.operation instanceof TXTryCommitLogEntry) {
			// commit to each filename coordinator
			String[] files = ((TXTryCommitLogEntry) p.operation).filenames;
			createProposal(new TXTryCommitLogEntry(files), files);
			for (String file : txFiles){
				fileTransactionMap.put(file, null);
			}
		}

		else if (p.operation instanceof TXTryAbortLogEntry) {
			String[] files = ((TXTryAbortLogEntry) p.operation).filenames;
			createProposal(new TXTryAbortLogEntry(files), files);
			for (String file : txFiles){
				fileTransactionMap.put(file, null);
			}
		}
	}
	
	/**
	 * Creates a proposal using the appropriate log entry type, and send to each coordinator
	 * for the list files
	 * @param operation The operation to perform
	 * @param files The list of files involved in this tx
	 */
	public void createProposal(LogEntry operation, String[] files){
		for (String file : files) {
			List<Integer> coordinators = TDFSNode.getParticipants(file);
			Proposal newProposal = new Proposal(
					new TXCommitLogEntry(files), file,
					logFs.nextLogNumber(file), nextProposalNumber(file));

			for (Integer addr : coordinators){
				RIOSend(addr, MessageType.Prepare, newProposal.pack());
			}
		}
	}

	/**
	 * Same function as TDFSNode.nextProposalNumber. The only reason this is copy/pasted
	 * is that the lastProposalNumbersSent data structure is highly specific to each node.
	 * @param filename The filename 
	 * @return The next proposal number
	 * @throws NotListeningException
	 */
	public int nextProposalNumber(String filename) throws NotListeningException {
		List<Integer> participants = TDFSNode.getParticipants(filename);
		Integer lastNumSent = lastProposalNumbersSent.get(filename);
		if (lastNumSent == null) {
			// use offset
			Collections.sort(participants); // TODO: document that this is ok
			int number = participants.indexOf(addr);
			lastProposalNumbersSent.put(filename, number);
			return number;
		} else {
			// increment last sent by participation count
			int number = lastNumSent + participants.size();
			lastProposalNumbersSent.put(filename, number);
			return number;
		}

	}

	/**
	 * Aborts all tx for a client, assuming that each file is used in a tx at most once.
	 * That is, this method will cause all sorts of problems if multiple clients are allowed to
	 * start a tx on the same file.
	 * @param filename The filename key to abort
	 */
	public void abortClientTx(String filename) {
		String[] files = fileTransactionMap.get(filename);
	}

	/**
	 * Adds a transaction timeout for the given client. If the client hasn't
	 * committed their transaction by the time the lease expires, then
	 * 
	 * @param client
	 */
	public void addTxTimeout(String filename) {
		Method cbMethod = null;
		try {
			String[] params = { "java.lang.String" };
			cbMethod = Callback.getMethod("abortClientTx", this, params);
			cbMethod.setAccessible(true); // HACK
		} catch (Exception e) {
			printError(e);
			e.printStackTrace();
		}
		String[] args = { filename };
		Callback cb = new Callback(cbMethod, this, args);
		addTimeout(cb, this.TXTIMEOUT);
	}

	@Override
	public void onCommand(String command) {
		Logger.error(this, "2PC node does not accept commands");

	}

}
